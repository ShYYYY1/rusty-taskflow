use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::Parser,
    parse_macro_input,
    parse_quote,
    spanned::Spanned,
    FnArg,
    ImplItem,
    ImplItemFn,
    ItemImpl,
    LitStr,
    Pat,
    ReturnType,
    Type,
};

/// Turn an inherent `impl Block { fn run(...) -> ... }` into a
/// `SyncTask`/`AsyncTask` trait implementation for the taskflow scheduler.
///
/// ## Accepted `run` signatures
///
/// ```ignore
/// // 1. No inputs, no context (source task).
/// fn run(self) -> Out;
///
/// // 2. Only DAG inputs.
/// fn run(self, a: &A, b: &B) -> Out;
///
/// // 3. With the runtime FlowContext. MUST be the first non-self parameter,
/// //    typed as `&FlowContext` (match by trailing path segment, so any
/// //    import alias that still names the type `FlowContext` works).
/// fn run(self, ctx: &FlowContext, a: &A) -> Out;
/// ```
///
/// DAG inputs must be shared references `&T` (the scheduler stores outputs as
/// `Arc<T>` and hands out a borrow). Owned and `&mut` parameters are rejected.
///
/// ## Context injection details
///
/// The generated trait impl always takes `ctx: &FlowContext`. If the user did
/// not declare one, the generated body discards it with `let _ = ctx;`. If the
/// user did declare one, it is forwarded as the first argument to the inherent
/// `run` call. Nothing else in the user's signature changes.
///
/// ## The `path = "..."` attribute
///
/// When the macro is used outside the taskflow crate itself, pass
/// `path = "::taskflow"` (or the relevant re-export root) so the generated
/// code can refer to the runtime traits. Inside the taskflow crate the
/// default `crate` path is used.
#[proc_macro_attribute]
pub fn sync_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_task(attr, item, false)
}

/// Async counterpart of [`macro@sync_task`]. The `run` method must be
/// `async fn` and all the rules about parameters (shared references, optional
/// leading `ctx: &FlowContext`) are identical.
#[proc_macro_attribute]
pub fn async_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_task(attr, item, true)
}

fn expand_task(attr: TokenStream, item: TokenStream, expect_async: bool) -> TokenStream {
    let input_impl = parse_macro_input!(item as ItemImpl);
    let root_path = match parse_root_path(attr) {
        Ok(path) => path,
        Err(err) => return err.to_compile_error().into(),
    };

    match build_task_impl(&input_impl, expect_async, &root_path) {
        Ok(expanded) => TokenStream::from(quote! {
            #input_impl
            #expanded
        }),
        Err(err) => err.to_compile_error().into(),
    }
}

fn parse_root_path(attr: TokenStream) -> core::result::Result <syn::Path, syn::Error> {
    if attr.is_empty() {
        return Ok(parse_quote!(crate));
    }

    let mut parsed_path = None::<syn::Path>;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("path") {
            let lit: LitStr = meta.value()?.parse()?;
            parsed_path = Some(lit.parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported argument; expected `path = \"::taskflow\"`"))
        }
    });

    parser.parse2(proc_macro2::TokenStream::from(attr))?;

    parsed_path.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "missing `path` argument; expected `path = \"::taskflow\"`",
        )
    })
}

fn build_task_impl(
    input_impl: &ItemImpl,
    expect_async: bool,
    root_path: &syn::Path,
) -> core::result::Result <proc_macro2::TokenStream, syn::Error> {
    let self_ty = &input_impl.self_ty;
    let run_fn = find_run_fn(input_impl)?;

    if run_fn.sig.asyncness.is_some() != expect_async {
        let msg = if expect_async {
            "#[async_task] requires `async fn run(...)`"
        } else {
            "#[sync_task] requires non-async `fn run(...)`"
        };
        return Err(syn::Error::new(run_fn.sig.span(), msg));
    }

    let (receiver_kind, has_ctx, arg_infos) = parse_signature(run_fn)?;
    let input_ty = build_input_type(&arg_infos);
    let output_ty = match &run_fn.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new(
                run_fn.sig.span(),
                "run method must have an explicit return type",
            ))
        }
        ReturnType::Type(_, ty) => ty.clone(),
    };

    let destructure = build_destructure(&arg_infos);
    let call_args: Vec<_> = arg_infos.iter().map(|arg| arg.call_expr.clone()).collect();
    let (receiver_setup, call_expr) =
        build_inherent_call(self_ty, receiver_kind, has_ctx, &call_args);

    // If the user's `run` does not declare `ctx: &FlowContext`, we still must
    // accept it in the generated trait impl — silence the unused warning with
    // a discard binding.
    let ctx_discard = if has_ctx {
        quote! {}
    } else {
        quote! { let _ = __tf_ctx; }
    };

    let trait_name = if expect_async {
        quote! { #root_path::tf::traits::AsyncTask }
    } else {
        quote! { #root_path::tf::traits::SyncTask }
    };

    let run_method = if expect_async {
        quote! {
            fn run(
                self,
                __tf_ctx: &#root_path::tf::component_registry::FlowContext,
                input: #root_path::tf::task::TaskInput<Self::Input>,
            ) -> impl std::future::Future<Output = #root_path::tf::task::TaskOutput<Self::Output>> + Send {
                async move {
                    #ctx_discard
                    #destructure
                    #receiver_setup
                    #root_path::tf::task::TaskOutput(#call_expr.await)
                }
            }
        }
    } else {
        quote! {
            fn run(
                self,
                __tf_ctx: &#root_path::tf::component_registry::FlowContext,
                input: #root_path::tf::task::TaskInput<Self::Input>,
            ) -> #root_path::tf::task::TaskOutput<Self::Output> {
                #ctx_discard
                #destructure
                #receiver_setup
                #root_path::tf::task::TaskOutput(#call_expr)
            }
        }
    };

    Ok(quote! {
        impl #trait_name for #self_ty {
            type Input = #input_ty;
            type Output = #output_ty;

            #run_method
        }
    })
}

fn find_run_fn(input_impl: &ItemImpl) -> core::result::Result <&ImplItemFn, syn::Error> {
    let mut run_fn: Option<&ImplItemFn> = None;

    for item in &input_impl.items {
        if let ImplItem::Fn(f) = item {
            if f.sig.ident == "run" {
                if run_fn.is_some() {
                    return Err(syn::Error::new(
                        f.sig.ident.span(),
                        "only one `run` method is allowed in #[sync_task]/#[async_task] impl",
                    ));
                }
                run_fn = Some(f);
            }
        }
    }

    run_fn.ok_or_else(|| {
        syn::Error::new(
            input_impl.self_ty.span(),
            "impl block annotated with #[sync_task]/#[async_task] must define `run`",
        )
    })
}

#[derive(Copy, Clone)]
enum ReceiverKind {
    None,
    Value,
    Ref,
    RefMut,
}

struct ArgInfo {
    binding: syn::Ident,
    input_ty: Type,
    call_expr: proc_macro2::TokenStream,
    needs_mut_binding: bool,
}

fn parse_signature(
    run_fn: &ImplItemFn,
) -> core::result::Result <(ReceiverKind, bool, std::vec::Vec <ArgInfo>), syn::Error> {
    let mut receiver = ReceiverKind::None;
    let mut args = Vec::new();
    let mut has_ctx = false;
    let mut typed_arg_index: usize = 0;

    for arg in &run_fn.sig.inputs {
        match arg {
            FnArg::Receiver(rcv) => {
                receiver = if rcv.reference.is_none() {
                    ReceiverKind::Value
                } else if rcv.mutability.is_some() {
                    ReceiverKind::RefMut
                } else {
                    ReceiverKind::Ref
                };
            }
            FnArg::Typed(typed) => {
                let Pat::Ident(pat_ident) = typed.pat.as_ref() else {
                    return Err(syn::Error::new(
                        typed.pat.span(),
                        "task `run` args must be simple identifiers",
                    ));
                };

                let ident = pat_ident.ident.clone();

                // Detect a leading `ctx: &FlowContext` argument. It must be
                // the first non-`self` typed parameter and is routed to the
                // runtime-provided FlowContext rather than being treated as a
                // DAG input. Match by the trailing `FlowContext` identifier so
                // users are free to `use ... as Foo` if they wish — but see
                // the macro docs for the recommended convention.
                if typed_arg_index == 0 {
                    if let Type::Reference(r) = typed.ty.as_ref() {
                        if r.mutability.is_none() && is_flow_context_path(r.elem.as_ref()) {
                            has_ctx = true;
                            typed_arg_index += 1;
                            continue;
                        }
                    }
                }
                typed_arg_index += 1;

                match typed.ty.as_ref() {
                    Type::Reference(r) if r.mutability.is_none() => {
                        let inner = (*r.elem).clone();
                        args.push(ArgInfo {
                            binding: ident.clone(),
                            input_ty: inner,
                            call_expr: quote! { &*#ident },
                            needs_mut_binding: false,
                        });
                    }
                    Type::Reference(r) if r.mutability.is_some() => {
                        return Err(syn::Error::new(
                            r.span(),
                            "task `run` args must use shared references `&T`; mutable refs `&mut T` are not supported",
                        ));
                    }
                    other_ty => {
                        return Err(syn::Error::new(
                            other_ty.span(),
                            "task `run` args must use shared references `&T`; by-value args are not supported",
                        ));
                    }
                }
            }
        }
    }

    Ok((receiver, has_ctx, args))
}

/// Matches `FlowContext` as the final path segment. Accepts `FlowContext`,
/// `taskflow::FlowContext`, `crate::tf::component_registry::FlowContext`, etc.
fn is_flow_context_path(ty: &Type) -> bool {
    if let Type::Path(p) = ty {
        if let Some(last) = p.path.segments.last() {
            return last.ident == "FlowContext";
        }
    }
    false
}

fn build_input_type(args: &[ArgInfo]) -> proc_macro2::TokenStream {
    match args {
        [] => quote! { () },
        _ => {
            let tys = args.iter().map(|arg| {
                let ty = &arg.input_ty;
                quote! { std::sync::Arc<#ty> }
            });
            quote! { ( #(#tys,)* ) }
        }
    }
}

fn build_destructure(args: &[ArgInfo]) -> proc_macro2::TokenStream {
    match args {
        [] => quote! { let _ = input; },
        _ => {
            let bindings = args.iter().map(|arg| {
                let ident = &arg.binding;
                if arg.needs_mut_binding {
                    quote! { mut #ident }
                } else {
                    quote! { #ident }
                }
            });
            quote! { let ( #(#bindings,)* ) = input.0; }
        }
    }
}

fn build_inherent_call(
    self_ty: &Type,
    receiver_kind: ReceiverKind,
    has_ctx: bool,
    call_args: &[proc_macro2::TokenStream],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    // If the user's run declared `ctx: &FlowContext`, prepend it to the
    // argument list so it flows from the runtime into their function.
    let ctx_arg: Vec<proc_macro2::TokenStream> = if has_ctx {
        vec![quote! { __tf_ctx }]
    } else {
        vec![]
    };
    let all_args: Vec<proc_macro2::TokenStream> = ctx_arg
        .into_iter()
        .chain(call_args.iter().cloned())
        .collect();

    match receiver_kind {
        ReceiverKind::None => {
            let call = if all_args.is_empty() {
                quote! { <#self_ty>::run() }
            } else {
                quote! { <#self_ty>::run(#(#all_args),*) }
            };
            (quote! {}, call)
        }
        ReceiverKind::Value => {
            let call = if all_args.is_empty() {
                quote! { <#self_ty>::run(self) }
            } else {
                quote! { <#self_ty>::run(self, #(#all_args),*) }
            };
            (quote! {}, call)
        }
        ReceiverKind::Ref => {
            let call = if all_args.is_empty() {
                quote! { <#self_ty>::run(&self) }
            } else {
                quote! { <#self_ty>::run(&self, #(#all_args),*) }
            };
            (quote! {}, call)
        }
        ReceiverKind::RefMut => {
            let call = if all_args.is_empty() {
                quote! { <#self_ty>::run(&mut __task) }
            } else {
                quote! { <#self_ty>::run(&mut __task, #(#all_args),*) }
            };
            (quote! { let mut __task = self; }, call)
        }
    }
}
