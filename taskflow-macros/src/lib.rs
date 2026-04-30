use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, spanned::Spanned, FnArg, ImplItem, ImplItemFn, ItemImpl, Pat, ReturnType,
    Type,
};

#[proc_macro_attribute]
pub fn sync_task(_attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_task(item, false)
}

#[proc_macro_attribute]
pub fn async_task(_attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_task(item, true)
}

fn expand_task(item: TokenStream, expect_async: bool) -> TokenStream {
    let input_impl = parse_macro_input!(item as ItemImpl);

    match build_task_impl(&input_impl, expect_async) {
        Ok(expanded) => TokenStream::from(quote! {
            #input_impl
            #expanded
        }),
        Err(err) => err.to_compile_error().into(),
    }
}

fn build_task_impl(
    input_impl: &ItemImpl,
    expect_async: bool,
) -> syn::Result<proc_macro2::TokenStream> {
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

    let (receiver_kind, arg_infos) = parse_signature(run_fn)?;
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

    let (receiver_setup, call_expr) = build_inherent_call(self_ty, receiver_kind, &call_args);

    let trait_name = if expect_async {
        quote! { crate::tf::traits::AsyncTask }
    } else {
        quote! { crate::tf::traits::SyncTask }
    };

    let run_method = if expect_async {
        quote! {
            fn run(
                self,
                input: crate::tf::task::TaskInput<Self::Input>,
            ) -> impl std::future::Future<Output = crate::tf::task::TaskOutput<Self::Output>> + Send {
                async move {
                    #destructure
                    #receiver_setup
                    crate::tf::task::TaskOutput(#call_expr.await)
                }
            }
        }
    } else {
        quote! {
            fn run(
                self,
                input: crate::tf::task::TaskInput<Self::Input>,
            ) -> crate::tf::task::TaskOutput<Self::Output> {
                #destructure
                #receiver_setup
                crate::tf::task::TaskOutput(#call_expr)
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

fn find_run_fn(input_impl: &ItemImpl) -> syn::Result<&ImplItemFn> {
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

fn parse_signature(run_fn: &ImplItemFn) -> syn::Result<(ReceiverKind, Vec<ArgInfo>)> {
    let mut receiver = ReceiverKind::None;
    let mut args = Vec::new();

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
                // 为避免运行时按值入参触发 Arc::try_unwrap + clone，
                // 强制要求业务层 task `run` 的所有依赖入参都写成 `&T`。
                // 允许:  &T
                // 禁止:  T / &mut T
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

    Ok((receiver, args))
}

fn build_input_type(args: &[ArgInfo]) -> proc_macro2::TokenStream {
    match args {
        [] => quote! { () },
        _ => {
            // 每个参数类型包装为 Arc<T>，与 FromAnyVec 的 (Arc<A>, Arc<B>, ...) 实现一致
            // 尾逗号确保 1-tuple 语法正确
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
            // 统一元组解构: 1-arg → let (data,) = input.0;
            //               N-arg → let (a, b,) = input.0;
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
    call_args: &[proc_macro2::TokenStream],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    match receiver_kind {
        ReceiverKind::None => {
            let call = if call_args.is_empty() {
                quote! { <#self_ty>::run() }
            } else {
                quote! { <#self_ty>::run(#(#call_args),*) }
            };
            (quote! {}, call)
        }
        ReceiverKind::Value => {
            let call = if call_args.is_empty() {
                quote! { <#self_ty>::run(self) }
            } else {
                quote! { <#self_ty>::run(self, #(#call_args),*) }
            };
            (quote! {}, call)
        }
        ReceiverKind::Ref => {
            let call = if call_args.is_empty() {
                quote! { <#self_ty>::run(&self) }
            } else {
                quote! { <#self_ty>::run(&self, #(#call_args),*) }
            };
            (quote! {}, call)
        }
        ReceiverKind::RefMut => {
            let call = if call_args.is_empty() {
                quote! { <#self_ty>::run(&mut __task) }
            } else {
                quote! { <#self_ty>::run(&mut __task, #(#call_args),*) }
            };
            (quote! { let mut __task = self; }, call)
        }
    }
}
