//! Process-wide component registry and the `FlowContext` that carries it.
//!
//! The taskflow runtime feeds every task invocation with a shared
//! `&FlowContext`, giving tasks a place to look up long-lived infrastructure
//! (databases, HTTP clients, configuration, metrics registries, ...) without
//! having to pipe those values through DAG edges.
//!
//! # Registering components
//!
//! Two macros put components into the inventory that
//! [`init_components`] collects at startup:
//!
//! - [`register_singleton!`](crate::register_singleton): one instance per
//!   process, exposed as `&T` via [`FlowContext::get_singleton_component`].
//! - [`register_factory!`](crate::register_factory): constructor registered
//!   by name, invoked per call via [`FlowContext::create_component`], returns
//!   `Box<T>`.
//!
//! ```ignore
//! use rusty_taskflow::{register_singleton, register_factory};
//!
//! pub struct Db { /* ... */ }
//! impl Db { pub fn new() -> Self { Db {} } }
//! register_singleton!(Db, "db", Db::new);
//!
//! pub struct RequestId(pub u64);
//! impl RequestId { pub fn new() -> Self { RequestId(42) } }
//! register_factory!(RequestId, "request_id", RequestId::new);
//! ```
//!
//! # Consuming components inside a task
//!
//! Declare `ctx: &FlowContext` as the first non-`self` parameter of your
//! `run`; the proc macros forward it from the scheduler and do not treat it
//! as a DAG input:
//!
//! ```ignore
//! #[sync_task]
//! impl MyTask {
//!     fn run(self, ctx: &FlowContext, upstream: &u64) -> u64 {
//!         let db = ctx.get_singleton_component::<Db>("db").unwrap();
//!         let req = ctx.create_component::<RequestId>("request_id").unwrap();
//!         // ... use db and req ...
//!         *upstream
//!     }
//! }
//! ```
//!
//! # Runtime-constructed contexts
//!
//! For tests or custom wiring, skip the inventory path and build the context
//! imperatively with [`FlowContext::insert_singleton`] /
//! [`FlowContext::insert_factory`], then pass it into
//! [`crate::tf::flow::Flow::with_context`].

use std::{any::Any, collections::HashMap};

// Re-export `inventory` from the taskflow crate so downstream users who only
// depend on `taskflow` can still invoke `register_singleton!` /
// `register_factory!` without adding `inventory` to their own Cargo.toml.
#[doc(hidden)]
pub use inventory;

#[doc(hidden)]
pub enum ComponentEntry {
    Singleton(Box<dyn Any + Send + Sync>),
    Factory(Box<dyn Fn() -> Box<dyn Any + Send + Sync> + Send + Sync>),
}

/// A process-wide registry of shared components that the taskflow runtime
/// injects into every task invocation as `&FlowContext`.
///
/// Two kinds of components are supported:
///
/// - **Singletons**: constructed once at [`init_components`] time, shared by
///   reference (`get_singleton_component::<T>`).
/// - **Factories**: registered as a constructor, invoked on every
///   `create_component::<T>` call, returning a fresh `Box<T>` owned by the
///   caller.
///
/// Components are normally declared at module scope with the
/// [`register_singleton!`](crate::register_singleton) and
/// [`register_factory!`](crate::register_factory) macros and auto-collected by
/// [`init_components`]. For tests or dynamic wiring, use
/// [`FlowContext::insert_singleton`] / [`FlowContext::insert_factory`] to add
/// entries imperatively.
pub struct FlowContext {
    components: HashMap<&'static str, ComponentEntry>
}

impl FlowContext {
    /// Creates an empty context. Use [`init_components`] when you want the
    /// globally-registered components pre-populated.
    pub fn new() -> Self {
        FlowContext { components: HashMap::default() }
    }

    /// Imperatively inserts a pre-constructed singleton. Useful in tests or
    /// when the value depends on runtime state that the inventory-based
    /// `register_singleton!` cannot capture.
    ///
    /// Panics if `name` is already registered.
    pub fn insert_singleton<T: Send + Sync + 'static>(&mut self, name: &'static str, value: T) {
        if self
            .components
            .insert(name, ComponentEntry::Singleton(Box::new(value)))
            .is_some()
        {
            panic!("duplicate component: {name}");
        }
    }

    /// Imperatively inserts a factory component. Accepts any `Fn() -> T`
    /// closure (capturing or not) that is `Send + Sync + 'static`. Each call
    /// to `create_component::<T>(name)` invokes it to build a fresh instance.
    ///
    /// Panics if `name` is already registered.
    pub fn insert_factory<T, F>(&mut self, name: &'static str, factory: F)
    where
        T: Send + Sync + 'static,
        F: Fn() -> T + Send + Sync + 'static,
    {
        let erased: Box<dyn Fn() -> Box<dyn Any + Send + Sync> + Send + Sync> =
            Box::new(move || Box::new(factory()) as Box<dyn Any + Send + Sync>);
        if self
            .components
            .insert(name, ComponentEntry::Factory(erased))
            .is_some()
        {
            panic!("duplicate component: {name}");
        }
    }

    /// Borrows a singleton component by name. Returns `None` when the name is
    /// unknown, is registered as a factory instead of a singleton, or when the
    /// stored type does not match `T`.
    pub fn get_singleton_component<T: 'static>(&self, name: &str) -> Option<&T> {
        match self.components.get(name)? {
            ComponentEntry::Singleton(v) => v.downcast_ref::<T>(),
            ComponentEntry::Factory(_) => None
        }
    }

    /// Invokes the registered factory and returns ownership of a fresh
    /// instance. Returns `None` for unknown names, singleton-typed entries, or
    /// type mismatches.
    pub fn create_component<T: 'static>(&self, name: &str) -> Option<Box<T>> {
        match self.components.get(name)? {
            ComponentEntry::Singleton(_) => None,
            ComponentEntry::Factory(f) => {
                let v = f();
                v.downcast::<T>().ok()
            }
        }
    }
}

/// Inventory entry emitted by [`register_singleton!`] /
/// [`register_factory!`]. Collected at [`init_components`] time.
pub struct ComponentFactory {
    pub name: &'static str,
    pub creator: fn() -> ComponentEntry,
}

inventory::collect!(ComponentFactory);

/// Registers a **singleton component** into the global inventory.
///
/// The instance is constructed **exactly once** when [`init_components`] is called,
/// then owned by [`FlowContext`]. All callers obtain a shared reference `&T` via
/// `get_singleton_component::<T>(name)`.
///
/// # Usage
///
/// ```ignore
/// struct DbConnection { /* ... */ }
/// impl DbConnection {
///     fn new() -> Self { /* ... */ }
/// }
///
/// // Pass a function path
/// register_singleton!(DbConnection, "db", DbConnection::new);
///
/// // Pass a non-capturing closure
/// register_singleton!(DbConnection, "db", || DbConnection::new());
/// ```
///
/// # Constraints
///
/// - `$type` must be `Send + Sync + 'static` (the singleton is shared across threads).
/// - The return type of `$creator` must match `$type`; otherwise
///   `get_singleton_component::<$type>` returns `None`.
/// - `$name` must be globally unique across all registered components. Duplicates
///   cause [`init_components`] to panic.
/// - `$creator` must be **non-capturing** (a function path, or a closure that
///   captures nothing). See the next section.
///
/// # Why `$creator` must be non-capturing
///
/// This macro expands inside the **module-level ctor function** generated by
/// `inventory::submit!`. That function body has no user scope to capture from,
/// and the creator is ultimately coerced to a `fn` pointer. The following will
/// fail to compile:
///
/// ```ignore
/// // ❌ Captures an outer variable; cannot coerce to a fn pointer.
/// let url = std::env::var("DB_URL").unwrap();
/// register_singleton!(DbConnection, "db", move || DbConnection::with_url(url.clone()));
/// ```
///
/// If you need a "configured singleton", put the configuration in a global
/// `const` or `OnceLock` and read it from inside `$creator` — that is not a
/// capture.
#[macro_export]
macro_rules! register_singleton {
    ($type: ty, $name: literal, $creator: expr) => {
        $crate::tf::component_registry::inventory::submit! {
            $crate::tf::component_registry::ComponentFactory {
                name: $name,
                creator: || $crate::tf::component_registry::ComponentEntry::Singleton(
                    ::std::boxed::Box::new(($creator)())
                ),
            }
        }
    };
}

/// Registers a **factory component** into the global inventory.
///
/// Unlike singletons, factory components are **not instantiated** during
/// [`init_components`]; only the construction logic is registered. Every call to
/// `create_component::<T>(name)` invokes `$creator` to produce a fresh instance,
/// returning ownership to the caller as `Box<T>`.
///
/// # Usage
///
/// ```ignore
/// struct RequestHandler { /* ... */ }
/// impl RequestHandler {
///     fn new() -> Self { /* ... */ }
/// }
///
/// // Pass a function path
/// register_factory!(RequestHandler, "handler", RequestHandler::new);
///
/// // Pass a non-capturing closure
/// register_factory!(RequestHandler, "handler", || RequestHandler::new());
/// ```
///
/// # Constraints
///
/// - `$type` must be `Send + Sync + 'static`.
/// - The return type of `$creator` must match `$type`; otherwise
///   `create_component::<$type>` returns `None`.
/// - `$name` must be globally unique across all registered components.
/// - `$creator` must be **non-capturing**. See the next section.
///
/// # Why `$creator` must be non-capturing
///
/// `$creator` is wrapped in another zero-capture closure
/// `|| Box::new(($creator)()) as Box<dyn Any + ...>` and then coerced into a
/// `fn() -> Box<dyn Any + Send + Sync>` function pointer stored in the global
/// registry. Therefore `$creator` must be one of:
///
/// - A function path: `Timer::new`, `crate::factories::make_task`
/// - A non-capturing closure: `|| Timer::new()`, `|| Task { id: 0 }`
///
/// **The following will fail to compile** (`expected fn pointer, found closure`):
///
/// ```ignore
/// // ❌ The closure captures the local variable `prefix`.
/// let prefix = "x";
/// register_factory!(Task, "task", || Task::with_prefix(prefix));
///
/// // ❌ A `move` closure capturing `config`.
/// let config = load_config();
/// register_factory!(Task, "task", move || Task::from(config.clone()));
/// ```
///
/// Reason: the macro expands inside an `inventory` ctor function body, where
/// **no user-scoped local variables exist to capture**, and `fn` pointers
/// inherently cannot carry captured state.
///
/// # Need a parameterized factory?
///
/// Two workable options:
///
/// 1. **Store dependencies in global `const`s or `static`s** and read them from
///    inside `$creator` (this is not a capture):
///    ```ignore
///    static CONFIG: OnceLock<Config> = OnceLock::new();
///    register_factory!(Task, "task", || Task::new(CONFIG.get().unwrap()));
///    ```
///
/// 2. **Use a runtime registration API** (not provided by this macro): widen
///    `ComponentEntry::Factory` to `Box<dyn Fn() -> _ + Send + Sync>` to store
///    capturing closures.
///
/// # Performance notes
///
/// Each `create_component` call goes through: fn-pointer call → one `Box::new`
/// heap allocation → type erasure → downcast → unwrap of `Box<T>`. For small
/// types the dominant cost is the single heap allocation; if `$type` itself
/// owns heap fields (`Vec`, `String`, ...), the user-side construction usually
/// dwarfs the registry dispatch cost.
#[macro_export]
macro_rules! register_factory {
    ($type: ty, $name: literal, $creator: expr) => {
        $crate::tf::component_registry::inventory::submit! {
            $crate::tf::component_registry::ComponentFactory {
                name: $name,
                creator: || $crate::tf::component_registry::ComponentEntry::Factory(
                    ::std::boxed::Box::new(|| ::std::boxed::Box::new(($creator)())
                        as ::std::boxed::Box<dyn ::std::any::Any + ::std::marker::Send + ::std::marker::Sync>)
                )
            }
        }
    };
}

pub fn init_components() -> FlowContext {
    let mut ctx = FlowContext::new();
    let _: Vec<_> = inventory::iter::<ComponentFactory>
        .into_iter()
        .map(|component_factory|
            ctx.components
                .insert(component_factory.name, (component_factory.creator)())
                .map(|_| panic!("duplicate component: {}", component_factory.name))
        ).collect();
    ctx
}

#[cfg(test)]
mod test {
    use super::*;

    struct DbConnection {
        url: String,
    }

    impl DbConnection {
        fn new() -> Self {
            Self { url: "postgres://localhost/mydb".to_string() }
        }

        fn url(&self) -> &str {
            &self.url
        }
    }

    struct RequestHandler {
        id: u64,
    }

    impl RequestHandler {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            Self {
                id: COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            }
        }

        fn id(&self) -> u64 {
            self.id
        }
    }

    register_singleton!(DbConnection, "db", DbConnection::new);
    register_factory!(RequestHandler, "handler", RequestHandler::new);

    #[test]
    fn test_singleton_same_instance() {
        let ctx = init_components();

        let conn1 = ctx.get_singleton_component::<DbConnection>("db").unwrap();
        let conn2 = ctx.get_singleton_component::<DbConnection>("db").unwrap();

        assert_eq!(conn1.url(), conn2.url());
        assert!(std::ptr::eq(conn1, conn2));
    }

    #[test]
    fn test_factory_new_instance_each_time() {
        let ctx = init_components();

        let h1 = ctx.create_component::<RequestHandler>("handler").unwrap();
        let h2 = ctx.create_component::<RequestHandler>("handler").unwrap();

        assert_ne!(h1.id(), h2.id());
    }

    #[test]
    fn test_type_mismatch_returns_none() {
        let ctx = init_components();

        assert!(ctx.get_singleton_component::<RequestHandler>("db").is_none());
        assert!(ctx.create_component::<DbConnection>("handler").is_none());
    }
}