use std::{any::Any, collections::HashMap};

pub struct FlowContext {
    components: HashMap<&'static str, Box<dyn Any + Send + Sync>>
}

impl FlowContext {
    pub fn new() -> Self {
        FlowContext { components: HashMap::default() }
    }

    pub fn get_component<T: 'static>(&self, name: impl Into<String>) -> Option<&T> {
        if let Some(inner_value) = self.components.get(name.into().as_str()) {
            inner_value.downcast_ref::<T>()
        } else {
            None
        }
    }
}

pub struct ComponentFactory {
    name: &'static str,
    creator: fn() -> Box<dyn Any + Send + Sync>
}

inventory::collect!(ComponentFactory);

// register_with_type_name!(Timer, "timer");
#[macro_export]
macro_rules! register_with_type_name {
    ($type: ty, $name: literal) => {
        inventory::submit! {
            ComponentFactory {
                name: $name,
                creator: || Box::new($type::new()),
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