use std::marker::PhantomData;

use crate::tf::{flow::{Flow, TaskId}, traits::IntoDependencies};

pub struct OutputWrapper<O> {
    pub(crate) id: TaskId,
    phantom: PhantomData<O>
}

impl<O> OutputWrapper<O> {
    pub(crate) fn new(id: TaskId) -> Self {
        Self { id: id, phantom: PhantomData }
    }
}

impl IntoDependencies<()> for () {
    fn register(self, _flow: &mut Flow, _target: &TaskId) {}
}

impl<A> IntoDependencies<(A,)> for OutputWrapper<A> {
    fn register(self, flow: &mut Flow, target: &TaskId) {
        flow.add_edges(self.id, target.clone());
    }
}

macro_rules! impl_into_dependencies {
    ($($idx:tt : $T:ident),+) => {
        impl<$($T),+> IntoDependencies<($($T,)+)> for ($(OutputWrapper<$T>),+) {
            fn register(self, flow: &mut Flow, target: &TaskId) {
                $(flow.add_edges(self.$idx.id, target.clone());)+
            }
        }
    };
}

impl_into_dependencies!(0:A, 1:B);

pub struct DependencyBuilder<'flow, I, O> {
    pub(crate) id: TaskId,
    pub(crate) flow: &'flow mut Flow,
    phantom_in: PhantomData<I>,
    phantom_out: PhantomData<O>,
}

impl<'flow, I, O> DependencyBuilder<'flow, I, O> {
    pub fn new(id: TaskId, f: &'flow mut Flow) -> Self {
        DependencyBuilder { id, flow: f, phantom_in: PhantomData, phantom_out: PhantomData }
    }

    pub fn with_dependency(self, deps: impl IntoDependencies<I>) -> OutputWrapper<O> {
        deps.register(self.flow, &self.id);
        OutputWrapper { id: self.id, phantom: PhantomData }
    }
}