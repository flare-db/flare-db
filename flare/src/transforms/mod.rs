use std::{any::Any, collections::HashMap, sync::LazyLock};

use beam_model_rs::v1::Elements;

use crate::{errors::TransformError, transforms::impluse::Impulse};

pub mod gbk;
pub mod impluse;

pub trait FlareTransform {
    type Context: TransformContext;
    fn urn() -> &'static str
    where
        Self: Sized;

    fn with(inputs: HashMap<String, String>, outputs: HashMap<String, String>) -> Self;

    fn execute(&self, ctx: &Self::Context) -> Result<Elements, TransformError>;
}

pub trait TransformContext {}

pub enum RunnerTransfrom {
    Impulse(Impulse),
    //GBK(GBK),
    // more later
}
