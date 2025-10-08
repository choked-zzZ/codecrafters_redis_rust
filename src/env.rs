use std::{collections::HashMap, time::SystemTime};

use crate::resp_decoder::Value;

#[derive(Debug, Default)]
pub struct Env {
    pub map: HashMap<Value, (Value, Option<SystemTime>)>,
}
