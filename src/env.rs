use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::SystemTime,
};

use tokio::sync::oneshot::Sender;

use crate::resp_decoder::Value;

#[derive(Debug, Default)]
pub struct Env {
    pub map: HashMap<Arc<Value>, Value>,
    pub expiry: HashMap<Arc<Value>, SystemTime>,
    pub waitlist: HashMap<Arc<Value>, VecDeque<Sender<Value>>>,
}
