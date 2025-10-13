use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use tokio::sync::oneshot::Sender;

use crate::{
    redis_command::RedisCommand,
    resp_decoder::{StreamID, Value},
    Args,
};

#[derive(Debug, Default)]
pub struct Env {
    pub map: HashMap<Arc<Bytes>, Value>,
    pub expiry: HashMap<Arc<Bytes>, SystemTime>,
    pub waitlist: HashMap<Arc<Bytes>, VecDeque<WaitFor>>,
    pub in_transaction: HashMap<SocketAddr, Vec<RedisCommand>>,
    pub replicas: HashMap<SocketAddr, Vec<SocketAddr>>,
}

#[derive(Debug)]
pub enum WaitFor {
    List(Sender<Value>),
    Stream(StreamID, Sender<Value>),
}
