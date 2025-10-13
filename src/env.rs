use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use tokio::{net::TcpStream, sync::oneshot::Sender};
use tokio_util::codec::Framed;

use crate::{
    redis_command::RedisCommand,
    resp_decoder::{RespParser, StreamID, Value},
    Args,
};

#[derive(Debug, Default)]
pub struct Env {
    pub map: HashMap<Arc<Bytes>, Value>,
    pub expiry: HashMap<Arc<Bytes>, SystemTime>,
    pub waitlist: HashMap<Arc<Bytes>, VecDeque<WaitFor>>,
    pub in_transaction: HashMap<SocketAddr, Vec<RedisCommand>>,
    pub replicas: HashMap<SocketAddr, Vec<Framed<TcpStream, RespParser>>>,
}

#[derive(Debug)]
pub enum WaitFor {
    List(Sender<Value>),
    Stream(StreamID, Sender<Value>),
}
