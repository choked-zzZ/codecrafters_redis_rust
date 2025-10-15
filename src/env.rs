use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

use crate::{
    redis_command::RedisCommand,
    resp_decoder::{RespParser, StreamID, Value},
};

pub type Map = HashMap<Arc<Bytes>, Value>;
pub type Expiry = HashMap<Arc<Bytes>, SystemTime>;

#[derive(Debug, Default)]
pub struct Env {
    pub map: Map,
    pub expiry: Expiry,
    pub waitlist: HashMap<Arc<Bytes>, VecDeque<WaitFor>>,
    pub in_transaction: HashMap<SocketAddr, Vec<RedisCommand>>,
    pub replicas: Vec<Framed<TcpStream, RespParser>>,
    pub ack: usize,
    pub file_path: Option<Box<Path>>,
    pub subscription: HashMap<SocketAddr, HashMap<Arc<Bytes>, Vec<mpsc::Sender<Value>>>>,
}

impl Env {
    pub fn is_in_sub_mode(&self, addr: &SocketAddr) -> bool {
        self.subscription.get(addr).is_some_and(|x| !x.is_empty())
    }
}

#[derive(Debug)]
pub enum WaitFor {
    List(oneshot::Sender<Value>),
    Stream(StreamID, oneshot::Sender<Value>),
}
