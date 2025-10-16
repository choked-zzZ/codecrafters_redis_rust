use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use futures::stream::SplitStream;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::codec::Framed;

use crate::{
    redis_command::RedisCommand,
    resp_decoder::{RespParser, StreamID, Value},
};

pub type Map = HashMap<Arc<Bytes>, Value>;
pub type Expiry = HashMap<Arc<Bytes>, SystemTime>;
pub type _Framed = Framed<TcpStream, RespParser>;

#[derive(Debug, Default)]
pub struct Env {
    pub map: Map,
    pub expiry: Expiry,
    pub waitlist: HashMap<Arc<Bytes>, VecDeque<WaitFor>>,
    pub in_transaction: HashMap<SocketAddr, Vec<RedisCommand>>,
    pub replicas: Vec<(Arc<mpsc::Sender<Value>>, SplitStream<_Framed>)>,
    pub ack: usize,
    pub file_path: Option<Box<Path>>,
    pub channels: HashMap<Arc<Bytes>, HashMap<SocketAddr, (mpsc::Sender<Value>, JoinHandle<()>)>>,
    pub subscription: HashMap<SocketAddr, HashSet<Arc<Bytes>>>,
}

impl Env {
    pub fn is_in_sub_mode(&self, addr: &SocketAddr) -> bool {
        eprintln!(
            "{:?} {:?} {:?}",
            self.subscription,
            addr,
            self.subscription.get(addr)
        );
        self.subscription.get(addr).is_some_and(|x| !x.is_empty())
    }
}

#[derive(Debug)]
pub enum WaitFor {
    List(oneshot::Sender<Value>),
    Stream(StreamID, oneshot::Sender<Value>),
}
