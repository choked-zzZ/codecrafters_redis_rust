use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
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
pub type Channel = HashMap<SocketAddr, (mpsc::Sender<Value>, JoinHandle<()>)>;

#[derive(Debug, Default)]
pub struct Env {
    pub map: Map,
    pub expiry: Expiry,
    pub waitlist: HashMap<Arc<Bytes>, VecDeque<WaitFor>>,
    pub in_transaction: HashMap<SocketAddr, Vec<RedisCommand>>,
    pub replicas: Vec<(Arc<mpsc::Sender<Value>>, SplitStream<_Framed>)>,
    pub ack: usize,
    pub file_path: Option<Box<Path>>,
    pub channels: HashMap<Arc<Bytes>, Channel>,
    pub subscription: HashMap<SocketAddr, HashSet<Arc<Bytes>>>,
    pub sorted_sets: HashMap<Arc<Bytes>, SortedSet>,
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

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Score(pub f64);

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Eq for Score {}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

type SSMap = HashMap<Arc<Bytes>, Score>;
type SSList = BTreeSet<(Score, Arc<Bytes>)>;

#[derive(Debug, Default)]
pub struct SortedSet {
    map: SSMap,
    list: SSList,
}

impl SortedSet {
    pub fn sep(&self) -> (&SSMap, &SSList) {
        (&self.map, &self.list)
    }

    pub fn insert(&mut self, key: Arc<Bytes>, val: f64) -> Option<f64> {
        if val.is_nan() {
            panic!()
        }
        let val = Score(val);
        self.list.insert((val, key.clone()));
        self.map.insert(key.clone(), val).map(|x| x.0)
    }
}
