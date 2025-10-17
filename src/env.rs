use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    net::SocketAddr,
    ops::Bound::Included,
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

type SSMap = HashMap<Arc<Bytes>, f64>;
type SSList = skiplist::OrderedSkipList<(f64, Arc<Bytes>)>;

#[derive(Debug)]
pub struct SortedSet {
    map: SSMap,
    list: SSList,
}

impl Default for SortedSet {
    fn default() -> Self {
        Self {
            map: SSMap::default(),
            list: unsafe {
                SSList::with_comp(|x, y| match x.0.partial_cmp(&y.0).unwrap() {
                    std::cmp::Ordering::Equal => x.1.cmp(&y.1),
                    res => res,
                })
            },
        }
    }
}

impl SortedSet {
    pub fn sep(&self) -> (&SSMap, &SSList) {
        (&self.map, &self.list)
    }

    pub fn insert(&mut self, key: Arc<Bytes>, val: f64) -> Option<f64> {
        if val.is_nan() {
            panic!()
        }
        self.list.insert((val, key.clone()));
        self.map.insert(key.clone(), val)
    }

    pub fn rank(&self, key: &Arc<Bytes>) -> Option<usize> {
        let (k, &v) = self.map.get_key_value(key)?;
        self.list.index_of(&(v, k.clone()))
    }

    fn size(&self) -> usize {
        self.list.len()
    }

    pub fn range(&self, l_bound: usize, r_bound: usize) -> VecDeque<Value> {
        let size = self.size();
        if l_bound >= size || l_bound > r_bound {
            return [].into();
        }
        let r_bound = size.min(r_bound + 1);
        self.list
            .index_range(l_bound..r_bound)
            .map(|(val, key)| Value::BulkString(key.as_ref().clone()))
            .collect()
    }
}
