use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::Bound::{Excluded, Unbounded};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::env::{Expiry, Map, WaitFor};
use crate::resp_decoder::{RespParser, Stream, StreamID};
use crate::{rdb_reader, REPL_ID};
use crate::{resp_decoder::Value, Env};
use crate::{Args, REPL_OFFSET};

#[derive(Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(Value),
    Set(Bytes, Value, Option<SystemTime>),
    Get(Bytes),
    RPush(Bytes, VecDeque<Value>),
    LRange(Bytes, isize, isize),
    LPush(Bytes, VecDeque<Value>),
    LLen(Bytes),
    LPop(Bytes),
    LPopMany(Bytes, usize),
    BLPop(Bytes, f64),
    Type(Bytes),
    XAdd(Bytes, Value, HashMap<Bytes, Value>),
    XRange(Bytes, Value, Value),
    XRead(Vec<Bytes>, Vec<Value>),
    BXRead(Vec<Bytes>, u64, Vec<Value>),
    Incr(Bytes),
    Multi,
    Exec,
    Discard,
    Info(Bytes),
    Replconf(Value, Value),
    PSync(Value, Value),
    Wait(u32, u64),
    Keys(Value),
    Config(Value, Value),
    Subscribe(Bytes),
    Publish(Bytes, Value),
    // Unsubscribe,
    // PSubscribe,
    // PUnsubscribe,
    // Quit,
    // Reset,
}

impl RedisCommand {
    fn show(&self) -> String {
        match self {
            Self::Ping => "PING",
            Self::Echo(_) => "ECHO",
            Self::Set(_, _, _) => "SET",
            Self::Get(_) => "GET",
            Self::RPush(_, _) => "RPUSH",
            Self::LRange(_, _, _) => "LRANGE",
            Self::LPush(_, _) => "LPUSH",
            Self::LLen(_) => "LLEN",
            Self::LPop(_) => "LPOP",
            Self::LPopMany(_, _) => "LPOPMANY",
            Self::BLPop(_, _) => "BLPOP",
            Self::Type(_) => "TYPE",
            Self::XAdd(_, _, _) => "XADD",
            Self::XRange(_, _, _) => "XRANGE",
            Self::XRead(_, _) => "XREAD",
            Self::BXRead(_, _, _) => "BXREAD",
            Self::Incr(_) => "INCR",
            Self::Multi => "MULTI",
            Self::Exec => "EXEC",
            Self::Discard => "DISCARD",
            Self::Info(_) => "INFO",
            Self::Replconf(_, _) => "REPLCONF",
            Self::PSync(_, _) => "PSYNC",
            Self::Wait(_, _) => "WAIT",
            Self::Keys(_) => "KEYS",
            Self::Config(_, _) => "CONFIG",
            Self::Subscribe(_) => "SUBSCRIBE",
            Self::Publish(_, _) => "PUBLISH",
        }
        .into()
    }

    pub fn can_modify(&self) -> bool {
        matches!(
            self,
            RedisCommand::Set(..)
                | RedisCommand::RPush(..)
                | RedisCommand::LPush(..)
                | RedisCommand::LPop(..)
                | RedisCommand::LPopMany(..)
                | RedisCommand::BLPop(..)
                | RedisCommand::XAdd(..)
                | RedisCommand::Incr(..)
                | RedisCommand::Multi
                | RedisCommand::Exec
                | RedisCommand::Discard
        )
    }

    fn allow_when_subscribe(&self) -> bool {
        matches!(
            self,
            RedisCommand::Subscribe(..) | RedisCommand::Ping | RedisCommand::Publish(..)
        )
    }

    async fn sub_mode_exec(
        self,
        env: Arc<Mutex<Env>>,
        _addr: SocketAddr,
        _argss: Arc<Args>,
    ) -> Value {
        if !self.allow_when_subscribe() {
            return Value::Error(format!("ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", self.show()).into());
        }
        match self {
            RedisCommand::Ping => Value::Array(
                [
                    Value::BulkString("pong".into()),
                    Value::BulkString("".into()),
                ]
                .into(),
            ),
            RedisCommand::Publish(channel, message) => {
                let env = env.lock().await;
                let channel = env.channels.get(&channel).unwrap();
                let publish = channel.len();
                for sender in channel {
                    sender.send(message.clone()).await.unwrap();
                }
                Value::Integer(publish as i64)
            }
            _ => unreachable!(),
        }
    }

    pub fn exec(
        self,
        env: Arc<Mutex<Env>>,
        addr: SocketAddr,
        args: Arc<Args>,
        framed: Arc<Mutex<Framed<TcpStream, RespParser>>>,
    ) -> Pin<Box<dyn Future<Output = Value> + Send>> {
        Box::pin(async move {
            if !matches!(self, RedisCommand::Exec | RedisCommand::Discard) {
                if let Some(transaction) = env.lock().await.in_transaction.get_mut(&addr) {
                    transaction.push(self);
                    return Value::String("QUEUED".into());
                }
            }
            let in_sub_mode = env.lock().await.is_in_sub_mode(&addr);
            if in_sub_mode {
                return self.sub_mode_exec(env, addr, args).await;
            }
            match self {
                RedisCommand::Ping => Value::String("PONG".into()),
                RedisCommand::Echo(msg) => msg,
                RedisCommand::Set(k, v, time) => {
                    let mut env = env.lock().await;
                    let k = Arc::new(k);
                    if let Some(time) = time {
                        env.expiry.insert(k.clone(), time);
                    }
                    env.map.insert(k, v);

                    Value::String("OK".into())
                }
                RedisCommand::Get(key) => {
                    let env = env.lock().await;
                    get(&env.map, &env.expiry, key)
                }
                RedisCommand::RPush(list_key, mut items) => {
                    let mut env = env.lock().await;
                    let list_key = Arc::new(list_key);
                    let len = match env.map.get_mut(&list_key) {
                        Some(val) => {
                            if let Value::Array(arr) = val {
                                arr.append(&mut items);
                                arr.len()
                            } else {
                                panic!("not a list.");
                            }
                        }
                        None => {
                            let len = items.len();
                            env.map.insert(Arc::clone(&list_key), Value::Array(items));
                            len
                        }
                    } as _;
                    list_update_alert(list_key, &mut env).await;
                    Value::Integer(len)
                }
                RedisCommand::LRange(list_key, l, r) => {
                    let env = env.lock().await;
                    let Some(Value::Array(arr)) = &env.map.get(&list_key) else {
                        return Value::Array(VecDeque::new());
                    };
                    let len = arr.len();
                    let l = if l >= 0 {
                        l as usize
                    } else {
                        len.checked_add_signed(l).unwrap_or(0)
                    };
                    let r = if r >= 0 {
                        (r as usize).min(len - 1)
                    } else {
                        len.checked_add_signed(r).unwrap_or(0)
                    };
                    if l >= len || l > r {
                        return Value::Array(VecDeque::new());
                    }
                    #[allow(clippy::map_clone)]
                    Value::Array(arr.range(l..=r).map(|x| x.clone()).collect())
                }
                RedisCommand::LPush(list_key, items) => {
                    let mut env = env.lock().await;
                    let list_key = Arc::new(list_key);
                    let len = match env.map.get_mut(&list_key) {
                        Some(val) => {
                            if let Value::Array(arr) = val {
                                items.into_iter().for_each(|x| arr.push_front(x));
                                arr.len()
                            } else {
                                panic!("not a list.");
                            }
                        }
                        None => {
                            let len = items.len();
                            env.map.insert(Arc::clone(&list_key), Value::Array(items));
                            len
                        }
                    } as _;
                    list_update_alert(list_key, &mut env).await;
                    Value::Integer(len)
                }
                RedisCommand::LLen(list_key) => {
                    let env = env.lock().await;
                    let len = env
                        .map
                        .get(&list_key)
                        .map(|arr| arr.as_array().unwrap().len())
                        .unwrap_or(0) as i64;
                    Value::Integer(len)
                }
                RedisCommand::LPop(list_key) => {
                    let mut env = env.lock().await;
                    env.map
                        .get_mut(&list_key)
                        .map(|arr| arr.as_array_mut().unwrap())
                        .and_then(|x| x.pop_front())
                        .unwrap_or(Value::NullBulkString)
                }
                RedisCommand::LPopMany(list_key, remove_size) => {
                    let mut env = env.lock().await;
                    env.map
                        .get_mut(&list_key)
                        .map(|arr| arr.as_array_mut().unwrap())
                        .map(|x| Value::Array(x.drain(..remove_size.min(x.len())).collect()))
                        .unwrap_or(Value::NullBulkString)
                }
                RedisCommand::BLPop(list_key, timeout_limit) => {
                    if let Some(Value::Array(arr)) = env.lock().await.map.get_mut(&list_key) {
                        if let Some(val) = arr.pop_front() {
                            return val;
                        }
                    }
                    let (tx, rx) = oneshot::channel();
                    env.lock()
                        .await
                        .waitlist
                        .entry(Arc::new(list_key))
                        .or_default()
                        .push_back(WaitFor::List(tx));
                    if timeout_limit != 0f64 {
                        timeout(Duration::from_secs_f64(timeout_limit), rx)
                            .await
                            .map(|x| {
                                x.expect("Call receiver after all the sender has been droped.")
                            })
                            .unwrap_or(Value::NullArray)
                    } else {
                        rx.await
                            .expect("Call the receiver after all the sender has been droped.")
                    }
                }
                RedisCommand::Type(key) => match env.lock().await.map.get(&key) {
                    None => Value::String("none".into()),
                    Some(val) => match val {
                        Value::BulkString(_) => Value::String("string".into()),
                        Value::Stream(_) => Value::String("stream".into()),
                        _ => todo!(),
                    },
                },
                RedisCommand::XAdd(stream_key, mut stream_entry_id, kvp) => {
                    time_now();
                    let mut env = env.lock().await;
                    let stream_key = Arc::new(stream_key);
                    match env.map.entry(Arc::clone(&stream_key)) {
                        Entry::Occupied(mut map_entry) => {
                            let stream = map_entry.get_mut().as_stream_mut().unwrap();
                            let stream_entry_key = stream_entry_id.as_entry_id(stream).unwrap();
                            let last = stream.last_key_value().unwrap().0;
                            if stream_entry_key.is_invalid() {
                                return Value::Error(
                                    "ERR The ID specified in XADD must be greater than 0-0".into(),
                                );
                            } else if stream_entry_key <= *last {
                                return Value::Error("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
                            }
                            stream.insert(stream_entry_key, kvp);
                        }
                        Entry::Vacant(map_entry) => {
                            let mut stream = Stream::new();
                            let stream_entry_key =
                                stream_entry_id.as_entry_id(&mut stream).unwrap();
                            stream.insert(stream_entry_key, kvp);
                            map_entry.insert(Value::Stream(stream));
                        }
                    }
                    eprintln!("ready to alert");
                    stream_update_alert(stream_key, &mut env).await;
                    stream_entry_id
                }
                RedisCommand::XRange(stream_key, left, right) => {
                    let l_bound = StreamID::parse(left, true, &None).unwrap();
                    let r_bound = StreamID::parse(right, false, &None).unwrap();
                    let env = env.lock().await;
                    let stream = env.map.get(&stream_key).unwrap().as_stream().unwrap();
                    let items = stream.range(l_bound..=r_bound);
                    let mut arr = VecDeque::new();
                    for (stream_id, kvp) in items {
                        let mut kvp_arr = VecDeque::new();
                        kvp.iter().for_each(|(k, v)| {
                            kvp_arr.push_back(Value::BulkString(k.clone()));
                            kvp_arr.push_back(v.clone());
                        });
                        let kvp_arr = Value::Array(kvp_arr);
                        let stream_entry = Value::Array(VecDeque::from([
                            stream_id.as_bulk_string().clone(),
                            kvp_arr,
                        ]));
                        arr.push_back(stream_entry);
                    }
                    Value::Array(arr)
                }
                RedisCommand::XRead(stream_keys, ids) => {
                    let env = env.lock().await;
                    let mut streams = VecDeque::new();
                    for (stream_key, id) in stream_keys.into_iter().zip(ids.into_iter()) {
                        let stream = env.map.get(&stream_key).unwrap().as_stream().unwrap();
                        let l_bound = StreamID::parse(id, true, &None).unwrap();
                        let items = stream.range((Excluded(l_bound), Unbounded));
                        let mut entries = VecDeque::new();
                        for (stream_id, fields) in items {
                            let mut fields_arr = VecDeque::new();
                            fields.iter().for_each(|(k, v)| {
                                fields_arr.push_back(Value::BulkString(k.clone()));
                                fields_arr.push_back(v.clone());
                            });
                            let fields_arr = Value::Array(fields_arr);
                            let stream_entry = Value::Array(VecDeque::from([
                                stream_id.as_bulk_string().clone(),
                                fields_arr,
                            ]));
                            entries.push_back(stream_entry);
                        }
                        let inner_arr = Value::Array(entries);
                        let single_stream = Value::Array(VecDeque::from([
                            Value::BulkString(stream_key),
                            inner_arr,
                        ]));
                        streams.push_back(single_stream);
                    }
                    Value::Array(streams)
                }
                RedisCommand::BXRead(stream_keys, block_milisec, ids) => {
                    let mut streams = VecDeque::new();
                    for (stream_key, id) in stream_keys.into_iter().zip(ids.into_iter()) {
                        let mut env = env.lock().await;
                        let stream = env.map.get(&stream_key).map(|x| x.as_stream().unwrap());
                        let l_bound = StreamID::parse(id, true, &stream).unwrap();
                        let mut items =
                            stream.map(|x| x.range((Excluded(l_bound), Unbounded)).peekable());
                        if items.as_mut().is_none_or(|x| x.peek().is_none()) {
                            eprintln!("no stream or content.");
                            let (tx, rx) = oneshot::channel();
                            env.waitlist
                                .entry(Arc::new(stream_key))
                                .or_default()
                                .push_back(WaitFor::Stream(l_bound, tx));
                            drop(env);
                            let single_stream =
                                if block_milisec == 0 {
                                    eprintln!("forever");
                                    Some(rx.await.expect(
                                        "Call receiver after all the sender has been droped.",
                                    ))
                                } else {
                                    eprintln!("wait {block_milisec} ms");
                                    time_now();
                                    timeout(Duration::from_millis(block_milisec), rx)
                                .await
                                .map(|x| {
                                    x.expect("Call receiver after all the sender has been droped.")
                                })
                                .ok()
                                    // .unwrap_or(Value::NullArray)
                                };
                            eprintln!("got a single stream {single_stream:?}");
                            if let Some(single_stream) = single_stream {
                                streams.push_back(single_stream);
                            } else {
                                return Value::NullArray;
                            }
                        } else {
                            eprintln!("Contents detected. no block.");
                            let items = items.unwrap();
                            let mut entries = VecDeque::new();
                            for (stream_id, fields) in items {
                                let mut fields_arr = VecDeque::new();
                                fields.iter().for_each(|(k, v)| {
                                    fields_arr.push_back(Value::BulkString(k.clone()));
                                    fields_arr.push_back(v.clone());
                                });
                                let fields_arr = Value::Array(fields_arr);
                                let stream_entry = Value::Array(VecDeque::from([
                                    stream_id.as_bulk_string().clone(),
                                    fields_arr,
                                ]));
                                entries.push_back(stream_entry);
                            }
                            let inner_arr = Value::Array(entries);
                            let single_stream = Value::Array(VecDeque::from([
                                Value::BulkString(stream_key),
                                inner_arr,
                            ]));
                            streams.push_back(single_stream);
                        }
                    }
                    Value::Array(streams)
                }
                RedisCommand::Incr(key) => {
                    let mut env = env.lock().await;
                    match env.map.entry(Arc::new(key)) {
                        Entry::Occupied(mut entry) => match entry.get_mut().incr() {
                            Ok(_) => entry.get().clone(),
                            Err(_) => {
                                Value::Error("ERR value is not an integer or out of range".into())
                            }
                        },
                        Entry::Vacant(entry) => {
                            entry.insert(Value::Integer(1));
                            Value::Integer(1)
                        }
                    }
                }
                RedisCommand::Multi => {
                    let mut env = env.lock().await;
                    env.in_transaction.insert(addr, Vec::new());
                    Value::String("OK".into())
                }
                RedisCommand::Exec => {
                    let mut env_unlocked = env.lock().await;
                    let transaction = env_unlocked.in_transaction.remove(&addr);
                    drop(env_unlocked);
                    match transaction {
                        None => Value::Error("ERR EXEC without MULTI".into()),
                        Some(transaction) => {
                            let mut arr = VecDeque::new();
                            for command in transaction {
                                arr.push_back(
                                    command
                                        .exec(env.clone(), addr, args.clone(), framed.clone())
                                        .await,
                                );
                            }
                            Value::Array(arr)
                        }
                    }
                }
                RedisCommand::Discard => match env.lock().await.in_transaction.remove(&addr) {
                    None => Value::Error("ERR DISCARD without MULTI".into()),
                    Some(_) => Value::String("OK".into()),
                },
                RedisCommand::Info(section) => {
                    let section = str::from_utf8(&section).unwrap().to_ascii_uppercase();
                    match section.as_str() {
                        "REPLICATION" => Value::String(
                            format!(
                                "# Replication\nrole:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
                                if args.replicaof.is_some() {
                                    "slave"
                                } else {
                                    "master"
                                },
                                REPL_ID,
                                REPL_OFFSET,
                            )
                            .into(),
                        ),
                        _ => todo!(),
                    }
                }
                RedisCommand::Replconf(fi, _se) => {
                    if str::from_utf8(&fi.into_bytes())
                        .unwrap()
                        .to_ascii_uppercase()
                        .as_str()
                        == "GETACK"
                    {
                        Value::Array(
                            [
                                Value::BulkString("REPLCONF".into()),
                                Value::BulkString("ACK".into()),
                                Value::BulkString(env.lock().await.ack.to_string().into()),
                            ]
                            .into(),
                        )
                    } else {
                        Value::String("OK".into())
                    }
                }
                RedisCommand::PSync(_fi, _se) => {
                    let mut buf = vec![b'$'];
                    let mut rdb_content = STANDARD.decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==").expect("decode_err");
                    // let mut rdb_content = fs::read("../empty.rdb").await.expect("read error");
                    buf.extend_from_slice(rdb_content.len().to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    buf.append(&mut rdb_content);
                    Value::Batch(
                        [
                            Value::String(format!("FULLRESYNC {} {}", REPL_ID, REPL_OFFSET).into()),
                            Value::RawBinary(buf),
                        ]
                        .into(),
                    )
                }
                RedisCommand::Wait(replicas_count, wait_milisecs) => {
                    if replicas_count == 0 {
                        return Value::Integer(0);
                    }
                    let satisified_count = Arc::new(Mutex::new(0));
                    let count = Arc::clone(&satisified_count);
                    let handle = async move {
                        let mut env = env.lock().await;
                        if env.ack == 0 {
                            return Value::Integer(env.replicas.len() as i64);
                        }
                        let command = Value::Array(
                            [
                                Value::BulkString("REPLCONF".into()),
                                Value::BulkString("GETACK".into()),
                                Value::BulkString("*".into()),
                            ]
                            .into(),
                        );
                        let ack_master_current = env.ack;
                        eprintln!("the ack need to match the number {ack_master_current}");
                        eprintln!("we got {} replica(s)", env.replicas.len());
                        for (cnt, replica) in env.replicas.iter_mut().enumerate() {
                            eprintln!("handling replica No.{cnt}");
                            replica
                                .lock()
                                .await
                                .send(&command)
                                .await
                                .expect("send error.");
                            eprintln!("send completed");
                            if cnt < replicas_count as usize {
                                if let Some(result) = replica.lock().await.next().await {
                                    match result {
                                        Err(e) => eprintln!("got error: {e:?}"),
                                        Ok(val) => {
                                            let ack = val
                                                .as_array()
                                                .unwrap()
                                                .get(2)
                                                .unwrap()
                                                .as_integer()
                                                .unwrap()
                                                as usize;
                                            eprintln!(
                                                "this replica got ack with {ack} meet? {}",
                                                ack == ack_master_current
                                            );
                                            if ack == ack_master_current {
                                                *count.lock().await += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        eprintln!("ack progress completed.");
                        env.ack += command.buf_size();
                        return Value::Integer(*count.lock().await);
                    };
                    match timeout(Duration::from_millis(wait_milisecs), handle).await {
                        Err(_) => {
                            let final_val = *satisified_count.lock().await;
                            Value::Integer(final_val)
                        }
                        Ok(val) => val,
                    }
                }
                RedisCommand::Config(operation, target) => {
                    let operation = str::from_utf8(operation.as_bulk_string().unwrap())
                        .unwrap()
                        .to_ascii_uppercase();
                    if operation == "GET" {
                        let op_target = str::from_utf8(target.as_bulk_string().unwrap())
                            .unwrap()
                            .to_ascii_uppercase();
                        if op_target == "DIR" {
                            let dir = Value::BulkString(args.dir.clone().unwrap().into());
                            Value::Array([target, dir].into())
                        } else {
                            todo!()
                        }
                    } else {
                        todo!()
                    }
                }
                RedisCommand::Keys(pattern) => {
                    let pattern = str::from_utf8(pattern.as_bulk_string().unwrap()).unwrap();
                    let re = glob::Pattern::new(pattern).unwrap();

                    let path = env.lock().await.file_path.clone().unwrap();

                    if !path.exists() {
                        eprintln!("file not exist");
                        tokio::fs::write(path.as_ref(), STANDARD.decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==").unwrap()).await.unwrap();
                    }
                    eprintln!("{:x?}", fs::read(path.as_ref()));
                    let key_val_pair = rdb_reader::rbd_reader(path.as_ref()).await.0;
                    Value::Array(
                        key_val_pair
                            .into_keys()
                            .filter(|x| re.matches(str::from_utf8(x).unwrap()))
                            .map(|x| Value::BulkString(x.as_ref().clone()))
                            .collect(),
                    )
                }
                RedisCommand::Subscribe(subscribe_to) => {
                    let mut env = env.lock().await;
                    let subscribe_to = Arc::new(subscribe_to);
                    let (tx, mut rx) = mpsc::channel::<Value>(32);
                    env.subscription
                        .entry(addr)
                        .or_insert(HashSet::new())
                        .insert(subscribe_to.clone());
                    env.channels
                        .entry(subscribe_to.clone())
                        .or_insert(Vec::new())
                        .push(tx);
                    let channel_name = subscribe_to.clone();
                    tokio::spawn(async move {
                        while let Some(val) = rx.recv().await {
                            let response = Value::Array(
                                [
                                    Value::BulkString("message".into()),
                                    Value::BulkString(channel_name.as_ref().clone()),
                                    val,
                                ]
                                .into(),
                            );
                            framed.lock().await.send(&response).await.unwrap();
                        }
                    });
                    Value::Array(
                        [
                            Value::BulkString("subscribe".into()),
                            Value::BulkString(subscribe_to.as_ref().clone()),
                            Value::Integer(env.subscription.get(&addr).unwrap().len() as _),
                        ]
                        .into(),
                    )
                }
                _ => unreachable!(),
            }
        })
    }

    pub fn parse_command(value: Arc<Value>) -> RedisCommand {
        match *value {
            Value::Array(ref arr) if !arr.is_empty() => {
                let Value::BulkString(command_bytes) = arr.front().unwrap() else {
                    unreachable!();
                };
                let command = str::from_utf8(command_bytes)
                    .expect("Should be utf-8 encoded command string.")
                    .to_ascii_uppercase();
                match command.as_str() {
                    "ECHO" => {
                        assert!(arr.len() == 2);
                        RedisCommand::Echo(arr.get(1).unwrap().clone())
                    }
                    "PING" => {
                        assert!(arr.len() == 1);
                        RedisCommand::Ping
                    }
                    "SET" => {
                        assert!(matches!(arr.len(), 3 | 5));
                        let time = if arr.len() == 5 {
                            let mut now = SystemTime::now();
                            let number =
                                str::from_utf8(arr.get(4).unwrap().as_bulk_string().unwrap())
                                    .unwrap()
                                    .parse::<u64>()
                                    .unwrap();
                            eprintln!("duration: {number}");
                            let Value::BulkString(s) = &arr[3] else {
                                panic!("bad argument.")
                            };
                            let duration = if s == "EX" {
                                Duration::from_secs(number)
                            } else if s == "PX" {
                                Duration::from_millis(number)
                            } else {
                                panic!("bad argument.");
                            };
                            now += duration;
                            Some(now)
                        } else {
                            None
                        };
                        RedisCommand::Set(
                            arr.get(1).unwrap().as_bulk_string().unwrap().clone(),
                            arr.get(2).unwrap().clone(),
                            time,
                        )
                    }
                    "GET" => {
                        assert!(arr.len() == 2);
                        RedisCommand::Get(arr.get(1).unwrap().as_bulk_string().unwrap().clone())
                    }
                    "RPUSH" => {
                        assert!(arr.len() >= 3);
                        let list_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let items = arr.iter().skip(2).cloned().collect();
                        RedisCommand::RPush(list_key, items)
                    }
                    "LRANGE" => {
                        assert!(arr.len() == 4);
                        let list_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let l = arr.get(2).unwrap().as_integer().unwrap() as isize;
                        let r = arr.get(3).unwrap().as_integer().unwrap() as isize;
                        RedisCommand::LRange(list_key, l, r)
                    }
                    "LPUSH" => {
                        assert!(arr.len() >= 3);
                        let list_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let items = arr.iter().skip(2).cloned().collect();
                        RedisCommand::LPush(list_key, items)
                    }
                    "LLEN" => {
                        assert!(arr.len() == 2);
                        let list_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        RedisCommand::LLen(list_key)
                    }
                    "LPOP" => {
                        assert!(matches!(arr.len(), 2 | 3));
                        let list_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        match arr.get(2).map(|x| x.as_integer().unwrap() as usize) {
                            Some(int) => RedisCommand::LPopMany(list_key, int),
                            None => RedisCommand::LPop(list_key),
                        }
                    }
                    "BLPOP" => {
                        assert!(arr.len() == 3);
                        let list_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let timeout = arr.get(2).unwrap().as_float().unwrap();
                        RedisCommand::BLPop(list_key, timeout)
                    }
                    "TYPE" => {
                        assert!(arr.len() == 2);
                        let key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        RedisCommand::Type(key)
                    }
                    "XADD" => {
                        let stream_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let entry_id = arr.get(2).unwrap().clone();
                        let mut kvp = HashMap::new();
                        for (k, v) in arr.iter().skip(3).tuples() {
                            kvp.insert(k.clone().into_bytes(), v.clone());
                        }
                        RedisCommand::XAdd(stream_key, entry_id, kvp)
                    }
                    "XRANGE" => {
                        let stream_key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let left = arr.get(2).unwrap().clone();
                        let right = arr.get(3).unwrap().clone();
                        RedisCommand::XRange(stream_key, left, right)
                    }
                    "XREAD" => {
                        let mode = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let mode = str::from_utf8(&mode).unwrap().to_ascii_uppercase();
                        match mode.as_str() {
                            "STREAMS" => {
                                let streams_count = arr.len() / 2 - 1;
                                let key = arr
                                    .iter()
                                    .skip(2)
                                    .take(streams_count)
                                    .map(|x| x.as_bulk_string().unwrap().clone())
                                    .collect();
                                let id = arr.iter().skip(2 + streams_count).cloned().collect();
                                RedisCommand::XRead(key, id)
                            }
                            "BLOCK" => {
                                let block_milisec =
                                    arr.get(2).unwrap().as_integer().unwrap() as u64;
                                let streams_count = arr.len() / 2 - 2;
                                let key = arr
                                    .iter()
                                    .skip(4)
                                    .take(streams_count)
                                    .map(|x| x.as_bulk_string().unwrap().clone())
                                    .collect();
                                let id = arr.iter().skip(4 + streams_count).cloned().collect();
                                eprintln!("{block_milisec} and {key:?} and {id:?}");
                                RedisCommand::BXRead(key, block_milisec, id)
                            }
                            _ => {
                                panic!("{mode}: Unknown command of XREAD.")
                            }
                        }
                    }
                    "INCR" => {
                        let key = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        RedisCommand::Incr(key)
                    }
                    "MULTI" => RedisCommand::Multi,
                    "EXEC" => RedisCommand::Exec,
                    "DISCARD" => RedisCommand::Discard,
                    "INFO" => {
                        let section = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        RedisCommand::Info(section)
                    }
                    "REPLCONF" => {
                        let fi = arr.get(1).unwrap().clone();
                        let se = arr.get(2).unwrap().clone();
                        RedisCommand::Replconf(fi, se)
                    }
                    "PSYNC" => {
                        let fi = arr.get(1).unwrap().clone();
                        let se = arr.get(2).unwrap().clone();
                        RedisCommand::PSync(fi, se)
                    }
                    "WAIT" => {
                        let replica_count = arr.get(1).unwrap().as_integer().unwrap() as u32;
                        let wait_milisecs = arr.get(2).unwrap().as_integer().unwrap() as u64;
                        RedisCommand::Wait(replica_count, wait_milisecs)
                    }
                    "KEYS" => {
                        let pattern = arr.get(1).unwrap().clone();
                        RedisCommand::Keys(pattern)
                    }
                    "CONFIG" => {
                        let operation = arr.get(1).unwrap().clone();
                        let target = arr.get(2).unwrap().clone();
                        RedisCommand::Config(operation, target)
                    }
                    "SUBSCRIBE" => {
                        let subscribe_to = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        RedisCommand::Subscribe(subscribe_to)
                    }
                    "PUBLISH" => {
                        let channel = arr.get(1).unwrap().as_bulk_string().unwrap().clone();
                        let message = arr.get(2).unwrap().clone();
                        RedisCommand::Publish(channel, message)
                    }
                    _ => panic!("Unknown command or invalid arguments"),
                }
            }
            ref cmd => panic!("Unknown command {cmd:?}"),
        }
    }
}

fn get(map: &Map, expiry: &Expiry, key: Bytes) -> Value {
    let item = match (map.get(&key), expiry.get(&key)) {
        (None, _) => &Value::NullBulkString,
        (Some(val), None) => val,
        (Some(val), Some(expire_time)) => {
            let now = SystemTime::now();
            eprintln!("now: {now:?}, expire_time: {expire_time:?}");
            if *expire_time < now {
                &Value::NullBulkString
            } else {
                val
            }
        }
    };
    eprintln!("get {item:?}");
    match item {
        val @ Value::BulkString(_) | val @ Value::NullBulkString => val.clone(),
        Value::Integer(i) => Value::BulkString(i.to_string().into()),
        val => todo!("{val:?}"),
    }
}

#[inline]
fn time_now() {
    eprintln!(
        "{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
}

async fn stream_update_alert(stream_key: Arc<Bytes>, env: &mut tokio::sync::MutexGuard<'_, Env>) {
    time_now();
    let Some(WaitFor::Stream(l_bound, sender)) = env
        .waitlist
        .get_mut(&stream_key)
        .and_then(|s| s.pop_front())
    else {
        eprintln!("nothing in the waitlist");
        return;
    };
    let Some(Value::Stream(stream)) = env.map.get_mut(&stream_key) else {
        unreachable!()
    };
    let mut items = stream.range((Excluded(l_bound), Unbounded)).peekable();
    let single_stream = if items.peek().is_none() {
        eprintln!("still no content available");
        return;
    } else {
        let mut arr = VecDeque::new();
        for (stream_id, kvp) in items {
            let mut kvp_arr = VecDeque::new();
            kvp.iter().for_each(|(k, v)| {
                kvp_arr.push_back(Value::BulkString(k.clone()));
                kvp_arr.push_back(v.clone());
            });
            let kvp_arr = Value::Array(kvp_arr);
            let stream_entry = Value::Array([stream_id.as_bulk_string().clone(), kvp_arr].into());
            arr.push_back(stream_entry);
        }
        Value::Array(
            [
                Value::BulkString(Arc::as_ref(&stream_key).clone()),
                Value::Array(arr),
            ]
            .into(),
        )
    };
    eprintln!("ready to send: {single_stream:?}");
    sender.send(single_stream).ok();
    eprintln!("sent");
}

async fn list_update_alert(list_key: Arc<Bytes>, env: &mut tokio::sync::MutexGuard<'_, Env>) {
    let Some(WaitFor::List(sender)) = env.waitlist.get_mut(&list_key).and_then(|s| s.pop_front())
    else {
        return;
    };
    let Some(Value::Array(arr)) = env.map.get_mut(&list_key) else {
        unreachable!()
    };
    let items = Value::Array(
        [
            Value::BulkString(Arc::as_ref(&list_key).clone()),
            if arr.is_empty() {
                return;
            } else {
                arr.pop_front().unwrap()
            },
        ]
        .into(),
    );
    sender.send(items).ok();
    eprintln!("send");
}
