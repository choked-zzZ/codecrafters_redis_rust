use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::{lock::Mutex, SinkExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::{
    resp_decoder::{RespParser, Value},
    Env,
};

pub enum RedisCommand {
    Ping,
    Echo(Value),
    Set(Value, Value, Option<SystemTime>),
    Get(Value),
    RPush(Value, VecDeque<Value>),
    LRange(Value, isize, isize),
    LPush(Value, VecDeque<Value>),
    LLen(Value),
    LPop(Value),
    LPopMany(Value, usize),
    BLPop(Value, usize),
}

impl RedisCommand {
    pub async fn exec(
        self,
        framed: &mut Framed<TcpStream, RespParser>,
        env: Arc<Mutex<Env>>,
    ) -> Result<(), std::io::Error> {
        match self {
            RedisCommand::Ping => {
                let response = Value::String("PONG".into());
                framed.send(response).await
            }
            RedisCommand::Echo(msg) => framed.send(msg).await,
            RedisCommand::Set(k, v, time) => {
                let mut env = env.lock().await;
                let k = Arc::new(k);
                if let Some(time) = time {
                    env.expiry.insert(k.clone(), time);
                }
                env.map.insert(k, v);
                framed.send(Value::String("OK".into())).await
            }
            RedisCommand::Get(k) => {
                let env = env.lock().await;
                let item = match (env.map.get(&k), env.expiry.get(&k)) {
                    (None, _) => Value::NullBulkString,
                    (Some(val), None) => val.clone(),
                    (Some(val), Some(expire_time)) => {
                        let now = SystemTime::now();
                        eprintln!("now: {now:?}, expire_time: {expire_time:?}");
                        if *expire_time < now {
                            Value::NullBulkString
                        } else {
                            val.clone()
                        }
                    }
                };
                framed.send(item).await
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
                alert(list_key, &mut env).await;
                framed.send(Value::Integer(len)).await
            }
            RedisCommand::LRange(list_key, l, r) => {
                let env = env.lock().await;
                let Some(Value::Array(arr)) = &env.map.get(&list_key) else {
                    return framed.send(Value::Array(VecDeque::new())).await;
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
                    return framed.send(Value::Array(VecDeque::new())).await;
                }
                #[allow(clippy::map_clone)]
                let slice = Value::Array(arr.range(l..=r).map(|x| x.clone()).collect());
                framed.send(slice).await
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
                alert(list_key, &mut env).await;
                framed.send(Value::Integer(len)).await
            }
            RedisCommand::LLen(list_key) => {
                let env = env.lock().await;
                let len = env
                    .map
                    .get(&list_key)
                    .map(|arr| arr.as_array().unwrap().len())
                    .unwrap_or(0) as i64;
                framed.send(Value::Integer(len)).await
            }
            RedisCommand::LPop(list_key) => {
                let mut env = env.lock().await;
                let item = env
                    .map
                    .get_mut(&list_key)
                    .map(|arr| arr.as_array_mut().unwrap())
                    .and_then(|x| x.pop_front())
                    .unwrap_or(Value::NullBulkString);
                framed.send(item).await
            }
            RedisCommand::LPopMany(list_key, remove_size) => {
                let mut env = env.lock().await;
                let item = env
                    .map
                    .get_mut(&list_key)
                    .map(|arr| arr.as_array_mut().unwrap())
                    .map(|x| Value::Array(x.drain(..remove_size.min(x.len())).collect()))
                    .unwrap_or(Value::NullBulkString);
                framed.send(item).await
            }
            RedisCommand::BLPop(list_key, timeout_limit) => {
                if let Some(Value::Array(arr)) = env.lock().await.map.get_mut(&list_key) {
                    if let Some(val) = arr.pop_front() {
                        return framed.send(val).await;
                    }
                }
                let (tx, rx) = oneshot::channel();
                env.lock()
                    .await
                    .waitlist
                    .entry(Arc::new(list_key))
                    .or_default()
                    .push_back(tx);
                let item = if timeout_limit != 0 {
                    timeout(Duration::from_secs(timeout_limit as u64), rx)
                        .await
                        .map(|x| x.expect("Call receiver after all the sender has been droped."))
                        .unwrap_or(Value::NullArray)
                } else {
                    rx.await
                        .expect("Call the receiver after all the sender has been droped.")
                };
                eprintln!("get item {item:?}");
                framed.send(item).await
            }
        }
    }
    pub fn parse_command(value: Value) -> RedisCommand {
        match value {
            Value::Array(arr) if !arr.is_empty() => {
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
                            arr.get(1).unwrap().clone(),
                            arr.get(2).unwrap().clone(),
                            time,
                        )
                    }
                    "GET" => {
                        assert!(arr.len() == 2);
                        RedisCommand::Get(arr.get(1).unwrap().clone())
                    }
                    "RPUSH" => {
                        assert!(arr.len() >= 3);
                        let list_key = arr.get(1).unwrap().clone();
                        let items = arr.into_iter().skip(2).collect();
                        RedisCommand::RPush(list_key, items)
                    }
                    "LRANGE" => {
                        assert!(arr.len() == 4);
                        let list_key = arr.get(1).unwrap().clone();
                        let l = arr.get(2).unwrap().as_integer().unwrap() as isize;
                        let r = arr.get(3).unwrap().as_integer().unwrap() as isize;
                        RedisCommand::LRange(list_key, l, r)
                    }
                    "LPUSH" => {
                        assert!(arr.len() >= 3);
                        let list_key = arr.get(1).unwrap().clone();
                        let items = arr.into_iter().skip(2).collect();
                        RedisCommand::LPush(list_key, items)
                    }
                    "LLEN" => {
                        assert!(arr.len() == 2);
                        let list_key = arr.get(1).unwrap().clone();
                        RedisCommand::LLen(list_key)
                    }
                    "LPOP" => {
                        assert!(matches!(arr.len(), 2 | 3));
                        let list_key = arr.get(1).unwrap().clone();
                        match arr.get(2).map(|x| x.as_integer().unwrap() as usize) {
                            Some(int) => RedisCommand::LPopMany(list_key, int),
                            None => RedisCommand::LPop(list_key),
                        }
                    }
                    "BLPOP" => {
                        assert!(arr.len() == 3);
                        let list_key = arr.get(1).unwrap().clone();
                        let timeout = arr.get(2).unwrap().as_integer().unwrap() as usize;
                        RedisCommand::BLPop(list_key, timeout)
                    }
                    _ => panic!("Unknown command or invalid arguments"),
                }
            }
            _ => panic!("Unknown command"),
        }
    }
}

async fn alert(list_key: Arc<Value>, env: &mut futures::lock::MutexGuard<'_, Env>) {
    let Some(Value::Array(arr)) = env.map.get_mut(&list_key) else {
        unreachable!()
    };
    let items = Value::Array(
        [
            Arc::as_ref(&list_key).clone(),
            if arr.is_empty() {
                return;
            } else {
                Value::Array(arr.drain(..1).collect())
            },
        ]
        .into(),
    );
    let Some(sender) = env.waitlist.get_mut(&list_key).and_then(|s| s.pop_front()) else {
        return;
    };
    sender.send(items).ok();
    eprintln!("send");
}
