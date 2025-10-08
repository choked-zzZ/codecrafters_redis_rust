use core::panic;
use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::{lock::Mutex, SinkExt};
use tokio::net::TcpStream;
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
                env.map.insert(k, (v, time));
                framed.send(Value::String("OK".into())).await
            }
            RedisCommand::Get(k) => {
                let env = env.lock().await;
                let item = match env.map.get(&k) {
                    None => Value::NullBulkString,
                    Some((val, expire_time)) => {
                        let now = SystemTime::now();
                        eprintln!("now: {now:?}, expire_time: {expire_time:?}");
                        if expire_time.is_some() && expire_time.unwrap() < now {
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
                let len = match env.map.entry(list_key) {
                    Entry::Occupied(mut ent) => {
                        if let Value::Array(ref mut arr) = ent.get_mut().0 {
                            arr.append(&mut items);
                            arr.len()
                        } else {
                            panic!("not a list.");
                        }
                    }
                    Entry::Vacant(e) => {
                        let Value::Array(arr) = &e.insert((Value::Array(items), None)).0 else {
                            unreachable!()
                        };
                        arr.len()
                    }
                };
                framed.send(Value::Integer(len as i64)).await
            }
            RedisCommand::LRange(list_key, l, r) => {
                let env = env.lock().await;
                let Some((Value::Array(arr), _)) = &env.map.get(&list_key) else {
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
                let len = match env.map.entry(list_key) {
                    Entry::Occupied(mut entry) => {
                        if let Value::Array(ref mut arr) = entry.get_mut().0 {
                            items.into_iter().for_each(|x| arr.push_front(x));
                            eprintln!("{arr:?}");
                            arr.len()
                        } else {
                            panic!("not a list.")
                        }
                    }
                    Entry::Vacant(e) => {
                        let Value::Array(arr) = &e.insert((Value::Array(items), None)).0 else {
                            unreachable!()
                        };
                        arr.len()
                    }
                } as _;
                framed.send(Value::Integer(len)).await
            }
            RedisCommand::LLen(list_key) => {
                let env = env.lock().await;
                let len = env
                    .map
                    .get(&list_key)
                    .map(|(arr, _)| arr.as_array().unwrap().len())
                    .unwrap_or(0) as i64;
                framed.send(Value::Integer(len)).await
            }
        }
    }
    pub fn parse_command(value: Value) -> RedisCommand {
        match value {
            Value::Array(arr) if !arr.is_empty() => {
                let Value::BulkString(command_bytes) = arr.front().unwrap() else {
                    unreachable!();
                };
                let command = str::from_utf8(&command_bytes)
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
                    _ => panic!("Unknown command or invalid arguments"),
                }
            }
            _ => panic!("Unknown command"),
        }
    }
}
