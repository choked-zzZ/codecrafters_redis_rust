use core::panic;
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
        }
    }
    pub fn parse_command(value: Value) -> RedisCommand {
        match value {
            Value::Array(arr) if !arr.is_empty() => {
                let command = arr.first().unwrap();
                match command {
                    Value::BulkString(s) if s == "ECHO" => {
                        assert!(arr.len() == 2);
                        RedisCommand::Echo(arr.get(1).unwrap().clone())
                    }
                    Value::BulkString(s) if s == "PING" => {
                        assert!(arr.len() == 1);
                        RedisCommand::Ping
                    }
                    Value::BulkString(s) if s == "SET" => {
                        assert!(matches!(arr.len(), 3 | 5));
                        let time = if s.len() == 5 {
                            let mut now = SystemTime::now();
                            let number = arr[4].as_integer().unwrap();
                            let Value::BulkString(s) = &arr[3] else {
                                panic!("bad argument.")
                            };
                            let duration = if s == "EX" {
                                Duration::from_secs(number as u64)
                            } else if s == "PX" {
                                Duration::from_millis(number as u64)
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
                    Value::BulkString(s) if s == "GET" => {
                        assert!(arr.len() == 2);
                        RedisCommand::Get(arr.get(1).unwrap().clone())
                    }
                    _ => panic!("Unknown command or invalid arguments"),
                }
            }
            _ => panic!("Unknown command"),
        }
    }
}
