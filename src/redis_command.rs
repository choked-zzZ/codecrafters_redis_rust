use std::sync::Arc;

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
    Set(Value, Value),
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
            RedisCommand::Set(k, v) => {
                let mut env = env.lock().await;
                env.map.insert(k, v);
                framed.send(Value::String("OK".into())).await
            }
            RedisCommand::Get(k) => {
                let env = env.lock().await;
                framed
                    .send(env.map.get(&k).unwrap_or(&Value::NullBulkString).clone())
                    .await
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
                    _ => panic!("Unknown command or invalid arguments"),
                }
            }
            _ => panic!("Unknown command"),
        }
    }
}
