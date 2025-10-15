use crate::Value;
use regex::Regex;
use std::collections::VecDeque;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

use tokio::fs::OpenOptions;

pub async fn rbd_reader(path: &Path) {
    let mut fp = OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .expect("File pointer create failed.");

    let mut header = [0; 9];
    fp.read_exact(&mut header)
        .await
        .expect("read header failed");
    assert_eq!(&header, b"REDIS0011");
    assert_eq!(fp.read_u8().await.unwrap(), 0xFA);
    let mut metadata = Vec::new();
    loop {
        // TODO: Fix length encoding and string encoding.
        let length = fp.read_u8().await.unwrap();
        if length == 0xFE {
            break;
        }
        let mut buf = vec![0; length as usize];
        fp.read_exact(&mut buf).await.unwrap();
        metadata.push(buf);
    }
    let index = fp.read_u8().await.unwrap();
    assert_eq!(fp.read_u8().await.unwrap(), 0xFB);
    let kvp_count = fp.read_u8().await.unwrap();
    let expiry_count = fp.read_u8().await.unwrap();
    let mut data = VecDeque::new();
    loop {
        let mut indicator = fp.read_u8().await.unwrap();
        let mut skip = false;
        match indicator {
            0xFF => break,
            0xFC => {
                let expire_milisecs = fp.read_u64_le().await.unwrap();
                let milisecs_to_now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                if expire_milisecs as u128 >= milisecs_to_now {
                    skip = true;
                }
                indicator = fp.read_u8().await.unwrap();
            }
            0xFD => {
                let expire_secs = fp.read_u32_le().await.unwrap();
                let secs_to_now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if expire_secs as u64 >= secs_to_now {
                    skip = true;
                }
                indicator = fp.read_u8().await.unwrap();
            }
            _ => {}
        }
        let len = fp.read_u8().await.unwrap();
        let mut key = vec![0; len as usize];
        let matched = re.is_match(str::from_utf8(&key).expect("not a valid utf-8 encoded string"));
        fp.read_exact(&mut key).await.unwrap();
        match indicator {
            0x00 => {
                let len = fp.read_u8().await.unwrap();
                let mut val = vec![0; len as usize];
                fp.read_exact(&mut val).await.unwrap();
                if matched {
                    data.push_back(Value::BulkString(val.into()));
                }
            }
            _ => unreachable!(),
        }
    }
}
