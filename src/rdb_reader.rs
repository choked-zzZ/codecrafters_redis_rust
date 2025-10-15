use crate::Value;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

use tokio::fs::{File, OpenOptions};

pub async fn rbd_reader(path: &Path) -> Value {
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
        let key = get_content(&mut fp).await;
        metadata.push(key);
        let val = get_content(&mut fp).await;
        metadata.push(val);
        let indicator = fp.read_u8().await.unwrap();
        match indicator {
            0xFA => continue,
            0xFE => break,
            _ => unreachable!(),
        }
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
        let key = get_content(&mut fp).await;
        match indicator {
            0x00 => {
                let val = get_content(&mut fp).await;
                data.push_back(val);
            }
            _ => unreachable!(),
        }
    }
    Value::Array([].into())
}

async fn get_content(fp: &mut File) -> Value {
    let byte = fp.read_u8().await.unwrap();
    let length = match byte >> 6 {
        0b00 => byte as usize,
        0b01 => {
            let leading = ((byte ^ 0x40) << 8) as usize;
            let trailing = fp.read_u8().await.unwrap() as usize;
            leading ^ trailing
        }
        0b10 => fp.read_u64().await.unwrap() as usize,
        0b11 => {
            eprintln!("number literal");
            let num = match byte {
                0xC0 => fp.read_u8().await.unwrap() as u32,
                0xC1 => fp.read_u16().await.unwrap() as u32,
                0xC2 => fp.read_u32().await.unwrap(),
                _ => todo!(),
            };
            return Value::BulkString(num.to_string().into());
        }
        left => unreachable!("you met {left:x} but you shouldn't..."),
    };
    eprintln!("get length: {length}");
    let mut content = BytesMut::zeroed(length);
    fp.read_exact(&mut content).await.unwrap();
    let content = content.freeze();
    Value::BulkString(content)
}
