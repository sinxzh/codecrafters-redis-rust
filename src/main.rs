use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::kv_store::KvStore;
use crate::resp::{Request, Response};

pub mod kv_store;
pub mod resp;

fn handle_connection(stream: TcpStream, mut kv: KvStore) {
    println!("accepted new connection");

    let mut req = Request::new(&stream);
    loop {
        match req.read_command() {
            Ok(()) => {
                let mut resp = Response::new(&stream);
                if let Err(e) = resp.exec_command(&req.commands, &mut kv) {
                    println!("error executing command: {}", e);
                    break;
                }
            }
            Err(e) => {
                println!("error reading command: {}", e);
                break;
            }
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let mut handles = vec![];
    let kv = KvStore::new();

    for stream in listener.incoming() {
        let kv_clone = KvStore::clone(&kv);

        match stream {
            Ok(_stream) => {
                let handle = thread::spawn(move || {
                    handle_connection(_stream, kv_clone);
                });
                handles.push(handle);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
