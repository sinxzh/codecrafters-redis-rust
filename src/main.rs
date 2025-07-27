use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use clap::Parser;
use rand::seq::IndexedRandom;

use crate::kv_store::KvStore;
use crate::protocol::{Request, Response, ServerInfo, ServerRole};

pub mod kv_store;
pub mod protocol;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "6379")]
    port: u16,
    #[arg(long = "replicaof", default_value = None)]
    replica_of: Option<String>,
}

struct Server {
    listener: TcpListener,
    info: Arc<RwLock<ServerInfo>>,
    kv_store: Arc<RwLock<KvStore>>,
}

impl Server {
    fn new(info: ServerInfo) -> Result<Server, std::io::Error> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", info.port))?;

        Ok(Server {
            listener,
            info: Arc::new(RwLock::new(info)),
            kv_store: Arc::new(RwLock::new(KvStore::new())),
        })
    }

    fn run(&self) {
        let mut handles = vec![];

        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let kv_store = Arc::clone(&self.kv_store);
                    let server_info = Arc::clone(&self.info);
                    let handle = thread::spawn(move || {
                        Server::handle_connection(stream, kv_store, server_info);
                    });
                    handles.push(handle);
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                }
            }
        }
    }

    fn handle_connection(
        stream: TcpStream,
        kv_store: Arc<RwLock<KvStore>>,
        server_info: Arc<RwLock<ServerInfo>>,
    ) {
        let mut req = Request::new(&stream);
        let mut resp = Response::new(&stream);

        loop {
            match req.read_command() {
                Ok(()) => {
                    // let mut kv_store = kv_store.write().unwrap();
                    // let server_info = server_info.read().unwrap();
                    if let Err(e) = resp.process_command(&req.command, &kv_store, &server_info) {
                        eprintln!("error executing command: {}", e);
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
}

fn generate_random_alphanumeric(count: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

    let mut rng = rand::rng();

    let random_string: String = (0..count)
        .map(|_| {
            let random_char = CHARSET.choose(&mut rng).unwrap();
            *random_char as char
        })
        .collect();

    random_string
}

fn main() {
    let args = Args::parse();
    let port = args.port;
    let role = if args.replica_of.is_none() {
        ServerRole::Master("master")
    } else {
        ServerRole::Slave("slave")
    };

    let server_info = ServerInfo::new(generate_random_alphanumeric(40), port, role);

    if let Ok(server) = Server::new(server_info) {
        println!("Starting server on port {}, role: {}", port, role);
        server.run();
    } else {
        eprintln!("Failed to start server on port {}", port);
        std::process::exit(1);
    }
}
