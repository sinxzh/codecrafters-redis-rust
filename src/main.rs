use std::io::{prelude::*, BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::thread;

const PONG: &[u8] = b"+PONG\r\n";

fn handle_connection(stream: TcpStream) {
    println!("accepted new connection");

    let mut buf_reader = BufReader::new(&stream);
    let mut buf_writer = BufWriter::new(&stream);

    loop {
        let mut line = String::new();

        let n = buf_reader.read_line(&mut line).unwrap();
        if n == 0 {
            println!("connection closed by client");
            return;
        }

        println!("line: [{}]", line);

        if line.starts_with("PING") {
            buf_writer.write_all(PONG).unwrap();
            buf_writer.flush().unwrap();
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                thread::spawn(|| {
                    handle_connection(_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
