use std::collections::HashMap;
use std::io::{prelude::*, BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

struct ResponseWriter<'a> {
    writer: BufWriter<&'a TcpStream>,
}

impl<'a> ResponseWriter<'a> {
    fn new(stream: &'a TcpStream) -> ResponseWriter {
        ResponseWriter {
            writer: BufWriter::new(&stream),
        }
    }

    fn write_simple_string(&mut self, content: &str) {
        self.writer.write_all(b"+").unwrap();
        self.writer.write_all(content.as_bytes()).unwrap();
        self.writer.write_all(b"\r\n").unwrap();
        self.writer.flush().unwrap();
    }

    fn write_bulk_string(&mut self, content: &str) {
        let str_len = format!("${}\r\n", content.len());
        self.writer.write_all(str_len.as_bytes()).unwrap();

        self.writer.write_all(content.as_bytes()).unwrap();
        self.writer.write_all(b"\r\n").unwrap();
        self.writer.flush().unwrap();
    }

    fn write_null_bulk_string(&mut self) {
        self.writer.write_all(b"$-1\r\n").unwrap();
        self.writer.flush().unwrap();
    }
}

struct Request<'a> {
    cnt: usize,

    buf_reader: BufReader<&'a TcpStream>,
    buf: String,

    resp_writer: ResponseWriter<'a>,

    kvs: Arc<Mutex<HashMap<String, String>>>,
}

impl<'a> Request<'a> {
    fn new(stream: &'a TcpStream, kvs: Arc<Mutex<HashMap<String, String>>>) -> Request<'a> {
        Request {
            cnt: 0,
            buf_reader: BufReader::new(&stream),
            buf: String::new(),
            resp_writer: ResponseWriter::new(&stream),
            kvs,
        }
    }

    fn read_line(&mut self) -> usize {
        self.buf.clear();
        let n = self.buf_reader.read_line(&mut self.buf).unwrap();
        println!("read line: [{}]", self.buf);
        n
    }

    fn read_bulk_string(&mut self) {
        assert!(self.read_line() > 0);

        assert!(self.buf.starts_with('$'));
        let str_len: usize = self.buf[1..].trim().parse().unwrap();

        assert!(self.read_line() > 0);
        assert!(str_len == self.buf.trim().len());

        self.cnt -= 1;
    }

    fn parse(&mut self) {
        loop {
            let n = self.read_line();
            if n == 0 {
                println!("connection closed by client");
                return;
            }

            assert!(self.buf.starts_with('*'));
            self.cnt = self.buf[1..].trim().parse().unwrap();

            while self.cnt > 0 {
                self.parse_command();
            }
        }
    }

    fn parse_command(&mut self) {
        assert!(self.read_line() > 0);
        assert!(self.buf.starts_with('$'));

        assert!(self.read_line() > 0);
        let cmd = self.buf.trim().to_uppercase();
        println!("parse command: {}", cmd);

        match cmd.as_str() {
            "PING" => {
                self.resp_writer.write_simple_string("PONG");
            }
            "ECHO" => {
                self.read_bulk_string();
                let val = &self.buf.trim().to_string();
                self.resp_writer.write_bulk_string(val.as_str());
            }
            "SET" => {
                self.read_bulk_string();
                let key = self.buf.trim().to_string();

                self.read_bulk_string();
                let val = self.buf.trim().to_string();

                self.kvs.lock().unwrap().insert(key, val);

                self.resp_writer.write_simple_string("OK");
            }
            "GET" => {
                self.read_bulk_string();
                let key = self.buf.trim();

                if let Some(val) = self.kvs.lock().unwrap().get(key) {
                    self.resp_writer.write_bulk_string(val);
                } else {
                    self.resp_writer.write_null_bulk_string();
                }
            }
            _ => {
                panic!("read unknown command: {}", &self.buf);
            }
        }

        self.cnt -= 1;
    }
}

fn handle_connection(stream: TcpStream, kvs: Arc<Mutex<HashMap<String, String>>>) {
    println!("accepted new connection");

    let mut req = Request::new(&stream, kvs);
    req.parse();
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let mut handles = vec![];
    let kvs: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        let kvs = Arc::clone(&kvs);

        match stream {
            Ok(_stream) => {
                handles.push(thread::spawn(move || {
                    handle_connection(_stream, kvs);
                }));
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
