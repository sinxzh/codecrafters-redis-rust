use std::collections::HashMap;
use std::io::{BufReader, BufWriter, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

struct Value {
    val: String,
    expire_at: Option<Instant>,
}

impl Value {
    fn new(val: String, expire_mills: Option<u64>) -> Value {
        let expire_at = expire_mills.map(|mills| Instant::now() + Duration::from_millis(mills));
        Value { val, expire_at }
    }
}

struct KV {
    kvs: Arc<Mutex<HashMap<String, Value>>>,
}

impl KV {
    fn new() -> KV {
        KV {
            kvs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn clone(&self) -> KV {
        KV {
            kvs: Arc::clone(&self.kvs),
        }
    }

    fn set(&mut self, key: String, val: Value) {
        self.kvs.lock().unwrap().insert(key, val);
    }

    fn get_del<F>(&self, key: &str, cb: F) -> bool
    where
        F: FnOnce(Option<&mut Value>),
    {
        let mut kvs_guard = self.kvs.lock().unwrap();

        let mut cb_arg: Option<&mut Value> = None;
        let mut del = false;
        let mut not_exist = false;

        if let Some(val) = kvs_guard.get_mut(key) {
            match val.expire_at {
                Some(exp) => {
                    if exp > Instant::now() {
                        cb_arg = Some(val);
                    } else {
                        del = true;
                    }
                }
                None => {
                    cb_arg = Some(val);
                    not_exist = true;
                }
            }
        }
        cb(cb_arg);

        if del {
            kvs_guard.remove(key);
        }

        return if del || not_exist { true } else { false };
    }
}

struct ResponseWriter<'a> {
    writer: BufWriter<&'a TcpStream>,
}

impl<'a> ResponseWriter<'a> {
    fn new(stream: &'a TcpStream) -> ResponseWriter<'a> {
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

    fn write_integer(&mut self, num: i64) {
        let num_str = format!(":{}\r\n", num);
        self.writer.write_all(num_str.as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }
}

struct Request<'a> {
    buf_reader: BufReader<&'a TcpStream>,
    buf: String,
    arg_cnt: usize,

    resp_writer: ResponseWriter<'a>,

    kvs: KV,
}

impl<'a> Request<'a> {
    fn new(stream: &'a TcpStream, kvs: KV) -> Request<'a> {
        Request {
            buf_reader: BufReader::new(&stream),
            buf: String::new(),
            arg_cnt: 0,
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
    }

    fn parse(&mut self) {
        loop {
            let n = self.read_line();
            if n == 0 {
                println!("connection closed by client");
                return;
            }

            assert!(self.buf.starts_with('*'));
            self.arg_cnt = self.buf[1..].trim().parse().unwrap();
            self.arg_cnt -= 1;

            self.parse_command();
        }
    }

    fn parse_set_arg(&mut self, val: &mut Value) {
        let mut i = 0;
        println!("arg cnt: {}", self.arg_cnt);
        while i < self.arg_cnt {
            println!("{} arg", i);
            self.read_bulk_string();
            i += 1;
            let arg = self.buf.trim().to_uppercase();
            println!("parse set arg: {}", arg);

            match arg.as_str() {
                "PX" => {
                    self.read_bulk_string();
                    i += 1;
                    let mills: u64 = self.buf.trim().parse().unwrap();
                    val.expire_at = Some(Instant::now() + Duration::from_millis(mills));
                }
                _ => {
                    panic!("unknown set arg: {}", arg);
                }
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
                self.arg_cnt -= 1;
                let val = &self.buf.trim().to_string();
                self.resp_writer.write_bulk_string(val.as_str());
            }
            "SET" => {
                self.read_bulk_string();
                self.arg_cnt -= 1;
                let key = self.buf.trim().to_string();

                self.read_bulk_string();
                self.arg_cnt -= 1;
                let mut val = Value::new(self.buf.trim().to_string(), None);

                if self.arg_cnt > 0 {
                    self.parse_set_arg(&mut val);
                }

                self.kvs.set(key, val);

                self.resp_writer.write_simple_string("OK");
            }
            "GET" => {
                self.read_bulk_string();
                self.arg_cnt -= 1;
                let key = self.buf.trim();

                let resp_writer = &mut self.resp_writer;

                let get_cb = move |val: Option<&mut Value>| {
                    if let Some(_val) = val {
                        resp_writer.write_bulk_string(_val.val.as_str());
                    } else {
                        resp_writer.write_null_bulk_string();
                    }
                };

                self.kvs.get_del(key, get_cb);
            }
            "INCR" => {
                self.read_bulk_string();
                self.arg_cnt -= 1;
                let key = self.buf.trim();

                let resp_writer = &mut self.resp_writer;

                let incr_cb = move |val: Option<&mut Value>| {
                    if let Some(_val) = val {
                        let mut num: i64 = _val.val.parse().unwrap();
                        num += 1;
                        _val.val = format!("{}", num);
                        resp_writer.write_integer(num);
                    }
                };

                if !self.kvs.get_del(key, incr_cb) {
                    self.kvs
                        .set(key.to_string(), Value::new("1".to_string(), None));

                    self.resp_writer.write_integer(1);
                }
            }
            _ => {
                panic!("read unknown command: {}", &self.buf);
            }
        }
    }
}

fn handle_connection(stream: TcpStream, kv: KV) {
    println!("accepted new connection");

    let mut req = Request::new(&stream, kv);
    req.parse();
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let mut handles = vec![];
    let kv = KV::new();

    for stream in listener.incoming() {
        let kv = kv.clone();

        match stream {
            Ok(_stream) => {
                handles.push(thread::spawn(move || {
                    handle_connection(_stream, kv);
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
