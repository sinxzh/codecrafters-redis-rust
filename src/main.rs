use std::io::{prelude::*, BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::thread;

#[derive(Debug)]
enum Command {
    NULL,
    PING,
    ECHO,
}

struct Element {
    cmd: Command,
    key: Option<String>,
    val: Option<String>,
}

impl Element {
    fn new() -> Element {
        Element {
            cmd: Command::NULL,
            key: None,
            val: None,
        }
    }
}

struct Request<'a> {
    cnt: usize,
    eles: Vec<Element>,

    buf_reader: BufReader<&'a TcpStream>,
    buf_writer: BufWriter<&'a TcpStream>,
    buf: String,
}

impl<'a> Request<'a> {
    fn new(stream: &'a TcpStream) -> Request<'a> {
        Request {
            cnt: 0,
            eles: Vec::new(),
            buf_reader: BufReader::new(&stream),
            buf_writer: BufWriter::new(&stream),
            buf: String::new(),
        }
    }

    fn write_simple_string(&mut self, content: &str) {
        self.buf_writer.write_all(b"+").unwrap();
        self.buf_writer.write_all(content.as_bytes()).unwrap();
        self.buf_writer.write_all(b"\r\n").unwrap();
        self.buf_writer.flush().unwrap();
    }

    fn write_bulk_string(&mut self) {
        let str_len = format!("${}\r\n", self.buf.trim().len());
        self.buf_writer.write_all(str_len.as_bytes()).unwrap();

        self.buf_writer.write_all(self.buf.as_bytes()).unwrap();
        self.buf_writer.flush().unwrap();
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

    fn read_command(&mut self) {
        assert!(self.read_line() > 0);
        assert!(self.buf.starts_with('$'));

        assert!(self.read_line() > 0);

        let mut ele = Element::new();
        match self.buf.trim().to_uppercase().as_str() {
            "PING" => {
                ele.cmd = Command::PING;
            }
            "ECHO" => {
                ele.cmd = Command::ECHO;

                self.read_bulk_string();
                ele.val = Some(self.buf.trim().to_string());
            }
            _ => {
                panic!("read unknown command: {}", &self.buf);
            }
        }

        self.eles.push(ele);
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

            let mut cmd_idx = 0;
            while self.cnt > 0 {
                self.read_command();
                self.parse_command(cmd_idx);
                cmd_idx += 1;
            }
        }
    }

    fn parse_command(&mut self, idx: usize) {
        let ele = &self.eles[idx];
        println!("parse command: {:?}", ele.cmd);

        match ele.cmd {
            Command::PING => {
                self.write_simple_string("PONG");
            }
            Command::ECHO => {
                self.write_bulk_string();
            }
            _ => {
                panic!("parse unknown command");
            }
        }
    }
}

fn handle_connection(stream: TcpStream) {
    println!("accepted new connection");

    let mut req = Request::new(&stream);
    req.parse();
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
