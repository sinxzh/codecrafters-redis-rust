use std::io::{BufReader, BufWriter, prelude::*};
use std::net::TcpStream;

use anyhow::Error;

use crate::kv_store::{KvItem, KvStore};

pub struct Request<'a> {
    reader: BufReader<&'a TcpStream>,
    buffer: String,
    pub commands: Vec<Command>,
}

impl<'a> Request<'a> {
    pub fn new(stream: &'a TcpStream) -> Request<'a> {
        Request {
            reader: BufReader::new(stream),
            buffer: String::new(),
            commands: Vec::new(),
        }
    }

    pub fn read_command(&mut self) -> Result<(), Error> {
        self.buffer.clear();

        let n = self.reader.read_line(&mut self.buffer)?;
        if n == 0 {
            return Err(Error::msg("Connection closed by client"));
        }

        let line_cnt = self.buffer.trim_end()[1..].parse::<usize>()?;
        for _ in 0..line_cnt {
            let mut line = String::new();
            let n = self.reader.read_line(&mut line)?;
            if n == 0 {
                return Err(Error::msg("Connection closed by client"));
            }

            if line.starts_with('$') {
                let len = line[1..].trim_end().parse::<usize>()?;
                if len == 0 {
                    continue;
                }
                let n = self.reader.read_line(&mut self.buffer)?;
                if n == 0 {
                    return Err(Error::msg("Connection closed by client"));
                }
            } else {
                todo!("Unsupported command format: {}", line);
            }
        }
        println!("read command: [{}]", self.buffer.trim_end());

        let command = Command::from_str(&self.buffer);
        if command.name == "MULTI" && self.commands.len() == 0 {
            todo!("delete");
            self.commands.push(command);
            self.read_multi_commands()?
        } else {
            self.commands.push(command)
        }

        println!("commands: {:?}", self.commands);

        Ok(())
    }

    fn read_multi_commands(&mut self) -> Result<(), Error> {
        todo!("delete");
        while let Ok(()) = self.read_command() {
            if let Some(command) = self.commands.last()
                && command.name == "EXEC"
            {
                return Ok(());
            }
        }
        Err(Error::msg("Unexpected end of MULTI command sequence"))
    }
}

pub struct Response<'a> {
    writer: BufWriter<&'a TcpStream>,
    buffer: String,
}

enum ResponseType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    SimpleError(String),
}

impl<'a> Response<'a> {
    pub fn new(stream: &'a TcpStream) -> Response<'a> {
        Response {
            writer: BufWriter::new(stream),
            buffer: String::new(),
        }
    }

    pub fn push_simple_string(&mut self, content: &str) {
        self.buffer.push('+');
        self.buffer.push_str(content);
        self.buffer.push_str("\r\n");
    }

    pub fn push_bulk_string(&mut self, content: &str) {
        self.buffer.push('$');
        self.buffer.push_str(&content.len().to_string());
        self.buffer.push_str("\r\n");
        self.buffer.push_str(content);
        self.buffer.push_str("\r\n");
    }

    pub fn push_null_bulk_string(&mut self) {
        self.buffer.push_str("$-1\r\n");
    }

    pub fn push_integer(&mut self, num: i64) {
        self.buffer.push(':');
        self.buffer.push_str(&num.to_string());
        self.buffer.push_str("\r\n");
    }

    pub fn push_simple_error(&mut self, content: &str) {
        self.buffer.push('-');
        self.buffer.push_str(content);
        self.buffer.push_str("\r\n");
    }

    pub fn send(&mut self) -> Result<(), Error> {
        self.writer.write_all(self.buffer.as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn exec_command(
        &mut self,
        commands: &Vec<Command>,
        kv_store: &mut KvStore,
    ) -> Result<(), Error> {
        let mut resps = Vec::new();

        for command in commands.iter() {
            match command.name.as_str() {
                "COMMAND" => {
                    resps.push(ResponseType::SimpleString("".to_string()));
                }
                "PING" => {
                    resps.push(ResponseType::SimpleString("PONG".to_string()));
                }
                "ECHO" => {
                    if command.args.is_empty() {
                        resps.push(ResponseType::SimpleError(
                            "ERR wrong number of arguments for 'echo' command".to_string(),
                        ));
                    } else {
                        resps.push(ResponseType::BulkString(command.args[0].clone()));
                    }
                }
                "SET" => {
                    if command.args.len() < 2 {
                        resps.push(ResponseType::SimpleError(
                            "ERR wrong number of arguments for 'set' command".to_string(),
                        ));
                    } else {
                        let key = &command.args[0];
                        let val = &command.args[1];
                        if command.args.len() > 2 {
                            match command.args[2].to_uppercase().as_str() {
                                "PX" => {
                                    if let Some(expire_mills) =
                                        command.args.get(3).and_then(|s| s.parse::<u64>().ok())
                                    {
                                        kv_store.insert(
                                            key.clone(),
                                            KvItem::new(val.clone(), Some(expire_mills)),
                                        );
                                    } else {
                                        resps.push(ResponseType::SimpleError(
                                            "ERR invalid expire time".to_string(),
                                        ));
                                        continue;
                                    }
                                }
                                _ => {
                                    resps.push(ResponseType::SimpleError(
                                        "ERR unknown option for 'set' command".to_string(),
                                    ));
                                    continue;
                                }
                            }
                        } else {
                            kv_store.insert(key.clone(), KvItem::new(val.clone(), None));
                        }
                        resps.push(ResponseType::SimpleString("OK".to_string()));
                    }
                }
                "GET" => {
                    if command.args.is_empty() {
                        resps.push(ResponseType::SimpleError(
                            "ERR wrong number of arguments for 'get' command".to_string(),
                        ));
                    } else {
                        let key = &command.args[0];
                        if let Some(item) = kv_store.get_clone(key) {
                            resps.push(ResponseType::BulkString(item.val));
                        } else {
                            resps.push(ResponseType::NullBulkString);
                        }
                    }
                }
                "INCR" => {
                    if command.args.is_empty() {
                        resps.push(ResponseType::SimpleError(
                            "ERR wrong number of arguments for 'incr' command".to_string(),
                        ));
                    } else {
                        let key = &command.args[0];
                        if let Some(item) = kv_store.get_clone(key) {
                            if let Ok(mut num) = item.val.parse::<i64>() {
                                num += 1;
                                kv_store.insert(key.clone(), KvItem::new(num.to_string(), None));
                                resps.push(ResponseType::Integer(num));
                            } else {
                                resps.push(ResponseType::SimpleError(
                                    "ERR value is not an integer".to_string(),
                                ));
                            }
                        } else {
                            kv_store.insert(key.clone(), KvItem::new("1".to_string(), None));
                            resps.push(ResponseType::Integer(1));
                        }
                    }
                }
                "MULTI" => {
                    resps.push(ResponseType::SimpleString("OK".to_string()));
                    todo!("Handle MULTI command");
                }
                "EXEC" => {
                    resps.push(ResponseType::SimpleError(
                        "ERR EXEC without MULTI".to_string(),
                    ));
                }
                _ => {
                    resps.push(ResponseType::SimpleError("ERR unknown command".to_string()));
                }
            }
        }

        for resp in resps {
            match resp {
                ResponseType::SimpleString(content) => self.push_simple_string(&content),
                ResponseType::BulkString(content) => self.push_bulk_string(&content),
                ResponseType::NullBulkString => self.push_null_bulk_string(),
                ResponseType::Integer(num) => self.push_integer(num),
                ResponseType::SimpleError(content) => self.push_simple_error(&content),
            }
        }

        self.send()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Command {
    name: String,
    args: Vec<String>,
}

impl Command {
    fn new(name: String, args: Vec<String>) -> Command {
        Command { name, args }
    }

    fn from_str(command_str: &str) -> Command {
        let parts: Vec<&str> = command_str.trim().split_whitespace().collect();
        let name = parts[1].to_string();
        let args: Vec<String> = parts[2..].iter().map(|&s| s.to_string()).collect();
        Command::new(name, args)
    }
}
