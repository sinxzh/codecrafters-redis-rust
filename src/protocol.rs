use std::io::{BufReader, BufWriter, prelude::*};
use std::net::TcpStream;

use anyhow::Error;

use crate::kv_store::{KvItem, KvStore};

pub struct Request<'a> {
    reader: BufReader<&'a TcpStream>,
    buffer: String,
    pub command: Command,
}

impl<'a> Request<'a> {
    pub fn new(stream: &'a TcpStream) -> Request<'a> {
        Request {
            reader: BufReader::new(stream),
            buffer: String::new(),
            command: Command::new(String::new(), Vec::new()),
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

        self.command = Command::from_str(&self.buffer);
        println!("command: {:?}", self.command);

        Ok(())
    }
}

pub struct Response<'a> {
    writer: BufWriter<&'a TcpStream>,
    buffer: String,
    state: ResponseState,
    commands: Option<Vec<Command>>,
}

enum ResponseType<'a> {
    SimpleString(&'a str),
    BulkString(&'a str),
    NullBulkString,
    Integer(i64),
    SimpleError(&'a str),
    EmptyArray,
}

#[derive(Debug, PartialEq)]
enum ResponseState {
    Exec,
    Queue,
}

impl<'a> Response<'a> {
    pub fn new(stream: &'a TcpStream) -> Response<'a> {
        Response {
            writer: BufWriter::new(stream),
            buffer: String::new(),
            state: ResponseState::Exec,
            commands: None,
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

    pub fn push_empty_array(&mut self) {
        self.buffer.push_str("*0\r\n");
    }

    pub fn send(&mut self) -> Result<(), Error> {
        self.writer.write_all(self.buffer.as_bytes())?;
        self.writer.flush()?;
        self.buffer.clear();
        Ok(())
    }

    pub fn process_command(
        &mut self,
        command: &Command,
        kv_store: &mut KvStore,
    ) -> Result<(), Error> {
        println!("State: {:?} | process: {:?}", self.state, command);

        match self.state {
            ResponseState::Exec => match command.name.as_str() {
                "COMMAND" | "PING" | "ECHO" | "SET" | "GET" | "INCR" | "EXEC" => {
                    self.exec_command(command, kv_store)?
                }
                "MULTI" => {
                    self.commands = Some(Vec::new());
                    self.exec_command(command, kv_store)?;
                    self.state = ResponseState::Queue;
                }
                _ => {
                    return Err(Error::msg(format!("Unknown command: {}", command.name)));
                }
            },
            ResponseState::Queue => match command.name.as_str() {
                "EXEC" => {
                    if let Some(commands) = &self.commands.take()
                        && !commands.is_empty()
                    {
                        for command in commands {
                            self.exec_command(command, kv_store)?;
                        }
                    } else {
                        self.exec_command(command, kv_store)?;
                    }
                    self.state = ResponseState::Exec;
                }
                _ => self.queue_command(command)?,
            },
        }

        Ok(())
    }

    fn queue_command(&mut self, command: &Command) -> Result<(), Error> {
        println!("State: {:?} | queue: {:?}", self.state, command);

        if let Some(commands) = &mut self.commands {
            commands.push(command.clone());
        } else {
            return Err(Error::msg("Commands list not initialized"));
        }

        self.push_simple_string("QUEUED");
        self.send()?;
        Ok(())
    }

    fn exec_command(&mut self, command: &Command, kv_store: &mut KvStore) -> Result<(), Error> {
        println!("State: {:?} | exec: {:?}", self.state, command);

        let mut resps = Vec::new();

        match command.name.as_str() {
            "COMMAND" => {
                resps.push(ResponseType::EmptyArray);
            }
            "PING" => {
                resps.push(ResponseType::SimpleString("PONG"));
            }
            "ECHO" => {
                if command.args.is_empty() {
                    resps.push(ResponseType::SimpleError(
                        "ERR wrong number of arguments for 'echo' command",
                    ));
                } else {
                    resps.push(ResponseType::BulkString(&command.args[0]));
                }
            }
            "SET" => {
                if command.args.len() < 2 {
                    resps.push(ResponseType::SimpleError(
                        "ERR wrong number of arguments for 'set' command",
                    ));
                } else {
                    let key = &command.args[0];
                    let val = &command.args[1];
                    let mut resp = ResponseType::SimpleString("OK");
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
                                    resp = ResponseType::SimpleError("ERR invalid expire time");
                                }
                            }
                            _ => {
                                resp = ResponseType::SimpleError(
                                    "ERR unknown option for 'set' command",
                                );
                            }
                        }
                    } else {
                        kv_store.insert(key.clone(), KvItem::new(val.clone(), None));
                    }
                    resps.push(resp);
                }
            }
            "GET" => {
                if command.args.is_empty() {
                    resps.push(ResponseType::SimpleError(
                        "ERR wrong number of arguments for 'get' command",
                    ));
                } else {
                    let key = &command.args[0];
                    if let Some(item) = kv_store.get_clone(key) {
                        // the item is not lived enough, so we need to send it right now
                        self.push_bulk_string(&item.val);
                        self.send()?;
                        return Ok(());
                    } else {
                        resps.push(ResponseType::NullBulkString);
                    }
                }
            }
            "INCR" => {
                if command.args.is_empty() {
                    resps.push(ResponseType::SimpleError(
                        "ERR wrong number of arguments for 'incr' command",
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
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    } else {
                        kv_store.insert(key.clone(), KvItem::new("1".to_string(), None));
                        resps.push(ResponseType::Integer(1));
                    }
                }
            }
            "MULTI" => {
                resps.push(ResponseType::SimpleString("OK"));
            }
            "EXEC" => match self.state {
                ResponseState::Exec => {
                    resps.push(ResponseType::SimpleError("ERR EXEC without MULTI"));
                }
                ResponseState::Queue => {
                    resps.push(ResponseType::EmptyArray);
                }
            },
            _ => {
                resps.push(ResponseType::SimpleError("ERR unknown command"));
            }
        }

        for resp in resps {
            match resp {
                ResponseType::SimpleString(content) => self.push_simple_string(&content),
                ResponseType::BulkString(content) => self.push_bulk_string(&content),
                ResponseType::NullBulkString => self.push_null_bulk_string(),
                ResponseType::Integer(num) => self.push_integer(num),
                ResponseType::SimpleError(content) => self.push_simple_error(&content),
                ResponseType::EmptyArray => self.push_empty_array(),
            }
        }

        self.send()?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
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
        let name = parts[1].to_string().to_uppercase();
        let args: Vec<String> = parts[2..].iter().map(|&s| s.to_string()).collect();
        Command::new(name, args)
    }
}
