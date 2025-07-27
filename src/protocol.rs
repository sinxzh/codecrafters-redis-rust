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
    ArrayHeader(usize),
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

    fn write(&mut self, resp_type: ResponseType) {
        let buffer = &mut self.buffer;
        match resp_type {
            ResponseType::SimpleString(content) => {
                buffer.push_str(format!("+{}\r\n", content).as_str());
            }
            ResponseType::BulkString(content) => {
                buffer.push_str(format!("${}\r\n{}\r\n", content.len(), content).as_str());
            }
            ResponseType::NullBulkString => {
                buffer.push_str("$-1\r\n");
            }
            ResponseType::Integer(num) => {
                buffer.push_str(format!(":{}\r\n", num).as_str());
            }
            ResponseType::SimpleError(content) => {
                buffer.push_str(format!("-{}\r\n", content).as_str());
            }
            ResponseType::ArrayHeader(cnt) => {
                buffer.push_str(format!("*{}\r\n", cnt).as_str());
            }
        }
    }

    fn send(&mut self) -> Result<(), Error> {
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
                "COMMAND" | "PING" | "ECHO" | "SET" | "GET" | "INCR" | "EXEC" | "DISCARD" => {
                    self.exec_command(command, kv_store)?;
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
                    self.exec_command(command, kv_store)?; // Write array header
                    if let Some(commands) = &self.commands.take() {
                        for command in commands {
                            self.exec_command(command, kv_store)?;
                        }
                    }

                    self.state = ResponseState::Exec;
                }
                "DISCARD" => {
                    self.queue_command(command)?;

                    self.state = ResponseState::Exec;
                }
                _ => self.queue_command(command)?,
            },
        }

        Ok(())
    }

    fn queue_command(&mut self, command: &Command) -> Result<(), Error> {
        println!("State: {:?} | queue: {:?}", self.state, command);

        match command.name.as_str() {
            "DISCARD" => {
                self.commands = None;
                self.write(ResponseType::SimpleString("OK"));
            }
            _ => {
                if let Some(commands) = &mut self.commands {
                    commands.push(command.clone());
                    self.write(ResponseType::SimpleString("QUEUED"));
                } else {
                    return Err(Error::msg("Commands list not initialized"));
                }
            }
        }

        self.send()?;
        Ok(())
    }

    fn exec_command(&mut self, command: &Command, kv_store: &mut KvStore) -> Result<(), Error> {
        println!("State: {:?} | exec: {:?}", self.state, command);

        match command.name.as_str() {
            "COMMAND" => {
                self.write(ResponseType::ArrayHeader(0));
            }
            "PING" => {
                self.write(ResponseType::SimpleString("PONG"));
            }
            "ECHO" => {
                if command.args.is_empty() {
                    self.write(ResponseType::SimpleError(
                        "ERR wrong number of arguments for 'echo' command",
                    ));
                } else {
                    self.write(ResponseType::BulkString(&command.args[0]));
                }
            }
            "SET" => {
                if command.args.len() < 2 {
                    self.write(ResponseType::SimpleError(
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
                    self.write(resp);
                }
            }
            "GET" => {
                if command.args.is_empty() {
                    self.write(ResponseType::SimpleError(
                        "ERR wrong number of arguments for 'get' command",
                    ));
                } else {
                    let key = &command.args[0];

                    let get_action = |_: &str, item: Option<&mut KvItem>| {
                        if let Some(item) = item {
                            self.write(ResponseType::BulkString(&item.val));
                        } else {
                            self.write(ResponseType::NullBulkString);
                        }
                    };

                    kv_store.do_action(key, get_action);
                }
            }
            "INCR" => {
                if command.args.is_empty() {
                    self.write(ResponseType::SimpleError(
                        "ERR wrong number of arguments for 'incr' command",
                    ));
                } else {
                    let key = &command.args[0];

                    let mut incr_result = Ok(0);
                    {
                        let incr_action = |_: &str, item: Option<&mut KvItem>| {
                            if let Some(item) = item {
                                if let Ok(mut num) = item.val.parse::<i64>() {
                                    num += 1;
                                    incr_result = Ok(num);
                                } else {
                                    incr_result = Err(Error::msg(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            } else {
                                incr_result = Ok(1);
                            }
                        };
                        kv_store.do_action(key, incr_action);
                    }

                    match incr_result {
                        Ok(num) => {
                            kv_store.insert(key.clone(), KvItem::new(num.to_string(), None));
                            self.write(ResponseType::Integer(num));
                        }
                        Err(e) => {
                            self.write(ResponseType::SimpleError(e.to_string().as_str()));
                        }
                    }
                }
            }
            "MULTI" => {
                self.write(ResponseType::SimpleString("OK"));
            }
            "EXEC" => match self.state {
                ResponseState::Exec => {
                    self.write(ResponseType::SimpleError("ERR EXEC without MULTI"));
                }
                ResponseState::Queue => {
                    if let Some(commands) = &self.commands {
                        self.write(ResponseType::ArrayHeader(commands.len()));
                    } else {
                        self.write(ResponseType::ArrayHeader(0));
                    }
                }
            },
            "DISCARD" => {
                self.write(ResponseType::SimpleError("ERR DISCARD without MULTI"));
            }
            _ => {
                self.write(ResponseType::SimpleError("ERR unknown command"));
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
