use bytes::Bytes;

use crate::parser::RedisValueRef;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum RedisCommand {
    Ping,
    Echo(Bytes),
    Set(Bytes, Bytes),
    SetEx(Bytes, Bytes, u64),
    Get(Bytes),
    Rpush(Bytes, Vec<Bytes>),
}

#[derive(Debug)]
pub enum CmdError {
    InvalidCommandType,
    InvalidCommand(String),
    InvalidArgument(String),
    InvalidArgumentNum,
}

impl std::error::Error for CmdError {}

impl fmt::Display for CmdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CmdError::InvalidCommandType => {
                write!(f, "commands must start with a string")
            }
            CmdError::InvalidCommand(cmd) => {
                write!(f, "unknown command '{}'", cmd)
            }
            CmdError::InvalidArgument(arg) => {
                write!(f, "invalid argument: {}", arg)
            }
            CmdError::InvalidArgumentNum => {
                write!(f, "wrong number of arguments")
            }
        }
    }
}

#[derive(Default)]
pub struct RedisInterpreter;

impl RedisInterpreter {
    pub fn new() -> Self {
        Self
    }

    pub fn interpret(&self, value: RedisValueRef) -> Result<RedisCommand, CmdError> {
        match value {
            RedisValueRef::Array(args) => {
                if args.is_empty() {
                    return Err(CmdError::InvalidArgumentNum);
                }

                let command = match &args[0] {
                    RedisValueRef::String(cmd_bytes) => {
                        String::from_utf8_lossy(cmd_bytes).to_uppercase()
                    }
                    _ => {
                        return Err(CmdError::InvalidCommandType);
                    }
                };

                match command.as_str() {
                    "PING" => self.ping(),
                    "ECHO" => self.echo(&args),
                    "SET" => self.set(&args),
                    "GET" => self.get(&args),
                    "RPUSH" => self.rpush(&args),
                    _ => Err(CmdError::InvalidCommand(command.to_string())),
                }
            }
            _ => Err(CmdError::InvalidCommandType),
        }
    }

    fn ping(&self) -> Result<RedisCommand, CmdError> {
        Ok(RedisCommand::Ping)
    }

    fn echo(&self, args: &[RedisValueRef]) -> Result<RedisCommand, CmdError> {
        if args.len() < 2 {
            Err(CmdError::InvalidArgumentNum)
        } else {
            Ok(RedisCommand::Echo(Bytes::from(args[1].to_string())))
        }
    }

    fn set(&self, args: &[RedisValueRef]) -> Result<RedisCommand, CmdError> {
        if args.len() < 3 {
            return Err(CmdError::InvalidArgumentNum);
        }
        let key = args[1]
            .clone()
            .as_string()
            .map_err(|_| CmdError::InvalidArgument("key must be a string".into()))?
            .clone();
        let value = args[2]
            .clone()
            .as_string()
            .map_err(|_| CmdError::InvalidArgument("value must be a string".into()))?;
        match args.len() {
            3 => Ok(RedisCommand::Set(key, value)),
            5 => {
                let ttl_string = args[4]
                    .clone()
                    .as_lossy_string()
                    .map_err(|_| CmdError::InvalidArgument("ttl value must be a string".into()))?;
                let ttl_arg = ttl_string
                    .parse::<u64>()
                    .map_err(|_| CmdError::InvalidArgument("Could not parse ttl value".into()))?;
                let ttl_type = args[3]
                    .clone()
                    .as_lossy_string()
                    .map_err(|_| CmdError::InvalidArgument("ttl type must be a string".into()))?;
                let ttl_val = match ttl_type.as_str() {
                    "EX" => ttl_arg * 1000,
                    "PX" => ttl_arg,
                    _ => return Err(CmdError::InvalidArgument(ttl_type)),
                };
                Ok(RedisCommand::SetEx(key, value, ttl_val))
            }
            _ => Err(CmdError::InvalidArgumentNum),
        }
    }

    fn get(&self, args: &[RedisValueRef]) -> Result<RedisCommand, CmdError> {
        if args.len() < 2 {
            Err(CmdError::InvalidArgumentNum)
        } else {
            let key = args[1]
                .clone()
                .as_string()
                .map_err(|_| CmdError::InvalidArgument("key must be a string".into()))?
                .clone();
            Ok(RedisCommand::Get(key))
        }
    }

    fn rpush(&self, args: &[RedisValueRef]) -> Result<RedisCommand, CmdError> {
        if args.len() < 3 {
            Err(CmdError::InvalidArgumentNum)
        } else {
            let key = args[1]
                .clone()
                .as_string()
                .map_err(|_| CmdError::InvalidArgument("key must be a string".into()))?
                .clone();
            let value = args[2]
                .clone()
                .as_string()
                .map_err(|_| CmdError::InvalidArgument("value must be a string".into()))?;
            Ok(RedisCommand::Rpush(key, vec![value]))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_ping() {
        let interpreter = RedisInterpreter::new();
        let command = interpreter
            .interpret(RedisValueRef::Array(vec![RedisValueRef::String(
                Bytes::from("PING"),
            )]))
            .unwrap();

        assert_eq!(command, RedisCommand::Ping);
    }

    #[test]
    fn test_echo() {
        let interpreter = RedisInterpreter::new();
        let command = interpreter
            .interpret(RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("ECHO")),
                RedisValueRef::String(Bytes::from("Hello")),
            ]))
            .unwrap();

        assert_eq!(command, RedisCommand::Echo(Bytes::from("Hello")));
    }
}
