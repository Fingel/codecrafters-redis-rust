use crate::parser::RedisValueRef;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum RedisCommand {
    Ping,
    Echo(RedisValueRef),
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
            Ok(RedisCommand::Echo(args[1].clone()))
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

        assert_eq!(
            command,
            RedisCommand::Echo(RedisValueRef::String(Bytes::from("Hello")))
        );
    }
}
