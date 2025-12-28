use crate::parser::RedisValueRef;

pub enum RedisCommand {
    Ping,
    Echo(RedisValueRef),
}

#[derive(Default)]
pub struct RedisInterpreter;

impl RedisInterpreter {
    pub fn new() -> Self {
        Self
    }

    pub fn interpret(&self, value: RedisValueRef) -> Result<RedisCommand, String> {
        match value {
            RedisValueRef::Array(args) => {
                if args.is_empty() {
                    return Err("Empty command array".to_string());
                }

                let command = match &args[0] {
                    RedisValueRef::String(cmd_bytes) => {
                        String::from_utf8_lossy(cmd_bytes).to_uppercase()
                    }
                    _ => return Err("Command must be a string".to_string()),
                };

                match command.as_str() {
                    "PING" => Ok(RedisCommand::Ping),
                    "ECHO" => {
                        if args.len() < 2 {
                            return Err("ECHO command requires an argument".to_string());
                        }
                        Ok(RedisCommand::Echo(args[1].clone()))
                    }
                    _ => Err(format!("Unknown command: {}", command)),
                }
            }
            _ => Err("Invalid command or not implemented".to_string()),
        }
    }
}
