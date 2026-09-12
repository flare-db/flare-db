/// Built-in REPL commands that are handled locally, never forwarded to SQLContext.
#[derive(Debug, PartialEq)]
pub enum Command {
    /// Display help text and continue the REPL.
    Help,
    /// Exit the REPL cleanly.
    Exit,
}

/// Recognise a built-in command from the trimmed input line.
///
/// Returns `None` if the line should be treated as SQL.
pub fn parse(input: &str) -> Option<Command> {
    let trimmed = input.trim();
    match trimmed.to_lowercase().as_str() {
        "help" => Some(Command::Help),
        "exit" | "quit" => Some(Command::Exit),
        _ => None,
    }
}

/// Print the help banner.
pub fn help_text() -> &'static str {
    r#"FlareDB Interactive SQL Shell

Commands:
  help                 Show this help message
  exit, quit           Exit the shell

SQL:
  Any valid Apache DataFusion SQL statement, terminated with a semicolon (;).

Examples:
  SHOW DATABASES;
  USE default;
  SHOW TABLES;
  CREATE TABLE users (id INT, name STRING);
  INSERT INTO users VALUES (1, 'Alice');
  SELECT * FROM users;
"#
}
