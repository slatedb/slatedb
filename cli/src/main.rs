use async_trait::async_trait;
use clap::Parser;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use shell_words::split;
use slatedb::config::{CompactorOptions, DbOptions, ObjectStoreCacheOptions};
use slatedb::db::Db;
use slatedb::inmemory_cache::InMemoryCacheOptions;
use std::error::Error;
use std::{sync::Arc, time::Duration};
use tokio;

#[derive(Parser, Debug)]
#[command(name = "SlateDB REPL and CLI")]
#[command(version = "0.1.0")]
#[command(about = "A command-line interface for SlateDB", long_about = None)]
struct Args {
    /// Path to the database storage directory
    #[arg(short, long, value_name = "PATH", default_value = "/tmp/test_kv_store")]
    storage_path: String,
}

struct DbAdapter {
    db: Db,
}

impl DbAdapter {
    async fn new(storage_path: &str) -> Result<Self, Box<dyn Error>> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions {
            flush_interval: Duration::from_millis(100),
            manifest_poll_interval: Duration::from_millis(100),
            min_filter_keys: 10,
            l0_sst_size_bytes: 128,
            l0_max_ssts: 8,
            max_unflushed_memtable: 2,
            compactor_options: Some(CompactorOptions::default()),
            compression_codec: None,
            block_cache_options: Some(InMemoryCacheOptions::default()),
            object_store_cache_options: ObjectStoreCacheOptions::default(),
        };
        let db = Db::open_with_opts(Path::from(storage_path), options, object_store).await?;
        Ok(Self { db })
    }

    async fn close(&self) -> Result<(), Box<dyn Error>> {
        self.db.close().await?;
        Ok(())
    }
}

#[async_trait]
trait CliCommand {
    fn usage(&self) -> &str;
    fn validate(&self, args: &Vec<String>) -> bool;
    async fn execute(
        &self,
        db_adapter: &DbAdapter,
        args: &Vec<String>,
    ) -> Result<(), Box<dyn Error>>;
}

struct GetCommand {}

#[async_trait]
impl CliCommand for GetCommand {
    fn usage(&self) -> &str {
        "get <key>"
    }

    fn validate(&self, args: &Vec<String>) -> bool {
        args.len() == 2
    }

    async fn execute(
        &self,
        db_adapter: &DbAdapter,
        args: &Vec<String>,
    ) -> Result<(), Box<dyn Error>> {
        match db_adapter.db.get(args[1].as_bytes()).await? {
            None => {
                println!("Key not found")
            }
            Some(bytes) => {
                let value = String::from_utf8(bytes.to_vec())?;
                println!("{}", value);
            }
        }
        Ok(())
    }
}

struct PutCommand {}

#[async_trait]
impl CliCommand for PutCommand {
    fn usage(&self) -> &str {
        "put <key> <value>"
    }

    fn validate(&self, args: &Vec<String>) -> bool {
        args.len() == 3
    }

    async fn execute(
        &self,
        db_adapter: &DbAdapter,
        args: &Vec<String>,
    ) -> Result<(), Box<dyn Error>> {
        db_adapter
            .db
            .put(args[1].as_bytes(), args[2].as_bytes())
            .await;
        println!("Successfully inserted key-value pair.");
        Ok(())
    }
}

struct DeleteCommand {}

#[async_trait]
impl CliCommand for DeleteCommand {
    fn usage(&self) -> &str {
        "delete <key>"
    }

    fn validate(&self, args: &Vec<String>) -> bool {
        args.len() == 2
    }

    async fn execute(
        &self,
        db_adapter: &DbAdapter,
        args: &Vec<String>,
    ) -> Result<(), Box<dyn Error>> {
        db_adapter.db.delete(args[1].as_bytes()).await;
        println!("Successfully removed key.");
        Ok(())
    }
}

struct HelpCommand {}

#[async_trait]
impl CliCommand for HelpCommand {
    fn usage(&self) -> &str {
        r#"Available commands:
    put <key> <value>           - Insert a key-value pair into SlateDB
    get <key>                   - Retrieve the value associated with <key>
    delete <key>                - Delete <key> for SlateDB
    help                        - Display this help message
    exit | quit                 - Exit
"#
    }

    fn validate(&self, _: &Vec<String>) -> bool {
        true
    }

    async fn execute(&self, _: &DbAdapter, _: &Vec<String>) -> Result<(), Box<dyn Error>> {
        println!("{}", self.usage());
        Ok(())
    }
}

struct UnknownCommand {}

#[async_trait]
impl CliCommand for UnknownCommand {
    fn usage(&self) -> &str {
        ""
    }

    fn validate(&self, _: &Vec<String>) -> bool {
        true
    }

    async fn execute(&self, _: &DbAdapter, args: &Vec<String>) -> Result<(), Box<dyn Error>> {
        println!(
            "Unknown command '{}'. Type 'help' to see available commands.",
            args[0]
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), ReadlineError> {
    let args = Args::parse();

    let db_adapter = match DbAdapter::new(&args.storage_path).await {
        Ok(cli) => cli,
        Err(e) => {
            eprintln!("Failed to initialize the database: {}", e);
            std::process::exit(1);
        }
    };

    // Initialize the REPL
    let mut rl = DefaultEditor::new()?;

    println!("Welcome to the SlateDB REPL!");
    println!("WARNING: This is currently intended for development use.");
    println!(
        r#"
  ____  _       _       ____  ____
 / ___|| | __ _| |_ ___|  _ \| __ )
 \___ \| |/ _` | __/ _ \ | | |  _ \
  ___) | | (_| | ||  __/ |_| | |_) |
 |____/|_|\__,_|\__\___|____/|____/
"#
    );
    println!("Type 'help' to see available commands.\n");

    loop {
        // Prompt the user for input
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                if let Err(e) = rl.add_history_entry(&line) {
                    eprintln!("Could not save line to history: {}", e)
                }

                let trimmed = line.trim();
                let parts = match split(trimmed) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Error parsing input: {}", e);
                        continue;
                    }
                };

                if parts.is_empty() {
                    continue;
                }

                let command = parts[0].to_lowercase();
                let cli_cmd: Box<dyn CliCommand> = match command.as_str() {
                    "put" => Box::new(PutCommand {}),
                    "get" => Box::new(GetCommand {}),
                    "delete" => Box::new(DeleteCommand {}),
                    "help" => Box::new(HelpCommand {}),
                    "exit" | "quit" => {
                        break;
                    }
                    _ => Box::new(UnknownCommand {}),
                };

                if !cli_cmd.validate(&parts) {
                    println!("{}", cli_cmd.usage());
                } else {
                    if let Err(e) = cli_cmd.execute(&db_adapter, &parts).await {
                        eprintln!("Failed to execute command {}: {}", trimmed, e)
                    };
                }
            }

            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                eprintln!("Error reading line: {}", err);
                break;
            }
        }
    }

    println!("Exiting REPL. Goodbye!");
    // Close the database before exiting
    if let Err(e) = db_adapter.close().await {
        eprintln!("Error closing the database: {}", e);
    }
    Ok(())
}
