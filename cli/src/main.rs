use clap::Parser;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use shell_words::split;
use slatedb::config::{CompactorOptions, DbOptions, ObjectStoreCacheOptions};
use slatedb::db::Db;
use slatedb::inmemory_cache::InMemoryCacheOptions;
use std::{sync::Arc, time::Duration};
use tokio;

#[derive(Parser, Debug)]
#[command(name = "SlateDB REPL CLI")]
#[command(version = "1.0")]
#[command(about = "A command-line interface for SlateDB", long_about = None)]
struct Args {
    /// Path to the database storage directory
    #[arg(short, long, value_name = "PATH", default_value = "/tmp/test_kv_store")]
    storage_path: String,
}

// this is the REPL
struct SlateDbREPL {
    db: Db,
}

impl SlateDbREPL {
    /// Initialize the DatabaseCLI with a connected database
    async fn new(storage_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
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
        let db = Db::open_with_opts(
            Path::from(storage_path),
            options,
            object_store,
        )
            .await?;
        Ok(SlateDbREPL { db })
    }

    async fn put(&self, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.db.put(key.as_bytes(), value.as_bytes()).await;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        if let Some(bytes) = self.db.get(key.as_bytes()).await? {
            let value = String::from_utf8(bytes.to_vec())?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.db.delete(key.as_bytes()).await;
        Ok(())
    }

    async fn close(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.db.close().await?;
        Ok(())
    }

    fn help() {
        println!("Available commands:");
        println!("  put <key> <value>    - Insert a key-value pair into SlateDB");
        println!("  get <key>            - Retrieve the value associated with a key");
        println!("  delete <key>         - Delete a key-value pair from SlateDB");
        println!("  help                 - Show this help message");
        println!("  exit | quit          - Exit");
    }
}

#[tokio::main]
async fn main() -> Result<(), ReadlineError> {
    let args = Args::parse();

    let db_cli = match SlateDbREPL::new(&args.storage_path).await {
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
    println!(r#"
  ____  _       _       ____  ____
 / ___|| | __ _| |_ ___|  _ \| __ )
 \___ \| |/ _` | __/ _ \ | | |  _ \
  ___) | | (_| | ||  __/ |_| | |_) |
 |____/|_|\__,_|\__\___|____/|____/
"#);
    println!("Type 'help' to see available commands.\n");

    loop {
        // Prompt the user for input
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match rl.add_history_entry(trimmed) {
                    Ok(_) => {}
                    Err(e) => {eprintln!("Could not save line to history: {}", e)}
                }

                // Parse the input using shell-words to handle quotes
                let parts_result = split(trimmed);
                let parts = match parts_result {
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

                match command.as_str() {
                    "put" => {
                        if parts.len() < 3 {
                            println!("Usage: put <key> <value>");
                            continue;
                        }
                        let key = &parts[1];
                        let value = parts[2..].join(" "); // Support spaces in value
                        match db_cli.put(key, &value).await {
                            Ok(_) => println!("Successfully inserted key-value pair."),
                            Err(e) => eprintln!("Error executing put: {}", e),
                        }
                    }
                    "get" => {
                        if parts.len() != 2 {
                            println!("Usage: get <key>");
                            continue;
                        }
                        let key = &parts[1];
                        match db_cli.get(key).await {
                            Ok(Some(value)) => println!("{}", value),
                            Ok(None) => println!("Key not found."),
                            Err(e) => eprintln!("Error executing get: {}", e),
                        }
                    }
                    "delete" => {
                        if parts.len() != 2 {
                            println!("Usage: delete <key>");
                            continue;
                        }
                        let key = &parts[1];
                        match db_cli.delete(key).await {
                            Ok(_) => println!("Successfully deleted key."),
                            Err(e) => eprintln!("Error executing delete: {}", e),
                        }
                    }
                    "help" => {
                        SlateDbREPL::help();
                    }
                    "exit" | "quit" => {
                        println!("Exiting REPL. Goodbye!");
                        // Close the database before exiting
                        if let Err(e) = db_cli.close().await {
                            eprintln!("Error closing the database: {}", e);
                        }
                        break;
                    }
                    _ => {
                        println!("Unknown command: '{}'. Type 'help' \
                        to see available commands.", command);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                println!("Exiting REPL. Goodbye!");
                if let Err(e) = db_cli.close().await {
                    eprintln!("Error closing the database: {}", e);
                }
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                println!("Exiting REPL. Goodbye!");
                if let Err(e) = db_cli.close().await {
                    eprintln!("Error closing the database: {}", e);
                }
                break;
            }
            Err(err) => {
                eprintln!("Error reading line: {}", err);
                break;
            }
        }
    }

    Ok(())
}
