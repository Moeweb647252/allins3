//! `allins3` server entry point.
//!
//! Usage:
//!
//! ```text
//! allins3 [config-path]              # validate, then serve
//! allins3 check-config [config-path] # validate only, exit 0/2
//! ```
//!
//! The config path may also come from `ALLINS3_CONFIG`.

const USAGE: &str = "usage: allins3 [check-config] [config-path]";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (check_only, path) = match args.as_slice() {
        [] => (false, default_path()),
        [flag] if flag == "check-config" => (true, default_path()),
        [flag, path] if flag == "check-config" => (true, path.clone()),
        [path] if path.starts_with('-') => {
            eprintln!("allins3: unknown option `{path}`\n{USAGE}");
            std::process::exit(2);
        }
        [path] => (false, path.clone()),
        _ => {
            eprintln!("allins3: too many arguments\n{USAGE}");
            std::process::exit(2);
        }
    };

    let config = match allins3::config::AppConfig::load(&path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("allins3: configuration error: {e}");
            std::process::exit(2);
        }
    };

    // `check-config` proves the file parses, every bucket binds to a namespace
    // a driver supports, and every kind is one this build registers — without
    // opening a listener. The kind check is part of the preflight so a config
    // that can never start fails here, not at `run`.
    if check_only {
        if let Err(e) = allins3::server::preflight(&config).await {
            eprintln!("allins3: configuration error: {e}");
            std::process::exit(2);
        }
        eprintln!("allins3: config ok ({})", config.redacted_summary());
        return;
    }

    if let Err(e) = allins3::server::run(config).await {
        eprintln!("allins3: fatal: {e}");
        std::process::exit(1);
    }
}

fn default_path() -> String {
    std::env::var("ALLINS3_CONFIG").unwrap_or_else(|_| "allins3.toml".to_string())
}
