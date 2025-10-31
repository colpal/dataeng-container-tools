use clap::Parser;

mod args;
mod docker;

use args::{Args, Command};

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let result = match args.command {
        Command::Build { path } => {
            docker::build_image(&path, args.global_options.verbose).await.map(|_| ())
        }
        Command::Test { path, bash } => {
            docker::test_container(&path, bash, args.global_options.verbose).await
        }
    };
    
    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
