use clap::Parser;

mod args;
mod docker;
mod args_md;

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
        Command::ArgparseMd { python_file } => {
            args_md::generate_markdown(&python_file, args.global_options.verbose).await
        }
    };
    
    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
