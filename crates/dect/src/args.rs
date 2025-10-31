use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    author,
    name = "dect",
    about = "Data Engineering Container Tools CLI",
    after_help = "For help with a specific command, see: `dect help <command>`."
)]
#[command(version)]
pub struct Args {
    #[command(subcommand)]
    pub(crate) command: Command,
    #[clap(flatten)]
    pub(crate) global_options: GlobalConfigArgs,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(about = "Build a container image for data engineering projects. Uses BuildKit Providerless build.")]
    Build {
        #[arg(default_value = ".", help = "Directory with the Dockerfile to build")]
        path: PathBuf,
    },
    #[command(about = "Build, run, and test a container image (cleans up afterwards)")]
    Test {
        #[arg(default_value = ".", help = "Directory with the Dockerfile to test")]
        path: PathBuf,

        #[arg(long, help = "Start an interactive bash session instead of running the default command")]
        bash: bool,
    },
}

#[derive(Debug, Parser)]
pub struct GlobalConfigArgs {
    /// Enable verbose output
    #[arg(short, long, global = true)]
    pub verbose: bool,
}
