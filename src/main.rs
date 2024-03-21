use clap::{Parser, Subcommand};
use env_logger;
use eyre::Result;

mod cmd;

#[derive(Subcommand, Debug)]
enum Commands {
    Cat(cmd::cat::Args),
    Meta(cmd::meta::Args),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Show debug output
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.verbose {
        std::env::set_var("RUST_LOG", "log");
    }
    env_logger::init();

    match args.command {
        Commands::Cat(args) => cmd::cat::cat_main(args),
        Commands::Meta(args) => cmd::meta::meta_main(args),
    }
}
