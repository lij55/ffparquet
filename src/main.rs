use clap::{Parser, Subcommand};
use env_logger;
use eyre::Result;

mod cmd;

#[derive(Subcommand, Debug)]
enum Commands {
    Cat(cmd::cat::Args),
    Meta(cmd::meta::Args),
    Merge(cmd::merge::Args),
    Split(cmd::split::Args),
    Df(cmd::df::Args),
}

#[derive(Parser, Debug)]
struct Args {
    /// Show debug output
    #[arg(short, long, help = "Show debug output")]
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
        Commands::Merge(args) => cmd::merge::merge_main(args),
        Commands::Split(args) => cmd::split::split_main(args),
        Commands::Df(args) => cmd::df::df_main(args),
    }
}
