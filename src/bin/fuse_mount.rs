//! Strata FUSE mount binary.

#[cfg(feature = "fuse")]
use clap::Parser;
#[cfg(feature = "fuse")]
use std::path::PathBuf;

#[cfg(feature = "fuse")]
#[derive(Parser)]
#[command(name = "strata-mount")]
#[command(about = "Mount a Strata filesystem")]
struct Args {
    /// Mount point
    mount_point: PathBuf,

    /// Metadata server address
    #[arg(short, long, default_value = "127.0.0.1:9000")]
    metadata_addr: String,

    /// Run in foreground
    #[arg(short, long)]
    foreground: bool,

    /// Enable debug output
    #[arg(short, long)]
    debug: bool,
}

#[cfg(feature = "fuse")]
fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.debug {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
    }

    println!("Mounting Strata filesystem at {:?}", args.mount_point);
    println!("Connecting to metadata server: {}", args.metadata_addr);

    strata::fuse::mount(&args.mount_point, &args.metadata_addr)?;

    Ok(())
}

#[cfg(not(feature = "fuse"))]
fn main() {
    eprintln!("FUSE support is not enabled. Rebuild with --features fuse");
    std::process::exit(1);
}
