//! Basic client usage example for Strata.
//!
//! This example demonstrates how to use the Strata client library
//! to perform basic metadata operations.
//!
//! # Running
//!
//! First, start a Strata metadata server, then run:
//! ```bash
//! cargo run --example basic_client
//! ```

// Note: In a real application, you would use:
// use strata::client::MetadataClient;
// use strata::error::Result;

fn main() {
    println!("Strata Basic Client Example");
    println!("===========================\n");

    // Example: Creating a metadata client
    println!("1. Creating a metadata client:");
    println!("   let addr: SocketAddr = \"127.0.0.1:8080\".parse().unwrap();");
    println!("   let client = MetadataClient::new(addr);");
    println!();

    // Example: Health check
    println!("2. Health check:");
    println!("   let is_healthy = client.health().await?;");
    println!("   println!(\"Server healthy: {{}}\", is_healthy);");
    println!();

    // Example: Lookup operation
    println!("3. Looking up a file:");
    println!("   // Lookup 'myfile.txt' in the root directory (inode 1)");
    println!("   let inode = client.lookup(1, \"myfile.txt\").await?;");
    println!("   match inode {{");
    println!("       Some(inode) => println!(\"Found: {{:?}}\", inode),");
    println!("       None => println!(\"File not found\"),");
    println!("   }}");
    println!();

    // Example: Create a file
    println!("4. Creating a file:");
    println!("   let response = client.create_file(");
    println!("       1,              // parent inode (root)");
    println!("       \"newfile.txt\", // filename");
    println!("       0o644,          // mode (rw-r--r--)");
    println!("       1000,           // uid");
    println!("       1000,           // gid");
    println!("   ).await?;");
    println!("   println!(\"Created inode: {{}}\", response.inode_id);");
    println!();

    // Example: Create a directory
    println!("5. Creating a directory:");
    println!("   let response = client.create_dir(");
    println!("       1,           // parent inode");
    println!("       \"mydir\",    // directory name");
    println!("       0o755,       // mode (rwxr-xr-x)");
    println!("       1000,        // uid");
    println!("       1000,        // gid");
    println!("   ).await?;");
    println!();

    // Example: Read directory
    println!("6. Reading directory contents:");
    println!("   let entries = client.readdir(1).await?;");
    println!("   for entry in entries {{");
    println!("       println!(\"  {{}} (inode {{}})\", entry.name, entry.inode_id);");
    println!("   }}");
    println!();

    // Example: Get file attributes
    println!("7. Getting file attributes:");
    println!("   let inode = client.getattr(inode_id).await?;");
    println!("   if let Some(inode) = inode {{");
    println!("       println!(\"Size: {{}} bytes\", inode.size);");
    println!("       println!(\"Mode: {{:o}}\", inode.mode);");
    println!("   }}");
    println!();

    println!("For a complete working example, ensure a Strata server is running");
    println!("and uncomment the actual client code in this file.");
}

// Uncomment and modify this for actual usage:
/*
use strata::client::MetadataClient;
use strata::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let client = MetadataClient::new(addr);

    // Health check
    let is_healthy = client.health().await?;
    println!("Server healthy: {}", is_healthy);

    // Lookup root directory
    let root = client.getattr(1).await?;
    println!("Root directory: {:?}", root);

    // Create a file
    let response = client.create_file(
        1,              // parent inode (root)
        "example.txt",  // filename
        0o644,          // mode
        1000,           // uid
        1000,           // gid
    ).await?;
    println!("Created file with inode: {}", response.inode_id);

    Ok(())
}
*/
