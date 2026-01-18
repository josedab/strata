//! S3 Gateway example for Strata.
//!
//! This example demonstrates how to use the S3-compatible API
//! to interact with Strata using standard S3 tools and SDKs.
//!
//! # Running
//!
//! First start the Strata server with S3 gateway enabled:
//! ```bash
//! cargo run -- --s3-enabled --s3-port 9000
//! ```
//!
//! Then run this example:
//! ```bash
//! cargo run --example s3_gateway
//! ```

fn main() {
    println!("Strata S3 Gateway Example");
    println!("=========================\n");

    // Example: S3 endpoint configuration
    println!("1. Configure S3 endpoint:");
    println!("   export AWS_ACCESS_KEY_ID=strata-access-key");
    println!("   export AWS_SECRET_ACCESS_KEY=strata-secret-key");
    println!("   export AWS_ENDPOINT_URL=http://localhost:9000");
    println!();

    // Example: Using AWS CLI
    println!("2. Using AWS CLI:");
    println!("   # Create a bucket");
    println!("   aws s3 mb s3://my-bucket");
    println!();
    println!("   # Upload a file");
    println!("   aws s3 cp myfile.txt s3://my-bucket/");
    println!();
    println!("   # List bucket contents");
    println!("   aws s3 ls s3://my-bucket/");
    println!();
    println!("   # Download a file");
    println!("   aws s3 cp s3://my-bucket/myfile.txt ./downloaded.txt");
    println!();

    // Example: Using Rust SDK
    println!("3. Using Rust AWS SDK:");
    println!("   let config = aws_config::defaults(BehaviorVersion::latest())");
    println!("       .endpoint_url(\"http://localhost:9000\")");
    println!("       .load()");
    println!("       .await;");
    println!("   let client = aws_sdk_s3::Client::new(&config);");
    println!();
    println!("   // Create bucket");
    println!("   client.create_bucket()");
    println!("       .bucket(\"my-bucket\")");
    println!("       .send()");
    println!("       .await?;");
    println!();
    println!("   // Put object");
    println!("   client.put_object()");
    println!("       .bucket(\"my-bucket\")");
    println!("       .key(\"hello.txt\")");
    println!("       .body(ByteStream::from_static(b\"Hello, World!\"))");
    println!("       .send()");
    println!("       .await?;");
    println!();

    // Example: Supported S3 operations
    println!("4. Supported S3 Operations:");
    println!("   Bucket Operations:");
    println!("   - CreateBucket");
    println!("   - DeleteBucket");
    println!("   - ListBuckets");
    println!("   - HeadBucket");
    println!();
    println!("   Object Operations:");
    println!("   - PutObject");
    println!("   - GetObject");
    println!("   - DeleteObject");
    println!("   - HeadObject");
    println!("   - ListObjectsV2");
    println!("   - CopyObject");
    println!();
    println!("   Multipart Upload:");
    println!("   - CreateMultipartUpload");
    println!("   - UploadPart");
    println!("   - CompleteMultipartUpload");
    println!("   - AbortMultipartUpload");
    println!();

    // Example: S3 to POSIX mapping
    println!("5. S3 to POSIX Mapping:");
    println!("   Strata provides unified access - files uploaded via S3");
    println!("   are accessible through FUSE mount and vice versa.");
    println!();
    println!("   S3 Path                    POSIX Path");
    println!("   ---------                  ----------");
    println!("   s3://bucket/key            /mnt/strata/bucket/key");
    println!("   s3://data/logs/app.log     /mnt/strata/data/logs/app.log");
    println!();

    println!("For S3 gateway configuration options,");
    println!("see docs/configuration.md#s3-gateway.");
}

// Uncomment for actual usage:
/*
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure S3 client to use Strata endpoint
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url("http://localhost:9000")
        .load()
        .await;

    let client = Client::new(&config);

    // Create a bucket
    client.create_bucket()
        .bucket("example-bucket")
        .send()
        .await?;
    println!("Created bucket: example-bucket");

    // Upload an object
    client.put_object()
        .bucket("example-bucket")
        .key("hello.txt")
        .body(ByteStream::from_static(b"Hello from Strata S3 Gateway!"))
        .send()
        .await?;
    println!("Uploaded: hello.txt");

    // List objects
    let response = client.list_objects_v2()
        .bucket("example-bucket")
        .send()
        .await?;

    println!("Bucket contents:");
    for object in response.contents() {
        println!("  - {} ({} bytes)",
            object.key().unwrap_or("?"),
            object.size().unwrap_or(0));
    }

    // Download the object
    let response = client.get_object()
        .bucket("example-bucket")
        .key("hello.txt")
        .send()
        .await?;

    let data = response.body.collect().await?.into_bytes();
    println!("Downloaded content: {}", String::from_utf8_lossy(&data));

    Ok(())
}
*/
