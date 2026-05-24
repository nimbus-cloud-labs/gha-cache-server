fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/cache.proto");
    println!("cargo:rerun-if-changed=proto/artifact.proto");
    prost_build::Config::new()
        .compile_protos(&["proto/cache.proto", "proto/artifact.proto"], &["proto"])?;
    Ok(())
}
