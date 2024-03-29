fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["proto/items.proto"], &["proto/"])?;
    Ok(())
}
