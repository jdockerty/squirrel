fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["proto/raft.proto"], &["proto/"])?;
    Ok(())
}
