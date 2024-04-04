fn main() -> std::io::Result<()> {
    tonic_build::compile_protos("proto/raft.proto")?;
    Ok(())
}
