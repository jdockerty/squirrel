fn main() -> std::io::Result<()> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".eraftpb", "::raft::eraftpb")
        .compile(&["proto/raft.proto"], &["proto/"])?;
    Ok(())
}
