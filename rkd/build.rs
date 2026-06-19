fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_proto("../proto/torrent.proto")?;
    Ok(())
}
