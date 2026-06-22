fn main() -> Result<(), Box<dyn std::error::Error>> {
    protoc_bin_vendored::protoc_bin_path().map(|path| unsafe {
        std::env::set_var("PROTOC", path);
    })?;
    tonic_build::compile_protos("proto/validator.proto")?;
    Ok(())
}
