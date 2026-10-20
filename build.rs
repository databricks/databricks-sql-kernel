fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/sea_driver_kernel.proto";
    println!("cargo:rerun-if-changed={proto}");

    tonic_prost_build::configure()
        .build_server(false)
        .type_attribute(
            "databricks.sqlexecapi.ColumnTypeName",
            "#[allow(clippy::enum_variant_names)]",
        )
        .compile_protos(&[proto], &["proto"])?;

    Ok(())
}
