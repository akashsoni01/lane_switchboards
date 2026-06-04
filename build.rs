fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/mesh_control.proto",
                "proto/mesh_data.proto",
                "proto/paxos.proto",
            ],
            &["proto/"],
        )
        .expect("compile protos");
}
