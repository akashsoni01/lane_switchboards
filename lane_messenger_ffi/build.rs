fn main() {
    if std::env::var("CARGO_FEATURE_UNIFFI").is_ok() {
        let udl = format!(
            "{}/src/lane_messenger.udl",
            env!("CARGO_MANIFEST_DIR")
        );
        uniffi::generate_scaffolding(&udl).expect("uniffi scaffolding");
    }
}
