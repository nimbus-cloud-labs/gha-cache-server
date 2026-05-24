pub mod cache {
    include!(concat!(env!("OUT_DIR"), "/v1.rs"));
}

pub mod artifact {
    include!(concat!(
        env!("OUT_DIR"),
        "/github.actions.results.api.v1.rs"
    ));
}
