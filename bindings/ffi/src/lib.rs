uniffi::setup_scaffolding!("slatedb");

#[uniffi::export]
pub fn binding_version() -> String {
    env!("CARGO_PKG_VERSION").to_owned()
}

#[cfg(test)]
mod tests {
    use super::binding_version;

    #[test]
    fn reports_binding_version() {
        assert_eq!(binding_version(), env!("CARGO_PKG_VERSION"));
    }
}
