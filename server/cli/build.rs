//! Rebuild when the embedded admin console bundle changes. rust-embed tracks the
//! files it embedded, but not files added after the last build, and not the
//! bundle appearing for the first time.
fn main() {
    println!("cargo:rerun-if-changed=../../web/admin/out");
}
