# duckfleet-adbc

The DuckFleet ADBC driver: a `cdylib` exporting `AdbcDuckfleetInit`, built on the `duckfleet` SDK. For installation, options, and behavior, see [docs/adbc.md](../../docs/adbc.md).

```sh
cargo build --release -p duckfleet-adbc   # target/release/libadbc_duckfleet.{so,dylib}
make test-adbc                            # real-client suite against Compose
```
