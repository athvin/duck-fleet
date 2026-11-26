#!/usr/bin/env sh
# Installs the DuckFleet ADBC driver for the current user so ADBC driver
# managers can load it by name ("duckfleet"). Run from the extracted release.
#   sh install.sh [--prefix <driver-directory>]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
case "$(uname -s)" in
  Darwin) os=macos; library=libadbc_duckfleet.dylib; default="$HOME/Library/Application Support/ADBC/Drivers" ;;
  Linux) os=linux; library=libadbc_duckfleet.so; default="${XDG_CONFIG_HOME:-$HOME/.config}/adbc/drivers" ;;
  *) echo "unsupported operating system: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) arch=arm64 ;;
  x86_64|amd64) arch=amd64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
target="$default"
if [ "${1:-}" = "--prefix" ]; then target="$2"; fi
[ -f "$here/$library" ] || { echo "$library is not next to install.sh" >&2; exit 1; }
mkdir -p "$target/duckfleet"
cp "$here/$library" "$target/duckfleet/$library"
version=$(cat "$here/VERSION")
cat > "$target/duckfleet.toml" <<MANIFEST
manifest_version = 1
name = "DuckFleet"
version = "$version"
publisher = "DuckFleet"
license = "Apache-2.0"

[ADBC]
version = "1.1.0"

[Driver]
entrypoint = "AdbcDuckfleetInit"

[Driver.shared]
${os}_${arch} = "$target/duckfleet/$library"
MANIFEST
echo "installed DuckFleet ADBC driver $version: $target/duckfleet.toml"
