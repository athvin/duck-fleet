"""Load and qualify the pinned extension bundle without network access."""
import duckdb

REQUIRED = {"httpfs", "postgres_scanner", "ducklake", "aws", "icu"}

connection = duckdb.connect()
connection.execute("SET extension_directory='/opt/duckdb/extensions'")
for extension in ["icu", "httpfs", "postgres", "ducklake", "aws"]:
    connection.execute(f"LOAD {extension}")
rows = connection.execute(
    "SELECT extension_name, loaded, installed, install_path, extension_version "
    "FROM duckdb_extensions() WHERE extension_name IN ('httpfs', 'postgres_scanner', 'ducklake', 'aws', 'icu') "
    "ORDER BY extension_name"
).fetchall()
assert {row[0] for row in rows} == REQUIRED, rows
for name, loaded, installed, path, version in rows:
    assert loaded and installed, rows
    assert path == "(BUILT-IN)" or "/v2.0.1/" in path, (name, path)
    assert version, (name, version)
    print(f"{name} {version} {path}")

calendar = connection.execute(
    "SELECT timezone('America/New_York', TIMESTAMPTZ '2026-07-01 12:00:00+00'), "
    "date_part('isodow', DATE '2026-11-21')"
).fetchone()
assert str(calendar[0]) == "2026-07-01 08:00:00", calendar
assert calendar[1] == 6, calendar
collation = connection.execute(
    "SELECT 'a' COLLATE NOCASE = 'A', 'a' COLLATE NOACCENT = 'á'"
).fetchone()
assert collation == (True, True), collation

connection.execute("SET allow_extension_repositories='forbidden'")
try:
    connection.execute("CREATE EXTENSION REPOSITORY example.org")
except duckdb.Error as error:
    assert "extension" in str(error).lower() and "repositories" in str(error).lower(), error
else:
    raise AssertionError("custom extension repositories were not forbidden")
print("PASS: offline 2.0.1 extensions, ICU calendar/collation behavior, and repository lockdown")
