"""Release smoke: an installed DuckFleet ADBC driver loads by manifest name.

Run by .github/workflows/adbc-release.yml with ADBC_DRIVER_PATH pointing at the
directory install.sh wrote. No server is needed: creating the database only
loads the driver and applies options.
"""

import adbc_driver_manager

database = adbc_driver_manager.AdbcDatabase(driver="duckfleet", uri="http://127.0.0.1:1", password="x")
database.close()
print("driver loaded by manifest name")
