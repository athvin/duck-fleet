use anyhow::{Result, bail};
use std::path::{Path, PathBuf};

const STAGING: usize = 20;
const CORE: usize = 50;
const INCREMENTAL: usize = 30;
const WIDE: usize = 10;

fn write(path: PathBuf, text: String) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, text)?;
    Ok(())
}

pub fn generate(root: &Path) -> Result<()> {
    write(
        root.join("dbt_project.yml"),
        r#"name: duckfleet_scale
version: '0.1.0'
config-version: 2
profile: duckfleet
model-paths: [models]
query-comment: null
models:
  duckfleet_scale:
    +meta:
      platform:
        tags:
          team: scale
"#
        .into(),
    )?;
    for i in 0..STAGING {
        write(
            root.join(format!("models/staging/stg_{i:02}.sql")),
            format!(
                "{{{{ config(materialized='view') }}}}\nselect range as id, range % {} as bucket, (range * {}) % 1000 as value\nfrom range(1500)\n",
                7 + i,
                i + 3
            ),
        )?;
    }
    for i in 0..CORE {
        let first = i % STAGING;
        let second = (i * 7 + 3) % STAGING;
        write(
            root.join(format!("models/core/core_{i:02}.sql")),
            format!(
                "{{{{ config(materialized='table') }}}}\nselect a.bucket, count(*) as rows_in_bucket, sum(a.value + b.value) as total\nfrom {{{{ ref('stg_{first:02}') }}}} a join {{{{ ref('stg_{second:02}') }}}} b using (id)\ngroup by a.bucket\n"
            ),
        )?;
    }
    for i in 0..INCREMENTAL {
        let source = (i * 3 + 1) % STAGING;
        write(
            root.join(format!("models/incremental/inc_{i:02}.sql")),
            format!(
                "{{{{ config(materialized='incremental', unique_key='id', incremental_strategy='delete+insert') }}}}\nselect id, bucket, value from {{{{ ref('stg_{source:02}') }}}}\n{{% if is_incremental() %}}\nwhere id > (select coalesce(max(id), -1) - 100 from {{{{ this }}}})\n{{% endif %}}\n"
            ),
        )?;
    }
    for i in 0..WIDE {
        let parts = (0..3)
            .map(|j| {
                format!(
                    "select * from {{{{ ref('core_{:02}') }}}}",
                    (i * 5 + j) % CORE
                )
            })
            .collect::<Vec<_>>()
            .join(" union all ");
        write(
            root.join(format!("models/wide/wide_{i}.sql")),
            format!(
                "{{{{ config(materialized='table', meta={{'platform': {{'tags': {{'team': 'scale'}}}}}}) }}}}\n{parts}\n"
            ),
        )?;
    }
    let schema = format!("version: 2\nmodels:\n{}", (0..STAGING).map(|i| format!("  - name: stg_{i:02}\n    columns:\n      - name: id\n        data_tests: [not_null, unique]\n")).collect::<String>());
    write(root.join("models/staging/schema.yml"), schema)?;
    println!(
        "generated {} models at {}",
        STAGING + CORE + INCREMENTAL + WIDE,
        root.display()
    );
    Ok(())
}

pub fn run(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: scale-generate <directory>");
    }
    generate(Path::new(&args[0]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_is_deterministic_and_complete() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        generate(first.path()).unwrap();
        generate(second.path()).unwrap();
        for (directory, count) in [
            ("staging", 21),
            ("core", 50),
            ("incremental", 30),
            ("wide", 10),
        ] {
            assert_eq!(
                std::fs::read_dir(first.path().join("models").join(directory))
                    .unwrap()
                    .count(),
                count
            );
        }
        assert_eq!(
            std::fs::read(first.path().join("models/core/core_00.sql")).unwrap(),
            std::fs::read(second.path().join("models/core/core_00.sql")).unwrap()
        );
    }
}
