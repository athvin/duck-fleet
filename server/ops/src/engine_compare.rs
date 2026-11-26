use crate::util;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn by_id(report: &Value) -> Result<BTreeMap<String, Value>> {
    let queries = report["queries"]
        .as_array()
        .context("qualification report has no query list")?;
    let mut result = BTreeMap::new();
    for query in queries {
        let id = query["id"].as_str().context("missing query id")?.to_owned();
        if result.insert(id.clone(), query.clone()).is_some() {
            bail!("duplicate query id: {id}");
        }
    }
    Ok(result)
}

fn comparable(query: &Value) -> Value {
    json!({"sql":query.get("sql"),"outcome":query.get("outcome"),"profile_status":query["profile"].get("status")})
}

fn performance(baseline: &Value, candidate: &Value) -> Value {
    let before = baseline["elapsed_ms"].as_f64();
    let after = candidate["elapsed_ms"].as_f64();
    let mut value = json!({"baseline_ms":baseline.get("elapsed_ms"),"candidate_ms":candidate.get("elapsed_ms")});
    if let (Some(before), Some(after)) = (before, after)
        && before > 0.0
    {
        value["candidate_over_baseline"] =
            json!((after / before * 1_000_000.0).round() / 1_000_000.0);
    }
    value
}

pub fn compare(baseline: &Value, candidate: &Value, expectations: &Value) -> Result<Value> {
    let accepted = util::string_set(expectations.get("accepted"));
    let fixed = util::string_set(expectations.get("fixed"));
    let before = by_id(baseline)?;
    let unknown: Vec<_> = accepted
        .union(&fixed)
        .filter(|id| !before.contains_key(*id))
        .collect();
    if !unknown.is_empty() {
        bail!("expectations refer to unknown baseline cases: {unknown:?}");
    }
    let overlap: Vec<_> = accepted.intersection(&fixed).collect();
    if !overlap.is_empty() {
        bail!("cases cannot be both accepted and fixed: {overlap:?}");
    }
    let after = by_id(candidate)?;
    let ids: BTreeSet<_> = before.keys().chain(after.keys()).collect();
    let mut queries = Vec::new();
    let mut counts = json!({"unchanged":0,"accepted":0,"fixed":0,"blocking":0});
    for id in ids {
        let old = before.get(id);
        let new = after.get(id);
        let (status, difference, performance) = match (old, new) {
            (Some(old), Some(new)) if comparable(old) == comparable(new) => {
                ("unchanged", None, performance(old, new))
            }
            (Some(old), Some(new)) => (
                if accepted.contains(id) {
                    "accepted"
                } else if fixed.contains(id) {
                    "fixed"
                } else {
                    "blocking"
                },
                Some(json!({"baseline":comparable(old),"candidate":comparable(new)})),
                performance(old, new),
            ),
            (old, new) => (
                "blocking",
                Some(json!({"baseline":old.is_some(),"candidate":new.is_some()})),
                json!({}),
            ),
        };
        counts[status] = json!(counts[status].as_u64().unwrap() + 1);
        let mut item = json!({"id":id,"status":status,"performance":performance});
        if let Some(difference) = difference {
            item["difference"] = difference;
        }
        if let Some(new) = new {
            item["sql"] = new.get("sql").cloned().unwrap_or(Value::Null);
        }
        queries.push(item);
    }
    let version_mismatch = baseline["engine_version"] == candidate["engine_version"]
        && expectations["allow_same_version"] != true;
    let passed = counts["blocking"] == 0 && !version_mismatch;
    Ok(json!({
        "schema_version":1,
        "baseline_engine":baseline.get("engine_version"),
        "candidate_engine":candidate.get("engine_version"),
        "passed":passed,
        "counts":counts,
        "version_mismatch":version_mismatch,
        "accepted_expectations":accepted,
        "fixed_expectations":fixed,
        "queries":queries,
    }))
}

pub fn run(args: Vec<String>) -> Result<()> {
    let mut args = args;
    let baseline = util::option(&mut args, "--baseline").context("--baseline is required")?;
    let candidate = util::option(&mut args, "--candidate").context("--candidate is required")?;
    let expectations =
        util::option(&mut args, "--expectations").context("--expectations is required")?;
    let output = util::option(&mut args, "--output").context("--output is required")?;
    util::reject_extra(&args, 0)?;
    let report = compare(
        &util::read_json(baseline)?,
        &util::read_json(candidate)?,
        &util::read_json(expectations)?,
    )?;
    util::write_json(output, &report)?;
    println!(
        "{}",
        json!({"passed":report["passed"],"counts":report["counts"],"version_mismatch":report["version_mismatch"]})
    );
    if report["passed"] != true {
        bail!("engine qualification has blocking differences");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(version: &str, outcome: Option<Value>) -> Value {
        json!({"engine_version":version,"queries":[{"id":"case","sql":"SELECT 1","outcome":outcome.unwrap_or_else(|| json!({"kind":"success","columns":["value"],"column_types":["Int32"],"rows":[["1"]]})),"elapsed_ms":10,"profile":{"status":"captured"}}]})
    }

    #[test]
    fn unchanged_and_classified_differences_pass() {
        let baseline = report("v1.5.4", None);
        let mut candidate = report("v2.0.1", None);
        candidate["queries"][0]["elapsed_ms"] = json!(15);
        let result = compare(&baseline, &candidate, &json!({})).unwrap();
        assert_eq!(result["counts"]["unchanged"], 1);
        assert_eq!(
            result["queries"][0]["performance"]["candidate_over_baseline"],
            1.5
        );
        for classification in ["accepted", "fixed"] {
            let changed = report(
                "v2.0.1",
                Some(
                    json!({"kind":"error","status":422,"failure_kind":"sql_error","retry_safe":false,"message":"changed"}),
                ),
            );
            let result = compare(&baseline, &changed, &json!({(classification):["case"]})).unwrap();
            assert_eq!(result["counts"][classification], 1);
        }
    }

    #[test]
    fn unclassified_missing_and_same_version_block() {
        let changed = report(
            "v2.0.1",
            Some(
                json!({"kind":"success","columns":["value"],"column_types":["Int64"],"rows":[["1"]]}),
            ),
        );
        assert_eq!(
            compare(&report("v1.5.4", None), &changed, &json!({})).unwrap()["counts"]["blocking"],
            1
        );
        assert_eq!(
            compare(
                &report("v1.5.4", None),
                &json!({"engine_version":"v2.0.1","queries":[]}),
                &json!({})
            )
            .unwrap()["counts"]["blocking"],
            1
        );
        assert!(
            compare(&report("v1.5.4", None), &report("v1.5.4", None), &json!({})).unwrap()["version_mismatch"]
                == true
        );
        assert!(
            compare(
                &report("v1.5.4", None),
                &report("v1.5.4", None),
                &json!({"allow_same_version":true})
            )
            .unwrap()["passed"]
                == true
        );
        assert!(
            compare(
                &report("v1.5.4", None),
                &report("v2.0.1", None),
                &json!({"accepted":["unknown"]})
            )
            .is_err()
        );
    }
}
