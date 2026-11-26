use anyhow::{Result, bail};

pub fn assert_draining(args: Vec<String>) -> Result<()> {
    if args.len() != 1 {
        bail!("usage: assert-draining <error.json>");
    }
    let body = crate::util::read_json(&args[0])?;
    if body["failure_kind"] != "draining" {
        bail!("expected draining classification: {body}");
    }
    if body["retry_safe"] != true {
        bail!("a draining refusal before SQL must be retry-safe: {body}");
    }
    if !body["error"]
        .as_str()
        .unwrap_or_default()
        .contains("no statement was executed")
    {
        bail!("draining refusal does not prove that nothing ran: {body}");
    }
    println!("PASS: a draining worker refused a new session as retry-safe with nothing run");
    Ok(())
}

pub fn assert_drain_order(args: Vec<String>) -> Result<()> {
    if args.is_empty() || args.len() > 4 {
        bail!(
            "usage: assert-drain-order <log> [signal-marker] [finished-marker] [complete-marker]"
        );
    }
    let signal_marker = args
        .get(1)
        .map_or("termination signal received", String::as_str);
    let finished_marker = args.get(2).map_or("query finished", String::as_str);
    let complete_marker = args.get(3).map_or("drain complete", String::as_str);
    let text = std::fs::read_to_string(&args[0])?;
    let lines: Vec<String> = text.lines().map(str::to_owned).collect();
    let first = |needle: &str, after: Option<usize>| {
        lines.iter().enumerate().find_map(|(index, line)| {
            (after.is_none_or(|previous| index > previous) && line.contains(needle))
                .then_some(index)
        })
    };
    let signal = first(signal_marker, None).ok_or_else(|| {
        anyhow::anyhow!("the process never logged {signal_marker:?}; the signal was not handled")
    })?;
    let finished = first(finished_marker, Some(signal)).ok_or_else(|| {
        anyhow::anyhow!("no statement completed after the termination signal; the in-flight statement must still be running when the signal lands")
    })?;
    first(complete_marker, Some(finished)).ok_or_else(|| {
        anyhow::anyhow!("the drain did not report completing after the statement finished")
    })?;
    println!(
        "PASS: the signal arrived first, a statement finished during the drain, and the drain ended after it"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn drain_order_requires_the_active_outcome_before_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("worker.log");
        std::fs::write(
            &path,
            "termination signal received\nquery finished\ndrain complete\n",
        )
        .unwrap();
        super::assert_drain_order(vec![path.to_string_lossy().into_owned()]).unwrap();
        std::fs::write(
            &path,
            "drain complete\ntermination signal received\nquery finished\n",
        )
        .unwrap();
        assert!(super::assert_drain_order(vec![path.to_string_lossy().into_owned()]).is_err());
    }

    #[test]
    fn draining_requires_a_classified_noop_refusal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("error.json");
        std::fs::write(&path, "{\"failure_kind\":\"draining\",\"retry_safe\":true,\"error\":\"no statement was executed\"}").unwrap();
        super::assert_draining(vec![path.to_string_lossy().into_owned()]).unwrap();
    }
}
