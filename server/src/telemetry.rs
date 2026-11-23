//! Bounded Prometheus text rendering for the authenticated operator endpoint.

pub(crate) fn reason(value: &str) -> &str {
    match value {
        "capacity"
        | "queue_capacity"
        | "compute_unavailable"
        | "not_placeable"
        | "provisioning_failed"
        | "worker_restarted"
        | "gateway_restarted"
        | "unknown" => value,
        _ => "other",
    }
}

pub(crate) fn counter(name: &str, labels: &[(&str, &str)], value: u64) -> String {
    sample(name, labels, &value.to_string())
}

pub(crate) fn gauge(name: &str, labels: &[(&str, &str)], value: &str) -> String {
    sample(name, labels, value)
}

fn sample(name: &str, labels: &[(&str, &str)], value: &str) -> String {
    debug_assert!(name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    let labels = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", value.replace(['\\', '"', '\n'], "_")))
        .collect::<Vec<_>>()
        .join(",");
    if labels.is_empty() {
        format!("{name} {value}\n")
    } else {
        format!("{name}{{{labels}}} {value}\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_are_bounded_and_label_text_cannot_escape() {
        assert_eq!(reason("capacity"), "capacity");
        assert_eq!(reason("principal-with-private-data"), "other");
        assert_eq!(reason("anything-else"), "other");
        assert_eq!(
            counter("duckfleet_test_total", &[("reason", "other")], 2),
            "duckfleet_test_total{reason=\"other\"} 2\n"
        );
        assert_eq!(
            gauge("duckfleet_test", &[("reason", "bad\"\nvalue")], "1"),
            "duckfleet_test{reason=\"bad__value\"} 1\n"
        );
    }
}
