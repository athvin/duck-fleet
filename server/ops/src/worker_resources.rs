use anyhow::{Result, bail};
use serde_json::Value;

pub fn cores(quantity: &str) -> Result<f64> {
    let value = if let Some(milli) = quantity.strip_suffix('m') {
        milli.parse::<f64>()? / 1000.0
    } else {
        quantity.parse::<f64>()?
    };
    if !value.is_finite() {
        bail!("invalid CPU quantity: {quantity}");
    }
    Ok(value)
}

pub fn quantity_bytes(quantity: &str) -> Result<u64> {
    let match_ = regex::Regex::new(r"^([0-9]+)(Ki|Mi|Gi)?$")?
        .captures(quantity)
        .ok_or_else(|| anyhow::anyhow!("invalid binary quantity: {quantity}"))?;
    let value = match_[1].parse::<u64>()?;
    let multiplier = match match_.get(2).map(|value| value.as_str()) {
        None => 1,
        Some("Ki") => 1024,
        Some("Mi") => 1024_u64.pow(2),
        Some("Gi") => 1024_u64.pow(3),
        Some(_) => unreachable!(),
    };
    Ok(value * multiplier)
}

pub fn assert_worker_resources(
    resources: &Value,
    cpu: u32,
    where_: &str,
    memory: Option<&str>,
    disk: Option<&str>,
) -> Result<()> {
    let limit = resources["limits"]["cpu"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("{where_}: missing CPU limit"))?;
    if cores(limit)? != f64::from(cpu) {
        bail!("{where_}: CPU limit {limit} is not the declared allocation {cpu}");
    }
    let fallback = format!("{cpu}Gi");
    let expected_memory = quantity_bytes(memory.unwrap_or(&fallback))?;
    if expected_memory < 256 * 1024_u64.pow(2) {
        bail!("{where_}: memory must be at least 256Mi");
    }
    for kind in ["requests", "limits"] {
        let actual = resources[kind]["memory"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("{where_}: missing {kind} memory"))?;
        if quantity_bytes(actual)? != expected_memory {
            bail!("{where_}: {kind} memory {actual} differs from the declared allocation");
        }
    }
    let request = resources["requests"]["cpu"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("{where_}: missing CPU request"))?;
    if cores(request)? <= 0.0 || cores(request)? > f64::from(cpu) {
        bail!("{where_}: CPU request {request} must be above zero and at most the limit of {cpu}");
    }
    if let Some(disk) = disk {
        let expected_disk = quantity_bytes(disk)?;
        if expected_disk < 1024_u64.pow(3) {
            bail!("{where_}: disk must be at least 1Gi");
        }
        if quantity_bytes(
            resources["requests"]["ephemeral-storage"]
                .as_str()
                .unwrap_or_default(),
        )? != expected_disk
        {
            bail!("{where_}: ephemeral-storage request differs from declared disk");
        }
        if quantity_bytes(
            resources["limits"]["ephemeral-storage"]
                .as_str()
                .unwrap_or_default(),
        )? != expected_disk + 1024_u64.pow(3)
        {
            bail!("{where_}: ephemeral-storage limit must include 1Gi of pod overhead");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn quantities_and_worker_allocations_are_validated() {
        assert_eq!(cores("250m").unwrap(), 0.25);
        assert_eq!(cores("2").unwrap(), 2.0);
        assert_eq!(quantity_bytes("2Gi").unwrap(), 2 * 1024_u64.pow(3));
        assert!(quantity_bytes("2GB").is_err());
        let mut resources = json!({
            "requests": {"cpu":"500m","memory":"2Gi","ephemeral-storage":"20Gi"},
            "limits": {"cpu":"2","memory":"2Gi","ephemeral-storage":"21Gi"}
        });
        assert_worker_resources(&resources, 2, "fixture", Some("2Gi"), Some("20Gi")).unwrap();
        resources["requests"]["cpu"] = json!("3");
        assert!(
            assert_worker_resources(&resources, 2, "fixture", Some("2Gi"), Some("20Gi")).is_err()
        );
    }
}
