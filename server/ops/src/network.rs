use anyhow::{Context, Result};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Network {
    address: IpAddr,
    prefix: u8,
}

impl Network {
    pub fn parse(value: &str) -> Result<Self> {
        let (address, prefix) = value
            .split_once('/')
            .with_context(|| format!("CIDR required: {value}"))?;
        let address: IpAddr = address.parse()?;
        let prefix = prefix.parse::<u8>()?;
        let maximum = if address.is_ipv4() { 32 } else { 128 };
        if prefix > maximum {
            anyhow::bail!("invalid network prefix: {value}");
        }
        let network = Self { address, prefix };
        if network.bits() & !network.mask() != 0 {
            anyhow::bail!("network has host bits set: {value}");
        }
        Ok(network)
    }

    fn bits(&self) -> u128 {
        match self.address {
            IpAddr::V4(address) => u128::from(u32::from(address)),
            IpAddr::V6(address) => u128::from(address),
        }
    }

    fn mask(&self) -> u128 {
        let width = if self.address.is_ipv4() { 32 } else { 128 };
        if self.prefix == 0 {
            0
        } else {
            u128::MAX << (width - self.prefix)
        }
    }

    pub fn contains(&self, address: IpAddr) -> bool {
        if std::mem::discriminant(&self.address) != std::mem::discriminant(&address) {
            return false;
        }
        let bits = match address {
            IpAddr::V4(address) => u128::from(u32::from(address)),
            IpAddr::V6(address) => u128::from(address),
        };
        bits & self.mask() == self.bits()
    }

    pub fn prefix_len(&self) -> u8 {
        self.prefix
    }

    pub fn is_private(&self) -> bool {
        match self.address {
            IpAddr::V4(address) => {
                address.is_private()
                    || address.is_loopback()
                    || address.is_link_local()
                    || address.is_unspecified()
                    || address.octets()[0] == 100 && (64..=127).contains(&address.octets()[1])
                    || matches!(
                        address.octets(),
                        [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
                    )
            }
            IpAddr::V6(address) => {
                address.is_loopback()
                    || address.is_unique_local()
                    || address.is_unicast_link_local()
            }
        }
    }

    pub fn address_plus(&self, offset: u32) -> Result<IpAddr> {
        match self.address {
            IpAddr::V4(address) => Ok(IpAddr::V4(Ipv4Addr::from(
                u32::from(address)
                    .checked_add(offset)
                    .context("address overflow")?,
            ))),
            IpAddr::V6(address) => Ok(IpAddr::V6(Ipv6Addr::from(
                u128::from(address)
                    .checked_add(u128::from(offset))
                    .context("address overflow")?,
            ))),
        }
    }
}

impl std::fmt::Display for Network {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.address, self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_membership_and_resolver_arithmetic() {
        let network = Network::parse("10.42.0.0/16").unwrap();
        assert!(network.contains("10.42.9.4".parse().unwrap()));
        assert!(!network.contains("10.43.0.1".parse().unwrap()));
        assert_eq!(
            network.address_plus(10).unwrap(),
            "10.42.0.10".parse::<IpAddr>().unwrap()
        );
        assert!(network.is_private());
        assert!(!Network::parse("8.8.8.0/24").unwrap().is_private());
        assert!(Network::parse("10.42.0.1/16").is_err());
    }
}
