//! Parse PostgreSQL version numbers.
//!
//! ```rust
//! # use pgdo::version::Version;
//! assert_eq!(Ok(Version::Pre10(9, 6, 17)), "9.6.17".parse());
//! assert_eq!(Ok(Version::Post10(14, 6)), "14.6".parse());
//! ```
//!
//! See the [PostgreSQL "Versioning Policy" page][versioning] for information on
//! PostgreSQL's versioning scheme.
//!
//! [versioning]: https://www.postgresql.org/support/versioning/

// TODO: Parse `server_version_num`/`PG_VERSION_NUM`, e.g. 120007 for version
// 12.7, 90624 for 9.6.24. See https://pgpedia.info/s/server_version_num.html
// and https://www.postgresql.org/docs/16/runtime-config-preset.html.

use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use regex::Regex;

use super::VersionError;

/// Represents a full PostgreSQL version. This is the kind of thing we see when
/// running `pg_ctl --version` for example.
///
/// The "Current minor" column shown on the [PostgreSQL "Versioning Policy"
/// page][versioning] is what this models.
///
/// [versioning]: https://www.postgresql.org/support/versioning/
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Version {
    /// Pre-PostgreSQL 10, with major, point, and minor version numbers, e.g.
    /// 9.6.17. It is an error to create this variant with a major number >= 10.
    Pre10(u32, u32, u32),
    /// PostgreSQL 10+, with major and minor version number, e.g. 10.3. It is an
    /// error to create this variant with a major number < 10.
    Post10(u32, u32),
}

impl fmt::Display for Version {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Version::Pre10(a, b, c) => fmt.pad(&format!("{a}.{b}.{c}")),
            Version::Post10(a, b) => fmt.pad(&format!("{a}.{b}")),
        }
    }
}

impl FromStr for Version {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"(?x) \b (\d+) [.] (\d+) (?: [.] (\d+) )? \b")
                .expect("invalid regex (for matching PostgreSQL versions)")
        });
        let badly_formed = |_| VersionError::BadlyFormed { text: Some(s.into()) };
        match RE.captures(s) {
            Some(caps) => {
                let a = caps[1].parse::<u32>().map_err(badly_formed)?;
                let b = caps[2].parse::<u32>().map_err(badly_formed)?;
                match caps.get(3) {
                    None if a >= 10 => Ok(Version::Post10(a, b)),
                    None => Err(VersionError::BadlyFormed { text: Some(s.into()) }),
                    Some(_) if a >= 10 => Err(VersionError::BadlyFormed { text: Some(s.into()) }),
                    Some(m) => Ok(m
                        .as_str()
                        .parse::<u32>()
                        .map(|c| Version::Pre10(a, b, c))
                        .map_err(badly_formed)?),
                }
            }
            None => Err(VersionError::NotFound { text: Some(s.into()) }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Version::{Post10, Pre10};
    use super::{Version, VersionError::*};

    use std::cmp::Ordering;

    #[test]
    fn parses_version_below_10() {
        assert_eq!(Ok(Pre10(9, 6, 17)), "9.6.17".parse());
    }

    #[test]
    fn parses_version_above_10() {
        assert_eq!(Ok(Post10(12, 2)), "12.2".parse());
    }

    #[test]
    fn parse_returns_error_when_version_is_invalid() {
        // 4294967295 is (2^32 + 1), so won't fit in a u32.
        assert!(matches!(
            "4294967296.0".parse::<Version>(),
            Err(BadlyFormed { .. })
        ));
    }

    #[test]
    fn parse_returns_error_when_version_not_found() {
        assert!(matches!("foo".parse::<Version>(), Err(NotFound { .. })));
    }

    #[test]
    fn displays_version_below_10() {
        assert_eq!("9.6.17", format!("{}", Pre10(9, 6, 17)));
    }

    #[test]
    fn displays_version_above_10() {
        assert_eq!("12.2", format!("{}", Post10(12, 2)));
    }

    #[test]
        #[rustfmt::skip]
        fn derive_partial_ord_works_as_expected() {
            assert_eq!(Pre10(9, 10, 11).partial_cmp(&Post10(10, 11)), Some(Ordering::Less));
            assert_eq!(Post10(10, 11).partial_cmp(&Pre10(9, 10, 11)), Some(Ordering::Greater));
            assert_eq!(Pre10(9, 10, 11).partial_cmp(&Pre10(9, 10, 11)), Some(Ordering::Equal));
            assert_eq!(Post10(10, 11).partial_cmp(&Post10(10, 11)), Some(Ordering::Equal));
        }

    #[test]
    fn derive_ord_works_as_expected() {
        let mut versions = vec![
            Pre10(9, 10, 11),
            Post10(10, 11),
            Post10(14, 2),
            Pre10(9, 10, 12),
            Post10(10, 12),
        ];
        versions.sort(); // Uses `Ord`.
        assert_eq!(
            versions,
            vec![
                Pre10(9, 10, 11),
                Pre10(9, 10, 12),
                Post10(10, 11),
                Post10(10, 12),
                Post10(14, 2)
            ]
        );
    }
}
