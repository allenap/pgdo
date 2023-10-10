use globset::{self, Glob, GlobBuilder, GlobMatcher};

use crate::version::{PartialVersion, VersionError};

use super::Runtime;

/// A constraint used when selecting a PostgreSQL runtime.
#[derive(Clone, Debug)]
pub enum Constraint {
    /// Match the runtime's `bindir`.
    BinDir(GlobMatcher),
    /// Match the given version.
    Version(PartialVersion),
    /// Either constraint can be satisfied.
    Either(Box<Constraint>, Box<Constraint>),
    /// Both constraints must be satisfied.
    Both(Box<Constraint>, Box<Constraint>),
    /// Invert the given constraint; use `!constraint` for the same effect.
    Not(Box<Constraint>),
    /// Match any runtime.
    Anything,
    /// Match no runtimes at all.
    Nothing,
}

impl Constraint {
    /// Match the given runtime's `bindir` against this glob pattern.
    ///
    /// The [syntax](https://docs.rs/globset/latest/globset/index.html#syntax)
    /// comes from the [globset](https://crates.io/crates/globset) crate.
    /// However, here we deviate from its default rules:
    ///
    /// - `*` and `?` do **not** match path separators (`/`); use `**` for that.
    /// - empty alternators, e.g. `{,.rs}` are allowed.
    ///
    /// Use [`glob`][`Self::glob`] if you want to select your own rules.
    pub fn path(pattern: &str) -> Result<Self, globset::Error> {
        Ok(Self::BinDir(
            GlobBuilder::new(pattern)
                .literal_separator(true)
                .empty_alternates(true)
                .build()?
                .compile_matcher(),
        ))
    }

    /// Match the given runtime's `bindir` against this glob.
    pub fn glob(glob: &Glob) -> Self {
        Self::BinDir(glob.compile_matcher())
    }

    /// Match the given runtime against this version.
    pub fn version(version: &str) -> Result<Self, VersionError> {
        Ok(Self::Version(version.parse()?))
    }

    /// Match **any** of the given constraints.
    pub fn any<C: IntoIterator<Item = Constraint>>(constraints: C) -> Self {
        constraints.into_iter().fold(Self::Nothing, |a, b| a | b)
    }

    /// Match **all** of the given constraints.
    pub fn all<C: IntoIterator<Item = Constraint>>(constraints: C) -> Self {
        constraints.into_iter().fold(Self::Anything, |a, b| a & b)
    }

    /// Does the given runtime match this constraint?
    pub fn matches(&self, runtime: &Runtime) -> bool {
        match self {
            Self::BinDir(matcher) => matcher.is_match(&runtime.bindir),
            Self::Version(version) => version.compatible(runtime.version),
            Self::Either(ca, cb) => ca.matches(runtime) || cb.matches(runtime),
            Self::Both(ca, cb) => ca.matches(runtime) && cb.matches(runtime),
            Self::Not(constraint) => !constraint.matches(runtime),
            Self::Anything => true,
            Self::Nothing => false,
        }
    }
}

impl std::ops::Not for Constraint {
    type Output = Self;

    /// Invert this constraint.
    fn not(self) -> Self::Output {
        match self {
            Self::Anything => Self::Nothing,
            Self::Nothing => Self::Anything,
            Self::Not(constraint) => *constraint,
            _ => Self::Not(Box::new(self)),
        }
    }
}

impl std::ops::BitOr for Constraint {
    type Output = Self;

    /// Match either of the constraints.
    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Anything, _) | (_, Self::Anything) => Self::Anything,
            (Self::Nothing, c) | (c, Self::Nothing) => c,
            (ca, cb) => Self::Either(Box::new(ca), Box::new(cb)),
        }
    }
}

impl std::ops::BitAnd for Constraint {
    type Output = Self;

    /// Match both the constraints.
    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Anything, c) | (c, Self::Anything) => c,
            (Self::Nothing, _) | (_, Self::Nothing) => Self::Nothing,
            (ca, cb) => Self::Both(Box::new(ca), Box::new(cb)),
        }
    }
}

impl From<PartialVersion> for Constraint {
    /// Convert a [`PartialVersion`] into a [`Constraint::Version`].
    fn from(version: PartialVersion) -> Self {
        Self::Version(version)
    }
}

#[cfg(test)]
mod test_constraints {
    use super::Constraint;
    use super::PartialVersion;

    /// An example constraint.
    const CONSTRAINT: Constraint = Constraint::Version(PartialVersion::Post10m(13));

    #[test]
    fn test_not() {
        let c1 = Constraint::Version(PartialVersion::Post10m(13));
        assert!(matches!(c1, Constraint::Version(_)));
        let c2 = !c1;
        assert!(matches!(c2, Constraint::Not(_)));
        let c3 = !c2;
        assert!(matches!(c3, Constraint::Version(_)));
    }

    #[test]
    fn test_not_anything_and_nothing() {
        let c1 = Constraint::Anything;
        let c2 = !c1;
        assert!(matches!(c2, Constraint::Nothing));
        let c3 = !c2;
        assert!(matches!(c3, Constraint::Anything));
    }

    #[test]
    fn test_or() {
        assert!(matches!(
            Constraint::Anything | CONSTRAINT.clone(),
            Constraint::Anything
        ));
        assert!(matches!(
            CONSTRAINT.clone() | Constraint::Anything,
            Constraint::Anything
        ));
        assert!(matches!(
            Constraint::Nothing | CONSTRAINT.clone(),
            Constraint::Version(_)
        ));
        assert!(matches!(
            CONSTRAINT.clone() | Constraint::Nothing,
            Constraint::Version(_)
        ));
    }

    #[test]
    fn test_or_anything_and_nothing() {
        assert!(matches!(
            Constraint::Anything | Constraint::Anything,
            Constraint::Anything
        ));
        assert!(matches!(
            Constraint::Nothing | Constraint::Anything,
            Constraint::Anything
        ));
        assert!(matches!(
            Constraint::Anything | Constraint::Nothing,
            Constraint::Anything
        ));
    }

    #[test]
    fn test_and() {
        assert!(matches!(
            Constraint::Anything & CONSTRAINT.clone(),
            Constraint::Version(_)
        ));
        assert!(matches!(
            CONSTRAINT.clone() & Constraint::Anything,
            Constraint::Version(_)
        ));
        assert!(matches!(
            Constraint::Nothing & CONSTRAINT.clone(),
            Constraint::Nothing
        ));
        assert!(matches!(
            CONSTRAINT.clone() & Constraint::Nothing,
            Constraint::Nothing
        ));
    }

    #[test]
    fn test_and_anything_and_nothing() {
        assert!(matches!(
            Constraint::Anything & Constraint::Anything,
            Constraint::Anything
        ));
        assert!(matches!(
            Constraint::Nothing & Constraint::Anything,
            Constraint::Nothing
        ));
        assert!(matches!(
            Constraint::Anything & Constraint::Nothing,
            Constraint::Nothing
        ));
    }

    #[test]
    fn test_any() {
        assert!(matches!(
            Constraint::any([
                Constraint::Anything,
                Constraint::Nothing,
                Constraint::Nothing
            ]),
            Constraint::Anything
        ));
        assert!(matches!(
            Constraint::any([Constraint::Nothing, CONSTRAINT.clone(), Constraint::Nothing]),
            Constraint::Version(_)
        ));
        assert!(matches!(
            Constraint::any([
                Constraint::Anything,
                CONSTRAINT.clone(),
                Constraint::Nothing
            ]),
            Constraint::Anything
        ));
        assert!(matches!(
            Constraint::any([CONSTRAINT.clone(), CONSTRAINT.clone()]),
            Constraint::Either(ca, cb)
                if matches!(*ca, Constraint::Version(_))
                && matches!(*cb, Constraint::Version(_))
        ));
    }

    #[test]
    fn test_all() {
        assert!(matches!(
            Constraint::all([
                Constraint::Anything,
                Constraint::Anything,
                Constraint::Anything
            ]),
            Constraint::Anything
        ));
        assert!(matches!(
            Constraint::all([
                Constraint::Anything,
                CONSTRAINT.clone(),
                Constraint::Anything,
            ]),
            Constraint::Version(_),
        ));
        assert!(matches!(
            Constraint::all([
                Constraint::Anything,
                CONSTRAINT.clone(),
                Constraint::Nothing,
            ]),
            Constraint::Nothing,
        ));
        assert!(matches!(
            Constraint::all([CONSTRAINT.clone(), CONSTRAINT.clone()]),
            Constraint::Both(ca, cb)
                if matches!(*ca, Constraint::Version(_))
                && matches!(*cb, Constraint::Version(_))
        ));
    }
}
