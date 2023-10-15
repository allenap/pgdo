use std::collections::VecDeque;
use std::env;
use std::path::{Path, PathBuf};

use super::{constraint::Constraint, Runtime};

pub type Runtimes<'a> = Box<dyn Iterator<Item = Runtime> + 'a>;

/// A strategy for finding PostgreSQL runtimes.
///
/// There are a few questions we want to answer:
///
/// 1. What runtimes are available?
/// 2. Which of those runtimes is best suited to running a given cluster?
/// 3. When there are no constraints, what runtime should we use?
///
/// This trait models those questions, and provides default implementations for
/// #2 and #3.
///
/// However, a good place to start is the [`Default`] implementation of
/// [`Strategy`]. It might do what you need.
pub trait StrategyLike: std::fmt::Debug + std::panic::RefUnwindSafe + 'static {
    /// Find all runtimes that this strategy knows about.
    fn runtimes(&self) -> Runtimes;

    /// Determine the most appropriate runtime known to this strategy for the
    /// given constraint.
    ///
    /// The default implementation narrows the list of runtimes to those that
    /// match the given constraint, then chooses the one with the highest
    /// version number. It might return [`None`].
    fn select(&self, constraint: &Constraint) -> Option<Runtime> {
        self.runtimes()
            .filter(|runtime| constraint.matches(runtime))
            .max_by(|ra, rb| ra.version.cmp(&rb.version))
    }

    /// The runtime to use when there are no constraints, e.g. when creating a
    /// new cluster.
    ///
    /// The default implementation selects the runtime with the highest version
    /// number.
    fn fallback(&self) -> Option<Runtime> {
        self.runtimes().max_by(|ra, rb| ra.version.cmp(&rb.version))
    }
}

/// Find runtimes on a given path.
///
/// Parses input according to platform conventions for the `PATH` environment
/// variable. See [`env::split_paths`] for details.
#[derive(Clone, Debug)]
pub struct RuntimesOnPath(PathBuf);

impl StrategyLike for RuntimesOnPath {
    fn runtimes(&self) -> Runtimes {
        Box::new(
            env::split_paths(&self.0)
                .filter(|bindir| bindir.join("pg_ctl").exists())
                // Throw away runtimes that we can't determine the version for.
                .filter_map(|bindir| Runtime::new(bindir).ok()),
        )
    }
}

/// Find runtimes on `PATH` (from the environment).
#[derive(Clone, Debug)]
pub struct RuntimesOnPathEnv;

impl StrategyLike for RuntimesOnPathEnv {
    fn runtimes(&self) -> Runtimes {
        Box::new(
            env::var_os("PATH")
                .map(|path| {
                    env::split_paths(&path)
                        .filter(|bindir| bindir.join("pg_ctl").exists())
                        // Throw away runtimes that we can't determine the version for.
                        .filter_map(|bindir| Runtime::new(bindir).ok())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
                .into_iter(),
        )
    }
}

/// Find runtimes using platform-specific knowledge.
///
/// For example:
/// - on Debian and Ubuntu, check subdirectories of `/usr/lib/postgresql`.
/// - on macOS, check Homebrew.
///
/// More platform-specific knowledge may be added to this strategy in the
/// future.
#[derive(Clone, Debug)]
pub struct RuntimesOnPlatform;

impl RuntimesOnPlatform {
    /// Find runtimes using platform-specific knowledge (Linux).
    ///
    /// For example: on Debian and Ubuntu, check `/usr/lib/postgresql`.
    #[cfg(any(doc, target_os = "linux"))]
    pub fn find() -> Vec<PathBuf> {
        glob::glob("/usr/lib/postgresql/*/bin/pg_ctl")
            .ok()
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|path| path.is_file())
                    .filter_map(|path| path.parent().map(Path::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find runtimes using platform-specific knowledge (macOS).
    ///
    /// For example: check Homebrew.
    #[cfg(any(doc, target_os = "macos"))]
    pub fn find() -> Vec<PathBuf> {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        std::process::Command::new("brew")
            .arg("--prefix")
            .output()
            .ok()
            .and_then(|output| {
                if output.status.success() {
                    Some(OsString::from_vec(output.stdout))
                } else {
                    None
                }
            })
            .and_then(|brew_prefix| {
                glob::glob(&format!(
                    "{}/Cellar/postgresql@*/*/bin/pg_ctl",
                    brew_prefix.to_string_lossy().trim_end()
                ))
                .ok()
            })
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|path| path.is_file())
                    .filter_map(|path| path.parent().map(Path::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl StrategyLike for RuntimesOnPlatform {
    fn runtimes(&self) -> Runtimes {
        Box::new(
            Self::find()
                .into_iter()
                // Throw away runtimes that we can't determine the version for.
                .filter_map(|bindir| Runtime::new(bindir).ok()),
        )
    }
}

/// Compose strategies for finding PostgreSQL runtimes.
#[derive(Debug)]
pub enum Strategy {
    /// Each strategy is consulted in turn.
    Chain(VecDeque<Strategy>),
    /// Delegate to another strategy; needed when implementing [`StrategyLike`].
    Delegated(Box<dyn StrategyLike>),
    /// A single runtime; it always picks itself.
    Single(Runtime),
}

impl Strategy {
    /// Push the given strategy to the front of the chain.
    ///
    /// If this isn't already, it is converted into a [`Strategy::Chain`].
    #[must_use]
    pub fn push_front<S: Into<Strategy>>(mut self, strategy: S) -> Self {
        match self {
            Self::Chain(ref mut chain) => {
                chain.push_front(strategy.into());
                self
            }
            Self::Delegated(_) | Self::Single(_) => {
                let mut chain: VecDeque<Strategy> = VecDeque::new();
                chain.push_front(strategy.into());
                chain.push_back(self);
                Self::Chain(chain)
            }
        }
    }

    /// Push the given strategy to the back of the chain.
    ///
    /// If this isn't already, it is converted into a [`Strategy::Chain`].
    #[must_use]
    pub fn push_back<S: Into<Strategy>>(mut self, strategy: S) -> Self {
        match self {
            Self::Chain(ref mut chain) => {
                chain.push_back(strategy.into());
                self
            }
            Self::Delegated(_) | Self::Single(_) => {
                let mut chain: VecDeque<Strategy> = VecDeque::new();
                chain.push_front(self);
                chain.push_back(strategy.into());
                Self::Chain(chain)
            }
        }
    }
}

impl Default for Strategy {
    /// Select runtimes from on `PATH` followed by platform-specific runtimes.
    fn default() -> Self {
        Self::Chain(VecDeque::new())
            .push_front(RuntimesOnPathEnv)
            .push_back(RuntimesOnPlatform)
    }
}

impl StrategyLike for Strategy {
    /// - For a [`Strategy::Chain`], yields runtimes known to all strategies, in
    ///   the same order as each strategy returns them.
    /// - For a [`Strategy::Delegated`], calls through to the wrapped strategy.
    /// - For a [`Strategy::Single`], yields the runtime it's holding.
    ///
    /// **Note** that for the first two, runtimes are deduplicated by version
    /// number, i.e. if a runtime with the same version number is yielded by
    /// multiple strategies, or is yielded multiple times by a single strategy,
    /// it will only be returned the first time it is seen.
    fn runtimes(&self) -> Runtimes {
        match self {
            Self::Chain(chain) => {
                let mut seen = std::collections::HashSet::new();
                Box::new(
                    chain
                        .iter()
                        .flat_map(|strategy| strategy.runtimes())
                        .filter(move |runtime| seen.insert(runtime.version)),
                )
            }
            Self::Delegated(strategy) => {
                let mut seen = std::collections::HashSet::new();
                Box::new(
                    strategy
                        .runtimes()
                        .filter(move |runtime| seen.insert(runtime.version)),
                )
            }
            Self::Single(runtime) => Box::new(std::iter::once(runtime.clone())),
        }
    }

    /// - For a [`Strategy::Chain`], asks each strategy in turn to select a
    ///   runtime. The first non-[`None`] answer is selected.
    /// - For a [`Strategy::Delegated`], calls through to the wrapped strategy.
    /// - For a [`Strategy::Single`], returns the runtime if it's compatible.
    fn select(&self, constraint: &Constraint) -> Option<Runtime> {
        match self {
            Self::Chain(c) => c.iter().find_map(|strategy| strategy.select(constraint)),
            Self::Delegated(strategy) => strategy.select(constraint),
            Self::Single(runtime) if constraint.matches(runtime) => Some(runtime.clone()),
            Self::Single(_) => None,
        }
    }

    /// - For a [`Strategy::Chain`], asks each strategy in turn for a fallback
    ///   runtime. The first non-[`None`] answer is selected.
    /// - For a [`Strategy::Delegated`], calls through to the wrapped strategy.
    /// - For a [`Strategy::Single`], returns the runtime it's holding.
    fn fallback(&self) -> Option<Runtime> {
        match self {
            Self::Chain(chain) => chain.iter().find_map(Strategy::fallback),
            Self::Delegated(strategy) => strategy.fallback(),
            Self::Single(runtime) => Some(runtime.clone()),
        }
    }
}

impl From<RuntimesOnPath> for Strategy {
    /// Converts the given strategy into a [`Strategy::Delegated`].
    fn from(strategy: RuntimesOnPath) -> Self {
        Self::Delegated(Box::new(strategy))
    }
}

impl From<RuntimesOnPathEnv> for Strategy {
    /// Converts the given strategy into a [`Strategy::Delegated`].
    fn from(strategy: RuntimesOnPathEnv) -> Self {
        Self::Delegated(Box::new(strategy))
    }
}

impl From<RuntimesOnPlatform> for Strategy {
    /// Converts the given strategy into a [`Strategy::Delegated`].
    fn from(strategy: RuntimesOnPlatform) -> Self {
        Self::Delegated(Box::new(strategy))
    }
}

impl From<Runtime> for Strategy {
    /// Converts the given runtime into a [`Strategy::Single`].
    fn from(runtime: Runtime) -> Self {
        Self::Single(runtime)
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::{RuntimesOnPath, RuntimesOnPathEnv, RuntimesOnPlatform, Strategy, StrategyLike};

    /// This will fail if there are no PostgreSQL runtimes installed.
    #[test]
    fn runtime_find_custom_path() {
        let path = env::var_os("PATH").expect("PATH not set");
        let strategy = RuntimesOnPath(path.into());
        let runtimes = strategy.runtimes();
        assert_ne!(0, runtimes.count());
    }

    /// This will fail if there are no PostgreSQL runtimes installed.
    #[test]
    fn runtime_find_env_path() {
        let runtimes = RuntimesOnPathEnv.runtimes();
        assert_ne!(0, runtimes.count());
    }

    /// This will fail if there are no PostgreSQL runtimes installed.
    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn runtime_find_on_platform() {
        let runtimes = RuntimesOnPlatform.runtimes();
        assert_ne!(0, runtimes.count());
    }

    /// This will fail if there are no PostgreSQL runtimes installed. It's also
    /// somewhat fragile because it relies upon knowing the implementation of
    /// the strategies of which the default [`StrategySet`] is composed.
    #[test]
    fn runtime_strategy_set_default() {
        let strategy = Strategy::default();
        // There is at least one runtime available.
        let runtimes = strategy.runtimes();
        assert_ne!(0, runtimes.count());
        // There is always a fallback.
        assert!(strategy.fallback().is_some());
    }
}
