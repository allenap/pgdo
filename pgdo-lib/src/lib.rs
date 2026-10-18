#![doc = include_str!("../README.md")]
#![warn(clippy::pedantic)]
#![warn(unreachable_pub)]
#![allow(clippy::enum_glob_use)]
#![allow(clippy::many_single_char_names)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::needless_continue)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::redundant_else)]

pub mod cluster;
mod lock;
pub mod runtime;
pub mod util;
pub mod version;
