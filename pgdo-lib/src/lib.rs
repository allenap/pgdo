#![doc = include_str!("../README.md")]
#![warn(clippy::pedantic)]
#![allow(clippy::enum_glob_use)]
#![allow(clippy::many_single_char_names)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

#[macro_use]
extern crate lazy_static;

pub mod cluster;
pub mod coordinate;
pub mod lock;
pub mod prelude;
pub mod runtime;
pub mod util;
pub mod version;
