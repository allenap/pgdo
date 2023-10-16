use std::{fmt, str::FromStr};

use postgres_protocol::escape::{escape_identifier, escape_literal};

use super::postgres;

/// Reload configuration using `pg_reload_conf`. Equivalent to `SIGHUP` or
/// `pg_ctl reload`.
pub fn reload(client: &mut postgres::Client) -> Result<(), postgres::Error> {
    client.execute("SELECT pg_reload_conf()", &[])?;
    Ok(())
}

pub enum AlterSystem {
    Set(Parameter, Value),
    Reset(Parameter),
    ResetAll,
}

impl AlterSystem {
    /// Alter the system. Changes made by `ALTER SYSTEM` may require a reload or
    /// even a full restart to take effect.
    pub fn apply(&self, client: &mut postgres::Client) -> Result<(), postgres::Error> {
        let command = self.to_string();
        client.execute(&command, &[])?;
        Ok(())
    }
}

impl fmt::Display for AlterSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterSystem::Set(p, v) => write!(f, "ALTER SYSTEM SET {p} TO {v}"),
            AlterSystem::Reset(p) => write!(f, "ALTER SYSTEM RESET {p}"),
            AlterSystem::ResetAll => write!(f, "ALTER SYSTEM RESET ALL"),
        }
    }
}

/// A setting as defined in `pg_catalog.pg_settings`.
///
/// This is fairly stringly-typed and mostly informational. For getting and
/// setting values, [`Parameter`] and [`Value`] may be more convenient.
#[derive(Debug, Clone)]
pub struct Setting {
    pub name: String,    // Never `NULL`.
    pub setting: String, // Never `NULL`.
    pub unit: Option<String>,
    pub category: String,   // Never `NULL`.
    pub short_desc: String, // Never `NULL`.
    pub extra_desc: Option<String>,
    pub context: String, // Never `NULL`.
    pub vartype: String, // Never `NULL`.
    pub source: String,  // Never `NULL`.
    pub min_val: Option<String>,
    pub max_val: Option<String>,
    pub enumvals: Option<Vec<String>>,
    pub boot_val: Option<String>,
    pub reset_val: Option<String>,
    pub sourcefile: Option<String>,
    pub sourceline: Option<i32>,
    pub pending_restart: bool, // Never `NULL`.
}

impl Setting {
    pub async fn list(pool: &sqlx::PgPool) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as!(
            Setting,
            r#"
            SELECT
                name "name!",
                setting "setting!",
                unit,
                category "category!",
                short_desc "short_desc!",
                extra_desc,
                context "context!",
                vartype "vartype!",
                source "source!",
                min_val,
                max_val,
                enumvals,
                boot_val,
                reset_val,
                sourcefile,
                sourceline,
                pending_restart "pending_restart!"
            FROM
                pg_catalog.pg_settings
            "#
        )
        .fetch_all(pool)
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Parameter(String);

impl Parameter {
    /// Get the current value for this parameter.
    pub fn get(&self, client: &mut postgres::Client) -> Result<Option<Value>, postgres::Error> {
        let query = r#"SELECT setting, unit, vartype FROM pg_settings WHERE name = $1"#;
        match client.query_opt(query, &[&self.0])? {
            None => Ok(None),
            Some(row) => {
                let setting: Option<String> = row.try_get("setting")?;
                let setting = match setting {
                    None => None,
                    Some(setting) => {
                        let unit: Option<&str> = row.try_get("unit")?;
                        let vartype: &str = row.try_get("vartype")?;
                        Some(match vartype {
                            // on, off, true, false, yes, no, 1, 0 (or any unambiguous prefix).
                            "bool" => match setting.as_ref() {
                                "on" | "true" | "tru" | "tr" | "t" | "yes" | "ye" | "y" | "1" => {
                                    Value::Boolean(true)
                                }
                                "off" | "of" | "false" | "fals" | "fal" | "fa" | "f" | "no"
                                | "n" | "0" => Value::Boolean(false),
                                _ => unreachable!(),
                            },
                            "enum" => Value::String(setting),
                            "integer" | "real" => match unit {
                                None => Value::Number(setting),
                                Some("8kB") => Value::Number(setting), // Special case.
                                Some(unit) => {
                                    if let Ok(unit) = unit.parse::<MemoryUnit>() {
                                        Value::Memory(setting, unit)
                                    } else if let Ok(unit) = unit.parse::<TimeUnit>() {
                                        Value::Time(setting, unit)
                                    } else {
                                        unreachable!()
                                    }
                                }
                            },
                            "string" => Value::String(setting),
                            _ => unreachable!(),
                        })
                    }
                };
                Ok(setting)
            }
        }
    }

    /// Set the current value for this parameter.
    pub fn set<V: Into<Value>>(
        &self,
        client: &mut postgres::Client,
        value: V,
    ) -> Result<(), postgres::Error> {
        AlterSystem::Set(self.clone(), value.into()).apply(client)?;
        Ok(())
    }

    /// Reset the value for this parameter.
    pub fn reset(&self, client: &mut postgres::Client) -> Result<(), postgres::Error> {
        AlterSystem::Reset(self.clone()).apply(client)?;
        Ok(())
    }
}

impl fmt::Display for Parameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", escape_identifier(&self.0))
    }
}

impl From<&str> for Parameter {
    fn from(name: &str) -> Self {
        Self(name.to_owned())
    }
}

impl From<Setting> for Parameter {
    fn from(setting: Setting) -> Self {
        Self(setting.name)
    }
}

impl From<&Setting> for Parameter {
    fn from(setting: &Setting) -> Self {
        Self(setting.name.clone())
    }
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Boolean(bool),
    String(String), // Or enumerated.
    Number(String),
    Memory(String, MemoryUnit),
    Time(String, TimeUnit),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Boolean(value) => write!(f, "{value}"),
            Value::String(value) => write!(f, "{}", escape_literal(value)),
            Value::Number(value) => write!(f, "{value}"),
            Value::Memory(value, unit) => {
                let value = format!("{value}{unit}");
                write!(f, "{}", escape_literal(&value))
            }
            Value::Time(value, unit) => {
                let value = format!("{value}{unit}");
                write!(f, "{}", escape_literal(&value))
            }
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

macro_rules! value_number_from {
    ($($from_type:ty),*) => {
        $(
            impl From<$from_type> for Value {
                fn from(number: $from_type) -> Self {
                    Value::Number(number.to_string())
                }
            }
        )*
    }
}

value_number_from!(i8, i16, i32, i64, i128);
value_number_from!(u8, u16, u32, u64, u128);
value_number_from!(f32, f64);
value_number_from!(usize, isize);

macro_rules! value_memory_from {
    ($($from_type:ty),*) => {
        $(
            impl From<($from_type, MemoryUnit)> for Value {
                fn from((number, unit): ($from_type, MemoryUnit)) -> Self {
                    Value::Memory(number.to_string(), unit)
                }
            }
        )*
    }
}

value_memory_from!(i8, i16, i32, i64, i128);
value_memory_from!(u8, u16, u32, u64, u128);
value_memory_from!(f32, f64);
value_memory_from!(usize, isize);

macro_rules! value_time_from {
    ($($from_type:ty),*) => {
        $(
            impl From<($from_type, TimeUnit)> for Value {
                fn from((number, unit): ($from_type, TimeUnit)) -> Self {
                    Value::Time(number.to_string(), unit)
                }
            }
        )*
    }
}

value_time_from!(i8, i16, i32, i64, i128);
value_time_from!(u8, u16, u32, u64, u128);
value_time_from!(f32, f64);
value_time_from!(usize, isize);

impl From<&Setting> for Value {
    fn from(setting: &Setting) -> Self {
        match setting.vartype.as_ref() {
            "bool" => match setting.setting.as_ref() {
                "on" | "true" | "tru" | "tr" | "t" => Self::Boolean(true),
                "yes" | "ye" | "y" | "1" => Self::Boolean(true),
                "off" | "of" | "false" | "fals" | "fal" | "fa" | "f" => Self::Boolean(false),
                "no" | "n" | "0" => Self::Boolean(false),
                _ => unreachable!(),
            },
            "integer" | "real" => match setting.unit.as_deref() {
                None => Self::Number(setting.setting.clone()),
                Some("8kB") => Self::Number(setting.setting.clone()), // Special case.
                Some(unit) => {
                    if let Ok(unit) = unit.parse::<MemoryUnit>() {
                        Self::Memory(setting.setting.clone(), unit)
                    } else if let Ok(unit) = unit.parse::<TimeUnit>() {
                        Self::Time(setting.setting.clone(), unit)
                    } else {
                        unreachable!()
                    }
                }
            },
            "string" => Self::String(setting.setting.clone()),
            "enum" => Self::String(setting.setting.clone()),
            _ => unreachable!(),
        }
    }
}

/// Memory units recognised in PostgreSQL parameter values.
/// <https://www.postgresql.org/docs/16/config-setting.html#CONFIG-SETTING-NAMES-VALUES>
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryUnit {
    Bytes,
    Kibibytes,
    Mebibytes,
    Gibibytes,
    Tebibytes,
}

impl fmt::Display for MemoryUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryUnit::Bytes => write!(f, "B"),
            MemoryUnit::Kibibytes => write!(f, "kB"),
            MemoryUnit::Mebibytes => write!(f, "MB"),
            MemoryUnit::Gibibytes => write!(f, "GB"),
            MemoryUnit::Tebibytes => write!(f, "TB"),
        }
    }
}

impl FromStr for MemoryUnit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "B" => Ok(MemoryUnit::Bytes),
            "kB" => Ok(MemoryUnit::Kibibytes),
            "MB" => Ok(MemoryUnit::Mebibytes),
            "GB" => Ok(MemoryUnit::Gibibytes),
            "TB" => Ok(MemoryUnit::Tebibytes),
            _ => Err(format!("invalid memory unit: {s:?}")),
        }
    }
}

/// Time units recognised in PostgreSQL parameter values.
/// <https://www.postgresql.org/docs/16/config-setting.html#CONFIG-SETTING-NAMES-VALUES>
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeUnit {
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeUnit::Microseconds => write!(f, "us"),
            TimeUnit::Milliseconds => write!(f, "ms"),
            TimeUnit::Seconds => write!(f, "s"),
            TimeUnit::Minutes => write!(f, "min"),
            TimeUnit::Hours => write!(f, "h"),
            TimeUnit::Days => write!(f, "d"),
        }
    }
}

impl FromStr for TimeUnit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "us" => Ok(TimeUnit::Microseconds),
            "ms" => Ok(TimeUnit::Milliseconds),
            "s" => Ok(TimeUnit::Seconds),
            "min" => Ok(TimeUnit::Minutes),
            "h" => Ok(TimeUnit::Hours),
            "d" => Ok(TimeUnit::Days),
            _ => Err(format!("invalid time unit: {s:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use paste::paste;

    use super::{
        MemoryUnit::{self, *},
        Parameter,
        TimeUnit::{self, *},
        Value,
    };

    #[test]
    fn test_parameter_display() {
        assert_eq!(format!("{}", Parameter::from("foo")), "\"foo\"");
        assert_eq!(format!("{}", Parameter::from("foo \\bar")), "\"foo \\bar\"");
        assert_eq!(format!("{}", Parameter::from("foo\"bar")), "\"foo\"\"bar\"");
    }

    #[test]
    fn test_value_display_bool() {
        assert_eq!(format!("{}", Value::Boolean(false)), "false");
        assert_eq!(format!("{}", Value::Boolean(true)), "true");
    }

    #[test]
    fn test_value_display_string() {
        assert_eq!(format!("{}", Value::from("foo")), "'foo'");
        assert_eq!(format!("{}", Value::from("foo \\bar")), " E'foo \\\\bar'");
        assert_eq!(format!("{}", Value::from("foo'\"'bar")), "'foo''\"''bar'");
    }

    #[test]
    fn test_value_display_number() {
        // Numbers are represented as strings, and displayed verbatim, with no
        // escaping. Not ideal. An alternative would be to have signed/unsigned
        // integers (as i128/u128) and floating points (as f64) separately. But
        // PostgreSQL also has arbitrary precision numbers. For now, we'll live
        // with this.
        assert_eq!(format!("{}", Value::Number("123".into())), "123");
        assert_eq!(format!("{}", Value::Number("123.456".into())), "123.456");
    }

    #[test]
    fn test_value_display_memory() {
        assert_eq!(
            format!("{}", Value::Memory("123.4".into(), Gibibytes)),
            "'123.4GB'",
        );
    }

    #[test]
    fn test_value_display_time() {
        assert_eq!(
            format!("{}", Value::Time("123.4".into(), Hours)),
            "'123.4h'",
        );
    }

    macro_rules! test_value_number_from {
        ($($from_type:ty),*) => {
            $(
                paste! {
                    #[test]
                    fn [< test_value_number_from_ $from_type >]() {
                        assert_eq!(Value::from(42 as $from_type), Value::Number("42".into()));
                    }
                }
            )*
        }
    }

    test_value_number_from!(i8, i16, i32, i64, i128);
    test_value_number_from!(u8, u16, u32, u64, u128);
    test_value_number_from!(f32, f64);
    test_value_number_from!(usize, isize);

    #[test]
    fn test_memory_unit_roundtrip() {
        let units = &[Bytes, Kibibytes, Mebibytes, Gibibytes, Tebibytes];
        for unit in units {
            assert_eq!(format!("{unit}").parse::<MemoryUnit>(), Ok(*unit));
        }
    }

    #[test]
    fn test_time_unit_roundtrip() {
        let units = &[Microseconds, Milliseconds, Seconds, Minutes, Hours, Days];
        for unit in units {
            assert_eq!(format!("{unit}").parse::<TimeUnit>(), Ok(*unit));
        }
    }
}
