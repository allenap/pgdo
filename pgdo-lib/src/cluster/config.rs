use std::{fmt, str::FromStr};

use postgres_protocol::escape::{escape_identifier, escape_literal};

use super::sqlx;

/// Reload configuration using `pg_reload_conf`. Equivalent to `SIGHUP` or
/// `pg_ctl reload`.
pub async fn reload(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_reload_conf()").execute(pool).await?;
    Ok(())
}

pub enum AlterSystem<'a> {
    Set(&'a Parameter<'a>, &'a Value),
    Reset(&'a Parameter<'a>),
    ResetAll,
}

impl<'a> AlterSystem<'a> {
    /// Alter the system. Changes made by `ALTER SYSTEM` may require a reload or
    /// even a full restart to take effect.
    pub async fn apply(&self, pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        let command = self.to_string();
        sqlx::query(&command).execute(pool).await?;
        Ok(())
    }
}

impl<'a> fmt::Display for AlterSystem<'a> {
    /// Render SQL that will apply this change.
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
///
/// **Note** that this does not work on PostgreSQL 9.4 and earlier because the
/// `pending_restart` column does not exist. PostgreSQL 9.4 has long been
/// obsolete so a workaround is not provided.
///
/// See the [documentation for
/// `pg_settings`](https://www.postgresql.org/docs/current/view-pg-settings.html).
#[derive(Debug, Clone)]
pub struct Setting {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub category: String,
    pub short_desc: String,
    pub extra_desc: Option<String>,
    pub context: String,
    pub vartype: String,
    pub source: String,
    pub min_val: Option<String>,
    pub max_val: Option<String>,
    pub enumvals: Option<Vec<String>>,
    pub boot_val: Option<String>,
    pub reset_val: Option<String>,
    pub sourcefile: Option<String>,
    pub sourceline: Option<i32>,
    pub pending_restart: bool,
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

    #[allow(clippy::missing_panics_doc)]
    pub async fn get(name: &str, pool: &sqlx::PgPool) -> Result<Option<Self>, sqlx::Error> {
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
            WHERE
                name = $1
            "#,
            name,
        )
        .fetch_optional(pool)
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Parameter<'a>(pub &'a str);

impl<'a> Parameter<'a> {
    /// Get the current value for this parameter.
    pub async fn get(&self, pool: &sqlx::PgPool) -> Result<Option<Value>, sqlx::Error> {
        let setting = Setting::get(self.0, pool).await?;
        Ok(setting.map(|setting| Value::from(&setting)))
    }

    /// Set the current value for this parameter.
    pub async fn set<V: Into<Value>>(
        &self,
        pool: &sqlx::PgPool,
        value: V,
    ) -> Result<(), sqlx::Error> {
        let value = value.into();
        AlterSystem::Set(self, &value).apply(pool).await?;
        Ok(())
    }

    /// Reset the value for this parameter.
    pub async fn reset(&self, pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        AlterSystem::Reset(self).apply(pool).await?;
        Ok(())
    }
}

impl<'a> fmt::Display for Parameter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", escape_identifier(self.0))
    }
}

impl<'a> From<&'a str> for Parameter<'a> {
    fn from(name: &'a str) -> Self {
        Self(name)
    }
}

impl<'a> From<&'a Setting> for Parameter<'a> {
    fn from(setting: &'a Setting) -> Self {
        Self(&setting.name)
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
        assert_eq!(format!("{}", Parameter("foo")), "\"foo\"");
        assert_eq!(format!("{}", Parameter("foo \\bar")), "\"foo \\bar\"");
        assert_eq!(format!("{}", Parameter("foo\"bar")), "\"foo\"\"bar\"");
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
