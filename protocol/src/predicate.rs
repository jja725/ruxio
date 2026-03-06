use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

/// Comparison operators for predicate expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

impl fmt::Display for CompOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompOp::Eq => write!(f, "="),
            CompOp::Ne => write!(f, "!="),
            CompOp::Lt => write!(f, "<"),
            CompOp::Gt => write!(f, ">"),
            CompOp::Le => write!(f, "<="),
            CompOp::Ge => write!(f, ">="),
        }
    }
}

/// Scalar values for predicate comparisons.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bytes(Vec<u8>),
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Bool(v) => write!(f, "{v}"),
            ScalarValue::Int64(v) => write!(f, "{v}"),
            ScalarValue::Float64(v) => write!(f, "{v}"),
            ScalarValue::Utf8(v) => write!(f, "'{v}'"),
            ScalarValue::Bytes(v) => {
                write!(f, "0x")?;
                for b in v {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
        }
    }
}

/// Predicate expression AST.
///
/// Sent by engines to ruxio. Evaluated at the cache layer against Parquet
/// row group statistics (min/max) to determine which pages to return.
/// This saves N round-trips: instead of the engine fetching metadata,
/// evaluating predicates, then requesting each matching page separately,
/// it sends one RPC with a predicate and ruxio streams back only matching pages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredicateExpr {
    /// Column comparison: `column op value`
    Comparison {
        column: String,
        op: CompOp,
        value: ScalarValue,
    },
    /// Logical AND
    And(Box<PredicateExpr>, Box<PredicateExpr>),
    /// Logical OR
    Or(Box<PredicateExpr>, Box<PredicateExpr>),
    /// Logical NOT
    Not(Box<PredicateExpr>),
    /// IN list: `column IN (v1, v2, ...)`
    In {
        column: String,
        values: Vec<ScalarValue>,
    },
    /// Range: `column BETWEEN low AND high`
    Between {
        column: String,
        low: ScalarValue,
        high: ScalarValue,
    },
    /// Null check: `column IS NULL`
    IsNull { column: String },
}

impl PredicateExpr {
    /// Compute a stable hash of this predicate for use as part of a cache key.
    pub fn cache_key_hash(&self) -> u64 {
        let serialized = serde_json::to_string(self).unwrap_or_default();
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        serialized.hash(&mut hasher);
        hasher.finish()
    }
}

impl fmt::Display for PredicateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PredicateExpr::Comparison { column, op, value } => {
                write!(f, "{column} {op} {value}")
            }
            PredicateExpr::And(left, right) => write!(f, "({left} AND {right})"),
            PredicateExpr::Or(left, right) => write!(f, "({left} OR {right})"),
            PredicateExpr::Not(expr) => write!(f, "NOT ({expr})"),
            PredicateExpr::In { column, values } => {
                let vals: Vec<String> = values.iter().map(|v| v.to_string()).collect();
                write!(f, "{column} IN ({})", vals.join(", "))
            }
            PredicateExpr::Between { column, low, high } => {
                write!(f, "{column} BETWEEN {low} AND {high}")
            }
            PredicateExpr::IsNull { column } => write!(f, "{column} IS NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_display() {
        let pred = PredicateExpr::And(
            Box::new(PredicateExpr::Comparison {
                column: "ts".into(),
                op: CompOp::Gt,
                value: ScalarValue::Int64(1704067200),
            }),
            Box::new(PredicateExpr::Comparison {
                column: "region".into(),
                op: CompOp::Eq,
                value: ScalarValue::Utf8("US".into()),
            }),
        );
        assert_eq!(pred.to_string(), "(ts > 1704067200 AND region = 'US')");
    }

    #[test]
    fn test_predicate_cache_key_deterministic() {
        let pred = PredicateExpr::Comparison {
            column: "x".into(),
            op: CompOp::Eq,
            value: ScalarValue::Int64(42),
        };
        assert_eq!(pred.cache_key_hash(), pred.cache_key_hash());
    }

    #[test]
    fn test_predicate_serde_roundtrip() {
        let pred = PredicateExpr::In {
            column: "status".into(),
            values: vec![
                ScalarValue::Utf8("active".into()),
                ScalarValue::Utf8("pending".into()),
            ],
        };
        let json = serde_json::to_string(&pred).unwrap();
        let decoded: PredicateExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(pred, decoded);
    }
}
