use ingodb_blob::{DocumentId, Value};

/// The "Liquid AST" — IngoDB's query interface.
///
/// No SQL. Queries are constructed as Rust data structures (AST nodes).
/// This is the interface between the application and the storage engine.
#[derive(Debug, Clone)]
pub enum Query {
    /// Point lookup by document ID
    Get { id: DocumentId },

    /// Scan with optional filter, sort, projection, and limit
    Scan {
        filter: Option<Filter>,
        sort: Option<Vec<SortField>>,
        project: Option<Vec<String>>,
        limit: Option<usize>,
    },

    /// Graph traversal as join-by-value.
    ///
    /// Starting from documents matching `start` filter, follow edges by joining
    /// `from_field` values against `to_field` values of other documents.
    /// A simple join is depth=1. Depth>1 repeats the join from the results.
    ///
    /// Example: find users referenced by orders
    /// ```text
    /// Traverse {
    ///     start: Some(Eq { field: "type", value: "order" }),
    ///     from_field: "user_id",
    ///     to_field: "_id",
    ///     depth: 1,
    /// }
    /// ```
    Traverse {
        /// Filter for starting documents (None = all documents)
        start: Option<Filter>,
        /// Field on source documents whose value is the join key
        from_field: String,
        /// Field on target documents to match against
        to_field: String,
        /// Number of hops (1 = simple join)
        depth: usize,
    },
}

/// Filter predicates for scan queries.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Field equals a value
    Eq { field: String, value: Value },
    /// Field is greater than a value
    Gt { field: String, value: Value },
    /// Field is less than a value
    Lt { field: String, value: Value },
    /// Field is within a range [low, high)
    Range {
        field: String,
        low: Value,
        high: Value,
    },
    /// Field exists in the document
    Exists { field: String },
    /// Logical AND of filters
    And(Vec<Filter>),
    /// Logical OR of filters
    Or(Vec<Filter>),
    /// Logical NOT
    Not(Box<Filter>),
}

/// Result of a query execution.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub documents: Vec<DocumentResult>,
}

/// A single document in a query result, possibly projected.
#[derive(Debug, Clone)]
pub struct DocumentResult {
    pub id: DocumentId,
    pub fields: Vec<(String, Value)>,
}

impl Filter {
    /// Evaluate this filter against a document's fields.
    pub fn matches(&self, get_field: &dyn Fn(&str) -> Option<Value>) -> bool {
        match self {
            Filter::Eq { field, value } => get_field(field).as_ref() == Some(value),

            Filter::Gt { field, value } => {
                get_field(field)
                    .as_ref()
                    .map_or(false, |v| compare_values(v, value) == Some(std::cmp::Ordering::Greater))
            }

            Filter::Lt { field, value } => {
                get_field(field)
                    .as_ref()
                    .map_or(false, |v| compare_values(v, value) == Some(std::cmp::Ordering::Less))
            }

            Filter::Range { field, low, high } => {
                get_field(field).as_ref().map_or(false, |v| {
                    matches!(compare_values(v, low), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                        && matches!(compare_values(v, high), Some(std::cmp::Ordering::Less))
                })
            }

            Filter::Exists { field } => get_field(field).is_some(),

            Filter::And(filters) => filters.iter().all(|f| f.matches(get_field)),

            Filter::Or(filters) => filters.iter().any(|f| f.matches(get_field)),

            Filter::Not(filter) => !filter.matches(get_field),
        }
    }

    /// Serialize this filter to a `Value` (a nested `Document`) so it can be
    /// stored — e.g. as the persisted `range` of a partial secondary index.
    /// Round-trips with [`Filter::from_value`].
    pub fn to_value(&self) -> Value {
        let doc = |pairs: Vec<(&str, Value)>| {
            Value::Document(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
        };
        match self {
            Filter::Eq { field, value } => doc(vec![("op", Value::String("eq".into())), ("field", Value::String(field.clone())), ("value", value.clone())]),
            Filter::Gt { field, value } => doc(vec![("op", Value::String("gt".into())), ("field", Value::String(field.clone())), ("value", value.clone())]),
            Filter::Lt { field, value } => doc(vec![("op", Value::String("lt".into())), ("field", Value::String(field.clone())), ("value", value.clone())]),
            Filter::Range { field, low, high } => doc(vec![("op", Value::String("range".into())), ("field", Value::String(field.clone())), ("low", low.clone()), ("high", high.clone())]),
            Filter::Exists { field } => doc(vec![("op", Value::String("exists".into())), ("field", Value::String(field.clone()))]),
            Filter::And(filters) => doc(vec![("op", Value::String("and".into())), ("filters", Value::Array(filters.iter().map(|f| f.to_value()).collect()))]),
            Filter::Or(filters) => doc(vec![("op", Value::String("or".into())), ("filters", Value::Array(filters.iter().map(|f| f.to_value()).collect()))]),
            Filter::Not(filter) => doc(vec![("op", Value::String("not".into())), ("filter", filter.to_value())]),
        }
    }

    /// Reconstruct a filter from [`Filter::to_value`] output. Returns `None` if
    /// the value is not a well-formed serialized filter.
    pub fn from_value(value: &Value) -> Option<Filter> {
        let fields = match value {
            Value::Document(pairs) => pairs,
            _ => return None,
        };
        let get = |key: &str| fields.iter().find(|(k, _)| k == key).map(|(_, v)| v);
        let field = || match get("field") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        };
        let op = match get("op") {
            Some(Value::String(s)) => s.as_str(),
            _ => return None,
        };
        match op {
            "eq" => Some(Filter::Eq { field: field()?, value: get("value")?.clone() }),
            "gt" => Some(Filter::Gt { field: field()?, value: get("value")?.clone() }),
            "lt" => Some(Filter::Lt { field: field()?, value: get("value")?.clone() }),
            "range" => Some(Filter::Range { field: field()?, low: get("low")?.clone(), high: get("high")?.clone() }),
            "exists" => Some(Filter::Exists { field: field()? }),
            "and" | "or" => {
                let arr = match get("filters") {
                    Some(Value::Array(a)) => a,
                    _ => return None,
                };
                let parsed: Option<Vec<Filter>> = arr.iter().map(Filter::from_value).collect();
                let parsed = parsed?;
                Some(if op == "and" { Filter::And(parsed) } else { Filter::Or(parsed) })
            }
            "not" => Some(Filter::Not(Box::new(Filter::from_value(get("filter")?)?))),
            _ => None,
        }
    }
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// A field to sort by, with direction.
#[derive(Debug, Clone)]
pub struct SortField {
    pub field: String,
    pub direction: SortDirection,
}

/// Compare two values of the same type. Returns None for incompatible types.
pub fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::I64(a), Value::I64(b)) => Some(a.cmp(b)),
        (Value::U64(a), Value::U64(b)) => Some(a.cmp(b)),
        (Value::F64(a), Value::F64(b)) => a.partial_cmp(b),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_value_roundtrip() {
        let filters = vec![
            Filter::Eq { field: "x".into(), value: Value::U64(5) },
            Filter::Gt { field: "y".into(), value: Value::I64(-3) },
            Filter::Lt { field: "z".into(), value: Value::String("m".into()) },
            Filter::Range { field: "p".into(), low: Value::F64(1.5), high: Value::F64(9.0) },
            Filter::Exists { field: "q".into() },
            Filter::And(vec![
                Filter::Gt { field: "a".into(), value: Value::U64(1) },
                Filter::Lt { field: "a".into(), value: Value::U64(10) },
            ]),
            Filter::Or(vec![
                Filter::Eq { field: "b".into(), value: Value::Bool(true) },
                Filter::Not(Box::new(Filter::Exists { field: "c".into() })),
            ]),
        ];
        for f in filters {
            let v = f.to_value();
            assert_eq!(Filter::from_value(&v), Some(f.clone()), "round-trip failed for {f:?}");
        }
        // Garbage decodes to None.
        assert_eq!(Filter::from_value(&Value::U64(1)), None);
        assert_eq!(Filter::from_value(&Value::Document(vec![])), None);
    }

    fn field_getter(fields: &[(String, Value)]) -> impl Fn(&str) -> Option<Value> + '_ {
        move |name: &str| {
            fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn test_eq_filter() {
        let fields = vec![
            ("name".to_string(), Value::String("Henrik".into())),
            ("age".to_string(), Value::U64(42)),
        ];
        let getter = field_getter(&fields);

        let filter = Filter::Eq {
            field: "name".into(),
            value: Value::String("Henrik".into()),
        };
        assert!(filter.matches(&getter));

        let filter = Filter::Eq {
            field: "name".into(),
            value: Value::String("NotHenrik".into()),
        };
        assert!(!filter.matches(&getter));
    }

    #[test]
    fn test_gt_lt_filters() {
        let fields = vec![("score".to_string(), Value::I64(75))];
        let getter = field_getter(&fields);

        assert!(Filter::Gt {
            field: "score".into(),
            value: Value::I64(50),
        }
        .matches(&getter));

        assert!(!Filter::Gt {
            field: "score".into(),
            value: Value::I64(100),
        }
        .matches(&getter));

        assert!(Filter::Lt {
            field: "score".into(),
            value: Value::I64(100),
        }
        .matches(&getter));
    }

    #[test]
    fn test_range_filter() {
        let fields = vec![("x".to_string(), Value::U64(50))];
        let getter = field_getter(&fields);

        assert!(Filter::Range {
            field: "x".into(),
            low: Value::U64(10),
            high: Value::U64(100),
        }
        .matches(&getter));

        assert!(!Filter::Range {
            field: "x".into(),
            low: Value::U64(60),
            high: Value::U64(100),
        }
        .matches(&getter));
    }

    #[test]
    fn test_exists_filter() {
        let fields = vec![("present".to_string(), Value::Null)];
        let getter = field_getter(&fields);

        assert!(Filter::Exists {
            field: "present".into(),
        }
        .matches(&getter));

        assert!(!Filter::Exists {
            field: "absent".into(),
        }
        .matches(&getter));
    }

    #[test]
    fn test_and_or_not() {
        let fields = vec![
            ("a".to_string(), Value::U64(10)),
            ("b".to_string(), Value::U64(20)),
        ];
        let getter = field_getter(&fields);

        let and = Filter::And(vec![
            Filter::Eq {
                field: "a".into(),
                value: Value::U64(10),
            },
            Filter::Eq {
                field: "b".into(),
                value: Value::U64(20),
            },
        ]);
        assert!(and.matches(&getter));

        let or = Filter::Or(vec![
            Filter::Eq {
                field: "a".into(),
                value: Value::U64(999),
            },
            Filter::Eq {
                field: "b".into(),
                value: Value::U64(20),
            },
        ]);
        assert!(or.matches(&getter));

        let not = Filter::Not(Box::new(Filter::Eq {
            field: "a".into(),
            value: Value::U64(999),
        }));
        assert!(not.matches(&getter));
    }

    #[test]
    fn test_compare_incompatible_types() {
        assert_eq!(compare_values(&Value::I64(1), &Value::String("x".into())), None);
    }
}
