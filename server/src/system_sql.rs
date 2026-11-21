//! Closed, typed operational SQL. Caller SQL and AST formatting never reach PostgreSQL.
use crate::{
    ApiError,
    authz::MetadataAccess,
    failure,
    metadata::{
        self, BindValue, Bindings, BoundSql, ColumnId, ColumnUses, Relation, ResultType,
        Sensitivity, SourceBounds, StorageSchema, SystemType,
    },
    sql_limits::{self, GuardError, Limits},
};
use serde::Deserialize;
use serde_json::Value;
use sqlparser::{
    ast::{
        self, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
        InTarget, JoinConstraint, JoinOperator, ObjectName, OrderByKind, OrderBySort, SelectItem,
        SetExpr, TableFactor, UnaryOperator,
    },
    dialect::PostgreSqlDialect,
    tokenizer::Token,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MAX_NODES: usize = 512;
const MAX_OUTPUTS: usize = 32;
const MAX_IDS: usize = 100;
const MAX_PARAMETERS: usize = 128;
const MAX_WINDOW: Duration = Duration::from_secs(31 * 86400);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BoundsInput {
    pub from: Option<String>,
    pub until: Option<String>,
    #[serde(default)]
    pub request_ids: Vec<String>,
}
pub(crate) type BoundsEnvelope = BTreeMap<Relation, BoundsInput>;
pub(crate) enum Discovery {
    Tables,
    Columns(Relation),
}
pub(crate) enum Statement {
    Discover(Discovery),
    Select(Box<SelectPlan>),
}
pub(crate) struct OutputColumn {
    pub name: String,
    pub ty: ResultType,
    pub nullable: bool,
    pub units: Option<&'static str>,
}
pub(crate) struct AppliedBounds {
    pub relation: Relation,
    pub bounds: SourceBounds,
}
pub(crate) struct SourceProbe {
    pub relation: Relation,
    pub query: BoundSql,
    pub row_limit: u32,
    pub payload_limit: u64,
}
pub(crate) struct ExecutablePlan {
    pub query: BoundSql,
    pub probes: Vec<SourceProbe>,
    pub outputs: Vec<OutputColumn>,
    pub applied_bounds: Vec<AppliedBounds>,
    pub user_limit: Option<u32>,
}

type Id = usize;
#[derive(Clone, Debug, PartialEq)]
enum Constant {
    Null(SystemType),
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Timestamp(SystemTime),
}
impl Constant {
    fn ty(&self) -> SystemType {
        match self {
            Self::Null(t) => *t,
            Self::Bool(_) => SystemType::Bool,
            Self::Int(_) => SystemType::Int64,
            Self::Float(_) => SystemType::Float64,
            Self::Text(_) => SystemType::Utf8,
            Self::Timestamp(_) => SystemType::TimestampUtc,
        }
    }
    fn binding(&self) -> BindValue {
        match self {
            Self::Null(t) => BindValue::Null(*t),
            Self::Bool(v) => BindValue::Bool(*v),
            Self::Int(v) => BindValue::I64(*v),
            Self::Float(v) => BindValue::F64(*v),
            Self::Text(v) => BindValue::Text(v.clone()),
            Self::Timestamp(v) => BindValue::Timestamp(*v),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Compare {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
impl Compare {
    fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }
    fn reverse(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Aggregate {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}
#[derive(Clone, Debug, PartialEq)]
enum Kind {
    Column(usize, ColumnId),
    Value(Constant),
    Compare(Compare, Id, Id),
    And(Id, Id),
    Or(Id, Id),
    Not(Id),
    IsNull(Id, bool),
    In(Id, Vec<Id>, bool),
    Aggregate(Aggregate, Option<Id>),
}
#[derive(Clone, Debug, PartialEq)]
struct Node {
    kind: Kind,
    ty: ResultType,
    nullable: bool,
}
struct Source {
    relation: Relation,
    alias: String,
    nullable: bool,
    bounds: Option<SourceBounds>,
    uses: ColumnUses,
}
struct Join {
    source: usize,
    left: bool,
    on: Id,
}
struct Output {
    expr: Id,
    name: String,
}
struct Order {
    expr: Id,
    descending: bool,
    nulls_first: Option<bool>,
}
pub(crate) struct SelectPlan {
    relations: Vec<Relation>,
    sources: Vec<Source>,
    nodes: Vec<Node>,
    joins: Vec<Join>,
    filter: Option<Id>,
    groups: Vec<Id>,
    having: Option<Id>,
    outputs: Vec<Output>,
    order: Vec<Order>,
    limit: Option<u32>,
}
impl SelectPlan {
    pub fn required_relations(&self) -> &[Relation] {
        &self.relations
    }
}
fn unsupported(message: &str) -> ApiError {
    ApiError::new(failure::SYSTEM_QUERY_UNSUPPORTED, message)
}
fn wide(message: &str) -> ApiError {
    ApiError::new(failure::SYSTEM_QUERY_TOO_WIDE, message)
}
fn scalar(ty: SystemType) -> ResultType {
    ResultType::Scalar(ty)
}
fn identifier(id: &Ident) -> Result<String, ApiError> {
    if id.quote_style.is_some_and(|q| q != '"') {
        return Err(unsupported(
            "only standard quoted identifiers are supported",
        ));
    }
    label(&id.value)?;
    Ok(if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_ascii_lowercase()
    })
}
fn label(value: &str) -> Result<(), ApiError> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ApiError::bad(
            "identifiers and output labels must be 1..128 bytes without control characters",
        ));
    }
    Ok(())
}
fn relation_name(parts: &[String]) -> Result<Relation, ApiError> {
    let name = match parts {
        [name] => name,
        [schema, name] if schema == "duckfleet_system" => name,
        _ => return Err(unsupported("only duckfleet_system relations are supported")),
    };
    metadata::find_relation(name).ok_or_else(|| unsupported("unknown system relation"))
}
fn object_name(name: &ObjectName) -> Result<Vec<String>, ApiError> {
    name.0
        .iter()
        .map(|p| {
            p.as_ident()
                .ok_or_else(|| unsupported("dynamic identifiers are unsupported"))
                .and_then(identifier)
        })
        .collect()
}

fn timestamp(value: &str) -> Result<SystemTime, ApiError> {
    if value.len() > 40 || !value.contains(['T', 't']) {
        return Err(ApiError::bad(
            "timestamps require RFC3339 with an explicit offset",
        ));
    }
    if let Some((_, fraction)) = value.split_once('.')
        && fraction.chars().take_while(char::is_ascii_digit).count() > 6
    {
        return Err(ApiError::bad(
            "timestamps support at most six fractional digits",
        ));
    }
    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| ApiError::bad("invalid RFC3339 timestamp or missing offset"))?;
    if parsed.timestamp_subsec_nanos() >= 1_000_000_000 {
        return Err(ApiError::bad("leap seconds are unsupported"));
    }
    let micros = parsed.timestamp_micros();
    let distance = Duration::from_micros(micros.unsigned_abs());
    (if micros >= 0 {
        UNIX_EPOCH.checked_add(distance)
    } else {
        UNIX_EPOCH.checked_sub(distance)
    })
    .ok_or_else(|| ApiError::bad("timestamp out of range"))
}
fn request_ids(values: &[String]) -> Result<BTreeSet<String>, ApiError> {
    if values.is_empty()
        || values.len() > MAX_IDS
        || values
            .iter()
            .any(|v| v.is_empty() || v.len() > 128 || v.chars().any(char::is_control))
    {
        return Err(ApiError::bad(
            "request IDs require 1..100 nonempty values of at most 128 bytes without control characters",
        ));
    }
    Ok(values.iter().cloned().collect())
}
#[derive(Default)]
struct Proof {
    from: Option<SystemTime>,
    until: Option<SystemTime>,
    ids: Option<BTreeSet<String>>,
}
impl Proof {
    fn add_ids(&mut self, ids: BTreeSet<String>) {
        self.ids = Some(match self.ids.take() {
            Some(old) => old.intersection(&ids).cloned().collect(),
            None => ids,
        });
    }
    fn finish(self) -> Result<SourceBounds, ApiError> {
        match (self.from, self.until, self.ids) {
            (Some(from), Some(until), ids) => {
                let width = until
                    .duration_since(from)
                    .map_err(|_| ApiError::bad("source time range must have from < until"))?;
                if width.is_zero() || width > MAX_WINDOW {
                    return Err(wide(
                        "source time ranges must be positive and no longer than 31 days",
                    ));
                }
                Ok(match ids {
                    Some(ids) if !ids.is_empty() => SourceBounds::Both {
                        from,
                        until,
                        ids: ids.into_iter().collect(),
                    },
                    Some(_) => {
                        return Err(ApiError::bad("source bounds have no request IDs in common"));
                    }
                    None => SourceBounds::TimeRange { from, until },
                })
            }
            (_, _, Some(ids)) if !ids.is_empty() => {
                Ok(SourceBounds::RequestIds(ids.into_iter().collect()))
            }
            _ => Err(wide(
                "each source needs its own positive time range or request-ID bounds",
            )),
        }
    }
}

struct Compiler<'a> {
    plan: SelectPlan,
    parameters: &'a [Value],
    parameter_types: BTreeMap<usize, SystemType>,
}
impl<'a> Compiler<'a> {
    fn new(parameters: &'a [Value]) -> Self {
        Self {
            plan: SelectPlan {
                relations: vec![],
                sources: vec![],
                nodes: vec![],
                joins: vec![],
                filter: None,
                groups: vec![],
                having: None,
                outputs: vec![],
                order: vec![],
                limit: None,
            },
            parameters,
            parameter_types: BTreeMap::new(),
        }
    }
    fn push(&mut self, node: Node) -> Result<Id, ApiError> {
        if let Some(id) = self
            .plan
            .nodes
            .iter()
            .position(|existing| *existing == node)
        {
            return Ok(id);
        }
        if self.plan.nodes.len() >= MAX_NODES {
            return Err(wide("too many system expressions"));
        }
        let id = self.plan.nodes.len();
        self.plan.nodes.push(node);
        Ok(id)
    }
    fn constant(&mut self, value: Constant) -> Result<Id, ApiError> {
        let nullable = matches!(value, Constant::Null(_));
        self.push(Node {
            ty: scalar(value.ty()),
            kind: Kind::Value(value),
            nullable,
        })
    }
    fn source(&mut self, factor: &TableFactor, nullable: bool) -> Result<(), ApiError> {
        let TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_directives,
            inheritance,
        } = factor
        else {
            return Err(unsupported("only named system relations are supported"));
        };
        if args.is_some()
            || !with_hints.is_empty()
            || version.is_some()
            || *with_ordinality
            || !partitions.is_empty()
            || json_path.is_some()
            || sample.is_some()
            || !index_directives.is_empty()
            || inheritance.is_some()
        {
            return Err(unsupported("table modifiers are unsupported"));
        }
        let names = object_name(name)?;
        let relation = relation_name(&names)?;
        if self.plan.relations.contains(&relation) {
            return Err(unsupported("self joins are unsupported"));
        }
        let alias = match alias {
            Some(a) if a.columns.is_empty() && a.at.is_none() => identifier(&a.name)?,
            Some(_) => return Err(unsupported("column and index aliases are unsupported")),
            None => names.last().unwrap().clone(),
        };
        if self.plan.sources.iter().any(|s| s.alias == alias) {
            return Err(ApiError::bad("duplicate source alias"));
        }
        self.plan.relations.push(relation);
        self.plan.sources.push(Source {
            relation,
            alias,
            nullable,
            bounds: None,
            uses: ColumnUses {
                projected: BTreeSet::new(),
                evaluated: BTreeSet::new(),
            },
        });
        Ok(())
    }
    fn column(&self, parts: &[Ident]) -> Result<(usize, ColumnId), ApiError> {
        let parts: Vec<_> = parts.iter().map(identifier).collect::<Result<_, _>>()?;
        let (qualifier, name) = match parts.as_slice() {
            [name] => (None, name),
            [qualifier, name] => (Some(qualifier), name),
            _ => return Err(unsupported("column references use column or alias.column")),
        };
        let mut found = None;
        for (i, s) in self.plan.sources.iter().enumerate() {
            if qualifier.is_some_and(|q| q != &s.alias) {
                continue;
            }
            if let Some(column) = metadata::find_column(s.relation, name) {
                if found.is_some() {
                    return Err(ApiError::bad("ambiguous column reference"));
                }
                found = Some((i, column));
            }
        }
        found.ok_or_else(|| ApiError::bad("unknown column or source alias"))
    }
    fn coerce_constant(
        &self,
        value: Constant,
        expected: Option<SystemType>,
    ) -> Result<Constant, ApiError> {
        match (value, expected) {
            (Constant::Text(v), Some(SystemType::TimestampUtc)) => {
                Ok(Constant::Timestamp(timestamp(&v)?))
            }
            (Constant::Int(v), Some(SystemType::Float64)) if v.unsigned_abs() <= (1u64 << 53) => {
                Ok(Constant::Float(v as f64))
            }
            (v, Some(t)) if v.ty() != t => Err(ApiError::bad(
                "literal or parameter type does not match expression",
            )),
            (v, _) => Ok(v),
        }
    }
    fn value(&mut self, value: &ast::Value, expected: Option<SystemType>) -> Result<Id, ApiError> {
        let value = match value {
            ast::Value::Null => Constant::Null(expected.unwrap_or(SystemType::Utf8)),
            ast::Value::Boolean(v) => Constant::Bool(*v),
            ast::Value::Number(n, false) => number(n)?,
            ast::Value::SingleQuotedString(v) => text_value(v)?,
            ast::Value::Placeholder(p) => {
                let index: usize = p
                    .strip_prefix('$')
                    .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                    .and_then(|v| v.parse().ok())
                    .filter(|i| *i > 0)
                    .ok_or_else(|| ApiError::bad("parameters use one-based $n placeholders"))?;
                let parameter = self
                    .parameters
                    .get(index - 1)
                    .ok_or_else(|| ApiError::bad("missing parameter"))?;
                let value = match parameter {
                    Value::Null => Constant::Null(expected.ok_or_else(|| {
                        ApiError::bad("NULL parameters need a typed expression context")
                    })?),
                    Value::Bool(v) => Constant::Bool(*v),
                    Value::String(v) => text_value(v)?,
                    Value::Number(v) if v.is_i64() => Constant::Int(v.as_i64().unwrap()),
                    Value::Number(v) => Constant::Float(
                        v.as_f64()
                            .filter(|n| n.is_finite())
                            .ok_or_else(|| ApiError::bad("numeric parameter out of range"))?,
                    ),
                    _ => {
                        return Err(ApiError::bad(
                            "system parameters must be scalar JSON values",
                        ));
                    }
                };
                // u64 values beyond i64 must never silently enter the float codec.
                if parameter.as_u64().is_some_and(|n| n > i64::MAX as u64) {
                    return Err(ApiError::bad("integer parameter exceeds Int64"));
                }
                let value = self.coerce_constant(value, expected)?;
                if self
                    .parameter_types
                    .insert(index, value.ty())
                    .is_some_and(|old| old != value.ty())
                {
                    return Err(ApiError::bad("repeated parameter has inconsistent types"));
                }
                return self.constant(value);
            }
            _ => return Err(unsupported("unsupported system literal")),
        };
        let value = self.coerce_constant(value, expected)?;
        self.constant(value)
    }
    fn hint(&self, expr: &Expr) -> Option<SystemType> {
        match expr {
            Expr::Identifier(i) => self.column(std::slice::from_ref(i)).ok(),
            Expr::CompoundIdentifier(p) => self.column(p).ok(),
            Expr::Nested(e) => return self.hint(e),
            Expr::Value(v) => {
                return match &v.value {
                    ast::Value::Boolean(_) => Some(SystemType::Bool),
                    ast::Value::Number(n, _) => number(n).ok().map(|v| v.ty()),
                    ast::Value::SingleQuotedString(_) => Some(SystemType::Utf8),
                    _ => None,
                };
            }
            Expr::TypedString(_) => return Some(SystemType::TimestampUtc),
            Expr::Function(f) => {
                let names = object_name(&f.name).ok()?;
                let name = names.first()?;
                if name == "count" {
                    return Some(SystemType::Int64);
                }
                if let FunctionArguments::List(args) = &f.args
                    && let [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] = args.args.as_slice()
                {
                    return self.hint(e);
                }
                return None;
            }
            _ => return None,
        }
        .and_then(|(i, c)| metadata::column_def(self.plan.sources[i].relation, c))
        .map(|d| d.ty)
    }
    fn expression(
        &mut self,
        expr: &Expr,
        expected: Option<SystemType>,
        aggregates: bool,
    ) -> Result<Id, ApiError> {
        let id = match expr {
            Expr::Nested(e) => return self.expression(e, expected, aggregates),
            Expr::Identifier(i) => {
                let (s, c) = self.column(std::slice::from_ref(i))?;
                self.column_node(s, c)?
            }
            Expr::CompoundIdentifier(p) => {
                let (s, c) = self.column(p)?;
                self.column_node(s, c)?
            }
            Expr::Value(v) => return self.value(&v.value, expected),
            Expr::TypedString(t)
                if !t.uses_odbc_syntax && matches!(t.data_type, ast::DataType::Timestamp(..)) =>
            {
                let ast::Value::SingleQuotedString(v) = &t.value.value else {
                    return Err(unsupported(
                        "timestamp literal requires a quoted RFC3339 value",
                    ));
                };
                self.constant(Constant::Timestamp(timestamp(v)?))?
            }
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => {
                let Expr::Value(v) = expr.as_ref() else {
                    return Err(unsupported(
                        "unary minus is allowed only on numeric literals",
                    ));
                };
                let ast::Value::Number(n, false) = &v.value else {
                    return Err(unsupported("unary minus requires a number"));
                };
                let value = self.coerce_constant(number(&format!("-{n}"))?, expected)?;
                return self.constant(value);
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => {
                let e = self.expression(expr, Some(SystemType::Bool), aggregates)?;
                self.push(Node {
                    kind: Kind::Not(e),
                    ty: scalar(SystemType::Bool),
                    nullable: self.plan.nodes[e].nullable,
                })?
            }
            Expr::BinaryOp { left, op, right }
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) =>
            {
                let l = self.expression(left, Some(SystemType::Bool), aggregates)?;
                let r = self.expression(right, Some(SystemType::Bool), aggregates)?;
                self.push(Node {
                    kind: if *op == BinaryOperator::And {
                        Kind::And(l, r)
                    } else {
                        Kind::Or(l, r)
                    },
                    ty: scalar(SystemType::Bool),
                    nullable: self.plan.nodes[l].nullable || self.plan.nodes[r].nullable,
                })?
            }
            Expr::BinaryOp { left, op, right } => {
                let cmp = match op {
                    BinaryOperator::Eq => Compare::Eq,
                    BinaryOperator::NotEq => Compare::Ne,
                    BinaryOperator::Lt => Compare::Lt,
                    BinaryOperator::LtEq => Compare::Le,
                    BinaryOperator::Gt => Compare::Gt,
                    BinaryOperator::GtEq => Compare::Ge,
                    _ => {
                        return Err(unsupported(
                            "only comparison and boolean operators are supported",
                        ));
                    }
                };
                let hint = self
                    .hint(left)
                    .filter(|t| *t != SystemType::Utf8)
                    .or_else(|| self.hint(right))
                    .or_else(|| self.hint(left));
                let l = self.expression(left, hint, aggregates)?;
                let r = self.expression(right, hint, aggregates)?;
                self.comparable(l, r)?;
                self.push(Node {
                    kind: Kind::Compare(cmp, l, r),
                    ty: scalar(SystemType::Bool),
                    nullable: self.plan.nodes[l].nullable || self.plan.nodes[r].nullable,
                })?
            }
            Expr::IsNull(e) | Expr::IsNotNull(e) => {
                let e = self.expression(e, None, aggregates)?;
                self.no_json(e)?;
                self.push(Node {
                    kind: Kind::IsNull(e, matches!(expr, Expr::IsNotNull(_))),
                    ty: scalar(SystemType::Bool),
                    nullable: false,
                })?
            }
            Expr::In {
                expr,
                target: InTarget::List(values),
                negated,
                global: false,
            } if !values.is_empty() && values.len() <= MAX_IDS => {
                let e = self.expression(expr, None, aggregates)?;
                self.no_json(e)?;
                let ty = match self.plan.nodes[e].ty {
                    ResultType::Scalar(t) => t,
                    _ => return Err(unsupported("IN on decimal aggregates is unsupported")),
                };
                let mut list = Vec::new();
                for value in values {
                    let id = self.expression(value, Some(ty), false)?;
                    if !matches!(self.plan.nodes[id].kind, Kind::Value(_)) {
                        return Err(unsupported("IN values must be constants"));
                    }
                    list.push(id);
                }
                let nullable = self.plan.nodes[e].nullable
                    || list.iter().any(|i| self.plan.nodes[*i].nullable);
                self.push(Node {
                    kind: Kind::In(e, list, *negated),
                    ty: scalar(SystemType::Bool),
                    nullable,
                })?
            }
            Expr::Function(f) if aggregates => self.aggregate(f)?,
            _ => {
                return Err(unsupported(
                    "expression is outside the supported system SQL subset",
                ));
            }
        };
        if let Some(expected) = expected {
            let ty = &self.plan.nodes[id].ty;
            if *ty != scalar(expected)
                && !(matches!(ty, ResultType::Decimal128 { .. }) && expected == SystemType::Int64)
            {
                return Err(ApiError::bad("expression type mismatch"));
            }
        }
        Ok(id)
    }
    fn column_node(&mut self, s: usize, c: ColumnId) -> Result<Id, ApiError> {
        let d = metadata::column_def(self.plan.sources[s].relation, c).unwrap();
        self.push(Node {
            kind: Kind::Column(s, c),
            ty: scalar(d.ty),
            nullable: d.nullable
                || self.plan.sources[s].nullable
                || d.sensitivity != Sensitivity::Metadata,
        })
    }
    fn no_json(&self, id: Id) -> Result<(), ApiError> {
        if self.plan.nodes[id].ty == scalar(SystemType::JsonText) {
            Err(unsupported(
                "JSON and raw profiles support direct projection only",
            ))
        } else {
            Ok(())
        }
    }
    fn comparable(&self, l: Id, r: Id) -> Result<(), ApiError> {
        self.no_json(l)?;
        self.no_json(r)?;
        let a = &self.plan.nodes[l].ty;
        let b = &self.plan.nodes[r].ty;
        if a == b
            || matches!(
                (a, b),
                (
                    ResultType::Decimal128 { .. },
                    ResultType::Scalar(SystemType::Int64)
                ) | (
                    ResultType::Scalar(SystemType::Int64),
                    ResultType::Decimal128 { .. }
                )
            )
        {
            Ok(())
        } else {
            Err(ApiError::bad("comparison operands have incompatible types"))
        }
    }
    fn aggregate(&mut self, f: &ast::Function) -> Result<Id, ApiError> {
        if f.uses_odbc_syntax
            || !matches!(f.parameters, FunctionArguments::None)
            || !f.within_group.is_empty()
            || f.filter.is_some()
            || f.null_treatment.is_some()
            || f.over.is_some()
        {
            return Err(unsupported(
                "aggregate modifiers and windows are unsupported",
            ));
        }
        let names = object_name(&f.name)?;
        let function = match names.as_slice() {
            [n] => match n.as_str() {
                "count" => Aggregate::Count,
                "sum" => Aggregate::Sum,
                "avg" => Aggregate::Avg,
                "min" => Aggregate::Min,
                "max" => Aggregate::Max,
                _ => return Err(unsupported("unsupported function")),
            },
            _ => return Err(unsupported("qualified functions are unsupported")),
        };
        let FunctionArguments::List(args) = &f.args else {
            return Err(unsupported("aggregate requires an argument"));
        };
        if args.duplicate_treatment.is_some() || !args.clauses.is_empty() {
            return Err(unsupported(
                "aggregate DISTINCT and modifiers are unsupported",
            ));
        }
        let argument = match args.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] if function == Aggregate::Count => {
                None
            }
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] => {
                Some(self.expression(e, None, false)?)
            }
            _ => {
                return Err(unsupported(
                    "aggregate requires one expression, or COUNT(*)",
                ));
            }
        };
        if let Some(a) = argument {
            self.no_json(a)?;
        }
        let ty = match (function, argument.map(|a| self.plan.nodes[a].ty)) {
            (Aggregate::Count, _) => scalar(SystemType::Int64),
            (Aggregate::Sum, Some(ResultType::Scalar(SystemType::Int64))) => {
                ResultType::Decimal128 {
                    precision: 38,
                    scale: 0,
                }
            }
            (Aggregate::Avg, Some(ResultType::Scalar(SystemType::Int64))) => {
                ResultType::Decimal128 {
                    precision: 38,
                    scale: 6,
                }
            }
            (Aggregate::Sum | Aggregate::Avg, Some(ResultType::Scalar(SystemType::Float64))) => {
                scalar(SystemType::Float64)
            }
            (Aggregate::Min | Aggregate::Max, Some(ResultType::Scalar(t)))
                if t != SystemType::Bool && t != SystemType::JsonText =>
            {
                scalar(t)
            }
            _ => return Err(ApiError::bad("unsupported aggregate input type")),
        };
        self.push(Node {
            kind: Kind::Aggregate(function, argument),
            ty,
            nullable: function != Aggregate::Count,
        })
    }
    fn uses(&mut self, id: Id, evaluated: bool) {
        let mut pending = vec![id];
        while let Some(id) = pending.pop() {
            match self.plan.nodes[id].kind.clone() {
                Kind::Column(s, c) => {
                    if evaluated {
                        self.plan.sources[s].uses.evaluated.insert(c);
                    } else {
                        self.plan.sources[s].uses.projected.insert(c);
                    }
                }
                Kind::Value(_) => {}
                Kind::Aggregate(_, a) => {
                    if let Some(a) = a {
                        self.uses(a, true);
                    }
                }
                Kind::Compare(_, a, b) | Kind::And(a, b) | Kind::Or(a, b) => {
                    pending.extend([a, b]);
                }
                Kind::Not(a) | Kind::IsNull(a, _) => pending.push(a),
                Kind::In(a, values, _) => {
                    pending.push(a);
                    pending.extend(values);
                }
            }
        }
    }
    fn conjuncts(&self, id: Id) -> Vec<Id> {
        let mut pending = vec![id];
        let mut result = vec![];
        while let Some(i) = pending.pop() {
            if let Kind::And(a, b) = self.plan.nodes[i].kind {
                pending.extend([a, b]);
            } else {
                result.push(i);
            }
        }
        result
    }
    fn certify_join(&self, on: Id, new: usize) -> Result<(), ApiError> {
        let matched = self.conjuncts(on).into_iter().any(|id| {
            let Kind::Compare(Compare::Eq, a, b) = self.plan.nodes[id].kind else {
                return false;
            };
            let (Kind::Column(sa, ca), Kind::Column(sb, cb)) =
                (&self.plan.nodes[a].kind, &self.plan.nodes[b].kind)
            else {
                return false;
            };
            ((*sa == new && *sb < new) || (*sb == new && *sa < new))
                && metadata::find_column(self.plan.sources[*sa].relation, "request_id") == Some(*ca)
                && metadata::find_column(self.plan.sources[*sb].relation, "request_id") == Some(*cb)
        });
        if matched {
            Ok(())
        } else {
            Err(unsupported(
                "each join needs a positive request_id equality connecting its new source to a previous source",
            ))
        }
    }
    fn proof(&self, id: Id, only: Option<usize>, proofs: &mut [Proof]) -> Result<(), ApiError> {
        for id in self.conjuncts(id) {
            match &self.plan.nodes[id].kind {
                Kind::Compare(cmp, l, r) => {
                    let (s, c, value, cmp) =
                        match (&self.plan.nodes[*l].kind, &self.plan.nodes[*r].kind) {
                            (Kind::Column(s, c), Kind::Value(v)) => (*s, *c, v, *cmp),
                            (Kind::Value(v), Kind::Column(s, c)) => (*s, *c, v, cmp.reverse()),
                            _ => continue,
                        };
                    if only.is_some_and(|i| i != s) {
                        continue;
                    }
                    let relation = self.plan.sources[s].relation;
                    if c == metadata::relation_def(relation).time_column
                        && let Constant::Timestamp(value) = value
                    {
                        match cmp {
                            Compare::Ge => {
                                proofs[s].from =
                                    Some(proofs[s].from.map_or(*value, |old| old.max(*value)))
                            }
                            Compare::Lt => {
                                proofs[s].until =
                                    Some(proofs[s].until.map_or(*value, |old| old.min(*value)))
                            }
                            _ => {}
                        }
                    }
                    if metadata::find_column(relation, "request_id") == Some(c)
                        && cmp == Compare::Eq
                        && let Constant::Text(value) = value
                    {
                        proofs[s].add_ids(request_ids(std::slice::from_ref(value))?);
                    }
                }
                Kind::In(e, values, false) => {
                    let Kind::Column(s, c) = self.plan.nodes[*e].kind else {
                        continue;
                    };
                    if only.is_some_and(|i| i != s)
                        || metadata::find_column(self.plan.sources[s].relation, "request_id")
                            != Some(c)
                    {
                        continue;
                    }
                    let ids: Option<Vec<_>> = values
                        .iter()
                        .map(|id| match &self.plan.nodes[*id].kind {
                            Kind::Value(Constant::Text(v)) => Some(v.clone()),
                            _ => None,
                        })
                        .collect();
                    if let Some(ids) = ids {
                        proofs[s].add_ids(request_ids(&ids)?);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn grouped(&self, id: Id) -> bool {
        if self.plan.groups.contains(&id) {
            return true;
        }
        match &self.plan.nodes[id].kind {
            Kind::Column(..) => false,
            Kind::Value(_) | Kind::Aggregate(..) => true,
            Kind::Compare(_, a, b) | Kind::And(a, b) | Kind::Or(a, b) => {
                self.grouped(*a) && self.grouped(*b)
            }
            Kind::Not(a) | Kind::IsNull(a, _) => self.grouped(*a),
            Kind::In(a, v, _) => self.grouped(*a) && v.iter().all(|i| self.grouped(*i)),
        }
    }
}

fn number(value: &str) -> Result<Constant, ApiError> {
    if !value.contains(['.', 'e', 'E']) {
        value
            .parse()
            .map(Constant::Int)
            .map_err(|_| ApiError::bad("integer literal exceeds Int64"))
    } else {
        value
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite())
            .map(Constant::Float)
            .ok_or_else(|| ApiError::bad("nonfinite or invalid numeric literal"))
    }
}
fn text_value(value: &str) -> Result<Constant, ApiError> {
    if value.len() > 16384 || value.contains('\0') {
        Err(ApiError::bad(
            "text values must be at most 16KiB without NUL",
        ))
    } else {
        Ok(Constant::Text(value.to_owned()))
    }
}

fn discovery(tokens: &sql_limits::CheckedTokens) -> Result<Option<Discovery>, ApiError> {
    let mut ts: Vec<_> = tokens.tokens().collect();
    if matches!(ts.last(), Some(Token::SemiColon)) {
        ts.pop();
    }
    let word = |token: &&Token, name: &str| matches!(token,Token::Word(w)if w.quote_style.is_none()&&w.value.eq_ignore_ascii_case(name));
    if ts.first().is_some_and(|t| word(t, "SHOW")) && ts.get(1).is_some_and(|t| word(t, "TABLES")) {
        let qualified = ts.len() == 4
            && ts.get(2).is_some_and(|t| word(t, "FROM"))
            && match ts[3] {
                Token::Word(w) => {
                    identifier(&Ident {
                        value: w.value.clone(),
                        quote_style: w.quote_style,
                        span: sqlparser::tokenizer::Span::empty(),
                    })? == "duckfleet_system"
                }
                _ => false,
            };
        return if ts.len() == 2 || qualified {
            Ok(Some(Discovery::Tables))
        } else {
            Err(unsupported(
                "SHOW TABLES accepts only FROM duckfleet_system",
            ))
        };
    }
    let start = if ts
        .first()
        .is_some_and(|t| word(t, "DESCRIBE") || word(t, "DESC"))
    {
        if ts.get(1).is_some_and(|t| word(t, "TABLE")) {
            2
        } else {
            1
        }
    } else if ts.first().is_some_and(|t| word(t, "SHOW"))
        && ts.get(1).is_some_and(|t| word(t, "COLUMNS"))
        && ts.get(2).is_some_and(|t| word(t, "FROM") || word(t, "IN"))
    {
        3
    } else {
        return Ok(None);
    };
    let mut names = Vec::new();
    let mut expect_name = true;
    for token in &ts[start..] {
        match token {
            Token::Word(w) if expect_name => {
                names.push(identifier(&Ident {
                    value: w.value.clone(),
                    quote_style: w.quote_style,
                    span: sqlparser::tokenizer::Span::empty(),
                })?);
                expect_name = false;
            }
            Token::Period if !expect_name => expect_name = true,
            _ => return Err(unsupported("unsupported discovery modifiers")),
        }
    }
    if expect_name {
        return Err(ApiError::bad("missing discovery relation"));
    }
    Ok(Some(Discovery::Columns(relation_name(&names)?)))
}

pub(crate) fn compile(
    sql: &str,
    parameters: &[Value],
    envelope: Option<&BoundsEnvelope>,
) -> Result<Statement, ApiError> {
    if parameters.len() > MAX_PARAMETERS
        || parameters
            .iter()
            .map(|v| match v {
                Value::String(s) => s.len(),
                _ => 32,
            })
            .sum::<usize>()
            > 65536
    {
        return Err(wide("system parameter payload is too large"));
    }
    let dialect = PostgreSqlDialect {};
    let tokens =
        sql_limits::checked_tokens(sql, &dialect, Limits::SYSTEM).map_err(|error| match error {
            GuardError::TooLarge => wide("system SQL exceeds parser complexity limits"),
            GuardError::Unsupported => unsupported("unsupported input character"),
            GuardError::ParseError => ApiError::bad("invalid system SQL"),
        })?;
    if let Some(discovery) = discovery(&tokens)? {
        if !parameters.is_empty() || envelope.is_some_and(|e| !e.is_empty()) {
            return Err(ApiError::bad(
                "discovery does not accept parameters or bounds",
            ));
        }
        return Ok(Statement::Discover(discovery));
    }
    let mut statements =
        sql_limits::parse(&tokens, &dialect).map_err(|_| ApiError::bad("invalid system SQL"))?;
    if statements.len() != 1 {
        return Err(unsupported("exactly one system statement is required"));
    }
    let ast::Statement::Query(query) = statements.pop().unwrap() else {
        return Err(unsupported(
            "only SELECT and system discovery are supported",
        ));
    };
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(unsupported(
            "query modifiers, CTEs and locks are unsupported",
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported("set operations and subqueries are unsupported"));
    };
    if !select.optimizer_hints.is_empty()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.top_before_distinct
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.window_before_qualify
        || select.value_table_mode.is_some()
        || select.flavor != ast::SelectFlavor::Standard
    {
        return Err(unsupported("unsupported SELECT modifier"));
    }
    if select.from.len() != 1 {
        return Err(unsupported("one FROM tree is required"));
    }
    if select.projection.is_empty() || select.projection.len() > MAX_OUTPUTS {
        return Err(wide("SELECT supports 1..32 explicit outputs"));
    }
    let mut c = Compiler::new(parameters);
    let from = &select.from[0];
    c.source(&from.relation, false)?;
    for join in &from.joins {
        if join.global {
            return Err(unsupported("GLOBAL joins are unsupported"));
        }
        let (left, constraint) = match &join.join_operator {
            JoinOperator::Join(on) | JoinOperator::Inner(on) => (false, on),
            JoinOperator::Left(on) | JoinOperator::LeftOuter(on) => (true, on),
            _ => return Err(unsupported("only INNER and LEFT joins are supported")),
        };
        let JoinConstraint::On(on) = constraint else {
            return Err(unsupported(
                "joins require an explicit ON request_id equality",
            ));
        };
        c.source(&join.relation, left)?;
        let source = c.plan.sources.len() - 1;
        let on = c.expression(on, Some(SystemType::Bool), false)?;
        c.certify_join(on, source)?;
        c.uses(on, true);
        c.plan.joins.push(Join { source, left, on });
    }
    if let Some(filter) = &select.selection {
        let id = c.expression(filter, Some(SystemType::Bool), false)?;
        c.uses(id, true);
        c.plan.filter = Some(id);
    }
    let mut labels = BTreeSet::new();
    for projection in &select.projection {
        let (expr, alias) = match projection {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => {
                return Err(unsupported(
                    "explicit projection expressions are required; only COUNT(*) accepts a wildcard",
                ));
            }
        };
        let id = c.expression(expr, None, true)?;
        if c.plan.nodes[id].ty == scalar(SystemType::JsonText)
            && !matches!(c.plan.nodes[id].kind, Kind::Column(..))
        {
            return Err(unsupported(
                "JSON/profile values support direct projection only",
            ));
        }
        let name = match alias {
            Some(a) => identifier(a)?,
            None => match &c.plan.nodes[id].kind {
                Kind::Column(s, column) => {
                    metadata::column_def(c.plan.sources[*s].relation, *column)
                        .unwrap()
                        .name
                        .to_owned()
                }
                Kind::Aggregate(f, _) => match f {
                    Aggregate::Count => "count",
                    Aggregate::Sum => "sum",
                    Aggregate::Avg => "avg",
                    Aggregate::Min => "min",
                    Aggregate::Max => "max",
                }
                .into(),
                _ => format!("column_{}", c.plan.outputs.len() + 1),
            },
        };
        label(&name)?;
        if !labels.insert(name.clone()) {
            return Err(ApiError::bad(
                "duplicate output label; use distinct aliases",
            ));
        }
        c.uses(id, !matches!(c.plan.nodes[id].kind, Kind::Column(..)));
        c.plan.outputs.push(Output { expr: id, name });
    }
    let ast::GroupByExpr::Expressions(groups, modifiers) = &select.group_by else {
        return Err(unsupported("GROUP BY ALL is unsupported"));
    };
    if !modifiers.is_empty() {
        return Err(unsupported("grouping modifiers are unsupported"));
    }
    for group in groups {
        let id = c.expression(group, None, false)?;
        if matches!(c.plan.nodes[id].kind, Kind::Value(_)) {
            return Err(unsupported(
                "GROUP BY ordinals and constants are unsupported",
            ));
        }
        c.no_json(id)?;
        c.uses(id, true);
        c.plan.groups.push(id);
    }
    if let Some(having) = &select.having {
        let id = c.expression(having, Some(SystemType::Bool), true)?;
        c.uses(id, true);
        c.plan.having = Some(id);
    }
    if let Some(order) = &query.order_by {
        if order.interpolate.is_some() {
            return Err(unsupported("ORDER interpolation is unsupported"));
        }
        let OrderByKind::Expressions(order) = &order.kind else {
            return Err(unsupported("ORDER BY ALL is unsupported"));
        };
        if order.len() > MAX_OUTPUTS {
            return Err(wide("too many ORDER BY expressions"));
        }
        for term in order {
            if term.with_fill.is_some() {
                return Err(unsupported("ORDER BY FILL is unsupported"));
            }
            let alias = if let Expr::Identifier(i) = &term.expr {
                let name = identifier(i)?;
                c.plan
                    .outputs
                    .iter()
                    .find(|o| o.name == name)
                    .map(|o| o.expr)
            } else {
                None
            };
            let id = match alias {
                Some(id) => id,
                None => c.expression(&term.expr, None, true)?,
            };
            if matches!(c.plan.nodes[id].kind, Kind::Value(_)) {
                return Err(unsupported(
                    "ORDER BY ordinals and constants are unsupported",
                ));
            }
            c.no_json(id)?;
            c.uses(id, true);
            let descending = match term.options.sort {
                None | Some(OrderBySort::Asc) => false,
                Some(OrderBySort::Desc) => true,
                _ => return Err(unsupported("custom ordering operators are unsupported")),
            };
            c.plan.order.push(Order {
                expr: id,
                descending,
                nulls_first: term.options.nulls_first,
            });
        }
    }
    if let Some(limit) = &query.limit_clause {
        let ast::LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            limit_by,
        } = limit
        else {
            return Err(unsupported("only LIMIT without OFFSET/BY is supported"));
        };
        if !limit_by.is_empty() {
            return Err(unsupported("LIMIT BY is unsupported"));
        }
        let id = c.expression(limit, Some(SystemType::Int64), false)?;
        let Kind::Value(Constant::Int(limit)) = c.plan.nodes[id].kind else {
            return Err(ApiError::bad(
                "LIMIT must be an integer literal or parameter",
            ));
        };
        if !(0..=10000).contains(&limit) {
            return Err(wide("LIMIT must be between 0 and 10000"));
        }
        c.plan.limit = Some(limit as u32);
    }
    let grouped = !c.plan.groups.is_empty()
        || c.plan.having.is_some()
        || c.plan
            .nodes
            .iter()
            .any(|n| matches!(n.kind, Kind::Aggregate(..)));
    if grouped
        && (c.plan.outputs.iter().any(|o| !c.grouped(o.expr))
            || c.plan.order.iter().any(|o| !c.grouped(o.expr))
            || c.plan.having.is_some_and(|id| !c.grouped(id)))
    {
        return Err(ApiError::bad(
            "nonaggregate columns must appear in GROUP BY",
        ));
    }
    if c.parameter_types.len() != parameters.len()
        || (1..=parameters.len()).any(|i| !c.parameter_types.contains_key(&i))
    {
        return Err(ApiError::bad("every supplied parameter must be used"));
    }
    let mut proofs: Vec<_> = (0..c.plan.sources.len())
        .map(|_| Proof::default())
        .collect();
    if let Some(envelope) = envelope {
        for (relation, input) in envelope {
            let Some(index) = c.plan.relations.iter().position(|r| r == relation) else {
                return Err(ApiError::bad("bounds envelope contains an unused relation"));
            };
            match (&input.from, &input.until) {
                (Some(from), Some(until)) => {
                    let from = timestamp(from)?;
                    let until = timestamp(until)?;
                    let width = until
                        .duration_since(from)
                        .map_err(|_| ApiError::bad("invalid source time range"))?;
                    if width.is_zero() || width > MAX_WINDOW {
                        return Err(wide(
                            "source time ranges must be positive and no longer than 31 days",
                        ));
                    }
                    proofs[index].from = Some(from);
                    proofs[index].until = Some(until);
                }
                (None, None) => {}
                _ => return Err(ApiError::bad("bounds require both from and until")),
            }
            if !input.request_ids.is_empty() {
                proofs[index].add_ids(request_ids(&input.request_ids)?);
            }
            if input.from.is_none() && input.request_ids.is_empty() {
                return Err(ApiError::bad("empty bounds envelope entry"));
            }
        }
    }
    if let Some(filter) = c.plan.filter {
        c.proof(filter, None, &mut proofs)?;
    }
    for join in &c.plan.joins {
        c.proof(join.on, join.left.then_some(join.source), &mut proofs)?;
    }
    for (source, proof) in c.plan.sources.iter_mut().zip(proofs) {
        source.bounds = Some(proof.finish()?);
    }
    Ok(Statement::Select(Box::new(c.plan)))
}

fn expression_sql(
    plan: &SelectPlan,
    id: Id,
    bindings: &mut Bindings,
    cache: &mut BTreeMap<Id, String>,
) -> String {
    if let Some(sql) = cache.get(&id) {
        return sql.clone();
    }
    let sql = match &plan.nodes[id].kind {
        Kind::Column(source, column) => format!(
            "s{source}.\"{}\"",
            metadata::column_def(plan.sources[*source].relation, *column)
                .unwrap()
                .name
        ),
        Kind::Value(value) => {
            let i = bindings.push(value.binding()).index();
            let ty = match value.ty() {
                SystemType::Bool => "boolean",
                SystemType::Int64 => "bigint",
                SystemType::Float64 => "double precision",
                SystemType::Utf8 | SystemType::JsonText => "text",
                SystemType::TimestampUtc => "timestamptz",
            };
            format!("(${i}::{ty})")
        }
        Kind::Compare(op, l, r) => {
            let mut a = expression_sql(plan, *l, bindings, cache);
            let mut b = expression_sql(plan, *r, bindings, cache);
            if matches!(plan.nodes[*l].ty, ResultType::Decimal128 { .. })
                && plan.nodes[*r].ty == scalar(SystemType::Int64)
            {
                b = format!("({b})::numeric");
            }
            if matches!(plan.nodes[*r].ty, ResultType::Decimal128 { .. })
                && plan.nodes[*l].ty == scalar(SystemType::Int64)
            {
                a = format!("({a})::numeric");
            }
            format!("({a} {} {b})", op.sql())
        }
        Kind::And(l, r) | Kind::Or(l, r) => {
            let a = expression_sql(plan, *l, bindings, cache);
            let b = expression_sql(plan, *r, bindings, cache);
            format!(
                "({a} {} {b})",
                if matches!(plan.nodes[id].kind, Kind::And(..)) {
                    "AND"
                } else {
                    "OR"
                }
            )
        }
        Kind::Not(e) => format!("(NOT {})", expression_sql(plan, *e, bindings, cache)),
        Kind::IsNull(e, negated) => format!(
            "({} IS {}NULL)",
            expression_sql(plan, *e, bindings, cache),
            if *negated { "NOT " } else { "" }
        ),
        Kind::In(e, values, negated) => {
            let e = expression_sql(plan, *e, bindings, cache);
            let values = values
                .iter()
                .map(|i| expression_sql(plan, *i, bindings, cache))
                .collect::<Vec<_>>()
                .join(",");
            format!("({e} {}IN ({values}))", if *negated { "NOT " } else { "" })
        }
        Kind::Aggregate(function, argument) => {
            let arg =
                argument.map_or_else(|| "*".into(), |a| expression_sql(plan, a, bindings, cache));
            let function = match function {
                Aggregate::Count => "COUNT",
                Aggregate::Sum => "SUM",
                Aggregate::Avg => "AVG",
                Aggregate::Min => "MIN",
                Aggregate::Max => "MAX",
            };
            let sql = format!("{function}({arg})");
            match plan.nodes[id].ty {
                ResultType::Decimal128 { precision, scale } => {
                    format!("({sql})::numeric({precision},{scale})")
                }
                _ => sql,
            }
        }
    };
    cache.insert(id, sql.clone());
    sql
}
fn units(plan: &SelectPlan, id: Id) -> Option<&'static str> {
    match plan.nodes[id].kind {
        Kind::Column(s, c) => {
            metadata::column_def(plan.sources[s].relation, c).and_then(|d| d.units)
        }
        Kind::Aggregate(
            Aggregate::Sum | Aggregate::Avg | Aggregate::Min | Aggregate::Max,
            Some(a),
        ) => units(plan, a),
        _ => None,
    }
}

// The driver receives a whole PostgreSQL DataRow before yielding it. Bound
// repeated text projections BEFORE execution, not merely after decoding a row.
// Every source probe counts all referenced text/JSON columns once; counting output
// occurrences here ensures the worst row stays below 7MiB, even for MIN/MAX and
// repeated aliases. The remaining MiB covers bounded literals and scalar fields.
fn emitting_source(plan: &SelectPlan, id: Id) -> Option<usize> {
    if !matches!(
        plan.nodes[id].ty,
        ResultType::Scalar(SystemType::Utf8 | SystemType::JsonText)
    ) {
        return None;
    }
    match plan.nodes[id].kind {
        Kind::Column(source, _) => Some(source),
        Kind::Aggregate(Aggregate::Min | Aggregate::Max, Some(argument)) => {
            emitting_source(plan, argument)
        }
        _ => None,
    }
}

pub(crate) fn render(
    plan: &SelectPlan,
    access: &MetadataAccess,
    schema: &StorageSchema,
) -> Result<ExecutablePlan, ApiError> {
    let mut bindings = Bindings::new();
    let mut ctes = Vec::new();
    let mut probes = Vec::new();
    let mut applied_bounds = Vec::new();
    let mut text_occurrences = vec![0usize; plan.sources.len()];
    for output in &plan.outputs {
        if let Some(source) = emitting_source(plan, output.expr) {
            text_occurrences[source] += 1;
        }
    }
    let total_occurrences = text_occurrences.iter().sum::<usize>();
    for (index, source) in plan.sources.iter().enumerate() {
        let bounds = source
            .bounds
            .as_ref()
            .expect("compiler certified every source");
        let authorized =
            metadata::source_plan(access, source.relation, bounds, &source.uses, schema)?;
        let raw_profile = source.uses.projected.iter().any(|c| {
            metadata::column_def(source.relation, *c)
                .is_some_and(|d| d.sensitivity == Sensitivity::RawProfile)
        });
        let row_limit = if raw_profile { 100 } else { 50000 };
        probes.push(SourceProbe {
            relation: source.relation,
            query: authorized.cap_probe(row_limit),
            row_limit,
            payload_limit: if text_occurrences[index] == 0 {
                8 * 1024 * 1024
            } else {
                (7 * 1024 * 1024) / total_occurrences as u64
            },
        });
        ctes.push(authorized.cte_sql(index as u8, &mut bindings));
        applied_bounds.push(AppliedBounds {
            relation: source.relation,
            bounds: bounds.clone(),
        });
    }
    let mut cache = BTreeMap::new();
    let mut projected = Vec::new();
    let mut outputs = Vec::new();
    for (index, output) in plan.outputs.iter().enumerate() {
        projected.push(format!(
            "{} AS c{index}",
            expression_sql(plan, output.expr, &mut bindings, &mut cache)
        ));
        let node = &plan.nodes[output.expr];
        outputs.push(OutputColumn {
            name: output.name.clone(),
            ty: node.ty,
            nullable: node.nullable,
            units: units(plan, output.expr),
        });
    }
    for (index, order) in plan.order.iter().enumerate() {
        projected.push(format!(
            "{} AS o{index}",
            expression_sql(plan, order.expr, &mut bindings, &mut cache)
        ));
    }
    let mut inner = format!("SELECT {} FROM s0", projected.join(","));
    for join in &plan.joins {
        inner.push_str(&format!(
            " {} JOIN s{} ON {}",
            if join.left { "LEFT" } else { "INNER" },
            join.source,
            expression_sql(plan, join.on, &mut bindings, &mut cache)
        ));
    }
    if let Some(filter) = plan.filter {
        inner.push_str(&format!(
            " WHERE {}",
            expression_sql(plan, filter, &mut bindings, &mut cache)
        ));
    }
    if !plan.groups.is_empty() {
        inner.push_str(&format!(
            " GROUP BY {}",
            plan.groups
                .iter()
                .map(|i| expression_sql(plan, *i, &mut bindings, &mut cache))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(having) = plan.having {
        inner.push_str(&format!(
            " HAVING {}",
            expression_sql(plan, having, &mut bindings, &mut cache)
        ));
    }
    let final_columns = outputs
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if matches!(c.ty, ResultType::Decimal128 { .. }) {
                format!("q.c{i}::text AS c{i}")
            } else {
                format!("q.c{i}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut sql = format!(
        "WITH {} SELECT {final_columns} FROM ({inner}) AS q",
        ctes.join(",")
    );
    if !plan.order.is_empty() {
        sql.push_str(&format!(
            " ORDER BY {}",
            plan.order
                .iter()
                .enumerate()
                .map(|(i, o)| format!(
                    "q.o{i} {}{}",
                    if o.descending { "DESC" } else { "ASC" },
                    match o.nulls_first {
                        Some(true) => " NULLS FIRST",
                        Some(false) => " NULLS LAST",
                        None => "",
                    }
                ))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    sql.push_str(&format!(" LIMIT {}", plan.limit.unwrap_or(10001)));
    Ok(ExecutablePlan {
        query: BoundSql::new(sql, bindings),
        probes,
        outputs,
        applied_bounds,
        user_limit: plan.limit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz::{Actor, ActorKind, Scope};
    use serde_json::json;
    fn access(sql: Option<Scope>, profile: Option<Scope>) -> MetadataAccess {
        MetadataAccess {
            actor: Actor {
                namespace_id: "namespace-a".into(),
                principal_id: Some("owner-a".into()),
                principal: "analyst".into(),
                credential_id: None,
                kind: ActorKind::Named,
            },
            history: Some(Scope::All),
            usage: Some(Scope::All),
            sql,
            profile,
        }
    }
    fn plan(sql: &str, parameters: &[Value]) -> SelectPlan {
        match compile(sql, parameters, None).unwrap() {
            Statement::Select(p) => *p,
            _ => panic!("expected select"),
        }
    }
    fn executable(sql: &str, parameters: &[Value]) -> ExecutablePlan {
        render(
            &plan(sql, parameters),
            &access(Some(Scope::All), Some(Scope::All)),
            &StorageSchema::validated("public").unwrap(),
        )
        .unwrap()
    }
    fn times() -> Vec<Value> {
        vec![json!("2026-11-01T00:00:00Z"), json!("2026-11-02T00:00:00Z")]
    }
    #[test]
    fn four_product_examples_compile_and_keep_numeric_sorting() {
        let examples = [
            "SELECT query_fingerprint, query_fingerprint_version, COUNT(*) AS runs, AVG(engine_elapsed_ms) AS average_engine_ms, AVG(queue_wait_ms) AS average_queue_ms FROM duckfleet_system.query_history WHERE accepted_at >= $1 AND accepted_at < $2 AND query_fingerprint IS NOT NULL GROUP BY query_fingerprint, query_fingerprint_version ORDER BY average_engine_ms DESC LIMIT 50",
            "SELECT failure_kind, outcome, COUNT(*) AS attempts FROM duckfleet_system.query_attempts WHERE accepted_at >= $1 AND accepted_at < $2 AND state='terminal' AND status<>'succeeded' GROUP BY failure_kind,outcome ORDER BY attempts DESC LIMIT 100",
            "SELECT owner_principal_name,model_id,rate_card_version,SUM(amount_micro_usd) AS amount_micro_usd FROM duckfleet_system.usage WHERE observed_at >= $1 AND observed_at < $2 GROUP BY owner_principal_name,model_id,rate_card_version ORDER BY amount_micro_usd DESC LIMIT 100",
            "SELECT h.request_id,h.status,h.engine_elapsed_ms,u.event_id,u.amount_micro_usd,u.rate_card_version,u.source FROM duckfleet_system.query_history h LEFT JOIN duckfleet_system.usage u ON h.request_id=u.request_id AND u.observed_at >= $3 AND u.observed_at < $4 WHERE h.accepted_at >= $1 AND h.accepted_at < $2 ORDER BY h.history_id DESC LIMIT 100",
        ];
        for (i, sql) in examples.iter().enumerate() {
            let mut parameters = times();
            if i == 3 {
                parameters.extend(times());
            }
            let p = executable(sql, &parameters);
            assert_eq!(p.probes.len(), if i == 3 { 2 } else { 1 });
            assert!(p.query.sql().contains("AS MATERIALIZED"));
            assert!(!p.query.sql().contains("2026-11"));
            assert!(p.query.sql().contains("ORDER BY q.o0 DESC"));
            assert!(p.query.params().len() > parameters.len());
        }
        let p = executable(examples[2], &times());
        assert_eq!(
            p.outputs[3].ty,
            ResultType::Decimal128 {
                precision: 38,
                scale: 0
            }
        );
        assert!(p.query.sql().contains("q.c3::text"));
        assert!(p.query.sql().contains("numeric(38,0) AS o0"));
        let p = executable(examples[0], &times());
        assert_eq!(
            p.outputs[3].ty,
            ResultType::Decimal128 {
                precision: 38,
                scale: 6
            }
        );
    }
    #[test]
    fn rejects_bounds_and_join_proof_escape_hatches() {
        for sql in [
            "SELECT request_id FROM query_history LIMIT 1",
            "SELECT request_id FROM query_history WHERE request_id='x' OR TRUE",
            "SELECT request_id FROM query_history WHERE NOT(request_id='x')",
            "SELECT h.request_id FROM query_history h LEFT JOIN usage u ON h.request_id=u.request_id AND h.request_id='x' AND u.request_id='x'",
            "SELECT h.request_id FROM query_history h INNER JOIN usage u ON h.request_id=h.request_id WHERE h.request_id='x' AND u.request_id='x'",
            "SELECT h.request_id FROM query_history h INNER JOIN usage u ON u.request_id=u.request_id WHERE h.request_id='x' AND u.request_id='x'",
            "SELECT h.request_id FROM query_history h INNER JOIN usage u ON h.request_id=u.request_id OR TRUE WHERE h.request_id='x' AND u.request_id='x'",
            "SELECT h.request_id FROM query_history h JOIN query_history b ON h.request_id=b.request_id WHERE h.request_id='x' AND b.request_id='x'",
        ] {
            assert!(compile(sql, &[], None).is_err(), "accepted {sql}");
        }
        let p = plan(
            "SELECT h.request_id,u.event_id FROM query_history h LEFT JOIN usage u ON h.request_id=u.request_id AND u.request_id='x' WHERE h.request_id='x'",
            &[],
        );
        assert_eq!(p.sources.len(), 2);
        assert!(p.nodes[p.outputs[1].expr].nullable);
        assert!(compile("SELECT request_id FROM query_history WHERE request_id='x' AND (status='failed' OR status='succeeded')",&[],None).is_ok());
    }
    #[test]
    fn validates_utc_bounds_placeholders_and_labels() {
        let sql =
            "SELECT accepted_at FROM query_history WHERE accepted_at >= $1 AND accepted_at < $2";
        assert!(compile(sql, &times(), None).is_ok());
        for parameters in [
            vec![json!("2026-11-01T00:00:00"), times()[1].clone()],
            vec![json!("2026-11-01T00:00:00.1234567Z"), times()[1].clone()],
            vec![times()[0].clone(), json!("2027-01-01T00:00:00Z")],
            vec![times()[0].clone()],
        ] {
            assert!(compile(sql, &parameters, None).is_err());
        }
        assert!(
            compile(
                "SELECT request_id FROM query_history WHERE request_id=$1 AND history_id=$1",
                &[json!("1")],
                None
            )
            .is_err()
        );
        assert!(
            compile(
                "SELECT request_id FROM query_history WHERE request_id='x'",
                &[json!(1)],
                None
            )
            .is_err()
        );
        for alias in ["bad\0name".into(), "bad\nname".into(), "x".repeat(129)] {
            assert!(
                compile(
                    &format!(
                        "SELECT request_id AS \"{alias}\" FROM query_history WHERE request_id='x'"
                    ),
                    &[],
                    None
                )
                .is_err()
            );
        }
        assert!(
            compile(
                "SELECT request_id AS id,status AS id FROM query_history WHERE request_id='x'",
                &[],
                None
            )
            .is_err()
        );
        assert_eq!(
            timestamp("2026-11-01T02:00:00+02:00").unwrap(),
            timestamp("2026-11-01T00:00:00Z").unwrap()
        );
    }
    #[test]
    fn envelope_is_explicit_and_cannot_hide_unused_relations() {
        let mut envelope = BoundsEnvelope::new();
        envelope.insert(
            Relation::History,
            BoundsInput {
                from: None,
                until: None,
                request_ids: vec!["x".into()],
            },
        );
        assert!(compile("SELECT COUNT(*) FROM query_history", &[], Some(&envelope)).is_ok());
        assert!(compile("SELECT COUNT(*) FROM usage", &[], Some(&envelope)).is_err());
        assert!(compile("SHOW TABLES", &[], Some(&envelope)).is_err());
        assert!(
            compile(
                "SELECT COUNT(*) FROM query_history WHERE request_id='other'",
                &[],
                Some(&envelope)
            )
            .is_err()
        );
    }
    #[test]
    fn sensitive_projection_can_mask_but_evaluation_cannot() {
        let schema = StorageSchema::validated("public").unwrap();
        let mixed = access(Some(Scope::Own), Some(Scope::Own));
        assert!(
            render(
                &plan(
                    "SELECT sql_text FROM query_history WHERE request_id='x'",
                    &[]
                ),
                &mixed,
                &schema
            )
            .is_ok()
        );
        for sql in [
            "SELECT COUNT(*) FROM query_history WHERE request_id='x' AND sql_text='hidden'",
            "SELECT sql_text FROM query_history WHERE request_id='x' ORDER BY sql_text",
            "SELECT COUNT(sql_text) FROM query_history WHERE request_id='x'",
            "SELECT query_fingerprint,COUNT(*) FROM query_history WHERE request_id='x' GROUP BY query_fingerprint",
        ] {
            let error = render(&plan(sql, &[]), &mixed, &schema).err().unwrap();
            assert_eq!(error.0.kind, "forbidden");
        }
        let profile = render(
            &plan(
                "SELECT profile_json FROM query_history WHERE request_id='x'",
                &[],
            ),
            &mixed,
            &schema,
        )
        .unwrap();
        assert_eq!(profile.probes[0].row_limit, 100);
        assert_eq!(profile.probes[0].payload_limit, 7 * 1024 * 1024);
        assert!(
            compile(
                "SELECT COUNT(profile_json) FROM query_history WHERE request_id='x'",
                &[],
                None
            )
            .is_err()
        );
    }
    #[test]
    fn closed_grammar_rejects_functions_state_and_ast_modifiers() {
        for sql in [
            "DELETE FROM query_history",
            "SELECT pg_sleep(1) FROM query_history WHERE request_id='x'",
            "SELECT * FROM query_history WHERE request_id='x'",
            "SELECT DISTINCT status FROM query_history WHERE request_id='x'",
            "SELECT SUM(history_id) OVER () FROM query_history WHERE request_id='x'",
            "SELECT COUNT(DISTINCT status) FROM query_history WHERE request_id='x'",
            "SELECT status,COUNT(*) FROM query_history WHERE request_id='x'",
            "SELECT COUNT(*) FROM query_history WHERE request_id='x' ORDER BY status",
            "SELECT COUNT(*) FROM query_history WHERE request_id='x' LIMIT 1 OFFSET 1",
            "SELECT request_id FROM query_history WHERE request_id='x' FOR UPDATE",
            "SELECT application_context->>'x' FROM query_history WHERE request_id='x'",
            "WITH h AS (SELECT 1) SELECT * FROM h",
            "SELECT COUNT(*) FROM query_history WHERE request_id='x'; SELECT 1",
            "EXPLAIN query_history",
            "SHOW TABLES LIKE '%'",
        ] {
            assert!(compile(sql, &[], None).is_err(), "accepted {sql}");
        }
        assert!(matches!(
            compile("SHOW TABLES;", &[], None).unwrap(),
            Statement::Discover(Discovery::Tables)
        ));
        assert!(matches!(
            compile("SHOW TABLES FROM duckfleet_system", &[], None).unwrap(),
            Statement::Discover(Discovery::Tables)
        ));
        assert!(compile("SHOW TABLES FROM public", &[], None).is_err());
        assert!(compile("SHOW TABLES FROM duckfleet_system.extra", &[], None).is_err());
        for sql in [
            "DESCRIBE duckfleet_system.query_history",
            "SHOW COLUMNS FROM usage",
        ] {
            assert!(matches!(
                compile(sql, &[], None).unwrap(),
                Statement::Discover(Discovery::Columns(_))
            ));
        }
    }
    #[test]
    fn bounded_lists_and_maximum_repeated_outputs_are_supported() {
        let ids = (0..100)
            .map(|i| format!("'id-{i}'"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(
            compile(
                &format!("SELECT COUNT(*) FROM query_history WHERE request_id IN ({ids})"),
                &[],
                None
            )
            .is_ok()
        );
        let outputs = (0..32)
            .map(|i| format!("sql_text AS column_{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let repeated = executable(
            &format!("SELECT {outputs} FROM query_history WHERE request_id='x'"),
            &[],
        );
        assert_eq!(repeated.outputs.len(), 32);
        assert_eq!(repeated.probes[0].payload_limit, 7 * 1024 * 1024 / 32);
        assert!(
            compile(
                "SELECT request_id FROM query_history WHERE request_id='x' ORDER BY 1",
                &[],
                None
            )
            .is_err()
        );
        assert!(
            compile(
                "SELECT COUNT(*) FROM query_history WHERE request_id='x' GROUP BY 1",
                &[],
                None
            )
            .is_err()
        );
    }

    #[test]
    fn repeated_payload_and_text_aggregates_have_a_preexecution_row_budget() {
        let repeated = executable(
            "SELECT sql_text AS first, sql_text AS second FROM query_history WHERE request_id='x'",
            &[],
        );
        assert_eq!(repeated.probes[0].payload_limit, 7 * 1024 * 1024 / 2);
        let aggregate = executable(
            "SELECT MIN(sql_text) AS first, MAX(sql_text) AS second FROM query_history WHERE request_id='x'",
            &[],
        );
        assert_eq!(
            aggregate.probes[0].payload_limit,
            repeated.probes[0].payload_limit
        );
        let evaluated = executable(
            "SELECT COUNT(*) FROM query_history WHERE request_id='x' AND sql_text='value'",
            &[],
        );
        assert_eq!(evaluated.probes[0].payload_limit, 8 * 1024 * 1024);
        let mixed = executable(
            "SELECT h.sql_text, u.application_context FROM query_history h LEFT JOIN usage u ON h.request_id=u.request_id AND u.request_id='x' WHERE h.request_id='x'",
            &[],
        );
        assert!(
            mixed
                .probes
                .iter()
                .all(|probe| probe.payload_limit == 7 * 1024 * 1024 / 2)
        );
    }

    #[test]
    fn rendered_sql_never_interpolates_values_or_output_labels() {
        let sentinel = "sensitive'; DROP TABLE credentials; --";
        let p = executable(
            "SELECT request_id AS \"safe but odd; label\" FROM query_history WHERE request_id=$1",
            &[json!(sentinel)],
        );
        assert!(!p.query.sql().contains(sentinel));
        assert!(!p.query.sql().contains("safe but odd"));
        assert_eq!(p.outputs[0].name, "safe but odd; label");
        for probe in p.probes {
            let n = probe.query.params().len();
            for i in 1..=n {
                assert!(probe.query.sql().contains(&format!("${i}")));
            }
            assert!(!probe.query.sql().contains(&format!("${}", n + 1)));
        }
    }
}
