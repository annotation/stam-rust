#![allow(unused_imports)]
#![allow(dead_code)]
use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::datakey::DataKeyHandle;
use crate::error::StamError;
use crate::textselection::TextSelectionOperator;
use crate::AnnotationDataSetHandle;
use crate::{store::*, ResultTextSelection};
use crate::{types::*, DataOperator};
use crate::{TextResource, TextResourceHandle};

use smallvec::SmallVec;
use std::collections::HashMap;

use std::borrow::Cow;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryType {
    Select,
}

pub struct Query<'a> {
    /// The variable name
    name: Option<&'a str>,

    querytype: QueryType,
    resulttype: Option<Type>,
    constraints: Vec<Constraint<'a>>,
    subquery: Option<Box<Query<'a>>>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ResourceQualifier {
    Any,
    AsMetadata,
    AsText,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// This qualifier is used for annotation constraints ([`Constraint::Annotation`] and
/// [`Constraint::AnnotationVariable`]) on other annotations. It determines whether to look 'up' or
/// 'down' in the annotation hierarchy tree formed by the AnnotationSelector.
/// When the constraint is applied to other items than annotations, the qualifier has no impact.
pub enum AnnotationQualifier {
    None,
    Recursive,
    Target,
    RecursiveTarget,
}

#[derive(Debug)]
pub enum Constraint<'a> {
    Id(&'a str),

    /// ID of the annotation
    Annotation(&'a str, AnnotationQualifier),

    /// ID of a TextResource
    TextResource(&'a str, ResourceQualifier),
    DataKey {
        set: &'a str,
        key: &'a str,
    },
    DataKeyVariable(&'a str),
    DataVariable(&'a str),
    ResourceVariable(&'a str, ResourceQualifier),
    TextVariable(&'a str),
    TextRelation {
        var: &'a str,
        operator: TextSelectionOperator,
    },
    KeyValue {
        set: &'a str,
        key: &'a str,
        operator: DataOperator<'a>,
    },
    Text(&'a str),
    /// Disjunction
    Union(Vec<Constraint<'a>>),
    AnnotationVariable(&'a str, AnnotationQualifier),

    /// Use a direct Filter as constraint.
    /// This is useful when constructing queryies
    /// programmatically, bypassing the query parser.
    Filter(Filter<'a>),

    /// Use a direct Filter to select an item.
    /// This is useful when constructing queryies
    /// programmatically, bypassing the query parser.
    /// Filters in this role are used analogous to the ID Constraint.
    Handle(Filter<'a>),
}

impl<'a> Constraint<'a> {
    pub fn keyword(&self) -> &'static str {
        match self {
            Self::Id(..) => "ID",
            Self::TextResource { .. } | Self::ResourceVariable(..) => "RESOURCE",
            Self::TextRelation { .. } => "RELATION",
            Self::KeyValue { .. } | Self::DataKey { .. } | Self::DataVariable(..) => "DATA",
            Self::DataKeyVariable(..) => "KEY",
            Self::Text { .. } | Self::TextVariable(..) => "TEXT",
            Self::AnnotationVariable(..) | Self::Annotation(..) => "ANNOTATION",
            Self::Union { .. } => "UNION",

            //not real keywords that can be parsed, only for debug printing purposes:
            Self::Handle(..) => "HANDLE",
            Self::Filter(Filter::TextResource(..))
            | Self::Filter(Filter::Resources(..))
            | Self::Filter(Filter::BorrowedResources(..)) => "FILTER(RESOURCE)",
            Self::Filter(Filter::Text(..))
            | Self::Filter(Filter::TextSelection(..))
            | Self::Filter(Filter::TextSelections(..))
            | Self::Filter(Filter::BorrowedText(..)) => "FILTER(TEXT)",
            Self::Filter(Filter::TextSelectionOperator(..)) => "FILTER(RELATION)",
            Self::Filter(Filter::Data(..))
            | Self::Filter(Filter::BorrowedData(..))
            | Self::Filter(Filter::DataKey(..))
            | Self::Filter(Filter::DataOperator(..))
            | Self::Filter(Filter::DataKeyAndOperator(..))
            | Self::Filter(Filter::AnnotationData(..)) => "FILTER(DATA)",
            Self::Filter(Filter::Annotation(..))
            | Self::Filter(Filter::Annotations(..))
            | Self::Filter(Filter::BorrowedAnnotations(..)) => "FILTER(ANNOTATION)",
            Self::Filter(Filter::AnnotationDataSet(..)) => "FILTER(DATASET)",
            Self::Filter(_) => "FILTER(UNKNOWN)",
        }
    }

    pub(crate) fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let constraint = match querystring.split(" ").next() {
            Some("ID") => {
                querystring = querystring["ID".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                Self::Id(arg)
            }
            Some("TEXT") => {
                querystring = querystring["TEXT".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::TextVariable(&arg[1..])
                } else {
                    Self::Text(arg)
                }
            }
            Some("ANNOTATION") => {
                querystring = querystring["ANNOTATION".len()..].trim_start();
                let (mut arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                let mut annotationqualifier = AnnotationQualifier::None;
                if arg == "AS" {
                    let (as_arg, remainder, _) = get_arg(querystring)?;
                    annotationqualifier = match as_arg {
                        "TARGET" => AnnotationQualifier::Target,
                        "RECURSIVE" => AnnotationQualifier::Recursive,
                        "RECURSIVETARGET" => AnnotationQualifier::RecursiveTarget,
                        _ => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Expected keyword TARGET, RECURSIVE or RECURSIVETARGET after ANNOTATION AS, got '{}'",
                                    remainder.split(" ").next().unwrap_or("(empty string)")
                                ),
                                "",
                            ))
                        }
                    };
                    let (newarg, remainder, _) = get_arg(querystring)?;
                    arg = newarg;
                    querystring = remainder;
                }
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::AnnotationVariable(&arg[1..], annotationqualifier)
                } else {
                    Self::Annotation(arg, annotationqualifier)
                }
            }
            Some("RESOURCE") => {
                querystring = querystring["RESOURCE".len()..].trim_start();
                let (mut arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                let mut resourcequalifier = ResourceQualifier::Any;
                if arg == "AS" {
                    let (as_arg, remainder, _) = get_arg(querystring)?;
                    resourcequalifier = match as_arg {
                        "METADATA" => ResourceQualifier::AsMetadata,
                        "TEXT" => ResourceQualifier::AsText,
                        _ => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Expected keyword METADATA or TEXT after RESOURCE AS, got '{}'",
                                    remainder.split(" ").next().unwrap_or("(empty string)")
                                ),
                                "",
                            ))
                        }
                    };
                    let (newarg, remainder, _) = get_arg(querystring)?;
                    arg = newarg;
                    querystring = remainder;
                }
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::ResourceVariable(&arg[1..], resourcequalifier)
                } else {
                    Self::TextResource(arg, resourcequalifier)
                }
            }
            Some("RELATION") => {
                querystring = querystring["RELATION".len()..].trim_start();
                let (var, remainder, _) = get_arg(querystring)?;
                if !var.starts_with("?") {
                    return Err(StamError::QuerySyntaxError(
                        format!(
                            "Expected variable after 'RELATION' keyword, got '{}'",
                            remainder.split(" ").next().unwrap_or("(empty string)")
                        ),
                        "",
                    ));
                }
                querystring = remainder;
                let (op, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                let operator = match op {
                    "EQUALS" => TextSelectionOperator::equals(),
                    "EMBEDS" => TextSelectionOperator::embeds(),
                    "EMBEDDED" => TextSelectionOperator::embedded(),
                    "OVERLAPS" => TextSelectionOperator::overlaps(),
                    "PRECEDES" => TextSelectionOperator::precedes(),
                    "SUCCEEDS" => TextSelectionOperator::succeeds(),
                    "SAMEBEGIN" => TextSelectionOperator::samebegin(),
                    "SAMEEND" => TextSelectionOperator::sameend(),
                    "BEFORE" => TextSelectionOperator::before(),
                    "AFTER" => TextSelectionOperator::after(),
                    _ => {
                        return Err(StamError::QuerySyntaxError(
                            format!(
                            "Expected text relation operator keyword for 'RELATION', got unexpected '{}'",
                            op
                        ),
                            "",
                        ))
                    }
                };
                Self::TextRelation {
                    var: &var[1..],
                    operator,
                }
            }
            Some("DATA") => {
                querystring = querystring["DATA".len()..].trim_start();
                if querystring.starts_with("?") {
                    let (var, remainder, _) = get_arg(querystring)?;
                    querystring = remainder;
                    Self::DataVariable(&var[1..])
                } else {
                    let (set, remainder, _) = get_arg(querystring)?;
                    let (key, remainder, _) = get_arg(remainder)?;
                    querystring = remainder;
                    if querystring.is_empty() {
                        Self::DataKey { set, key }
                    } else {
                        let (opstr, remainder, _) = get_arg(querystring)?;
                        let (value, remainder, valuetype) = get_arg(remainder)?;
                        querystring = remainder;
                        let operator = match (opstr, valuetype) {
                            ("=", ArgType::String) => DataOperator::Equals(value),
                            ("=", ArgType::Integer) => DataOperator::EqualsInt(
                                value.parse().expect("str->int conversion should work"),
                            ),
                            ("=", ArgType::Float) => DataOperator::EqualsFloat(
                                value.parse().expect("str->float conversion should work"),
                            ),
                            ("!=", ArgType::String) => {
                                DataOperator::Not(Box::new(DataOperator::Equals(value)))
                            }
                            ("!=", ArgType::Integer) => {
                                DataOperator::Not(Box::new(DataOperator::EqualsInt(
                                    value.parse().expect("str->int conversion should work"),
                                )))
                            }
                            ("!=", ArgType::Float) => {
                                DataOperator::Not(Box::new(DataOperator::EqualsFloat(
                                    value.parse().expect("str->float conversion should work"),
                                )))
                            }
                            (">", ArgType::Integer) => DataOperator::GreaterThan(
                                value.parse().expect("str->int conversion should work"),
                            ),
                            (">=", ArgType::Integer) => DataOperator::GreaterThanOrEqual(
                                value.parse().expect("str->int conversion should work"),
                            ),
                            ("<", ArgType::Integer) => DataOperator::LessThan(
                                value.parse().expect("str->int conversion should work"),
                            ),
                            ("<=", ArgType::Integer) => DataOperator::LessThanOrEqual(
                                value.parse().expect("str->int conversion should work"),
                            ),
                            (">", ArgType::Float) => DataOperator::GreaterThanFloat(
                                value.parse().expect("str->float conversion should work"),
                            ),
                            (">=", ArgType::Float) => DataOperator::GreaterThanOrEqualFloat(
                                value.parse().expect("str->float conversion should work"),
                            ),
                            ("<", ArgType::Float) => DataOperator::LessThanFloat(
                                value.parse().expect("str->float conversion should work"),
                            ),
                            ("<=", ArgType::Float) => DataOperator::LessThanOrEqualFloat(
                                value.parse().expect("str->float conversion should work"),
                            ),
                            ("=", ArgType::List) => {
                                let values: Vec<_> =
                                    value.split("|").map(|x| DataOperator::Equals(x)).collect();
                                DataOperator::Or(values)
                            }
                            _ => return Err(StamError::QuerySyntaxError(format!("Invalid combination of operator and value: '{}' and '{}', type {:?}", opstr,value,valuetype), ""))
                        };
                        Self::KeyValue { set, key, operator }
                    }
                }
            }
            Some("KEY") => {
                querystring = querystring["KEY".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                if arg.starts_with("?") && arg.len() > 1 {
                    querystring = remainder;
                    Self::DataKeyVariable(&arg[1..])
                } else {
                    let (set, remainder, _) = get_arg(querystring)?;
                    let (key, remainder, _) = get_arg(remainder)?;
                    querystring = remainder;
                    Self::DataKey { set, key }
                }
            }
            Some(x) => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected constraint type (DATA, TEXT), got '{}'", x),
                    "",
                ))
            }
            None => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected constraint type (DATA, TEXT), got end of string"),
                    "",
                ))
            }
        };
        Ok((constraint, querystring))
    }
}

impl<'a> Query<'a> {
    pub fn new(querytype: QueryType, resulttype: Option<Type>, name: Option<&'a str>) -> Self {
        Self {
            name,
            querytype,
            resulttype,
            constraints: Vec::new(),
            subquery: None,
        }
    }

    pub fn with_constraint(mut self, constraint: Constraint<'a>) -> Self {
        self.constraints.push(constraint);
        self
    }

    pub fn constrain(&mut self, constraint: Constraint<'a>) -> &mut Self {
        self.constraints.push(constraint);
        self
    }

    pub fn with_subquery(mut self, query: Query<'a>) -> Self {
        self.subquery = Some(Box::new(query));
        self
    }

    pub fn with_name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    pub fn subquery(&self) -> Option<&Query<'a>> {
        self.subquery.as_deref()
    }

    /// Iterates over all constraints in the Query
    pub fn iter(&self) -> std::slice::Iter<Constraint<'a>> {
        self.constraints.iter()
    }

    pub fn name(&self) -> Option<&'a str> {
        self.name
    }

    pub fn querytype(&self) -> QueryType {
        self.querytype
    }

    pub fn resulttype(&self) -> Option<Type> {
        self.resulttype
    }

    pub fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let mut end: usize;
        if let Some("SELECT") = querystring.split(" ").next() {
            end = 7;
        } else {
            return Err(StamError::QuerySyntaxError(
                format!(
                    "Expected SELECT, got '{}'",
                    querystring.split(" ").next().unwrap_or("(empty string)"),
                ),
                "",
            ));
        }
        querystring = querystring[end..].trim_start();
        let resulttype = match &querystring.split(" ").next() {
            Some("ANNOTATION") | Some("annotation") => {
                end = "ANNOTATION".len();
                Some(Type::Annotation)
            }
            Some("DATA") | Some("data") => {
                end = "DATA".len();
                Some(Type::AnnotationData)
            }
            Some("KEY") | Some("key") => {
                end = "KEY".len();
                Some(Type::DataKey)
            }
            Some("TEXT") | Some("text") => {
                end = "TEXT".len();
                Some(Type::TextSelection)
            }
            Some(x) => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected result type (ANNOTATION, DATA, TEXT), got '{}'", x),
                    "",
                ))
            }
            None => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected result type (ANNOTATION, DATA, TEXT), got end of string"),
                    "",
                ))
            }
        };
        querystring = querystring[end..].trim_start();
        let name = if let Some('?') = querystring.chars().next() {
            Some(querystring[1..].split(" ").next().unwrap())
        } else {
            None
        };
        if let Some(name) = name {
            querystring = querystring[1 + name.len()..].trim_start();
        }
        let mut constraints = Vec::new();
        match querystring.split(" ").next() {
            Some("WHERE") => querystring = querystring["WHERE".len()..].trim_start(),
            Some("{") | Some("") | None => {} //no-op (select all, end of query, no where clause)
            _ => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Expected WHERE, got '{}'",
                        querystring.split(" ").next().unwrap_or("(empty string)"),
                    ),
                    "",
                ));
            }
        }

        //parse constraints
        while !querystring.is_empty()
            && querystring.trim_start().chars().nth(0) != Some('{')
            && querystring.trim_start().chars().nth(0) != Some('}')
        {
            let (constraint, remainder) = Constraint::parse(querystring)?;
            querystring = remainder;
            constraints.push(constraint);
        }

        //parse subquery
        let mut subquery = None;
        if querystring.trim_start().chars().nth(0) == Some('{') {
            querystring = &querystring[1..].trim_start();
            if querystring.starts_with("SELECT") {
                let (sq, remainder) = Self::parse(querystring)?;
                subquery = Some(Box::new(sq));
                querystring = remainder.trim_start();
            }
            if querystring.trim_start().chars().nth(0) == Some('}') {
                querystring = &querystring[1..].trim_start();
            } else {
                return Err(StamError::QuerySyntaxError(
                    "Missing '}' to close subquery block".to_string(),
                    "",
                ));
            }
        }
        Ok((
            Self {
                name,
                querytype: QueryType::Select,
                resulttype,
                constraints,
                subquery,
            },
            querystring,
        ))
    }
}

impl<'a> TryFrom<&'a str> for Query<'a> {
    type Error = StamError;
    fn try_from(querystring: &'a str) -> Result<Self, Self::Error> {
        let (query, remainder) = Query::parse(querystring)?;
        if !remainder.trim().is_empty() {
            return Err(StamError::QuerySyntaxError(
                format!("Expected end of statement, got '{}'", remainder),
                "",
            ));
        }
        Ok(query)
    }
}

/// This type abstracts over all the main iterators
/// This abstraction uses dynamic dispatch so comes with a small performance cost
pub enum QueryResultIter<'store> {
    Annotations(Box<dyn Iterator<Item = ResultItem<'store, Annotation>> + 'store>),
    Data(Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>),
    TextSelections(Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>),
    Resources(Box<dyn Iterator<Item = ResultItem<'store, TextResource>> + 'store>),
}

#[derive(Clone, Debug)]
pub enum QueryResultItem<'store> {
    None,
    TextSelection(ResultTextSelection<'store>),
    Annotation(ResultItem<'store, Annotation>),
    TextResource(ResultItem<'store, TextResource>),
    AnnotationData(ResultItem<'store, AnnotationData>),
}

pub struct QueryState<'store> {
    /// The iterator for the current query
    iterator: QueryResultIter<'store>,

    // note: this captures the result of the current state, in order to make it available for subsequent deeper iterators
    result: QueryResultItem<'store>,
}

pub struct QueryIter<'store> {
    store: &'store AnnotationStore,

    /// A flattened representation of the queries
    queries: Vec<Query<'store>>,

    /// States in the stack hold iterators, each stack item corresponds to one further level of nesting
    statestack: Vec<QueryState<'store>>,

    /// Signals that we're done with the entire stack
    done: bool,
}

pub struct QueryNames(HashMap<String, usize>);

impl QueryNames {
    pub fn get(&self, mut name: &str) -> Result<usize, StamError> {
        if name.starts_with("&") {
            name = &name[1..];
        }
        self.0
            .get(name)
            .copied()
            .ok_or_else(|| StamError::QuerySyntaxError(format!("Variable ?{} not found", name), ""))
    }
}

/// Represents an entire result row, each result stems from a query
pub struct QueryResultItems<'store> {
    items: SmallVec<[QueryResultItem<'store>; 4]>,
}

impl<'store> AnnotationStore {
    /// Instantiates a query, returns an iterator.
    /// No actual querying is done yet until you use the iterator.
    pub fn query(&'store self, query: Query<'store>) -> QueryIter<'store> {
        let mut iter = QueryIter {
            store: self,
            queries: Vec::new(),
            statestack: Vec::new(),
            done: false,
        };
        //flatten nested subqueries into an array
        let mut query = Some(query);
        while let Some(mut q) = query.take() {
            let subquery = q.subquery.take();
            iter.queries.push(q);
            query = subquery.map(|x| *x);
        }
        iter
    }
}

impl<'store> QueryIter<'store> {
    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    pub fn names(&self) -> QueryNames {
        let mut map = HashMap::new();
        for (i, query) in self.queries.iter().enumerate() {
            if let Some(name) = query.name {
                map.insert(name.to_string(), i);
            }
        }
        QueryNames(map)
    }

    /// Initializes a new state
    pub fn init_state(&mut self) -> Result<bool, StamError> {
        let queryindex = self.statestack.len();
        let query = self.queries.get(queryindex).expect("query must exist");
        let store = self.store();
        let mut constraintsiter = query.constraints.iter();

        let iter = match query.resulttype {
            /////////////////////////////////////////////////////////////////////////
            Some(Type::TextResource) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, TextResource>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::Id(id)) | Some(&Constraint::TextResource(id,_)) => {
                            Box::new(Some(store.resource(id).or_fail()?).into_iter())
                        }
                        Some(&Constraint::Handle(Filter::TextResource(handle))) | Some(&Constraint::Filter(Filter::TextResource(handle)))=> {
                            Box::new(Some(store.resource(handle).or_fail()?).into_iter())
                        }
                        Some(&Constraint::DataKey { set, key }) => Box::new(
                            store
                                .key(set, key).or_fail()?
                                .annotations()
                                .resources(),
                        ),
                        Some(&Constraint::Filter(Filter::DataKey(set, key))) => {
                            Box::new(store.key(set, key).or_fail()?.annotations().resources())
                        }
                        Some(&Constraint::DataVariable(var)) => {
                            let data = self.resolve_datavar(var)?;
                            Box::new(data.annotations().resources())
                        }
                        Some(&Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        }) => Box::new(
                            store
                                .find_data(set, key, operator.clone())
                                .annotations()
                                .resources(),
                        ),
                        Some(&Constraint::Filter(Filter::DataKeyAndOperator(set, key, ref operator))) => {
                            Box::new(
                            store
                                .find_data(set, key, operator.clone())
                                .annotations()
                                .resources(),
                            )
                        }
                        Some(c) => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} (primary) is not implemented for queries over resources",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                        None => Box::new(store.resources()),
                    };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::DataKey { set, key } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key(&key));
                        }
                        &Constraint::Filter(Filter::DataKey(set, key)) => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key(&key));
                        }
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()));
                        }
                        &Constraint::Filter(Filter::DataKeyAndOperator(set, key, ref operator)) => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()));
                        },
                        c => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} (secondary) is not implemented for queries over resources",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                    }
                }
                Ok(QueryResultIter::Resources(iter))
            }
            Some(Type::Annotation) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, Annotation>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::Id(id)) => {
                            Box::new(Some(store.annotation(id).or_fail()?).into_iter())
                        }
                        Some(&Constraint::Handle(Filter::Annotation(handle))) => {
                            Box::new(Some(store.annotation(handle).or_fail()?).into_iter())
                        }
                        Some(&Constraint::TextResource(res, ResourceQualifier::Any)) => {
                            Box::new(store.resource(res).or_fail()?.annotations())
                        }
                        Some(&Constraint::TextResource(res, ResourceQualifier::AsMetadata)) => {
                            Box::new(store.resource(res).or_fail()?.annotations_as_metadata())
                        }
                        Some(&Constraint::TextResource(res, ResourceQualifier::AsText)) => {
                            Box::new(store.resource(res).or_fail()?.annotations_on_text())
                        }
                        Some(&Constraint::Filter(Filter::TextResource(res))) => {
                            Box::new(store.resource(res).or_fail()?.annotations())
                        }
                        Some(&Constraint::ResourceVariable(var, ResourceQualifier::Any)) => {
                            let resource = self.resolve_resourcevar(var)?;
                            Box::new(resource.annotations())
                        }
                        Some(&Constraint::ResourceVariable(var, ResourceQualifier::AsMetadata)) => {
                            let resource = self.resolve_resourcevar(var)?;
                            Box::new(resource.annotations_as_metadata())
                        }
                        Some(&Constraint::ResourceVariable(var, ResourceQualifier::AsText)) => {
                            let resource = self.resolve_resourcevar(var)?;
                            Box::new(resource.annotations_on_text())
                        }
                        Some(&Constraint::Annotation(annotation, AnnotationQualifier::None)) => {
                            Box::new(store.annotation(annotation).or_fail()?.annotations_in_targets(false))
                        }
                        Some(&Constraint::Annotation(annotation, AnnotationQualifier::Recursive)) => {
                            Box::new(store.annotation(annotation).or_fail()?.annotations_in_targets(true))
                        }
                        Some(&Constraint::Filter(Filter::Annotation(annotation))) => {
                            Box::new(store.annotation(annotation).or_fail()?.annotations_in_targets(false))
                        }
                        Some(&Constraint::Filter(Filter::AnnotationTargetsFor(annotation, recursive))) => { //TODO: recursive variant?
                            Box::new(store.annotation(annotation).or_fail()?.annotations())
                        }
                        Some(&Constraint::AnnotationVariable(var, AnnotationQualifier::None)) => {
                            let annotation = self.resolve_annotationvar(var)?;
                            Box::new(annotation.annotations_in_targets(false))
                        }
                        Some(&Constraint::AnnotationVariable(var, AnnotationQualifier::Recursive)) => {
                            let annotation = self.resolve_annotationvar(var)?;
                            Box::new(annotation.annotations_in_targets(true))
                        }
                        Some(&Constraint::AnnotationVariable(var, AnnotationQualifier::Target)) => { //TODO: Recursive variant?
                            let annotation = self.resolve_annotationvar(var)?;
                            Box::new(annotation.annotations())
                        }
                        Some(&Constraint::DataKey { set, key }) => {
                            Box::new(store.key(set, key).or_fail()?.annotations())
                        }
                        Some(&Constraint::Filter(Filter::DataKey(set, key))) => {
                            Box::new(store.key(set, key).or_fail()?.annotations())
                        }
                        Some(&Constraint::Filter(Filter::AnnotationData(set, key))) => {
                            Box::new(store.annotationdata(set, key).or_fail()?.annotations())
                        }
                        Some(&Constraint::DataVariable(var)) => {
                            let data = self.resolve_datavar(var)?;
                            Box::new(data.annotations())
                        }
                        Some(&Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        }) => Box::new(store.find_data(set, key, operator.clone()).annotations()),
                        Some(&Constraint::Filter(Filter::DataKeyAndOperator(set, key, ref operator))) => {
                            Box::new(store.find_data(set, key, operator.clone()).annotations())
                        }
                        Some(&Constraint::Text(text))
                        | Some(&Constraint::Filter(Filter::BorrowedText(text, ..))) => {
                            Box::new(store.find_text(text).annotations())
                        }
                        Some(&Constraint::TextVariable(var)) => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                Box::new(tsel.annotations())
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                Box::new(annotation.textselections().annotations())
                            } else {
                                return Err(StamError::QuerySyntaxError(
                                    format!(
                                        "Variable ?{} of type TEXT or ANNOTATION not found",
                                        var
                                    ),
                                    "",
                                ));
                            }
                        }
                        Some(&Constraint::TextRelation { var, operator }) => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                Box::new(tsel.related_text(operator).annotations())
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                Box::new(
                                    annotation
                                        .textselections()
                                        .related_text(operator)
                                        .annotations(),
                                )
                            } else {
                                return Err(StamError::QuerySyntaxError(
                                    format!(
                                        "Variable ?{} of type TEXT or ANNOTATION not found",
                                        var
                                    ),
                                    "",
                                ));
                            }
                        }
                        Some(&Constraint::Union(..)) => todo!("UNION not implemented yet"),
                        Some(c) => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} (primary) is not implemented for queries over annotations",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                        None => Box::new(store.annotations()),
                    };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::TextResource(res, ResourceQualifier::Any) => {
                            iter = Box::new(iter.filter_resource(&store.resource(res).or_fail()?));
                        }
                        &Constraint::TextResource(res, ResourceQualifier::AsMetadata) => {
                            iter = Box::new(
                                iter.filter_resource_as_metadata(&store.resource(res).or_fail()?),
                            );
                        }
                        &Constraint::TextResource(res, ResourceQualifier::AsText) => {
                            iter = Box::new(
                                iter.filter_resource_as_text(&store.resource(res).or_fail()?),
                            );
                        }
                        &Constraint::ResourceVariable(varname, ResourceQualifier::Any) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource(resource));
                        }
                        &Constraint::ResourceVariable(varname, ResourceQualifier::AsMetadata) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource_as_metadata(resource));
                        }
                        &Constraint::ResourceVariable(varname, ResourceQualifier::AsText) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource_as_text(resource));
                        }
                        &Constraint::DataKey { set, key } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key(&key));
                        }
                        &Constraint::DataVariable(varname) => {
                            let data = self.resolve_datavar(varname)?;
                            iter = Box::new(iter.filter_annotationdata(data));
                        }
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()));
                        }
                        &Constraint::Text(text) => {
                            iter = Box::new(iter.filter_text_byref(text, true, " "))
                        }
                        &Constraint::TextVariable(var) => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                iter = Box::new(
                                    iter.filter_annotations(tsel.annotations().to_handles(store)),
                                )
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                iter = Box::new(iter.filter_annotations(
                                    annotation.textselections().annotations().to_handles(store),
                                ))
                            } else {
                                return Err(StamError::QuerySyntaxError(
                                    format!(
                                        "Variable ?{} of type TEXT or ANNOTATION not found",
                                        var
                                    ),
                                    "",
                                ));
                            }
                        }
                        &Constraint::TextRelation { var, operator } => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                iter = Box::new(iter.filter_annotations(
                                    tsel.related_text(operator).annotations().to_handles(store),
                                ))
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                iter = Box::new(
                                    iter.filter_annotations(
                                        annotation
                                            .textselections()
                                            .related_text(operator)
                                            .annotations()
                                            .to_handles(store),
                                    ),
                                )
                            } else {
                                return Err(StamError::QuerySyntaxError(
                                    format!(
                                        "Variable ?{} of type TEXT or ANNOTATION not found",
                                        var
                                    ),
                                    "",
                                ));
                            }
                        }
                        &Constraint::Annotation(annotation, AnnotationQualifier::None) => {
                            iter = Box::new(
                                iter.filter_annotation(&store.annotation(annotation).or_fail()?),
                            );
                        }
                        &Constraint::Filter(Filter::Annotation(annotation)) => {
                            iter = Box::new(
                                iter.filter_annotation(&store.annotation(annotation).or_fail()?),
                            );
                        }
                        c => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} is not implemented for queries over annotations",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                    }
                }
                Ok(QueryResultIter::Annotations(iter))
            }
            /////////////////////////////////////////////////////////////////////////
            Some(Type::TextSelection) => {
                let mut iter: Box<dyn Iterator<Item = ResultTextSelection<'store>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::Handle(Filter::TextSelection(resource, handle))) => {
                            Box::new(store.textselection(resource,handle).into_iter())
                        }
                        Some(&Constraint::TextResource(res,_)) => {
                            Box::new(store.resource(res).or_fail()?.textselections())
                        }
                        Some(&Constraint::ResourceVariable(varname,_)) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            Box::new(resource.textselections())
                        }
                        Some(&Constraint::Annotation(id,_)) => {
                            Box::new(store.annotation(id).or_fail()?.textselections())
                        }
                        Some(&Constraint::AnnotationVariable(varname,_)) => {
                            let annotation = self.resolve_annotationvar(varname)?;
                            Box::new(annotation.textselections())
                        }
                        Some(&Constraint::DataKey { set, key }) => Box::new(
                            store
                                .find_data(set, key, DataOperator::Any)
                                .annotations()
                                .textselections(),
                        ),
                        Some(&Constraint::DataVariable(varname)) => {
                            let data = self.resolve_datavar(varname)?;
                            Box::new(data.annotations().textselections())
                        }
                        Some(&Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        }) => Box::new(
                            store
                                .find_data(set, key, operator.clone())
                                .annotations()
                                .textselections(),
                        ),
                        Some(&Constraint::Text(text)) => Box::new(store.find_text(text)),
                        Some(&Constraint::TextVariable(var)) => {
                            return Err(StamError::QuerySyntaxError(
                                format!("Constraint on TEXT ?{} is useless as a constraint", var),
                                "",
                            ));
                        }
                        Some(&Constraint::TextRelation { var, operator }) => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                Box::new(tsel.related_text(operator))
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                Box::new(annotation.textselections().related_text(operator))
                            } else {
                                return Err(StamError::QuerySyntaxError(
                                    format!(
                                        "Variable ?{} of type TEXT or ANNOTATION not found",
                                        var
                                    ),
                                    "",
                                ));
                            }
                        }
                        Some(&Constraint::Union(..)) => todo!("UNION not implemented yet"),
                        Some(c) => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} (primary) is not implemented for queries over text selections",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                        None => Box::new(store.annotations().textselections()),
                    };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::TextResource(res,_) => {
                            iter = Box::new(iter.filter_resource(&store.resource(res).or_fail()?));
                        }
                        &Constraint::ResourceVariable(varname,_) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource(&resource));
                        }
                        &Constraint::DataKey { set, key } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key(&key));
                        }
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()));
                        }
                        &Constraint::Text(text) => {
                            iter = Box::new(iter.filter_text_byref(text, true))
                        }
                        &Constraint::TextRelation { var, operator } => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                iter = Box::new(iter.filter_textselections(tsel.related_text(operator).filter_map(|x| x.as_resultitem().map(|x| x.clone())).to_handles(store)))
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                iter = Box::new(iter.filter_textselections(
                                    annotation.textselections().related_text(operator).filter_map(|x| x.as_resultitem().map(|x| x.clone())).to_handles(store)
                                ))
                            } else {
                                return Err(StamError::QuerySyntaxError(
                                    format!(
                                        "Variable ?{} of type TEXT or ANNOTATION not found",
                                        var
                                    ),
                                    "",
                                ));
                            }
                        }
                        c => return Err(StamError::QuerySyntaxError(
                            format!(
                                "Constraint {} (secondary) is not implemented for queries over text selections",
                                c.keyword()
                            ),
                            "",
                        )),
                    }
                }
                Ok(QueryResultIter::TextSelections(iter))
            }
            /////////////////////////////////////////////////////////////////////////
            Some(Type::AnnotationData) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::Annotation(id, _)) => {
                            Box::new(store.annotation(id).or_fail()?.data())
                        }
                        Some(&Constraint::AnnotationVariable(varname, _)) => {
                            let annotation = self.resolve_annotationvar(varname)?;
                            Box::new(annotation.data())
                        }
                        Some(&Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        }) => store.find_data(set, key, operator.clone()),
                        //TODO: KeyVariable
                        Some(c) => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} is not valid for DATA return type",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                        None => Box::new(store.data()),
                    };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        } => {
                            iter = Box::new(
                                iter.filter_data(
                                    store
                                        .find_data(set, key, operator.clone())
                                        .to_handles(store),
                                ),
                            );
                        }
                        c => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} is not implemented for queries over data",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                    }
                }
                Ok(QueryResultIter::Data(iter))
            }
            Some(Type::DataKey) => {
                todo!("implement datakey!") //TODO
            }
            None => unreachable!("Query must have a result type"),
            _ => unimplemented!("Query result type not implemented"),
        }?;

        self.statestack.push(QueryState {
            iterator: iter,
            result: QueryResultItem::None,
        });
        // Do the first iteration (may remove this and other elements from the stack again if it fails)
        Ok(self.next_state())
    }

    /// Advances the query state on the stack, return true if a new result was obtained (stored in the state's result buffer),
    /// Pops items off the stack if they no longer yield result.
    /// If no result at all can be obtained anymore, false is returned.
    pub fn next_state(&mut self) -> bool {
        while !self.statestack.is_empty() {
            if let Some(mut state) = self.statestack.pop() {
                //we pop the state off the stack (we put it back again in cases where it's an undepleted iterator)
                //but this allows us to take full ownership and not have a mutable borrow,
                //which would get in the way as we also need to inspect prior results from the stack (immutably)
                loop {
                    match &mut state.iterator {
                        QueryResultIter::TextSelections(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::TextSelection(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::Annotations(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::Annotation(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::Data(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::AnnotationData(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        _ => unimplemented!("further iterators not implemented yet"), //TODO
                    }
                }
            }
        }
        //mark as done (otherwise the iterator would restart from scratch again)
        self.done = true;
        false
    }

    pub(crate) fn build_result(&self) -> QueryResultItems<'store> {
        let mut items = SmallVec::new();
        for stackitem in self.statestack.iter() {
            items.push(stackitem.result.clone());
        }
        QueryResultItems { items }
    }

    fn resolve_datavar(
        &self,
        name: &str,
    ) -> Result<&ResultItem<'store, AnnotationData>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self.queries.get(i).expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::AnnotationData(data) = &state.result {
                    return Ok(data);
                }
            }
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type DATA not found", name),
            "",
        ));
    }

    fn resolve_annotationvar(
        &self,
        name: &str,
    ) -> Result<&ResultItem<'store, Annotation>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self.queries.get(i).expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::Annotation(annotation) = &state.result {
                    return Ok(annotation);
                }
            }
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type ANNOTATION not found", name),
            "",
        ));
    }

    fn resolve_textvar(&self, name: &str) -> Result<&ResultTextSelection<'store>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self.queries.get(i).expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::TextSelection(textsel) = &state.result {
                    return Ok(textsel);
                }
            }
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type TEXT not found", name),
            "",
        ));
    }

    fn resolve_resourcevar(
        &self,
        name: &str,
    ) -> Result<&ResultItem<'store, TextResource>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self.queries.get(i).expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::TextResource(resource) = &state.result {
                    return Ok(resource);
                }
            }
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type RESOURCE not found", name),
            "",
        ));
    }
}

impl<'store> Iterator for QueryIter<'store> {
    type Item = QueryResultItems<'store>;

    fn next<'q>(&'q mut self) -> Option<Self::Item> {
        if self.done {
            //iterator has been marked as done, do nothing else
            return None;
        }

        //populate the entire stack, producing a result at each level
        while self.statestack.len() < self.queries.len() {
            match self.init_state() {
                Ok(false) => {
                    //if we didn't succeed in preparing the next iteration, it means the entire stack is depleted and we're done
                    return None;
                }
                Err(e) => {
                    eprintln!("STAM Query error: {}", e);
                    return None;
                }
                Ok(true) => continue,
            }
        }

        //read the result in the stack's result buffer
        let result = self.build_result();

        // prepare the result buffer for next iteration
        self.next_state();

        return Some(result);
    }
}

impl<'store> QueryResultItems<'store> {
    /// Iterator over all results
    pub fn iter(&self) -> impl Iterator<Item = &QueryResultItem<'store>> {
        self.items.iter()
    }

    /// Returns a result by index number
    pub fn get(&self, index: usize) -> Option<&QueryResultItem<'store>> {
        self.items.get(index)
    }

    pub fn pop_last(&mut self) -> Option<QueryResultItem<'store>> {
        self.items.pop()
    }

    /// Returns a result by variable name
    pub fn get_by_name(
        &self,
        names: &QueryNames,
        var: &str,
    ) -> Result<&QueryResultItem<'store>, StamError> {
        self.items.get(names.get(var)?).ok_or_else(|| {
            StamError::QuerySyntaxError(
                format!("Variable ?{} not found in the result set", var),
                "",
            )
        })
    }
}

impl<'store> IntoIterator for QueryResultItems<'store> {
    type IntoIter = <SmallVec<[QueryResultItem<'store>; 4]> as IntoIterator>::IntoIter;
    type Item = QueryResultItem<'store>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

// Helper structs and functions

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArgType {
    String,
    Integer,
    Float,
    List,
}

fn get_arg_type(s: &str) -> ArgType {
    if s.is_empty() {
        return ArgType::String;
    }
    let mut numeric = true;
    let mut foundperiod = false;
    let mut prevc = None;
    for c in s.chars() {
        if !c.is_ascii_digit() {
            numeric = false;
            break;
        }
        if numeric && c == '.' {
            if foundperiod {
                numeric = false;
                break;
            }
            foundperiod = true
        }
        if c == '|' && prevc != Some('\\') {
            return ArgType::List;
        }
        prevc = Some(c)
    }
    if numeric {
        if foundperiod {
            ArgType::Float
        } else {
            ArgType::Integer
        }
    } else {
        ArgType::String
    }
}

fn get_arg<'a>(querystring: &'a str) -> Result<(&'a str, &'a str, ArgType), StamError> {
    let mut quote = false;
    let mut escaped = false;
    let mut begin = 0;
    for (i, c) in querystring.char_indices() {
        if c == '"' && !escaped {
            quote = !quote;
            if quote {
                begin = i + 1;
            } else {
                return Ok((
                    &querystring[begin..i],
                    &querystring[i + 1..]
                        .trim_start_matches(|c| c == ';' || c == ' ' || c == '\t' || c == '\n'),
                    ArgType::String,
                ));
            }
        }
        if !quote && (c == ';' || c == ' ' || c == '\n' || c == '\t') {
            let arg = &querystring[0..i];
            return Ok((arg, &querystring[i + 1..].trim_start(), get_arg_type(arg)));
        }
        escaped = c == '\\';
    }
    return Err(StamError::QuerySyntaxError(
        format!(
            "Failed to parse argument '{}'. Missing semicolon perhaps?",
            querystring
        ),
        "",
    ));
}
