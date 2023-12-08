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

#[derive(Debug)]
pub enum Constraint<'a> {
    //Id(&'a str),
    TextResource(&'a str),
    DataKey {
        set: &'a str,
        key: &'a str,
    },
    DataVariable(&'a str),
    ResourceVariable(&'a str),
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
}

impl<'a> Constraint<'a> {
    pub fn keyword(&self) -> &'static str {
        match self {
            //      Self::Id(..) => "ID",
            Self::TextResource { .. } | Self::ResourceVariable(..) => "RESOURCE",
            Self::TextRelation { .. } => "RELATION",
            Self::KeyValue { .. } | Self::DataKey { .. } | Self::DataVariable(..) => "DATA",
            Self::Text { .. } | Self::TextVariable(..) => "TEXT",
            Self::Union { .. } => "UNION",
        }
    }

    pub(crate) fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let constraint = match querystring.split(" ").next() {
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
            /*
            Some("ID") => {
                querystring = querystring["ID".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                Self::Id(arg)
            }
            */
            Some("RESOURCE") => {
                querystring = querystring["RESOURCE".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::ResourceVariable(&arg[1..])
                } else {
                    Self::TextResource(arg)
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

    pub fn constrain(&mut self, constraint: Constraint<'a>) -> &mut Self {
        self.constraints.push(constraint);
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
pub enum ResultIter<'store> {
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
    iterator: ResultIter<'store>,

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
                        Some(&Constraint::DataKey { set, key }) => Box::new(
                            store
                                .find_data(set, key, DataOperator::Any)
                                .annotations()
                                .resources(),
                        ),
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
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()));
                        }
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
                Ok(ResultIter::Resources(iter))
            }
            Some(Type::Annotation) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, Annotation>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::TextResource(res)) => {
                            Box::new(store.resource(res).or_fail()?.annotations())
                        }
                        Some(&Constraint::ResourceVariable(var)) => {
                            let resource = self.resolve_resourcevar(var)?;
                            Box::new(resource.annotations())
                        }
                        Some(&Constraint::DataKey { set, key }) => {
                            Box::new(store.find_data(set, key, DataOperator::Any).annotations())
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
                        Some(&Constraint::Text(text)) => {
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
                        None => Box::new(store.annotations()),
                    };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::TextResource(res) => {
                            iter = Box::new(iter.filter_resource(&store.resource(res).or_fail()?));
                        }
                        &Constraint::ResourceVariable(varname) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource(resource));
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
                Ok(ResultIter::Annotations(iter))
            }
            /////////////////////////////////////////////////////////////////////////
            Some(Type::TextSelection) => {
                let mut iter: Box<dyn Iterator<Item = ResultTextSelection<'store>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::TextResource(res)) => {
                            Box::new(store.resource(res).or_fail()?.textselections())
                        }
                        Some(&Constraint::ResourceVariable(varname)) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            Box::new(resource.textselections())
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
                        None => Box::new(store.annotations().textselections()),
                    };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::TextResource(res) => {
                            iter = Box::new(iter.filter_resource(&store.resource(res).or_fail()?));
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
                Ok(ResultIter::TextSelections(iter))
            }
            /////////////////////////////////////////////////////////////////////////
            Some(Type::AnnotationData) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                        }) => store.find_data(set, key, operator.clone()),
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
                Ok(ResultIter::Data(iter))
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
                        ResultIter::TextSelections(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::TextSelection(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        ResultIter::Annotations(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::Annotation(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        ResultIter::Data(iter) => {
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
