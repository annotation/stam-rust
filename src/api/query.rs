#![allow(unused_imports)]
#![allow(dead_code)]
use crate::annotation::AnnotationHandle;
use crate::annotationdata::AnnotationDataHandle;
use crate::annotationstore::AnnotationStore;
use crate::api::{AnnotationsIter, DataIter, TextSelectionsIter};
use crate::datakey::DataKeyHandle;
use crate::error::StamError;
use crate::store::*;
use crate::textselection::TextSelectionOperator;
use crate::AnnotationDataSetHandle;
use crate::TextResourceHandle;
use crate::{types::*, DataOperator};

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
}

#[derive(Debug)]
pub enum Constraint<'a> {
    TextResource(&'a str),
    DataKey {
        set: &'a str,
        key: &'a str,
    },
    FindData {
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
            Self::TextResource { .. } => "RESOURCE",
            Self::FindData { .. } | Self::DataKey { .. } => "DATA",
            Self::Text { .. } => "TEXT",
            Self::Union { .. } => "UNION",
        }
    }

    pub(crate) fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let constraint = match querystring.split(" ").next() {
            Some("TEXT") => {
                querystring = querystring[4..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                Self::Text(arg)
            }
            Some("DATA") => {
                querystring = querystring[4..].trim_start();
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
                    Self::FindData { set, key, operator }
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
        }
    }

    pub fn constrain(&mut self, constraint: Constraint<'a>) -> &mut Self {
        self.constraints.push(constraint);
        self
    }

    /// Iterates over all constraints in the Query
    pub fn iter(&self) -> std::slice::Iter<Constraint<'a>> {
        self.constraints.iter()
    }
}

impl<'a> TryFrom<&'a str> for Query<'a> {
    type Error = StamError;
    fn try_from(mut querystring: &'a str) -> Result<Self, Self::Error> {
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
            Some("WHERE") => querystring = querystring[end..].trim_start(),
            Some("") | None => {} //no-op (select all, end of query, no where clause)
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
        while !querystring.is_empty() {
            let (constraint, remainder) = Constraint::parse(querystring)?;
            querystring = remainder;
            constraints.push(constraint);
        }
        Ok(Self {
            name,
            querytype: QueryType::Select,
            resulttype,
            constraints,
        })
    }
}

/// This type abstracts over all the main iterators
pub enum ResultIter<'a> {
    Annotations(AnnotationsIter<'a>),
    Data(DataIter<'a>),
    TextSelections(TextSelectionsIter<'a>),
}

impl<'store> AnnotationStore {
    /// Instantiates a query, returns an iterator.
    /// No actual querying is done yet until you use the iterator.
    pub fn query(&'store self, query: Query<'store>) -> Result<ResultIter<'store>, StamError> {
        let mut constraintsiter = query.constraints.iter();
        match query.resulttype {
            Some(Type::Annotation) => {
                let mut iter = match constraintsiter.next() {
                    Some(&Constraint::TextResource(res)) => {
                        self.resource(res).or_fail()?.annotations()
                    }
                    Some(&Constraint::DataKey { set, key }) => {
                        self.find_data(set, key, DataOperator::Any).annotations()
                    }
                    Some(&Constraint::FindData {
                        set,
                        key,
                        ref operator,
                    }) => self.find_data(set, key, operator.clone()).annotations(),
                    Some(&Constraint::Text(text)) => {
                        TextSelectionsIter::new_with_iterator(Box::new(self.find_text(text)), self)
                            .annotations()
                    }
                    Some(&Constraint::Union(..)) => todo!("UNION not implemented yet"),
                    None => self.annotations(),
                };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::TextResource(res) => {
                            iter = iter.filter_resource(&self.resource(res).or_fail()?);
                        }
                        &Constraint::DataKey { set, key } => {
                            iter = iter.filter_find_data(set, key, DataOperator::Any)
                        }
                        &Constraint::FindData {
                            set,
                            key,
                            ref operator,
                        } => {
                            iter = iter.filter_find_data(set, key, operator.clone());
                        }
                        &Constraint::Text(text) => iter = iter.filter_text_byref(text, true, " "),
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
            Some(Type::AnnotationData) => {
                let mut iter = match constraintsiter.next() {
                    Some(&Constraint::FindData {
                        set,
                        key,
                        ref operator,
                    }) => self.find_data(set, key, operator.clone()),
                    Some(c) => {
                        return Err(StamError::QuerySyntaxError(
                            format!(
                                "Constraint {} is not valid for DATA return type",
                                c.keyword()
                            ),
                            "",
                        ))
                    }
                    None => self.data(),
                };
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::FindData {
                            set,
                            key,
                            ref operator,
                        } => {
                            iter = iter.filter_data(self.find_data(set, key, operator.clone()));
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
        }
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
