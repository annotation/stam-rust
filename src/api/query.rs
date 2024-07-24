#![allow(unused_imports)]
#![allow(dead_code)]
use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datakey::DataKeyHandle;
use crate::datavalue::DataValue;
use crate::error::StamError;
use crate::textselection::TextSelectionOperator;
use crate::Offset;
use crate::{api::*, Configurable};
use crate::{store::*, ResultTextSelection};
use crate::{types::*, DataOperator};
use crate::{
    AnnotationBuilder, AnnotationDataBuilder, SelectorBuilder, SelectorKind, TextResource,
    TextResourceHandle,
};

use chrono::{DateTime, FixedOffset};
use regex::Regex;
use smallvec::SmallVec;
use std::collections::HashMap;

use std::borrow::Cow;

const QUERYSPLITCHARS: &[char] = &[' ', '\n', '\r', '\t'];

#[derive(Clone, Copy, Debug, PartialEq)]
/// Holds the type of a [`Query`].
pub enum QueryType {
    /// A query that selects and returns data (STAMQL `SELECT` keyword).
    Select,

    Delete,
    Add,
}

impl QueryType {
    /// Returns the STAMQL keyword for this query type
    pub fn as_str(&self) -> &str {
        match self {
            QueryType::Select => "SELECT",
            QueryType::Delete => "DELETE",
            QueryType::Add => "ADD",
        }
    }

    /// Is this query one that requires a mutable store or not?
    pub fn readonly(&self) -> bool {
        match self {
            QueryType::Add | QueryType::Delete => false,
            QueryType::Select => true,
        }
    }
}

#[derive(Debug, Clone)]
/// This represents a query that can be performed on an [`AnnotationStore`] via
/// [`AnnotationStore::query()`] to obtain anything in the store. A query can be formulated in
/// [STAMQL](https://github.com/annotation/stam/tree/master/extensions/stam-query), a dedicated
/// query language (via [`Query::parse()`], or it can be instantiated programmatically via
/// [`Query::new()`].
///
/// A query consists of a query type ([`QueryType`]), a result type (subset of [`Type`]), a variable to bind to (optional), and
/// zero or more *constraints* ([`Constraint`]), and optionally a subquery.
///
/// ## Examples
///
/// *select all occurrences of the text "fly"*
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::parse("SELECT TEXT WHERE
///                                  TEXT \"fly\";")?;
/// # Ok(())
/// # }
/// ```
///
/// *the same query as above but constructed directly instead of via STAMQL, this is always more performant as it bypasses the parsing stage. It does not affect the runtime performance of the query evaluation itself though*:
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::new(QueryType::Select, Some(Type::TextSelection), None)
///                   .with_constraint(Constraint::Text("fly", TextMode::Exact));
/// # Ok(())
/// # }
/// ```
///
/// *select all annotations that targets the text "fly"*
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::parse("SELECT ANNOTATION WHERE
///                                  TEXT \"fly\";")?;
/// # Ok(())
/// # }
/// ```
///
/// *select all annotations with data 'part-of-speech' and value 'noun' (ad-hoc vocab!), bind the result to a variable*
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::parse("SELECT ANNOTATION ?noun WHERE
///                                  DATA \"myset\" \"part-of-speech\" = \"noun\";")?;
/// # Ok(())
/// # }
/// ```
/// *the same query as above but constructed programmatically*:
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::new(QueryType::Select, Some(Type::Annotation), Some("noun"))
///                   .with_constraint(Constraint::KeyValue {
///                            set: "myset",
///                            key: "fly",
///                            operator: DataOperator::Equals("noun"),
///                            qualifier: SelectionQualifier::Normal
///                   });
/// # Ok(())
/// # }
/// ```
///
/// *select all annotations that have a part-of-speech annotation (regardless of the value)*
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::parse("SELECT ANNOTATION ?pos WHERE
///                                  DATA \"myset\" \"part-of-speech\";")?;
/// # Ok(())
/// # }
/// ```
///
/// *select all annotations with data 'part-of-speech' made by a certain annotator (ad-hoc vocab!)*
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::parse("SELECT ANNOTATION WHERE
///                              DATA \"myset\" \"part-of-speech\" = \"noun\";
///                              DATA \"myset\" \"annotator\" = \"John Doe\";")?;
/// # Ok(())
/// # }
/// ```
///
///
/// *select sentences with a particular annotated text in it, as formulated via a subquery*
///
/// ```
/// # use stam::*;
/// # fn main() -> Result<(),StamError> {
/// let query = Query::parse("SELECT TEXT ?sentence WHERE
///                                  DATA \"myset\" \"type\" = \"sentence\";
///                                    {
///                                     SELECT TEXT ?fly WHERE
///                                         RELATION ?sentence EMBEDS;
///                                         DATA \"myset\" \"part-of-speech\" = \"noun\";
///                                         TEXT \"fly\";
///                                    }")?;
/// # Ok(())
/// # }
/// ```
///
pub struct Query<'a> {
    /// The variable name
    name: Option<&'a str>,

    querytype: QueryType,
    resulttype: Option<Type>,
    assignments: Vec<Assignment<'a>>, //only for ADD
    constraints: Vec<Constraint<'a>>,
    subqueries: Vec<Query<'a>>,

    /// Context variables (external), if there are subqueries, they do NOT hold contextvars, only the root query does
    /// This allows binding variables programmatically, using the bind_*var() methods
    contextvars: HashMap<String, ContextItem>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// This is determines whether a query [`Constraint`] is applied normally or with a particular altered meaning.
pub enum SelectionQualifier {
    /// Normal behaviour, no changes.
    Normal,

    /// This corresponds to the TARGET or METADATA keyword in STAMQL. It indicates that the item in the constrain is an explicit annotation TARGET. It causes the logic flow to go over methods like annotations_as_metadata() instead of annotations()
    Metadata,
}

impl SelectionQualifier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "",
            Self::Metadata => " AS METADATA",
        }
    }
}

impl Default for SelectionQualifier {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// This determines how far to look up or down in an annotation hierarchy tree formed by AnnotationSelectors.
pub enum AnnotationDepth {
    /// Apply only on the same level
    Zero,

    /// Minimal depth corresponds to a different of exactly one layer in the annotation hierarchy formed by AnnotationSelector (above or below) the current one.
    One,

    /// Maximal depth implies deep recursion accross all layers in an annotation hierarchy formed by AnnotationSelector
    Max,
}

impl Default for AnnotationDepth {
    fn default() -> Self {
        Self::One
    }
}

#[derive(Debug, Clone)]
/// A constraint is a part of a [`Query`] that poses specific selection criteria that must be met.
/// A query can have multiple constraints which must all be satisfied. See the documentation for
/// [`Query`] for examples.
pub enum Constraint<'a> {
    /// Constrain the selection (type is determined by the query result type) to one instance with a specific identifier (`ID` keyword in STAMQL).
    Id(&'a str),

    /// Constrain by a specific Annotation, referenced by ID (`ANNOTATION` keyword in STAMQL)
    Annotation(&'a str, SelectionQualifier, AnnotationDepth, Option<Offset>),

    /// Constrain by a specific TextResource, referenced by ID (`RESOURCE` keyword in STAMQL)
    TextResource(&'a str, SelectionQualifier, Option<Offset>),

    /// Constrain by a specific AnnotationDataSet, referenced by ID (`DATASET` keyword in STAMQL)
    DataSet(&'a str, SelectionQualifier),

    /// Constrain by a specific DataKey, referenced by set and key ID (`DATA` keyword, without operator/value, in STAMQL)
    DataKey {
        set: &'a str,
        key: &'a str,
        qualifier: SelectionQualifier,
    },

    /// Constrain by a specific DataKey, referenced by variable (`KEY` keyword in STAMQL), the variable name must *not* carry the `?` prefix here.
    KeyVariable(&'a str, SelectionQualifier),

    /// Constrain by a specific AnnotationData, referenced by variable (`DATA` keyword in STAMQL), the variable name must *not* carry the `?` prefix here.
    DataVariable(&'a str, SelectionQualifier),

    /// Constrain by a specific AnnotationDataSet, referenced by variable (`DATASET` keyword in STAMQL), the variable name must *not* carry the `?` prefix here.
    DataSetVariable(&'a str, SelectionQualifier),

    /// Constrain by a specific TextResource, referenced by variable (`RESOURCE` keyword in STAMQL), the variable name must *not* carry the `?` prefix here.
    ResourceVariable(&'a str, SelectionQualifier, Option<Offset>),

    /// Constrain by a specific text selection, referenced by variable (`TEXT` keyword in STAMQL), the variable name must *not* carry the `?` prefix here.
    TextVariable(&'a str),

    /// Constrain by a specific text selection and a particular relation textual relation (`RELATION` keyword in STAMQL), the text selection is referenced by variable (`TEXT` keyword in STAMQL), the variable name must *not* carry the `?` prefix here.
    TextRelation {
        var: &'a str,
        operator: TextSelectionOperator,
    },

    /// Constrain by a specific DataKey and value, referenced by set and key ID, and a [`DataOperator`] (`DATA` keyword in STAMQL)
    KeyValue {
        set: &'a str,
        key: &'a str,
        operator: DataOperator<'a>,
        qualifier: SelectionQualifier,
    },

    /// Constrain by a specific value (`VALUE` keyword, with operator/value, in STAMQL). This only makes sense in certain contexts like when querying keys.
    Value(DataOperator<'a>, SelectionQualifier),

    /// Constrain by a specific DataKey and value test [`DataOperator`], only the key is referenced by variable, the variable name must *not* carry the `?` prefix here.
    KeyValueVariable(&'a str, DataOperator<'a>, SelectionQualifier),

    /// Constrain by textual content (`TEXT` keyword in STAMQL). The `TextMode` determines whether comparisons are exact or case-insensitive (`TEXT AS NOCASE` in STAMQL). See also [`Constraint::Regex`]
    Text(&'a str, TextMode),

    /// Constrain by textual content via a regular expression (`TEXT AS REGEX` keyword in STAMQL)
    Regex(Regex),

    /// Disjunction
    Union(Vec<Constraint<'a>>),

    /// Constrain by a specific Annotation, referenced by variable (`Annotation` keyword in STAMQL), the variable name must *not* carry the `?` prefix here.
    AnnotationVariable(&'a str, SelectionQualifier, AnnotationDepth, Option<Offset>),

    /// Constrain by any of multiple annotations
    Annotations(Handles<'a, Annotation>, SelectionQualifier, AnnotationDepth),
    /// Constrain by any of multiple annotation data
    Data(Handles<'a, AnnotationData>, SelectionQualifier),
    /// Constrain by any of multiple data keys
    Keys(Handles<'a, DataKey>, SelectionQualifier),
    /// Constrain by any of multiple resources
    Resources(Handles<'a, TextResource>, SelectionQualifier),
    /// Constrain by any of multiple text selections
    TextSelections(Handles<'a, TextSelection>, SelectionQualifier),

    ///Constrain to a certain range
    Limit { begin: isize, end: isize },
}

impl<'a> Constraint<'a> {
    /// Returns the STAMQL keyword for this constraint
    pub fn keyword(&self) -> &'static str {
        match self {
            Self::Id(..) => "ID",
            Self::TextResource { .. } | Self::ResourceVariable(..) => "RESOURCE",
            Self::TextRelation { .. } => "RELATION",
            Self::KeyValue { .. }
            | Self::DataKey { .. }
            | Self::DataVariable(..)
            | Self::KeyValueVariable(..) => "DATA",
            Self::Value(..) => "VALUE",
            Self::KeyVariable(..) => "KEY",
            Self::DataSet { .. } | Self::DataSetVariable { .. } => "DATASET",
            Self::Text { .. } | Self::TextVariable(..) | Self::Regex(..) => "TEXT",
            Self::AnnotationVariable(..) | Self::Annotation(..) => "ANNOTATION",
            Self::Union { .. } => "UNION",
            Self::Limit { .. } => "LIMIT",

            //not real keywords that can be parsed, only for debug printing purposes:
            Self::Annotations(..) => "ANNOTATIONS",
            Self::Data(..) => "DATA",
            Self::Resources(..) => "RESOURCES",
            Self::TextSelections(..) => "TEXTSELECTIONS",
            Self::Keys(..) => "KEYS",
        }
    }

    fn closed(querystring: &str) -> bool {
        querystring.is_empty()
            || querystring.starts_with(";")
            || querystring.starts_with("OR ")
            || querystring.starts_with("]")
    }

    fn parse_offset<'b>(mut querystring: &'b str) -> Result<(Option<Offset>, &'b str), StamError> {
        if !Self::closed(querystring) && querystring.starts_with("OFFSET") {
            querystring = querystring["OFFSET".len()..].trim_start();
            let (arg, remainder, _) = get_arg(querystring)?;
            let begin = if arg == "WHOLE" || arg == "ALL" {
                Cursor::BeginAligned(0)
            } else {
                Cursor::try_from(arg).or_else(|_| {
                    Err(StamError::QuerySyntaxError(
                        format!(
                            "Expected integer for begin cursor after OFFSET, got '{}'",
                            arg
                        ),
                        "",
                    ))
                })?
            };
            querystring = remainder;
            let end = if Self::closed(querystring) {
                Cursor::EndAligned(0)
            } else {
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                Cursor::try_from(arg).or_else(|_| {
                    Err(StamError::QuerySyntaxError(
                        format!(
                            "Expected integer for end cursor, (optional) second parameter after OFFSET, got '{}'",
                            arg
                        ),
                        "",
                    ))
                })?
            };
            Ok((Some(Offset { begin, end }), querystring))
        } else {
            Ok((None, querystring))
        }
    }

    pub(crate) fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let constraint = match querystring.split(QUERYSPLITCHARS).next() {
            Some("ID") => {
                querystring = querystring["ID".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                Self::Id(arg)
            }
            Some("TEXT") => {
                querystring = querystring["TEXT".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, regex) = parse_text_qualifiers(arg, remainder)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::TextVariable(&arg[1..])
                } else if regex {
                    Self::Regex(Regex::new(arg).map_err(|err| {
                        StamError::RegexError(
                            err.into(),
                            "parsing TEXT AS REGEX constraint for query",
                        )
                    })?)
                } else {
                    Self::Text(arg, qualifier)
                }
            }
            Some("ANNOTATION") => {
                querystring = querystring["ANNOTATION".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, depth) = parse_qualifiers(arg, remainder)?;
                let (offset, remainder) = Self::parse_offset(remainder)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::AnnotationVariable(&arg[1..], qualifier, depth, offset)
                } else {
                    Self::Annotation(arg, qualifier, depth, offset)
                }
            }
            Some("RESOURCE") => {
                querystring = querystring["RESOURCE".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, _) = parse_qualifiers(arg, remainder)?;
                let (offset, remainder) = Self::parse_offset(remainder)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::ResourceVariable(&arg[1..], qualifier, offset)
                } else {
                    Self::TextResource(arg, qualifier, offset)
                }
            }
            Some("DATASET") => {
                querystring = querystring["DATASET".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, _) = parse_qualifiers(arg, remainder)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::DataSetVariable(&arg[1..], qualifier)
                } else {
                    Self::DataSet(arg, qualifier)
                }
            }
            Some("RELATION") => {
                querystring = querystring["RELATION".len()..].trim_start();
                let (var, remainder, _) = get_arg(querystring)?;
                if !var.starts_with("?") {
                    return Err(StamError::QuerySyntaxError(
                        format!(
                            "Expected variable after 'RELATION' keyword, got '{}'",
                            remainder
                                .split(QUERYSPLITCHARS)
                                .next()
                                .unwrap_or("(empty string)")
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
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, _) = parse_qualifiers(arg, remainder)?;
                querystring = remainder;
                if arg.starts_with("?") {
                    Self::DataVariable(&arg[1..], qualifier)
                } else {
                    let set = arg;
                    let (key, remainder, _) = get_arg(remainder)?;
                    querystring = remainder;
                    if Self::closed(querystring) {
                        Self::DataKey {
                            set,
                            key,
                            qualifier,
                        }
                    } else {
                        let (opstr, remainder, _) = get_arg(querystring)?;
                        let (value, remainder, valuetype) = get_arg(remainder)?;
                        querystring = remainder;
                        let operator = parse_dataoperator(opstr, value, valuetype)?;
                        Self::KeyValue {
                            set,
                            key,
                            operator,
                            qualifier,
                        }
                    }
                }
            }
            Some("VALUE") => {
                querystring = querystring["VALUE".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (opstr, remainder, qualifier, _) = parse_qualifiers(arg, remainder)?;
                let (value, remainder, valuetype) = get_arg(remainder)?;
                querystring = remainder;
                let operator = parse_dataoperator(opstr, value, valuetype)?;
                Self::Value(operator, qualifier)
            }
            Some("KEY") => {
                querystring = querystring["KEY".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, _) = parse_qualifiers(arg, remainder)?;
                if arg.starts_with("?") && arg.len() > 1 {
                    querystring = remainder;
                    Self::KeyVariable(&arg[1..], qualifier)
                } else {
                    return Err(StamError::QuerySyntaxError(
                        format!(
                            "Expected variable after KEY, got '{}'. Did you mean to just use DATA?",
                            arg
                        ),
                        "",
                    ));
                }
            }
            Some("[") => {
                let mut subconstraints: Vec<Constraint<'a>> = Vec::new();
                querystring = querystring[1..].trim_start();
                while !querystring.is_empty() {
                    let (subconstraint, remainder) = Self::parse(querystring)?;
                    subconstraints.push(subconstraint);
                    let remainder = remainder.trim_start();
                    if remainder.starts_with("OR ") {
                        querystring = &remainder[3..];
                        continue;
                    } else if remainder.starts_with("]") {
                        querystring = &remainder[1..];
                        break;
                    } else if remainder.is_empty() {
                        querystring = remainder;
                        break;
                    } else {
                        return Err(StamError::QuerySyntaxError(
                            format!("Expected OR or ] , got '{}'", remainder),
                            "Parsing [ ] block failed",
                        ));
                    }
                }
                if subconstraints.is_empty() {
                    return Err(StamError::QuerySyntaxError(
                        "Constraints for UNION operator is empty".to_string(),
                        "Parsing [ ] block failed",
                    ));
                }
                Self::Union(subconstraints)
            }
            Some("LIMIT") => {
                querystring = querystring["LIMIT".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                let arg = arg.parse::<isize>().map_err(|_| {
                    StamError::QuerySyntaxError(
                        format!("expected integer after LIMIT, got '{}'", arg),
                        "Parsing LIMIT constraint failed",
                    )
                })?;
                if Self::closed(querystring) {
                    //only one argument
                    if arg >= 0 {
                        Self::Limit { begin: 0, end: arg }
                    } else {
                        Self::Limit { begin: arg, end: 0 }
                    }
                } else {
                    let (end, remainder, _) = get_arg(querystring)?;
                    querystring = remainder;
                    let end = end.parse::<isize>().map_err(|_| {
                        StamError::QuerySyntaxError(
                            format!(
                                "expected integer as 2nd parameter after LIMIT, got '{}'",
                                end
                            ),
                            "Parsing LIMIT constraint failed",
                        )
                    })?;
                    Self::Limit { begin: arg, end }
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
        if querystring.starts_with(";") {
            querystring = &querystring[1..].trim_start();
        }
        Ok((constraint, querystring))
    }

    /// Serialize the constraint to a (partial) STAMQL String
    pub fn to_string(&self) -> Result<String, StamError> {
        let mut s = String::new();
        match self {
            Self::Id(id) => {
                s += &format!("ID \"{}\";", id);
            }
            Self::DataKey {
                set,
                key,
                qualifier,
            } => {
                s += &format!("DATA{} \"{}\" \"{}\";", qualifier.as_str(), set, key);
            }
            Self::KeyValue {
                set,
                key,
                operator,
                qualifier,
            } => {
                if let DataOperator::Any = operator {
                    s += &format!("DATA{} \"{}\" \"{}\";", qualifier.as_str(), set, key,);
                } else {
                    s += &format!(
                        "DATA{} \"{}\" \"{}\" {};",
                        qualifier.as_str(),
                        set,
                        key,
                        operator.to_string()?
                    );
                }
            }
            Self::Value(operator, qualifier) => {
                s += &format!("VALUE{} {};", qualifier.as_str(), operator.to_string()?);
            }
            Self::KeyVariable(varname, qualifier) => {
                s += &format!("DATA{} ?{};", qualifier.as_str(), varname);
            }
            Self::KeyValueVariable(varname, operator, qualifier) => {
                s += &format!(
                    "DATA{} ?{} {};",
                    qualifier.as_str(),
                    varname,
                    operator.to_string()?
                );
            }
            Self::AnnotationVariable(varname, qualifier, depth, offset) => {
                s += &format!(
                    "ANNOTATION{}{} ?{}",
                    qualifier.as_str(),
                    if depth == &AnnotationDepth::Max {
                        " RECURSIVE"
                    } else {
                        " "
                    },
                    varname
                );
                if let Some(offset) = offset {
                    s += &format!(" OFFSET {} {}", offset.begin, offset.end);
                }
                s.push(';')
            }
            Self::DataVariable(varname, qualifier) => {
                s += &format!("DATA{} ?{};", qualifier.as_str(), varname);
            }
            Self::DataSetVariable(varname, qualifier) => {
                s += &format!("DATASET{} ?{};", qualifier.as_str(), varname);
            }
            Self::ResourceVariable(varname, qualifier, offset) => {
                s += &format!("RESOURCE{} ?{}", qualifier.as_str(), varname);
                if let Some(offset) = offset {
                    s += &format!(" OFFSET {} {}", offset.begin, offset.end);
                }
                s.push(';')
            }
            Self::TextVariable(varname) => {
                s += &format!("TEXT{};", varname);
            }
            Self::Annotation(id, qualifier, depth, offset) => {
                s += &format!(
                    "ANNOTATION{}{} \"{}\"",
                    qualifier.as_str(),
                    if depth == &AnnotationDepth::Max {
                        " RECURSIVE"
                    } else {
                        " "
                    },
                    id
                );
                if let Some(offset) = offset {
                    s += &format!(" OFFSET {} {}", offset.begin, offset.end);
                }
                s.push(';')
            }
            Self::TextResource(id, qualifier, offset) => {
                s += &format!("RESOURCE{} \"{}\"", qualifier.as_str(), id);
                if let Some(offset) = offset {
                    s += &format!(" OFFSET {} {}", offset.begin, offset.end);
                }
                s.push(';')
            }
            Self::DataSet(id, qualifier) => {
                s += &format!("DATASET{} \"{}\";", qualifier.as_str(), id);
            }
            Self::Text(text, TextMode::Exact) => {
                s += &format!("TEXT \"{}\";", text);
            }
            Self::Text(text, TextMode::CaseInsensitive) => {
                s += &format!("TEXT AS NOCASE \"{}\";", text);
            }
            Self::Regex(regex) => {
                s += &format!("TEXT AS REGEX \"{}\";", regex.as_str());
            }
            Self::TextRelation { var, operator } => {
                s += &format!("RELATION ?{} {};", var, operator.as_str());
            }
            Self::Union(subconstraints) => {
                s += "[ ";
                for (i, subconstraint) in subconstraints.iter().enumerate() {
                    s += subconstraint.to_string()?.as_str();
                    if i < subconstraints.len() - 1 {
                        s += " OR ";
                    }
                }
                s += " ];";
            }
            Self::Limit { begin, end } => {
                s += &format!(" LIMIT {} {};", begin, end);
            }
            Self::Annotations(..)
            | Self::Data(..)
            | Self::TextSelections(..)
            | Self::Keys(..)
            | Self::Resources(..) => {
                return Err(StamError::QuerySyntaxError(
                    "Query contains internal constraints that can not be serialized to STAMQL"
                        .into(),
                    "Constraint::to_string()",
                ));
            }
        }
        Ok(s)
    }
}

impl<'a> Query<'a> {
    /// Instantiate a new query. See the top-level documentation of [`Query`] for examples.
    pub fn new(querytype: QueryType, resulttype: Option<Type>, name: Option<&'a str>) -> Self {
        Self {
            name,
            querytype,
            resulttype,
            constraints: Vec::new(),
            subqueries: Vec::new(),
            contextvars: HashMap::new(),
            assignments: Vec::new(),
        }
    }

    /// Selects this query or a specific subquery anywhere in its tree based on a given query path.
    /// A querypath is a list of indices selecting subqueries, e.g. [0,1] for first subquery, then second subquery or [] for the main query).
    /// Return None when the path does not resolve.
    pub fn select_by_path(&self, querypath: QueryPathRef) -> Option<&Query<'a>> {
        let mut q = self;
        for i in querypath.iter() {
            if let Some(sq) = q.subqueries().nth(*i) {
                q = sq;
            } else {
                return None;
            }
        }
        return Some(q);
    }

    /// Add a constraint to the query
    pub fn with_constraint(mut self, constraint: Constraint<'a>) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Add a constraint to the query
    pub fn constrain(&mut self, constraint: Constraint<'a>) -> &mut Self {
        self.constraints.push(constraint);
        self
    }

    /// Set the subquery for this query
    pub fn with_subquery(mut self, query: Query<'a>) -> Self {
        self.subqueries.push(query);
        self
    }

    // Set the variable name this query will use in the result output. The name should not include the `?` prefix that STAMQL uses.
    pub fn with_name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    /// Does this query have subqueries?
    pub fn has_subqueries(&self) -> bool {
        !self.subqueries.is_empty()
    }

    /// Returns the number of subqueries
    pub fn subqueries_len(&self) -> usize {
        !self.subqueries.len()
    }

    /// Returns an iterator over the direct subqueries (i.e. non-recursively)
    /// Use queries() for recursion.
    pub fn subqueries(&self) -> impl Iterator<Item = &Query<'a>> {
        self.subqueries.iter()
    }

    /// Return all querypaths (relative to the current query as root!), including the one referencing self
    pub fn querypaths(&self) -> Vec<QueryPath> {
        let mut paths = vec![QueryPath::new()];
        self.compute_querypaths(&mut paths);
        paths
    }

    /// Recursive helper function for paths()
    pub(crate) fn compute_querypaths(&self, paths: &mut Vec<QueryPath>) {
        for (i, q) in self.subqueries().enumerate() {
            let mut newpath = if let Some(path) = paths.iter().last() {
                path.clone()
            } else {
                QueryPath::new()
            };
            newpath.push(i);
            paths.push(newpath);
            q.compute_querypaths(paths);
        }
    }

    /// Returns an iterator over self and all subqueries (i.e. recursively)
    /// Note: this always returns self as first element
    pub fn queries(&self) -> impl Iterator<Item = (QueryPath, &Query<'a>)> {
        self.querypaths().into_iter().filter_map(|querypath| {
            if let Some(query) = self.select_by_path(&querypath) {
                Some((querypath, query))
            } else {
                None
            }
        })
    }

    /// Gives the name of the item described by `index` in  a [`QueryPath`].
    /// You usually don't need to query this directly.
    pub(crate) fn querypath_to_name(
        &self,
        querypath: QueryPathRef,
        index: usize,
    ) -> Option<&'a str> {
        let mut q = self;
        if index == 0 {
            return q.name;
        }
        for i in querypath.iter() {
            if let Some(sq) = q.subqueries.iter().nth(*i) {
                if index == *i + 1 {
                    return sq.name;
                }
                q = sq;
            } else {
                //invalid querypath
                return None;
            }
        }
        None
    }

    /// Returns all names (may contain duplicates if multiple subqueries in different branches have the same name)
    /// Returns an empty vector if the query path is not valid
    pub(crate) fn querypath_to_names<'b>(
        &self,
        querypath: QueryPathRef<'b>,
    ) -> Result<SmallVec<[Option<&'a str>; 4]>, StamError> {
        let mut q = self;
        let mut names = SmallVec::with_capacity(querypath.len() + 1);
        for i in querypath.iter() {
            if let Some(sq) = q.subqueries.iter().nth(*i) {
                names.push(sq.name);
                q = sq;
            } else {
                //invalid querypath
                return Err(StamError::OtherError("Invalid querypath"));
            }
        }
        Ok(names)
    }

    /// Get the index corresponding to a variable name, given a querypath
    /// You usually don't need to invoke this directly.
    pub fn index(&self, mut name: &str, querypath: QueryPathRef) -> Option<usize> {
        if name.starts_with("?") {
            //strip leading ? from variable name
            name = &name[1..];
        }
        let mut q = self;
        if q.name == Some(name) {
            return Some(0);
        }
        for i in querypath.iter() {
            if let Some(sq) = q.subqueries.iter().nth(*i) {
                if sq.name == Some(name) {
                    return Some(i + 1);
                }
                q = sq;
            } else {
                //invalid querypath
                return None;
            }
        }
        None
    }

    /// Iterates over all constraints in the Query
    pub fn constraints(&self) -> std::slice::Iter<Constraint<'a>> {
        self.constraints.iter()
    }

    /// Iterates over all constraints in the Query
    /// Alias for `constraints()`,
    pub fn iter(&self) -> std::slice::Iter<Constraint<'a>> {
        self.constraints.iter()
    }

    /// Iterates over all assignments in the Query
    pub fn assignments(&self) -> std::slice::Iter<Assignment<'a>> {
        self.assignments.iter()
    }

    /// Returns the variable name of the Query, the ? prefix STAMQL uses is never included.
    pub fn name(&self) -> Option<&'a str> {
        self.name
    }

    /// Returns the type of the query
    pub fn querytype(&self) -> QueryType {
        self.querytype
    }

    /// Returns the type of the results that this query produces
    pub fn resulttype(&self) -> Option<Type> {
        self.resulttype
    }

    /// Returns the type of the results that this query produces, as a STAMQL keyword.
    pub fn resulttype_as_str(&self) -> Option<&'static str> {
        match self.resulttype() {
            Some(Type::Annotation) => Some("ANNOTATION"),
            Some(Type::AnnotationData) => Some("DATA"),
            Some(Type::AnnotationDataSet) => Some("DATASET"),
            Some(Type::TextResource) => Some("RESOURCE"),
            Some(Type::TextSelection) => Some("TEXT"),
            Some(Type::DataKey) => Some("KEY"),
            _ => None,
        }
    }

    /// Parses a query formulated in [STAMQL](https://github.com/annotation/stam/tree/master/extensions/stam-query).
    /// Returns the [`Query`] if successful, it can subsequently by passed to [`AnnotationStore.query()`] or a [`StamError::QuerySyntaxError`]
    /// if the query is not valid. See the documentation on [`Query`] itself for examples.
    pub fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        querystring = querystring.trim();
        if let Some("SELECT") = querystring.split(QUERYSPLITCHARS).next() {
            Self::parse_select(querystring)
        } else if let Some("ADD") = querystring.split(QUERYSPLITCHARS).next() {
            Self::parse_add(querystring)
        } else if let Some("DELETE") = querystring.split(QUERYSPLITCHARS).next() {
            Self::parse_delete(querystring)
        } else {
            return Err(StamError::QuerySyntaxError(
                format!(
                    "Expected SELECT, ADD or  DELETE, got '{}'",
                    querystring
                        .split(QUERYSPLITCHARS)
                        .next()
                        .unwrap_or("(empty string)"),
                ),
                "",
            ));
        }
    }

    fn parse_select(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let mut end = 7;
        querystring = querystring[end..].trim_start();
        let resulttype = match &querystring.split(QUERYSPLITCHARS).next() {
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
            Some("RESOURCE") | Some("resource") => {
                end = "RESOURCE".len();
                Some(Type::TextResource)
            }
            Some("DATASET") | Some("dataset") => {
                end = "DATASET".len();
                Some(Type::AnnotationDataSet)
            }
            Some(x) => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected result type (ANNOTATION, DATA, TEXT, KEY, DATASET, RESOURCE), got '{}'", x),
                    "",
                ))
            }
            None => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected result type (ANNOTATION, DATA, TEXT, KEY, DATASET, RESOURCE), got end of string"),
                    "",
                ))
            }
        };
        querystring = querystring[end..].trim_start();
        let (name, remainder) = Self::parse_name(querystring)?;
        querystring = remainder;

        let mut constraints = Vec::new();
        match querystring.split(QUERYSPLITCHARS).next() {
            Some("WHERE") => querystring = querystring["WHERE".len()..].trim_start(),
            Some("{") | Some("") | None => {} //no-op (select all, end of query, no where clause)
            _ => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Expected WHERE, got '{}'",
                        querystring
                            .split(QUERYSPLITCHARS)
                            .next()
                            .unwrap_or("(empty string)"),
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
        let (subqueries, remainder) = Self::parse_subqueries(querystring, false)?;

        querystring = remainder;
        Ok((
            Self {
                name,
                querytype: QueryType::Select,
                resulttype,
                constraints,
                subqueries,
                contextvars: HashMap::new(),
                assignments: Vec::new(),
            },
            querystring,
        ))
    }

    fn parse_add(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let mut end = 4;
        querystring = querystring[end..].trim_start();
        let resulttype = match &querystring.split(QUERYSPLITCHARS).next() {
            Some("ANNOTATION") | Some("annotation") => {
                end = "ANNOTATION".len();
                Some(Type::Annotation)
            }
            Some(x) => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected result type ANNOTATION for ADD query, got '{}'", x),
                    "",
                ))
            }
            None => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected result type ANNOTATION for ADD query got end of string"),
                    "",
                ))
            }
        };
        querystring = querystring[end..].trim_start();
        let (name, remainder) = Self::parse_name(querystring)?;
        querystring = remainder;

        let mut assignments = Vec::new();
        match querystring.split(QUERYSPLITCHARS).next() {
            Some("WITH") => querystring = querystring["WITH".len()..].trim_start(),
            Some("{") | Some("") | None => {} //no-op (select all, end of query, no where clause)
            _ => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Expected WITH, got '{}'",
                        querystring
                            .split(QUERYSPLITCHARS)
                            .next()
                            .unwrap_or("(empty string)"),
                    ),
                    "",
                ));
            }
        }

        //parse assignments
        while !querystring.is_empty()
            && querystring.trim_start().chars().nth(0) != Some('{')
            && querystring.trim_start().chars().nth(0) != Some('}')
        {
            let (assignment, remainder) = Assignment::parse(querystring)?;
            querystring = remainder;
            assignments.push(assignment);
        }

        //parse subquery
        let (subquery, remainder) = Self::parse_subqueries(querystring, false)?;
        querystring = remainder;
        Ok((
            Self {
                name,
                querytype: QueryType::Add,
                resulttype,
                constraints: Vec::new(),
                subqueries: subquery,
                contextvars: HashMap::new(),
                assignments,
            },
            querystring,
        ))
    }

    fn parse_delete(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let mut end = 7;
        querystring = querystring[end..].trim_start();
        let resulttype = match &querystring.split(QUERYSPLITCHARS).next() {
            Some("ANNOTATION") | Some("annotation") => {
                end = "ANNOTATION".len();
                Some(Type::Annotation)
            }
            Some(x) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Expected result type ANNOTATION for DELETE query, got '{}'",
                        x
                    ),
                    "",
                ))
            }
            None => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected result type ANNOTATION for DELETE query got end of string"),
                    "",
                ))
            }
        };
        querystring = querystring[end..].trim_start();
        let (name, remainder) = Self::parse_name(querystring)?;
        querystring = remainder;

        //parse subquery
        let (subquery, remainder) = Self::parse_subqueries(querystring, false)?;
        querystring = remainder;
        Ok((
            Self {
                name,
                querytype: QueryType::Delete,
                resulttype,
                constraints: Vec::new(),
                subqueries: subquery,
                contextvars: HashMap::new(),
                assignments: Vec::new(),
            },
            querystring,
        ))
    }

    fn parse_name(mut querystring: &'a str) -> Result<(Option<&'a str>, &'a str), StamError> {
        let name = if let Some('?') = querystring.chars().next() {
            Some(
                querystring[1..]
                    .split(QUERYSPLITCHARS)
                    .next()
                    .unwrap()
                    .trim_end_matches(';'),
            )
        } else {
            None
        };
        if let Some(name) = name {
            querystring = querystring[1 + name.len()..].trim_start();
        }
        Ok((name, querystring))
    }

    fn parse_subqueries(
        mut querystring: &'a str,
        mutable: bool,
    ) -> Result<(Vec<Self>, &'a str), StamError> {
        let mut subqueries = Vec::new();
        if querystring.trim_start().chars().nth(0) == Some('{') {
            loop {
                querystring = &querystring[1..].trim_start();
                if querystring.starts_with("SELECT") {
                    let (subquery, remainder) = Self::parse_select(querystring)?;
                    subqueries.push(subquery);
                    querystring = remainder.trim_start();
                } else if mutable
                    && (querystring.starts_with("ADD") || querystring.starts_with("DELETE"))
                {
                    let (subquery, remainder) = Self::parse(querystring)?;
                    subqueries.push(subquery);
                    querystring = remainder.trim_start();
                }
                if querystring.trim_start().chars().nth(0) == Some('}') {
                    querystring = &querystring[1..].trim_start();
                    break;
                } else {
                    return Err(StamError::QuerySyntaxError(
                        "Missing '}' to close subquery block".to_string(),
                        "",
                    ));
                }
            }
        }
        Ok((subqueries, querystring))
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn with_annotationvar(
        mut self,
        name: impl Into<String>,
        annotation: &ResultItem<Annotation>,
    ) -> Self {
        self.contextvars
            .insert(name.into(), ContextItem::Annotation(annotation.handle()));
        self
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn bind_annotationvar(
        &mut self,
        name: impl Into<String>,
        annotation: &ResultItem<Annotation>,
    ) {
        self.contextvars
            .insert(name.into(), ContextItem::Annotation(annotation.handle()));
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn with_datavar(
        mut self,
        name: impl Into<String>,
        data: &ResultItem<AnnotationData>,
    ) -> Self {
        self.contextvars.insert(
            name.into(),
            ContextItem::AnnotationData(data.set().handle(), data.handle()),
        );
        self
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn bind_datavar(&mut self, name: impl Into<String>, data: &ResultItem<AnnotationData>) {
        self.contextvars.insert(
            name.into(),
            ContextItem::AnnotationData(data.set().handle(), data.handle()),
        );
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn with_keyvar(mut self, name: impl Into<String>, key: &ResultItem<DataKey>) -> Self {
        self.contextvars.insert(
            name.into(),
            ContextItem::DataKey(key.set().handle(), key.handle()),
        );
        self
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    pub fn bind_keyvar(&mut self, name: impl Into<String>, key: &ResultItem<DataKey>) {
        self.contextvars.insert(
            name.into(),
            ContextItem::DataKey(key.set().handle(), key.handle()),
        );
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn with_textvar(
        mut self,
        name: impl Into<String>,
        textselection: &ResultTextSelection,
    ) -> Self {
        self.contextvars.insert(
            name.into(),
            ContextItem::TextSelection(
                textselection.resource().handle(),
                textselection.inner().clone(),
            ),
        );
        self
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn bind_textvar(&mut self, name: impl Into<String>, textselection: &ResultTextSelection) {
        self.contextvars.insert(
            name.into(),
            ContextItem::TextSelection(
                textselection.resource().handle(),
                textselection.inner().clone(),
            ),
        );
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn with_resourcevar(
        mut self,
        name: impl Into<String>,
        resource: &ResultItem<TextResource>,
    ) -> Self {
        self.contextvars
            .insert(name.into(), ContextItem::TextResource(resource.handle()));
        self
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn bind_resourcevar(
        &mut self,
        name: impl Into<String>,
        resource: &ResultItem<TextResource>,
    ) {
        self.contextvars
            .insert(name.into(), ContextItem::TextResource(resource.handle()));
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn with_datasetvar(
        mut self,
        name: impl Into<String>,
        dataset: &ResultItem<AnnotationDataSet>,
    ) -> Self {
        self.contextvars.insert(
            name.into(),
            ContextItem::AnnotationDataSet(dataset.handle()),
        );
        self
    }

    /// Bind a variable, the name should not include the ? prefix STAMQL uses.
    /// This is a context variable that will be available to the query, but will not be propagated to the results.
    pub fn bind_datasetvar(
        &mut self,
        name: impl Into<String>,
        dataset: &ResultItem<AnnotationDataSet>,
    ) {
        self.contextvars.insert(
            name.into(),
            ContextItem::AnnotationDataSet(dataset.handle()),
        );
    }

    /// Bind any variable from a [`QueryResultItem`] as a context variable in this query
    pub fn bind_from_result(&mut self, varname: impl Into<String>, resultitem: &QueryResultItem) {
        match resultitem {
            QueryResultItem::None => {}
            QueryResultItem::Annotation(x) => {
                self.bind_annotationvar(varname, x);
            }
            QueryResultItem::TextSelection(x) => {
                self.bind_textvar(varname, x);
            }
            QueryResultItem::AnnotationData(x) => {
                self.bind_datavar(varname, x);
            }
            QueryResultItem::TextResource(x) => {
                self.bind_resourcevar(varname, x);
            }
            QueryResultItem::AnnotationDataSet(x) => {
                self.bind_datasetvar(varname, x);
            }
            QueryResultItem::DataKey(x) => {
                self.bind_keyvar(varname, x);
            }
        }
    }

    fn resolve_contextvars<'b>(
        &self,
        store: &'b AnnotationStore,
    ) -> Result<HashMap<String, QueryResultItem<'b>>, StamError> {
        let mut contextvars = HashMap::new();
        for (name, item) in self.contextvars.iter() {
            match item {
                ContextItem::Annotation(handle) => contextvars.insert(
                    name.clone(),
                    QueryResultItem::Annotation(store.annotation(*handle).or_fail()?),
                ),
                ContextItem::TextResource(handle) => contextvars.insert(
                    name.clone(),
                    QueryResultItem::TextResource(store.resource(*handle).or_fail()?),
                ),
                ContextItem::AnnotationDataSet(handle) => contextvars.insert(
                    name.clone(),
                    QueryResultItem::AnnotationDataSet(store.dataset(*handle).or_fail()?),
                ),
                ContextItem::AnnotationData(set, handle) => contextvars.insert(
                    name.clone(),
                    QueryResultItem::AnnotationData(store.annotationdata(*set, *handle).or_fail()?),
                ),
                ContextItem::DataKey(set, handle) => contextvars.insert(
                    name.clone(),
                    QueryResultItem::DataKey(store.key(*set, *handle).or_fail()?),
                ),
                ContextItem::TextSelection(resource, textselection) => contextvars.insert(
                    name.clone(),
                    if let Some(handle) = textselection.handle() {
                        QueryResultItem::TextSelection(
                            store.textselection(*resource, handle).or_fail()?,
                        )
                    } else {
                        QueryResultItem::TextSelection({
                            let resource = store.resource(*resource).or_fail()?;
                            resource.textselection(&textselection.into())?
                        })
                    },
                ),
            };
        }
        Ok(contextvars)
    }

    /// Serialize the query to a STAMQL String
    pub fn to_string(&self) -> Result<String, StamError> {
        let mut s = String::new();
        s += self.querytype().as_str();
        s += " ";
        if let Some(resulttype) = self.resulttype_as_str() {
            s += resulttype;
        }
        if let Some(name) = self.name() {
            s += " ?";
            s += name;
        }
        if !self.constraints.is_empty() {
            s += " WHERE\n";
            for constraint in self.iter() {
                s += &constraint.to_string()?;
                s.push('\n');
            }
        }

        if self.has_subqueries() {
            s += "{\n";
            for subquery in self.subqueries() {
                s += &subquery.to_string()?;
            }
            s += "\n}";
        }
        Ok(s)
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

/// This type abstracts over all the main iterators.
/// This abstraction uses dynamic dispatch so comes with a small performance cost
pub enum QueryResultIter<'store> {
    /// Corresponds to result type `ANNOTATION` in STAMQL.
    /// The contained iterator implements trait [`AnnotationIterator`].
    Annotations(Box<dyn Iterator<Item = ResultItem<'store, Annotation>> + 'store>),
    /// Corresponds to result type `DATA` in STAMQL
    /// The contained iterator implements trait [`DataIterator`].
    Data(Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>),
    /// Corresponds to result type `KEY` in STAMQL
    /// The contained iterator implements trait [`KeyIterator`].
    Keys(Box<dyn Iterator<Item = ResultItem<'store, DataKey>> + 'store>),
    /// Corresponds to result type `DATASET` in STAMQL
    DataSets(Box<dyn Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store>),
    /// Corresponds to result type `TEXT` in STAMQL
    /// The contained iterator implements trait [`TextSelectionIterator`].
    TextSelections(Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>),
    /// Corresponds to result type `RESOURCE` in STAMQL
    /// The contained iterator implements trait [`ResourcesIterator`].
    Resources(Box<dyn Iterator<Item = ResultItem<'store, TextResource>> + 'store>),
}

#[derive(Clone, Debug)]
/// This structure encapsulates the different kind of result items that can be returned from queries.
/// See [`AnnotationStore::query()`] for an example of it in use.
pub enum QueryResultItem<'store> {
    /// No result
    None,

    /// Corresponds to result type `TEXT` in STAMQL
    TextSelection(ResultTextSelection<'store>),

    /// Corresponds to result type `ANNOTATION` in STAMQL
    Annotation(ResultItem<'store, Annotation>),

    /// Corresponds to result type `RESOURCE` in STAMQL
    TextResource(ResultItem<'store, TextResource>),

    /// Corresponds to result type `KEY` in STAMQL
    DataKey(ResultItem<'store, DataKey>),

    /// Corresponds to result type `DATA` in STAMQL
    AnnotationData(ResultItem<'store, AnnotationData>),

    /// Corresponds to result type `DATASET` in STAMQL
    AnnotationDataSet(ResultItem<'store, AnnotationDataSet>),
}

#[derive(Clone, Debug)]
pub(crate) enum ContextItem {
    Annotation(AnnotationHandle),
    TextResource(TextResourceHandle),
    DataKey(AnnotationDataSetHandle, DataKeyHandle),
    AnnotationData(AnnotationDataSetHandle, AnnotationDataHandle),
    AnnotationDataSet(AnnotationDataSetHandle),
    TextSelection(TextResourceHandle, TextSelection),
}

pub(crate) struct QueryState<'store> {
    /// The iterator for the current query
    iterator: QueryResultIter<'store>,

    // note: this captures the result of the current state, in order to make it available for subsequent deeper iterators
    result: QueryResultItem<'store>,
}

/// This points to a particular subquery inside a query
pub type QueryPath = SmallVec<[usize; 4]>;
pub type QueryPathRef<'a> = &'a [usize];

/// Iterator over the results of a [`Query`]. Querying will be performed as the iterator is
/// consumed (lazy evaluation). If it is not consumed, no actual querying will be done.
/// See [`AnnotationStore::query()`] for an example.
pub struct QueryIter<'store> {
    store: &'store AnnotationStore,

    query: Option<Query<'store>>,

    /// States in the stack hold iterators, each stack item corresponds to one further level of nesting
    statestack: Vec<QueryState<'store>>,

    /// This indicates which subquery is currently being processed (index number, zero based), because there may be multiple siblings
    /// this has |statestack| - 1 elements
    querypath: QueryPath,

    /// Signals that we're done with the entire stack
    done: bool,

    /// Context variables
    contextvars: HashMap<String, QueryResultItem<'store>>,
}

/// Represents an entire result row, each result stems from a query
pub struct QueryResultItems<'store> {
    querypath: QueryPath,
    names: SmallVec<[Option<&'store str>; 4]>,
    items: SmallVec<[QueryResultItem<'store>; 4]>,
}

impl<'store> AnnotationStore {
    /// Initializes a SELECT [`Query`] and returns an iterator ([`QueryIter`]) to loop over the results.
    /// Note that no actual querying is done yet until you use the iterator, the [`Query`] is lazy-evaluated.
    /// This method can only process immutable queries (SELECT), use [`query_mut()`] for queries that modify the store.
    ///
    /// ## Examples
    ///
    /// ```ignore
    /// let query: Query = "SELECT ANNOTATION ?a WHERE DATA myset type = phrase;".try_into()?;
    /// for results in store.query(query) {
    ///     for result in results.iter() {
    ///        if let QueryResultItem::Annotation(annotation) = result {
    ///           unimplemented!("do something with the result...");
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// or by name:
    ///
    /// ```ignore
    /// let query: Query = "SELECT ANNOTATION ?a WHERE DATA myset type = phrase;".try_into()?;
    /// let iter = store.query(query);
    /// let names = iter.names();
    /// for results in iter {
    ///     if let Ok(result) = results.get_by_name(&names, "a") {
    ///        if let QueryResultItem::Annotation(annotation) = result {
    ///           unimplemented!("do something with the result...");
    ///         }
    ///     }
    /// }
    /// ```
    pub fn query(&'store self, query: Query<'store>) -> Result<QueryIter<'store>, StamError> {
        if !query.querytype().readonly() {
            return Err(StamError::QuerySyntaxError(
                format!("AnnotationStore.query() cant not handle mutable queries (ADD/DELETE), expected an immutable one (SELECT)"),
                "",
            ));
        }
        Ok(QueryIter {
            store: self,
            contextvars: query.resolve_contextvars(self)?,
            query: Some(query),
            statestack: Vec::new(),
            querypath: QueryPath::new(),
            done: false,
        })
    }

    /// This method processes mutable queries (ADD/DELETE) and modifies the store accordingly.
    /// Note that the modifying queries are evaluated eagerly, so they will run even if the resulting iterator is not consumed.
    pub fn query_mut(
        &'store mut self,
        query: Query<'store>,
    ) -> Result<QueryIter<'store>, StamError> {
        if query.querytype().readonly() {
            //no need for mutability, just fall back:
            self.query(query)
        } else {
            //TODO: currently only supports ONE subquery!!!!
            if let Some(subquery) = query.subqueries().next() {
                let mut subquery = subquery.clone();
                subquery.contextvars = query.contextvars.clone();

                //run the subquery
                let iter = if !subquery.querytype().readonly() {
                    self.query_mut(subquery)?
                } else {
                    self.query(subquery)?
                };

                if query.querytype() == QueryType::Add {
                    //prepare the builders, following the assignments in the the mutating part of the query, and  given the subquery results
                    let mut builders: Vec<AnnotationBuilder> = Vec::new();
                    for resultrow in iter {
                        let mut selectors = Vec::new();
                        let mut databuilders = Vec::new();
                        let mut id: Option<String> = None;
                        let mut complextarget = None;
                        for assignment in query.assignments() {
                            match assignment {
                                Assignment::Id(s) => id = Some(s.to_string()),
                                Assignment::Data { set, key, value } => {
                                    let mut databuilder = AnnotationDataBuilder::new();
                                    databuilder.dataset = set.to_string().into();
                                    databuilder.key = key.to_string().into();
                                    databuilder.value = value.clone();
                                    databuilders.push(databuilder);
                                }
                                Assignment::Target { name, offset } => {
                                    match resultrow.get_by_name(name)? {
                                        QueryResultItem::Annotation(annotation) => {
                                            selectors.push(SelectorBuilder::AnnotationSelector(
                                                BuildItem::Handle(annotation.handle()),
                                                offset.clone(),
                                            ))
                                        }
                                        QueryResultItem::TextSelection(textselection) => {
                                            if let Some(offset) = offset {
                                                let textselection =
                                                    textselection.textselection(offset)?;
                                                selectors.push(SelectorBuilder::TextSelector(
                                                    BuildItem::Handle(
                                                        textselection.resource().handle(),
                                                    ),
                                                    (&textselection).into(),
                                                ))
                                            } else {
                                                selectors.push(SelectorBuilder::TextSelector(
                                                    BuildItem::Handle(
                                                        textselection.resource().handle(),
                                                    ),
                                                    textselection.into(),
                                                ))
                                            }
                                        }
                                        QueryResultItem::TextResource(resource) => {
                                            selectors.push(SelectorBuilder::ResourceSelector(
                                                BuildItem::Handle(resource.handle()),
                                            ))
                                        }
                                        QueryResultItem::AnnotationDataSet(dataset) => selectors
                                            .push(SelectorBuilder::DataSetSelector(
                                                BuildItem::Handle(dataset.handle()),
                                            )),
                                        QueryResultItem::AnnotationData(data) => {
                                            selectors.push(SelectorBuilder::AnnotationDataSelector(
                                                BuildItem::Handle(data.set().handle()),
                                                BuildItem::Handle(data.handle()),
                                            ))
                                        }
                                        QueryResultItem::DataKey(key) => {
                                            selectors.push(SelectorBuilder::DataKeySelector(
                                                BuildItem::Handle(key.set().handle()),
                                                BuildItem::Handle(key.handle()),
                                            ))
                                        }
                                        QueryResultItem::None => {}
                                    }
                                }
                                Assignment::ComplexTarget(kind) => {
                                    complextarget = Some(kind.clone())
                                }
                                x => {
                                    return Err(StamError::QuerySyntaxError(
                                        format!("Invalid assignment for ANNOTATION: {:?}", x),
                                        "",
                                    ));
                                }
                            }
                        }
                        let mut builder = AnnotationBuilder::new();
                        for databuilder in databuilders {
                            builder = builder.with_data_builder(databuilder);
                        }
                        if let Some(id) = id {
                            builder = builder.with_id(id);
                        }
                        match selectors.len() {
                            0 => {
                                return Err(StamError::QuerySyntaxError(
                                    format!("ADD query has no TARGET"),
                                    "",
                                ));
                            }
                            1 if complextarget == None => {
                                builder = builder.with_target(
                                    selectors.into_iter().next().expect("must have one"),
                                );
                            }
                            _ => match complextarget {
                                Some(SelectorKind::CompositeSelector) | None => {
                                    builder = builder
                                        .with_target(SelectorBuilder::CompositeSelector(selectors));
                                }
                                Some(SelectorKind::MultiSelector) => {
                                    builder = builder
                                        .with_target(SelectorBuilder::MultiSelector(selectors));
                                }
                                Some(SelectorKind::DirectionalSelector) => {
                                    builder = builder.with_target(
                                        SelectorBuilder::DirectionalSelector(selectors),
                                    );
                                }
                                _ => {
                                    unreachable!("Invalid value for complextarget");
                                }
                            },
                        }
                        builders.push(builder);
                    }

                    // Annotate and capture result
                    let annotations = self.annotate_from_iter(builders)?;

                    //construct a new query that returns the results
                    let query = Query::new(QueryType::Select, query.resulttype(), query.name())
                        .with_constraint(Constraint::Annotations(
                            Handles::new(Cow::Owned(annotations), false, self),
                            SelectionQualifier::Normal,
                            AnnotationDepth::Zero,
                        ));
                    self.query(query)
                } else if query.querytype() == QueryType::Delete {
                    let mut remove_annotations: Vec<AnnotationHandle> = Vec::new();
                    let mut remove_resources: Vec<TextResourceHandle> = Vec::new();
                    let mut remove_datasets: Vec<AnnotationDataSetHandle> = Vec::new();
                    let mut remove_data: Vec<(AnnotationDataSetHandle, AnnotationDataHandle)> =
                        Vec::new();
                    let mut remove_keys: Vec<(AnnotationDataSetHandle, DataKeyHandle)> = Vec::new();
                    for resultrow in iter {
                        match resultrow.get_by_name_or_first(query.name())? {
                            QueryResultItem::Annotation(annotation)
                                if query.resulttype() == Some(Type::Annotation) =>
                            {
                                remove_annotations.push(annotation.handle());
                            }
                            QueryResultItem::TextResource(resource)
                                if query.resulttype() == Some(Type::TextResource) =>
                            {
                                remove_resources.push(resource.handle())
                            }
                            QueryResultItem::AnnotationDataSet(dataset)
                                if query.resulttype() == Some(Type::AnnotationDataSet) =>
                            {
                                remove_datasets.push(dataset.handle())
                            }
                            QueryResultItem::AnnotationData(data)
                                if query.resulttype() == Some(Type::AnnotationData) =>
                            {
                                remove_data.push((data.set().handle(), data.handle()));
                            }
                            QueryResultItem::DataKey(key)
                                if query.resulttype() == Some(Type::DataKey) =>
                            {
                                remove_keys.push((key.set().handle(), key.handle()));
                            }
                            QueryResultItem::None => {
                                //nothing to do
                            }
                            x => {
                                return Err(StamError::QuerySyntaxError(
                                    format!("Return type in DELETE query and subquery must match, got unexpected {:?}", x),
                                    "",
                                ));
                            }
                        }
                    }

                    for resource in remove_resources {
                        self.remove(resource)?;
                    }
                    for annotation in remove_annotations {
                        self.remove(annotation)?;
                    }
                    for (set, key) in remove_keys {
                        self.remove_key(set, key, true)?;
                    }
                    for (set, data) in remove_data {
                        self.remove_data(set, data, true)?;
                    }

                    //just return an empty iterator
                    Ok(QueryIter {
                        store: self,
                        query: None,
                        statestack: Vec::new(),
                        querypath: QueryPath::new(),
                        contextvars: HashMap::new(),
                        done: true,
                    })
                } else {
                    unreachable!("unknown query type");
                }
            } else {
                unreachable!("mutable query must have subquery");
            }
        }
    }
}

impl<'store> QueryIter<'store> {
    /// Returns the store against which the query is evaluated
    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    /// Return the query/subquery given a querypath
    /// A querypath is a list of indices selecting subqueries, e.g. [0,1] for first subquery, then second subquery or [] for the main query).
    /// Returns `None` on any invalid index
    pub fn get_query(&self, querypath: QueryPathRef) -> Option<&Query<'store>> {
        if let Some(query) = self.query.as_ref() {
            query.select_by_path(&querypath)
        } else {
            None
        }
    }

    /// Initializes a new state
    pub(crate) fn init_state(&mut self) -> Result<bool, StamError> {
        //let queryindex = self.statestack.len();
        let query = self.get_query(&self.querypath).expect("query must exist");
        let mut constraintsiter = query.constraints.iter();

        let iter = match query.resulttype {
            ///////////////////////////// target= RESOURCE ////////////////////////////////////////////
            Some(Type::TextResource) => {
                let mut iter = self.init_state_resources(constraintsiter.next())?;
                while let Some(constraint) = constraintsiter.next() {
                    iter = self.update_state_resources(constraint, iter)?;
                }
                Ok::<_, StamError>(QueryResultIter::Resources(iter))
            }
            ///////////////////////////// target= ANNOTATION ////////////////////////////////////////////
            Some(Type::Annotation) => {
                let mut iter = self.init_state_annotations(constraintsiter.next())?;
                while let Some(constraint) = constraintsiter.next() {
                    iter = self.update_state_annotations(constraint, iter)?;
                }
                Ok(QueryResultIter::Annotations(iter))
            }
            /////////////////////////////////// target=TEXT //////////////////////////////////////
            Some(Type::TextSelection) => {
                let mut iter = self.init_state_textselections(constraintsiter.next())?;
                while let Some(constraint) = constraintsiter.next() {
                    iter = self.update_state_textselections(constraint, iter)?;
                }
                Ok(QueryResultIter::TextSelections(iter))
            }
            ///////////////////////////////// target= DATA ////////////////////////////////////////
            Some(Type::AnnotationData) => {
                let mut iter = self.init_state_data(constraintsiter.next())?;
                while let Some(constraint) = constraintsiter.next() {
                    iter = self.update_state_data(constraint, iter)?;
                }
                Ok(QueryResultIter::Data(iter))
            }
            ///////////////////////////////// target= KEY ////////////////////////////////////////
            Some(Type::DataKey) => {
                let mut iter = self.init_state_keys(constraintsiter.next())?;
                //secondary contraints for target KEY
                while let Some(constraint) = constraintsiter.next() {
                    iter = self.update_state_keys(constraint, iter)?;
                }
                Ok(QueryResultIter::Keys(iter))
            }
            ///////////////////////////////// target= DATASET ////////////////////////////////////////
            Some(Type::AnnotationDataSet) => {
                let mut iter = self.init_state_datasets(constraintsiter.next())?;
                //secondary contraints for target KEY
                while let Some(constraint) = constraintsiter.next() {
                    iter = self.update_state_datasets(constraint, iter)?;
                }
                Ok(QueryResultIter::DataSets(iter))
            }
            None => unreachable!("Query must have a result type"),
            _ => unimplemented!("Query result type not implemented"),
        }?;

        self.statestack.push(QueryState {
            iterator: iter,
            result: QueryResultItem::None,
        });
        self.querypath.push(0);
        // Do the first iteration (may remove this and other elements from the stack again if it fails)
        Ok(self.next_state())
    }

    /// Advances the query state on the stack, return true if a new result was obtained (stored in the state's result buffer),
    /// Pops items off the stack if they no longer yield result.
    /// If no result at all can be obtained anymore, false is returned.
    pub(crate) fn next_state(&mut self) -> bool {
        while !self.statestack.is_empty() {
            if let Some(mut state) = self.statestack.pop() {
                //we pop the state off the stack (we put it back again in cases where it's an undepleted iterator)
                //but this allows us to take full ownership and not have a mutable borrow,
                //which would get in the way as we also need to inspect prior results from the stack (immutably)
                //
                // keep track of the current subquery_index (pop it and push it back)
                let current_querypath = self.querypath.clone();
                let subquery_index = self.querypath.pop();
                loop {
                    match &mut state.iterator {
                        QueryResultIter::TextSelections(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::TextSelection(result);
                                self.statestack.push(state);
                                if let Some(subquery_index) = subquery_index {
                                    self.querypath.push(subquery_index);
                                }
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::Annotations(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::Annotation(result);
                                self.statestack.push(state);
                                if let Some(subquery_index) = subquery_index {
                                    self.querypath.push(subquery_index);
                                }
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::Data(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::AnnotationData(result);
                                self.statestack.push(state);
                                if let Some(subquery_index) = subquery_index {
                                    self.querypath.push(subquery_index);
                                }
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::Resources(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::TextResource(result);
                                self.statestack.push(state);
                                if let Some(subquery_index) = subquery_index {
                                    self.querypath.push(subquery_index);
                                }
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::Keys(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::DataKey(result);
                                self.statestack.push(state);
                                if let Some(subquery_index) = subquery_index {
                                    self.querypath.push(subquery_index);
                                }
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::DataSets(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::AnnotationDataSet(result);
                                self.statestack.push(state);
                                if let Some(subquery_index) = subquery_index {
                                    self.querypath.push(subquery_index);
                                }
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                    }
                }
                //iterator depleted, advance to next subquery if there is one
                let query = self
                    .get_query(&current_querypath)
                    .expect("query must exist");
                if let Some(subquery_index) = subquery_index {
                    if subquery_index < query.subqueries_len() - 1 {
                        self.querypath.push(subquery_index + 1);
                    }
                }
            }
        }
        //mark as done (otherwise the iterator would restart from scratch again)
        self.done = true;
        false
    }

    /// Apply primary constraint for ANNOTATION result type
    pub(crate) fn init_state_annotations(
        &self,
        constraint: Option<&Constraint<'store>>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, Annotation>> + 'store>, StamError> {
        let store: &'store AnnotationStore = self.store();
        //primary constraints for ANNOTATION
        Ok(match constraint {
            Some(&Constraint::Id(id)) => {
                Box::new(Some(store.annotation(id).or_fail()?).into_iter())
            }
            Some(&Constraint::Annotations(
                ref handles,
                SelectionQualifier::Normal,
                AnnotationDepth::Zero,
            )) => Box::new(FromHandles::new(handles.clone().into_iter(), store)),
            Some(&Constraint::Annotations(ref handles, SelectionQualifier::Normal, depth)) => {
                Box::new(
                    FromHandles::new(handles.clone().into_iter(), store)
                        .annotations_in_targets(depth),
                )
            }
            Some(&Constraint::Annotations(
                ref handles,
                SelectionQualifier::Metadata,
                AnnotationDepth::One,
            )) => Box::new(FromHandles::new(handles.clone().into_iter(), store).annotations()),
            Some(&Constraint::AnnotationVariable(var, SelectionQualifier::Normal, depth, None)) => {
                let annotation = self.resolve_annotationvar(var)?;
                Box::new(annotation.annotations_in_targets(depth))
            }
            Some(&Constraint::AnnotationVariable(
                var,
                SelectionQualifier::Metadata,
                AnnotationDepth::One,
                None,
            )) => {
                //TODO LATER: handle Recursive variant?
                let annotation = self.resolve_annotationvar(var)?;
                Box::new(annotation.annotations())
            }
            Some(&Constraint::TextResource(res, SelectionQualifier::Normal, None)) => {
                Box::new(store.resource(res).or_fail()?.annotations())
            }
            Some(&Constraint::TextResource(res, SelectionQualifier::Metadata, None)) => {
                Box::new(store.resource(res).or_fail()?.annotations_as_metadata())
            }
            Some(&Constraint::ResourceVariable(var, SelectionQualifier::Normal, None)) => {
                let resource = self.resolve_resourcevar(var)?;
                Box::new(resource.annotations())
            }
            Some(&Constraint::ResourceVariable(var, SelectionQualifier::Metadata, None)) => {
                let resource = self.resolve_resourcevar(var)?;
                Box::new(resource.annotations_as_metadata())
            }
            Some(&Constraint::Annotation(annotation, SelectionQualifier::Normal, depth, None)) => {
                Box::new(
                    store
                        .annotation(annotation)
                        .or_fail()?
                        .annotations_in_targets(depth),
                )
            }
            Some(&Constraint::Annotation(
                annotation,
                SelectionQualifier::Metadata,
                AnnotationDepth::One,
                None,
            )) => Box::new(store.annotation(annotation).or_fail()?.annotations()),
            Some(&Constraint::DataSet(set, SelectionQualifier::Normal)) => {
                let dataset = store.dataset(set).or_fail()?;
                Box::new(store.annotations().filter_set(&dataset))
            }
            Some(&Constraint::DataSet(set, SelectionQualifier::Metadata)) => {
                Box::new(store.dataset(set).or_fail()?.annotations())
            }
            Some(&Constraint::DataSetVariable(varname, SelectionQualifier::Normal)) => {
                let dataset = self.resolve_datasetvar(varname)?;
                Box::new(store.annotations().filter_set(&dataset))
            }
            Some(&Constraint::DataKey {
                set,
                key,
                qualifier: SelectionQualifier::Normal,
            }) => Box::new(store.key(set, key).or_fail()?.annotations()),
            Some(&Constraint::DataVariable(var, SelectionQualifier::Normal)) => {
                let data = self.resolve_datavar(var)?;
                Box::new(data.annotations())
            }
            Some(&Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: _,
            }) => Box::new(store.find_data(set, key, operator.clone()).annotations()),
            Some(&Constraint::Value(ref operator, _)) => Box::new(
                store
                    .find_data(false, false, operator.clone())
                    .annotations(),
            ),
            Some(&Constraint::Text(text, TextMode::Exact)) => {
                Box::new(store.find_text(text).annotations())
            }
            Some(&Constraint::Text(text, TextMode::CaseInsensitive)) => {
                Box::new(store.find_text_nocase(text).annotations())
            }
            Some(Constraint::Regex(_regex)) => {
                todo!("regex constraint not implemented yet"); //TODO
                                                               //Box::new(store.find_text_regex(&regex).annotations())
            }
            Some(&Constraint::TextVariable(var)) => {
                if let Ok(tsel) = self.resolve_textvar(var) {
                    Box::new(tsel.annotations())
                } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                    Box::new(annotation.textselections().annotations())
                } else {
                    return Err(StamError::QuerySyntaxError(
                        format!("Variable ?{} of type TEXT or ANNOTATION not found", var),
                        "",
                    ));
                }
            }
            Some(&Constraint::TextSelections(ref handles, SelectionQualifier::Normal)) => Box::new(
                ResultTextSelections::new(FromHandles::new(handles.clone().into_iter(), store))
                    .annotations(),
            ),
            Some(&Constraint::KeyVariable(varname, SelectionQualifier::Normal)) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(key.data().filter_key(key).annotations())
            }
            Some(&Constraint::KeyValueVariable(
                varname,
                ref operator,
                SelectionQualifier::Normal,
            )) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(key.data().filter_value(operator.clone()).annotations())
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
                        format!("Variable ?{} of type TEXT or ANNOTATION not found", var),
                        "",
                    ));
                }
            }
            Some(&Constraint::Union(ref subconstraints)) => {
                let mut handles: Handles<'store, Annotation> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_annotations(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(handles.into_items())
            }
            Some(&Constraint::Limit { begin, end }) => {
                Box::new(store.annotations().limit(begin, end))
            }
            Some(c) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                    "Constraint {} (primary) is not implemented for queries over annotations: {:?}",
                    c.keyword(),
                    c
                ),
                    "",
                ))
            }
            None => Box::new(store.annotations()),
        })
    }

    /// Apply secondary constraint for ANNOTATION result type
    pub(crate) fn update_state_annotations(
        &self,
        constraint: &Constraint<'store>,
        iter: Box<dyn Iterator<Item = ResultItem<'store, Annotation>> + 'store>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, Annotation>> + 'store>, StamError> {
        let store = self.store();
        Ok(match constraint {
            &Constraint::TextResource(res, SelectionQualifier::Normal,None) => {
                Box::new(iter.filter_resource(&store.resource(res).or_fail()?))
            }
            &Constraint::TextResource(res, SelectionQualifier::Metadata,None) => {
                Box::new(iter.filter_resource_as_metadata(&store.resource(res).or_fail()?))
            }
            &Constraint::ResourceVariable(varname, SelectionQualifier::Normal,None) => {
                let resource = self.resolve_resourcevar(varname)?;
                Box::new(iter.filter_resource(resource))
            }
            &Constraint::ResourceVariable(varname, SelectionQualifier::Metadata,None) => {
                let resource = self.resolve_resourcevar(varname)?;
                Box::new(iter.filter_resource_as_metadata(resource))
            }
            &Constraint::AnnotationVariable(
                var,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
                None
            ) => {
                let annotation = self.resolve_annotationvar(var)?;
                Box::new(iter.filter_annotation(annotation))
            }
            &Constraint::AnnotationVariable(var, _, AnnotationDepth::Zero,None) => {
                let annotation = self.resolve_annotationvar(var)?;
                Box::new(iter.filter_one(annotation))
            }
            &Constraint::AnnotationVariable(var, SelectionQualifier::Metadata, depth,None) => {
                let annotation = self.resolve_annotationvar(var)?;
                Box::new(iter.filter_annotation_in_targets(annotation, depth))
            }
            &Constraint::DataVariable(varname, SelectionQualifier::Normal) => {
                let data = self.resolve_datavar(varname)?;
                Box::new(iter.filter_annotationdata(data))
            }
            &Constraint::DataKey {
                set,
                key,
                qualifier: SelectionQualifier::Normal,
            } => {
                let key = store.key(set, key).or_fail()?;
                Box::new(iter.filter_key(&key))
            }
            &Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: SelectionQualifier::Normal,
            } => {
                let key = store.key(set, key).or_fail()?;
                Box::new(iter.filter_key_value(&key, operator.clone()))
            }
            &Constraint::Value(ref operator, SelectionQualifier::Normal) => {
                Box::new(iter.filter_value(operator.clone()))
            }
            &Constraint::KeyVariable(varname, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key(&key))
            }
            &Constraint::KeyValueVariable(varname, ref operator, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key_value(&key, operator.clone()))
            }
            &Constraint::DataSet(set, SelectionQualifier::Normal) => {
                let dataset = store.dataset(set).or_fail()?;
                Box::new(iter.filter_set(&dataset))
            }
            &Constraint::Text(text, TextMode::Exact) => {
                Box::new(iter.filter_text_byref(text, true, " "))
            }
            &Constraint::Text(text, TextMode::CaseInsensitive) => {
                Box::new(iter.filter_text_byref(text, false, " "))
            }
            Constraint::Regex(regex) => Box::new(iter.filter_text_regex(regex.clone(), " ")),
            &Constraint::TextVariable(var) => {
                if let Ok(tsel) = self.resolve_textvar(var) {
                    Box::new(iter.filter_any(tsel.annotations().to_handles(store)))
                } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                    Box::new(
                        iter.filter_any(
                            annotation.textselections().annotations().to_handles(store),
                        ),
                    )
                } else {
                    return Err(StamError::QuerySyntaxError(
                        format!("Variable ?{} of type TEXT or ANNOTATION not found", var),
                        "",
                    ));
                }
            }
            &Constraint::TextRelation { var, operator } => {
                if let Ok(tsel) = self.resolve_textvar(var) {
                    Box::new(
                        iter.filter_any(
                            tsel.related_text(operator).annotations().to_handles(store),
                        ),
                    )
                } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                    Box::new(
                        iter.filter_any(
                            annotation
                                .textselections()
                                .related_text(operator)
                                .annotations()
                                .to_handles(store),
                        ),
                    )
                } else {
                    return Err(StamError::QuerySyntaxError(
                        format!("Variable ?{} of type TEXT or ANNOTATION not found", var),
                        "",
                    ));
                }
            }
            &Constraint::Annotation(
                annotation,
                SelectionQualifier::Normal,
                AnnotationDepth::One,
                None
            ) => Box::new(iter.filter_annotation(&store.annotation(annotation).or_fail()?)),
            &Constraint::Annotation(annotation, _, AnnotationDepth::Zero,None) => {
                Box::new(iter.filter_one(&store.annotation(annotation).or_fail()?))
            }
            &Constraint::Annotation(annotation, SelectionQualifier::Metadata, depth, None) => Box::new(
                iter.filter_annotation_in_targets(&store.annotation(annotation).or_fail()?, depth),
            ),
            &Constraint::Union(ref subconstraints) => {
                let mut handles: Handles<'store, Annotation> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_annotations(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(iter.filter_any(handles))
            }
            &Constraint::Limit { begin, end } => {
                Box::new(iter.limit(begin, end))
            }
            c => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (secondary) is not implemented for queries over annotations: {:?}",
                        c.keyword(),
                        c
                    ),
                    "",
                ))
            }
        })
    }

    /// Apply primary constraint for DATA result type
    pub(crate) fn init_state_data(
        &self,
        constraint: Option<&Constraint<'store>>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>, StamError>
    {
        let store = self.store();
        Ok(match constraint {
            Some(&Constraint::DataVariable(varname, SelectionQualifier::Normal)) => {
                let data = self.resolve_datavar(varname)?;
                Box::new(Some(data.clone()).into_iter())
            }
            Some(&Constraint::Data(ref handles, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store))
            }
            Some(&Constraint::Annotations(ref handles, SelectionQualifier::Normal, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store).data())
            }
            Some(&Constraint::Annotations(ref handles, SelectionQualifier::Metadata, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store).data_as_metadata())
            }
            Some(&Constraint::Annotation(id, SelectionQualifier::Normal, _, None)) => {
                Box::new(store.annotation(id).or_fail()?.data())
            }
            Some(&Constraint::Annotation(id, SelectionQualifier::Metadata, _, None)) => {
                Box::new(store.annotation(id).or_fail()?.data_as_metadata())
            }
            Some(&Constraint::AnnotationVariable(varname, SelectionQualifier::Normal, _, None)) => {
                let annotation = self.resolve_annotationvar(varname)?;
                Box::new(annotation.data())
            }
            Some(&Constraint::AnnotationVariable(
                varname,
                SelectionQualifier::Metadata,
                _,
                None,
            )) => {
                let annotation = self.resolve_annotationvar(varname)?;
                Box::new(annotation.data_as_metadata())
            }
            Some(&Constraint::TextVariable(varname)) => {
                let textselection = self.resolve_textvar(varname)?;
                Box::new(textselection.annotations().data())
            }
            Some(&Constraint::TextSelections(ref handles, SelectionQualifier::Normal)) => Box::new(
                ResultTextSelections::new(FromHandles::new(handles.clone().into_iter(), store))
                    .annotations()
                    .data(),
            ),
            Some(&Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: SelectionQualifier::Normal,
            }) => store.find_data(set, key, operator.clone()),
            Some(&Constraint::Value(ref operator, SelectionQualifier::Normal)) => {
                store.find_data(false, false, operator.clone())
            }
            Some(&Constraint::KeyValueVariable(
                varname,
                ref operator,
                SelectionQualifier::Normal,
            )) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(key.data().filter_value(operator.clone()))
            }
            Some(&Constraint::KeyVariable(varname, SelectionQualifier::Normal)) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(key.data())
            }
            Some(&Constraint::DataKey {
                set,
                key,
                qualifier: SelectionQualifier::Normal,
            }) => Box::new(store.key(set, key).or_fail()?.data()),
            Some(&Constraint::DataSet(id, SelectionQualifier::Normal)) => {
                Box::new(store.dataset(id).or_fail()?.data())
            }
            Some(&Constraint::DataSetVariable(varname, SelectionQualifier::Normal)) => {
                let dataset = self.resolve_datasetvar(varname)?;
                Box::new(dataset.data())
            }
            Some(&Constraint::Union(ref subconstraints)) => {
                let mut handles: Handles<'store, AnnotationData> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_data(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(handles.into_items())
            }
            Some(&Constraint::Limit { begin, end }) => Box::new(store.data().limit(begin, end)),
            Some(c) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (primary) is not valid for DATA return type",
                        c.keyword()
                    ),
                    "",
                ))
            }
            None => Box::new(store.data()),
        })
    }

    /// Apply secondary constraint for DATA result type
    pub(crate) fn update_state_data(
        &self,
        constraint: &Constraint<'store>,
        iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>, StamError>
    {
        let store = self.store();
        Ok(match constraint {
            &Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: SelectionQualifier::Normal,
            } => Box::new(
                iter.filter_any(
                    store
                        .find_data(set, key, operator.clone())
                        .to_handles(store),
                ),
            ),
            &Constraint::Value(ref operator, SelectionQualifier::Normal) => {
                Box::new(iter.filter_value(operator.clone()))
            }
            &Constraint::KeyVariable(varname, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key(&key))
            }
            &Constraint::KeyValueVariable(varname, ref operator, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key(&key).filter_value(operator.clone()))
            }
            &Constraint::AnnotationVariable(varname, SelectionQualifier::Normal, _, None) => {
                let annotation = self.resolve_annotationvar(varname)?;
                Box::new(iter.filter_annotation(annotation))
            }
            &Constraint::Union(ref subconstraints) => {
                let mut handles: Handles<'store, AnnotationData> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_data(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(iter.filter_any(handles))
            }
            &Constraint::Limit { begin, end } => Box::new(iter.limit(begin, end)),
            c => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (secondary) is not implemented for queries over DATA",
                        c.keyword()
                    ),
                    "",
                ))
            }
        })
    }

    /// Apply primary constraint for KEY result type
    pub(crate) fn init_state_keys(
        &self,
        constraint: Option<&Constraint<'store>>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, DataKey>> + 'store>, StamError> {
        let store = self.store();
        Ok(match constraint {
            Some(&Constraint::KeyVariable(varname, SelectionQualifier::Normal)) => {
                let data = self.resolve_keyvar(varname)?;
                Box::new(Some(data.clone()).into_iter())
            }
            Some(&Constraint::DataVariable(varname, SelectionQualifier::Normal)) => {
                let data = self.resolve_datavar(varname)?;
                Box::new(Some(data.key().clone()).into_iter())
            }
            Some(&Constraint::DataSetVariable(varname, SelectionQualifier::Normal)) => {
                let dataset = self.resolve_datasetvar(varname)?;
                Box::new(dataset.keys())
            }
            Some(&Constraint::DataSet(id, SelectionQualifier::Normal)) => {
                Box::new(store.dataset(id).or_fail()?.keys())
            }
            Some(&Constraint::Keys(ref handles, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store))
            }
            Some(&Constraint::Annotations(ref handles, SelectionQualifier::Normal, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store).keys())
            }
            Some(&Constraint::Annotations(ref handles, SelectionQualifier::Metadata, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store).keys_as_metadata())
            }
            Some(&Constraint::Annotation(id, SelectionQualifier::Normal, _, None)) => {
                Box::new(store.annotation(id).or_fail()?.keys())
            }
            Some(&Constraint::Annotation(id, SelectionQualifier::Metadata, _, None)) => {
                Box::new(store.annotation(id).or_fail()?.keys_as_metadata())
            }
            Some(&Constraint::AnnotationVariable(varname, SelectionQualifier::Normal, _, None)) => {
                let annotation = self.resolve_annotationvar(varname)?;
                Box::new(annotation.keys())
            }
            Some(&Constraint::AnnotationVariable(
                varname,
                SelectionQualifier::Metadata,
                _,
                None,
            )) => {
                let annotation = self.resolve_annotationvar(varname)?;
                Box::new(annotation.keys_as_metadata())
            }
            Some(&Constraint::Union(ref subconstraints)) => {
                let mut handles: Handles<'store, DataKey> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_keys(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(handles.into_items())
            }
            Some(&Constraint::Limit { begin, end }) => Box::new(store.keys().limit(begin, end)),
            Some(c) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (primary) is not valid for KEY return type",
                        c.keyword()
                    ),
                    "",
                ))
            }
            None => Box::new(store.keys()),
        })
    }

    /// Apply secondary constraint for KEY result type
    pub(crate) fn update_state_keys(
        &self,
        constraint: &Constraint<'store>,
        iter: Box<dyn Iterator<Item = ResultItem<'store, DataKey>> + 'store>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, DataKey>> + 'store>, StamError> {
        let store = self.store();
        Ok(match constraint {
            //TODO: implement constraints
            &Constraint::Union(ref subconstraints) => {
                let mut handles: Handles<'store, DataKey> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_keys(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(iter.filter_any(handles))
            }
            &Constraint::Limit { begin, end } => Box::new(iter.limit(begin, end)),
            c => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (secondary) is not implemented for queries over KEY",
                        c.keyword()
                    ),
                    "",
                ))
            }
        })
    }

    /// Apply primary constraint for DATASET result type
    pub(crate) fn init_state_datasets(
        &self,
        constraint: Option<&Constraint<'store>>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store>, StamError>
    {
        let store = self.store();
        Ok(match constraint {
            Some(&Constraint::Id(id)) | Some(&Constraint::DataSet(id, _)) => {
                Box::new(Some(store.dataset(id).or_fail()?).into_iter())
            }
            Some(&Constraint::DataSetVariable(varname, SelectionQualifier::Normal)) => {
                let dataset = self.resolve_datasetvar(varname)?;
                Box::new(Some(dataset.clone()).into_iter())
            }
            Some(&Constraint::KeyVariable(varname, SelectionQualifier::Normal)) => {
                let data = self.resolve_keyvar(varname)?;
                Box::new(Some(data.set().clone()).into_iter())
            }
            Some(&Constraint::DataVariable(varname, SelectionQualifier::Normal)) => {
                let data = self.resolve_datavar(varname)?;
                Box::new(Some(data.set().clone()).into_iter())
            }
            Some(&Constraint::Union(ref subconstraints)) => {
                let mut handles: Handles<'store, AnnotationDataSet> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_datasets(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(handles.into_items())
            }
            Some(&Constraint::Limit { begin, end }) => Box::new(store.datasets().limit(begin, end)),
            Some(c) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (primary) is not valid for DATASET return type",
                        c.keyword()
                    ),
                    "",
                ))
            }
            None => Box::new(store.datasets()),
        })
    }

    /// Apply secondary constraint for DATASET result type
    pub(crate) fn update_state_datasets(
        &self,
        constraint: &Constraint<'store>,
        iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store>, StamError>
    {
        let store = self.store();
        Ok(match constraint {
            &Constraint::Union(ref subconstraints) => {
                let mut handles: Handles<'store, AnnotationDataSet> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_datasets(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(iter.filter_any(handles))
            }
            &Constraint::Limit { begin, end } => Box::new(iter.limit(begin, end)),
            c => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (secondary) is not implemented for queries over DATASET",
                        c.keyword()
                    ),
                    "",
                ))
            }
        })
    }

    /// Apply primary constraint for TEXT result type
    pub(crate) fn init_state_textselections(
        &self,
        constraint: Option<&Constraint<'store>>,
    ) -> Result<Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>, StamError> {
        let store = self.store();
        Ok(match constraint {
            Some(&Constraint::TextSelections(ref handles, _)) => Box::new(
                ResultTextSelections::new(FromHandles::new(handles.clone().into_iter(), store)),
            ),
            Some(&Constraint::TextVariable(varname)) => {
                let textselection = self.resolve_textvar(varname)?;
                Box::new(Some(textselection.clone()).into_iter())
            }
            Some(&Constraint::Annotations(ref handles, _, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store).textselections())
            }
            Some(&Constraint::TextResource(res, _, None)) => {
                Box::new(store.resource(res).or_fail()?.textselections())
            }
            Some(&Constraint::TextResource(res, _, Some(ref offset))) => {
                if let Ok(textselection) = store.resource(res).or_fail()?.textselection(&offset) {
                    Box::new(Some(textselection.clone()).into_iter())
                } else {
                    Box::new(None.into_iter())
                }
            }
            Some(&Constraint::ResourceVariable(varname, _, None)) => {
                let resource = self.resolve_resourcevar(varname)?;
                Box::new(resource.textselections())
            }
            Some(&Constraint::ResourceVariable(varname, _, Some(ref offset))) => {
                let resource = self.resolve_resourcevar(varname)?;
                if let Ok(textselection) = resource.textselection(offset) {
                    Box::new(Some(textselection.clone()).into_iter())
                } else {
                    Box::new(None.into_iter())
                }
            }
            Some(&Constraint::Annotation(id, _, _, None)) => {
                Box::new(store.annotation(id).or_fail()?.textselections())
            }
            Some(&Constraint::Annotation(id, _, _, Some(ref offset))) => {
                if let Some(textselection) = store.annotation(id).or_fail()?.textselections().next()
                {
                    if let Ok(textselection) = textselection.textselection(offset) {
                        Box::new(Some(textselection.clone()).into_iter())
                    } else {
                        Box::new(None.into_iter())
                    }
                } else {
                    Box::new(None.into_iter())
                }
            }
            Some(&Constraint::AnnotationVariable(varname, _, _, None)) => {
                let annotation = self.resolve_annotationvar(varname)?;
                Box::new(annotation.textselections())
            }
            Some(&Constraint::AnnotationVariable(varname, _, _, Some(ref offset))) => {
                let annotation = self.resolve_annotationvar(varname)?;
                if let Some(textselection) = annotation.textselections().next() {
                    if let Ok(textselection) = textselection.textselection(offset) {
                        Box::new(Some(textselection.clone()).into_iter())
                    } else {
                        Box::new(None.into_iter())
                    }
                } else {
                    Box::new(None.into_iter())
                }
            }
            Some(&Constraint::DataKey {
                set,
                key,
                qualifier: _,
            }) => Box::new(
                store
                    .find_data(set, key, DataOperator::Any)
                    .annotations()
                    .textselections(),
            ),
            Some(&Constraint::DataVariable(varname, _)) => {
                let data = self.resolve_datavar(varname)?;
                Box::new(data.annotations().textselections())
            }
            Some(&Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: _,
            }) => Box::new(
                store
                    .find_data(set, key, operator.clone())
                    .annotations()
                    .textselections(),
            ),
            Some(&Constraint::KeyVariable(varname, SelectionQualifier::Normal)) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(key.data().filter_key(key).annotations().textselections())
            }
            Some(&Constraint::KeyValueVariable(
                varname,
                ref operator,
                SelectionQualifier::Normal,
            )) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(
                    key.data()
                        .filter_value(operator.clone())
                        .annotations()
                        .textselections(),
                )
            }
            Some(&Constraint::Value(ref operator, _)) => Box::new(
                store
                    .find_data(false, false, operator.clone())
                    .annotations()
                    .textselections(),
            ),
            Some(&Constraint::Text(text, TextMode::Exact)) => Box::new(store.find_text(text)),
            Some(&Constraint::Text(text, TextMode::CaseInsensitive)) => {
                Box::new(store.find_text_nocase(&text.to_lowercase()))
            }
            Some(&Constraint::TextRelation { var, operator }) => {
                if let Ok(tsel) = self.resolve_textvar(var) {
                    Box::new(tsel.related_text(operator))
                } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                    Box::new(annotation.textselections().related_text(operator))
                } else {
                    return Err(StamError::QuerySyntaxError(
                        format!("Variable ?{} of type TEXT or ANNOTATION not found", var),
                        "",
                    ));
                }
            }
            Some(&Constraint::Union(..)) => todo!("UNION not implemented yet"),
            Some(&Constraint::Limit { begin, end }) => {
                Box::new(store.annotations().textselections().limit(begin, end))
            }
            Some(c) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                    "Constraint {} (primary) is not implemented for queries over TEXT selections",
                    c.keyword()
                ),
                    "",
                ))
            }
            None => Box::new(store.annotations().textselections()),
        })
    }

    /// Apply secondary constraint for TEXT result type
    pub(crate) fn update_state_textselections(
        &self,
        constraint: &Constraint<'store>,
        iter: Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>,
    ) -> Result<Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>, StamError> {
        let store = self.store();
        Ok(match constraint {
            &Constraint::TextResource(res, _, None) => {
                Box::new(iter.filter_resource(&store.resource(res).or_fail()?))
            }
            &Constraint::ResourceVariable(varname, _, None) => {
                let resource = self.resolve_resourcevar(varname)?;
                Box::new(iter.filter_resource(&resource))
            }
            &Constraint::DataKey {
                set,
                key,
                qualifier: _,
            } => {
                let key = store.key(set, key).or_fail()?;
                Box::new(iter.filter_key(&key))
            }
            &Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: _,
            } => {
                let key = store.key(set, key).or_fail()?;
                Box::new(iter.filter_key_value(&key, operator.clone()))
            }
            &Constraint::KeyVariable(varname, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key(&key))
            }
            &Constraint::KeyValueVariable(varname, ref operator, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key_value(&key, operator.clone()))
            }
            &Constraint::Value(ref operator, _) => Box::new(iter.filter_value(operator.clone())),
            &Constraint::Text(text, TextMode::Exact) => {
                Box::new(iter.filter_text_byref(text, true))
            }
            &Constraint::Text(text, TextMode::CaseInsensitive) => {
                Box::new(iter.filter_text_byref(text, false))
            }
            Constraint::Regex(regex) => Box::new(iter.filter_text_regex(regex.clone())),
            &Constraint::TextRelation { var, operator } => {
                if let Ok(tsel) = self.resolve_textvar(var) {
                    Box::new(
                        iter.filter_any(
                            tsel.related_text(operator)
                                .filter_map(|x| x.as_resultitem().map(|x| x.clone()))
                                .to_handles(store),
                        ),
                    )
                } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                    Box::new(
                        iter.filter_any(
                            annotation
                                .textselections()
                                .related_text(operator)
                                .filter_map(|x| x.as_resultitem().map(|x| x.clone()))
                                .to_handles(store),
                        ),
                    )
                } else {
                    return Err(StamError::QuerySyntaxError(
                        format!("Variable ?{} of type TEXT or ANNOTATION not found", var),
                        "",
                    ));
                }
            }
            &Constraint::Limit { begin, end } => Box::new(iter.limit(begin, end)),
            c => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                    "Constraint {} (secondary) is not implemented for queries over TEXT selections",
                    c.keyword()
                ),
                    "",
                ))
            }
        })
    }

    /// Apply primary constraint for RESOURCE result type
    pub(crate) fn init_state_resources(
        &self,
        constraint: Option<&Constraint<'store>>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, TextResource>> + 'store>, StamError>
    {
        let store = self.store();
        Ok(match constraint {
            Some(&Constraint::Id(id)) | Some(&Constraint::TextResource(id, _, None)) => {
                Box::new(Some(store.resource(id).or_fail()?).into_iter())
            }
            Some(&Constraint::Resources(ref handles, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store))
            }
            Some(&Constraint::Annotations(ref handles, SelectionQualifier::Normal, _)) => {
                Box::new(FromHandles::new(handles.clone().into_iter(), store).resources())
            }
            Some(&Constraint::Annotations(ref handles, SelectionQualifier::Metadata, _)) => {
                Box::new(
                    FromHandles::new(handles.clone().into_iter(), store).resources_as_metadata(),
                )
            }
            Some(&Constraint::DataKey {
                set,
                key,
                qualifier: SelectionQualifier::Normal,
            }) => Box::new(store.key(set, key).or_fail()?.annotations().resources()),
            //KEY AS METADATA
            Some(&Constraint::DataKey {
                set,
                key,
                qualifier: SelectionQualifier::Metadata,
            }) => Box::new(
                store
                    .key(set, key)
                    .or_fail()?
                    .annotations()
                    .resources_as_metadata(),
            ),
            Some(&Constraint::DataVariable(var, SelectionQualifier::Normal)) => {
                let data = self.resolve_datavar(var)?;
                Box::new(data.annotations().resources())
            }
            //DATA AS METADATA
            Some(&Constraint::DataVariable(var, SelectionQualifier::Metadata)) => {
                let data = self.resolve_datavar(var)?;
                Box::new(data.annotations().resources_as_metadata())
            }
            Some(&Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: SelectionQualifier::Normal,
            }) => Box::new(
                store
                    .find_data(set, key, operator.clone())
                    .annotations()
                    .resources(),
            ),
            Some(&Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: SelectionQualifier::Metadata,
            }) => Box::new(
                store
                    .find_data(set, key, operator.clone())
                    .annotations()
                    .resources_as_metadata(),
            ),
            Some(&Constraint::KeyVariable(varname, SelectionQualifier::Normal)) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(key.data().annotations().resources())
            }
            Some(&Constraint::KeyVariable(varname, SelectionQualifier::Metadata)) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(key.data().annotations().resources_as_metadata())
            }
            Some(&Constraint::KeyValueVariable(
                varname,
                ref operator,
                SelectionQualifier::Normal,
            )) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(
                    key.data()
                        .filter_value(operator.clone())
                        .annotations()
                        .resources(),
                )
            }
            Some(&Constraint::KeyValueVariable(
                varname,
                ref operator,
                SelectionQualifier::Metadata,
            )) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(
                    key.data()
                        .filter_value(operator.clone())
                        .annotations()
                        .resources_as_metadata(),
                )
            }
            Some(&Constraint::Union(ref subconstraints)) => {
                let mut handles: Handles<'store, TextResource> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_resources(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(handles.into_items())
            }
            Some(&Constraint::Limit { begin, end }) => {
                Box::new(store.resources().limit(begin, end))
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
        })
    }

    /// Apply secondary constraint for RESOURCE result type
    pub(crate) fn update_state_resources(
        &self,
        constraint: &Constraint<'store>,
        iter: Box<dyn Iterator<Item = ResultItem<'store, TextResource>> + 'store>,
    ) -> Result<Box<dyn Iterator<Item = ResultItem<'store, TextResource>> + 'store>, StamError>
    {
        let store = self.store();
        Ok(match constraint {
            &Constraint::DataKey {
                set,
                key,
                qualifier: SelectionQualifier::Metadata,
            } => {
                let key = store.key(set, key).or_fail()?;
                Box::new(iter.filter_key_in_metadata(&key))
            }
            &Constraint::DataKey {
                set,
                key,
                qualifier: SelectionQualifier::Normal,
            } => {
                let key = store.key(set, key).or_fail()?;
                Box::new(iter.filter_key_on_text(&key))
            }
            &Constraint::KeyVariable(var, SelectionQualifier::Metadata) => {
                let key = self.resolve_keyvar(var)?;
                Box::new(iter.filter_key_in_metadata(&key))
            }
            &Constraint::KeyVariable(var, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(var)?;
                Box::new(iter.filter_key_on_text(&key))
            }
            &Constraint::DataVariable(var, SelectionQualifier::Metadata) => {
                let data = self.resolve_datavar(var)?;
                Box::new(iter.filter_annotationdata_in_metadata(&data))
            }
            &Constraint::DataVariable(var, SelectionQualifier::Normal) => {
                let data = self.resolve_datavar(var)?;
                Box::new(iter.filter_annotationdata_on_text(&data))
            }
            &Constraint::KeyValue {
                set,
                key,
                ref operator,
                qualifier: SelectionQualifier::Metadata,
            } => {
                let key = store.key(set, key).or_fail()?;
                Box::new(iter.filter_key_value_in_metadata(&key, operator.clone()))
            }
            &Constraint::KeyValueVariable(varname, ref operator, SelectionQualifier::Normal) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key_value_on_text(&key, operator.clone()))
            }
            &Constraint::KeyValueVariable(varname, ref operator, SelectionQualifier::Metadata) => {
                let key = self.resolve_keyvar(varname)?;
                Box::new(iter.filter_key_value_in_metadata(&key, operator.clone()))
            }
            &Constraint::Union(ref subconstraints) => {
                let mut handles: Handles<'store, TextResource> = Handles::new_empty(store);
                for subconstraint in subconstraints {
                    let mut iter = self.init_state_resources(Some(subconstraint))?;
                    handles.union(&iter.to_handles(store));
                }
                Box::new(iter.filter_any(handles))
            }
            &Constraint::Limit { begin, end } => Box::new(iter.limit(begin, end)),
            c => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Constraint {} (secondary) is not implemented for queries over resources",
                        c.keyword()
                    ),
                    "",
                ))
            }
        })
    }

    pub(crate) fn build_result(&self) -> QueryResultItems<'store> {
        let mut items = SmallVec::new();
        for stackitem in self.statestack.iter() {
            items.push(stackitem.result.clone());
        }
        let names = if let Some(query) = self.query.as_ref() {
            query
                .querypath_to_names(&self.querypath)
                .expect("name extraction from querypath must succeed")
        } else {
            SmallVec::new()
        };
        QueryResultItems {
            names,
            querypath: self.querypath.clone(),
            items,
        }
    }

    fn resolve_datavar(
        &self,
        name: &str,
    ) -> Result<&ResultItem<'store, AnnotationData>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self
                .get_query(&self.querypath[..i])
                .expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::AnnotationData(data) = &state.result {
                    return Ok(data);
                }
            }
        }
        match self.contextvars.get(name) {
            Some(QueryResultItem::AnnotationData(data)) => return Ok(data),
            Some(_) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Variable ?{} was found in context but does not have expected type DATA",
                        name
                    ),
                    "",
                ))
            }
            None => {}
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type DATA not found", name),
            "",
        ));
    }

    fn resolve_keyvar(&self, name: &str) -> Result<&ResultItem<'store, DataKey>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self
                .get_query(&self.querypath[..i])
                .expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::DataKey(key) = &state.result {
                    return Ok(key);
                }
            }
        }
        match self.contextvars.get(name) {
            Some(QueryResultItem::DataKey(key)) => return Ok(key),
            Some(_) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Variable ?{} was found in context but does not have expected type KEY",
                        name
                    ),
                    "",
                ))
            }
            None => {}
        }
        return Err(StamError::QuerySyntaxError(
            format!(
                "Variable ?{} of type KEY not found - QUERY DEBUG: {:#?}",
                name, self.query
            ),
            "",
        ));
    }

    fn resolve_datasetvar(
        &self,
        name: &str,
    ) -> Result<&ResultItem<'store, AnnotationDataSet>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self
                .get_query(&self.querypath[..i])
                .expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::AnnotationDataSet(dataset) = &state.result {
                    return Ok(dataset);
                }
            }
        }
        match self.contextvars.get(name) {
            Some(QueryResultItem::AnnotationDataSet(dataset)) => return Ok(dataset),
            Some(_) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Variable ?{} was found in context but does not have expected type DATASET",
                        name
                    ),
                    "",
                ))
            }
            None => {}
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type DATASET not found", name),
            "",
        ));
    }

    fn resolve_annotationvar(
        &self,
        name: &str,
    ) -> Result<&ResultItem<'store, Annotation>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self
                .get_query(&self.querypath[..i])
                .expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::Annotation(annotation) = &state.result {
                    return Ok(annotation);
                }
            }
        }
        match self.contextvars.get(name) {
            Some(QueryResultItem::Annotation(annotation)) => return Ok(annotation),
            Some(_) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                    "Variable ?{} was found in context but does not have expected type ANNOTATION",
                    name
                ),
                    "",
                ))
            }
            None => {}
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type ANNOTATION not found", name),
            "",
        ));
    }

    fn resolve_textvar(&self, name: &str) -> Result<&ResultTextSelection<'store>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self
                .get_query(&self.querypath[..i])
                .expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::TextSelection(textsel) = &state.result {
                    return Ok(textsel);
                }
            }
        }
        match self.contextvars.get(name) {
            Some(QueryResultItem::TextSelection(textselection)) => return Ok(textselection),
            Some(_) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Variable ?{} was found in context but does not have expected type TEXT",
                        name
                    ),
                    "",
                ))
            }
            None => {}
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
            let query = self
                .get_query(&self.querypath[..i])
                .expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::TextResource(resource) = &state.result {
                    return Ok(resource);
                }
            }
        }
        match self.contextvars.get(name) {
            Some(QueryResultItem::TextResource(resource)) => return Ok(resource),
            Some(_) => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                    "Variable ?{} was found in context but does not have expected type RESOURCE",
                    name
                ),
                    "",
                ))
            }
            None => {}
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
        if self.done || self.query.is_none() {
            //iterator has been marked as done, do nothing else
            return None;
        }

        //populate the entire stack, producing a result at each level
        //while self.statestack.len() < self.query.len() {
        let mut statestack_full = false;
        while !statestack_full {
            let has_subqueries = self
                .get_query(&self.querypath)
                .expect("query must exist")
                .has_subqueries();
            match self.init_state() {
                Ok(false) => {
                    //if we didn't succeed in preparing the next iteration, it means the entire stack is depleted and we're done
                    return None;
                }
                Err(e) => {
                    eprintln!("STAM Query error: {}", e);
                    return None;
                }
                Ok(true) => {
                    statestack_full = !has_subqueries;
                    continue;
                }
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

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns a result by index number
    pub fn get(&self, index: usize) -> Option<&QueryResultItem<'store>> {
        self.items.get(index)
    }

    pub fn pop_last(&mut self) -> Option<QueryResultItem<'store>> {
        self.items.pop()
    }

    /// Returns a result by variable name (if specified), or get the first result.
    /// Raises an error if there are no results.
    pub fn get_by_name_or_first(
        &self,
        var: Option<&str>,
    ) -> Result<&QueryResultItem<'store>, StamError> {
        if let Some(var) = var {
            self.get_by_name(var)
        } else {
            self.iter().next().ok_or(StamError::QuerySyntaxError(
                "Query returned no results".to_string(),
                "(get_by_name_or_first)",
            ))
        }
    }

    /// Returns a result by variable name (if specified), or get the last result.
    /// Raises an error if there are no results.
    pub fn get_by_name_or_last(
        &self,
        var: Option<&str>,
    ) -> Result<&QueryResultItem<'store>, StamError> {
        if let Some(var) = var {
            self.get_by_name(var)
        } else {
            self.iter().last().ok_or(StamError::QuerySyntaxError(
                "Query returned no results".to_string(),
                "(get_by_name_or_last)",
            ))
        }
    }

    /// Returns a result by variable name
    pub fn get_by_name(&self, var: &str) -> Result<&QueryResultItem<'store>, StamError> {
        for (name, item) in self.names.iter().zip(self.items.iter()) {
            if name == &Some(var) {
                return Ok(item);
            }
        }
        Err(StamError::QuerySyntaxError(
            format!("Variable ?{} not found in the result set", var),
            "",
        ))
    }

    /// Iterator over all the variable names in this result row (without leading '?')
    pub fn names<'a>(&'a self) -> impl Iterator<Item = Option<&'store str>> + 'a {
        self.names.iter().copied()
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
    UnquotedList,
    List,
    Null,
    Bool,
    Datetime,
    Any,
}

fn get_arg_type(s: &str, quoted: bool) -> ArgType {
    if s.is_empty() {
        return ArgType::String;
    }
    let mut numeric = !quoted;
    let mut foundperiod = false;
    let mut prevc = None;
    for c in s.chars() {
        if c == '|' && prevc != Some('\\') {
            if !quoted {
                return ArgType::UnquotedList;
            } else {
                return ArgType::List;
            }
        }
        if !c.is_ascii_digit() {
            if c != '-' || prevc != None {
                numeric = false;
            }
        }
        if numeric && c == '.' {
            if foundperiod {
                numeric = false;
            }
            foundperiod = true
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
        match s {
            "null" => ArgType::Null,
            "any" => ArgType::Any,
            "true" | "false" => ArgType::Bool,
            s => {
                if DateTime::parse_from_rfc3339(&s).is_ok() {
                    ArgType::Datetime
                } else {
                    ArgType::String
                }
            }
        }
    }
}

// returns (argument,remainder,type)
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
                let s = &querystring[begin..i];
                return Ok((s, &querystring[i + 1..].trim_start(), get_arg_type(s, true)));
            }
        }
        if !quote && querystring[i..].starts_with(" OR ") {
            let arg = &querystring[0..i];
            return Ok((
                arg,
                &querystring[i + 1..].trim_start(),
                get_arg_type(arg, false),
            ));
        } else if !quote && (c == ';' || c == ' ' || c == ']' || c == '\n' || c == '\t') {
            let arg = &querystring[0..i];
            return Ok((
                arg,
                &querystring[i..].trim_start(),
                get_arg_type(arg, false),
            ));
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

fn parse_qualifiers<'a>(
    arg: &'a str,
    querystring: &'a str,
) -> Result<(&'a str, &'a str, SelectionQualifier, AnnotationDepth), StamError> {
    if arg == "AS" {
        let (as_arg, remainder, _) = get_arg(querystring)?;
        let qualifier = match as_arg {
            "TARGET" | "METADATA" => SelectionQualifier::Metadata,
            _ => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Expected keyword TARGET or METADATA (same meaning) after RESOURCE AS, got '{}'",
                        remainder.split(QUERYSPLITCHARS).next().unwrap_or("(empty string)")
                    ),
                    "",
                ))
            }
        };
        let (newarg, remainder, _) = get_arg(querystring)?;
        if newarg == "RECURSIVE" {
            let (newarg, remainder, _) = get_arg(querystring)?;
            Ok((newarg, remainder, qualifier, AnnotationDepth::Max))
        } else {
            Ok((newarg, remainder, qualifier, AnnotationDepth::One))
        }
    } else {
        Ok((
            arg,
            querystring,
            SelectionQualifier::Normal,
            AnnotationDepth::One,
        ))
    }
}

fn parse_text_qualifiers<'a>(
    arg: &'a str,
    querystring: &'a str,
) -> Result<(&'a str, &'a str, TextMode, bool), StamError> {
    if arg == "AS" {
        let (as_arg, remainder, _) = get_arg(querystring)?;
        let mut regex = false;
        let qualifier = match as_arg {
            "REGEXP" | "REGEX" => {
                regex = true;
                TextMode::Exact
            }
            "NOCASE" => TextMode::CaseInsensitive,
            _ => {
                return Err(StamError::QuerySyntaxError(
                    format!(
                        "Expected keyword REGEX or NOCASE after TEXT AS, got '{}'",
                        remainder
                            .split(QUERYSPLITCHARS)
                            .next()
                            .unwrap_or("(empty string)")
                    ),
                    "",
                ))
            }
        };
        let (newarg, remainder, _) = get_arg(querystring)?;
        Ok((newarg, remainder, qualifier, regex))
    } else {
        Ok((arg, querystring, TextMode::Exact, false))
    }
}

fn parse_dataoperator<'a>(
    opstr: &'a str,
    value: &'a str,
    valuetype: ArgType,
) -> Result<DataOperator<'a>, StamError> {
    let operator = match (opstr, valuetype) {
        ("=", ArgType::String) => DataOperator::Equals(value),
        ("=", ArgType::Null) => DataOperator::Null,
        ("=", ArgType::Any) => DataOperator::Any,
        ("=", ArgType::Bool) => match value {
            "true" => DataOperator::True,
            "false" => DataOperator::False,
            _ => unreachable!("boolean should be true or false"),
        },
        ("=", ArgType::Integer) => {
            DataOperator::EqualsInt(value.parse().expect("str->int conversion should work"))
        }
        ("=", ArgType::Float) => {
            DataOperator::EqualsFloat(value.parse().expect("str->float conversion should work"))
        }
        ("!=", ArgType::String) => DataOperator::Not(Box::new(DataOperator::Equals(value))),
        ("!=", ArgType::Integer) => DataOperator::Not(Box::new(DataOperator::EqualsInt(
            value.parse().expect("str->int conversion should work"),
        ))),
        ("!=", ArgType::Float) => DataOperator::Not(Box::new(DataOperator::EqualsFloat(
            value.parse().expect("str->float conversion should work"),
        ))),
        ("!=", ArgType::Null) => DataOperator::Not(Box::new(DataOperator::Null)),
        ("!=", ArgType::Any) => DataOperator::Not(Box::new(DataOperator::Any)), //this is a tautology, always fails
        ("!=", ArgType::Bool) => match value {
            "true" => DataOperator::Not(Box::new(DataOperator::True)),
            "false" => DataOperator::Not(Box::new(DataOperator::False)),
            _ => unreachable!("boolean should be true or false"),
        },
        ("!=", ArgType::List) => {
            let values: Vec<_> = value.split("|").map(|x| DataOperator::Equals(x)).collect();
            DataOperator::Not(Box::new(DataOperator::Or(values)))
        }
        ("!=", ArgType::UnquotedList) => {
            let values: Vec<_> = value
                .split("|")
                .map(|x| {
                    if let Ok(x) = x.parse::<isize>() {
                        DataOperator::EqualsInt(x)
                    } else if let Ok(x) = x.parse::<f64>() {
                        DataOperator::EqualsFloat(x)
                    } else {
                        DataOperator::Equals(x)
                    }
                })
                .collect();
            DataOperator::Not(Box::new(DataOperator::Or(values)))
        }
        (">", ArgType::Integer) => {
            DataOperator::GreaterThan(value.parse().expect("str->int conversion should work"))
        }
        (">=", ArgType::Integer) => DataOperator::GreaterThanOrEqual(
            value.parse().expect("str->int conversion should work"),
        ),
        ("<", ArgType::Integer) => {
            DataOperator::LessThan(value.parse().expect("str->int conversion should work"))
        }
        ("<=", ArgType::Integer) => {
            DataOperator::LessThanOrEqual(value.parse().expect("str->int conversion should work"))
        }
        (">", ArgType::Float) => DataOperator::GreaterThanFloat(
            value.parse().expect("str->float conversion should work"),
        ),
        (">=", ArgType::Float) => DataOperator::GreaterThanOrEqualFloat(
            value.parse().expect("str->float conversion should work"),
        ),
        ("<", ArgType::Float) => {
            DataOperator::LessThanFloat(value.parse().expect("str->float conversion should work"))
        }
        ("<=", ArgType::Float) => DataOperator::LessThanOrEqualFloat(
            value.parse().expect("str->float conversion should work"),
        ),
        ("=", ArgType::List) => {
            let values: Vec<_> = value.split("|").map(|x| DataOperator::Equals(x)).collect();
            DataOperator::Or(values)
        }
        ("=", ArgType::UnquotedList) => {
            let values: Vec<_> = value
                .split("|")
                .map(|x| {
                    if let Ok(x) = x.parse::<isize>() {
                        DataOperator::EqualsInt(x)
                    } else if let Ok(x) = x.parse::<f64>() {
                        DataOperator::EqualsFloat(x)
                    } else {
                        DataOperator::Equals(x)
                    }
                })
                .collect();
            DataOperator::Or(values)
        }
        ("=", ArgType::Datetime) => DataOperator::ExactDatetime(
            DateTime::parse_from_rfc3339(value).expect("datetime RFC3339 parsing should work"),
        ),
        (">", ArgType::Datetime) => DataOperator::AfterDatetime(
            DateTime::parse_from_rfc3339(value).expect("datetime RFC3339 parsing should work"),
        ),
        (">=", ArgType::Datetime) => DataOperator::AtOrAfterDatetime(
            DateTime::parse_from_rfc3339(value).expect("datetime RFC3339 parsing should work"),
        ),
        ("<", ArgType::Datetime) => DataOperator::BeforeDatetime(
            DateTime::parse_from_rfc3339(value).expect("datetime RFC3339 parsing should work"),
        ),
        ("<=", ArgType::Datetime) => DataOperator::AtOrBeforeDatetime(
            DateTime::parse_from_rfc3339(value).expect("datetime RFC3339 parsing should work"),
        ),
        _ => {
            return Err(StamError::QuerySyntaxError(
                format!(
                    "Invalid combination of operator and value: '{}' and '{}', type {:?}",
                    opstr, value, valuetype
                ),
                "",
            ))
        }
    };
    Ok(operator)
}

#[derive(Debug, Clone)]
/// An assignemnt is a part of an ADD [`Query`] that assigns data to a new annotation
pub enum Assignment<'a> {
    /// Constrain the selection (type is determined by the query result type) to one instance with a specific identifier (`ID` keyword in STAMQL).
    Id(&'a str),
    Target {
        name: &'a str,
        offset: Option<Offset>,
    },
    ComplexTarget(SelectorKind),
    Data {
        set: &'a str,
        key: &'a str,
        value: DataValue,
    },
    Text(&'a str),
    Filename(&'a str),
}

impl<'a> Assignment<'a> {
    pub(crate) fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let assignment = match querystring.split(QUERYSPLITCHARS).next() {
            Some("ID") => {
                querystring = querystring["ID".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                Self::Id(arg)
            }
            Some("DATA") => {
                querystring = querystring["DATA".len()..].trim_start();
                let (set, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                let (key, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                let value = if Self::closed(querystring) {
                    DataValue::Null
                } else {
                    let (value, remainder, valuetype) = get_arg(querystring)?;
                    querystring = remainder;
                    match valuetype {
                        ArgType::Bool => match value.to_lowercase().as_str() {
                            "1" | "yes" | "true" | "enabled" | "on" => DataValue::Bool(true),
                            _ => DataValue::Bool(false),
                        },
                        ArgType::Integer => DataValue::try_from(value).or_else(|_| {
                            Err(StamError::QuerySyntaxError(
                                format!("Expected integer in assignment, got '{}'", value),
                                "",
                            ))
                        })?,
                        ArgType::Float => DataValue::try_from(value).or_else(|_| {
                            Err(StamError::QuerySyntaxError(
                                format!("Expected integer in assignment, got '{}'", value),
                                "",
                            ))
                        })?,
                        ArgType::String => DataValue::String(value.to_string()),
                        _ => unreachable!("argtype should not occur"),
                    }
                };
                Self::Data { set, key, value }
            }
            Some("TARGET") => {
                querystring = querystring["TARGET".len()..].trim_start();
                if let (Some(name), remainder) = Self::parse_name(querystring)? {
                    let (offset, remainder) = Self::parse_offset(remainder)?;
                    querystring = remainder;
                    Self::Target { name, offset }
                } else {
                    return Err(StamError::QuerySyntaxError(
                        format!("Expected name for TARGET"),
                        "",
                    ));
                }
            }
            Some("COMPOSITE") => {
                querystring = querystring["COMPOSITE".len()..].trim_start();
                Self::ComplexTarget(SelectorKind::CompositeSelector)
            }
            Some("MULTI") => {
                querystring = querystring["MULTI".len()..].trim_start();
                Self::ComplexTarget(SelectorKind::MultiSelector)
            }
            Some("DIRECTIONAL") => {
                querystring = querystring["DIRECTIONAL".len()..].trim_start();
                Self::ComplexTarget(SelectorKind::DirectionalSelector)
            }
            Some(x) => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected assignment type (DATA, ID, TARGET), got '{}'", x),
                    "",
                ))
            }
            None => {
                return Err(StamError::QuerySyntaxError(
                    format!("Expected assignment type (DATA, ID, TARGET), got end of string"),
                    "",
                ))
            }
        };
        if querystring.starts_with(";") {
            querystring = &querystring[1..].trim_start();
        }
        Ok((assignment, querystring))
    }

    fn closed(querystring: &str) -> bool {
        querystring.is_empty()
            || querystring.starts_with(";")
            || querystring.starts_with("OR ")
            || querystring.starts_with("]")
    }

    fn parse_name(mut querystring: &'a str) -> Result<(Option<&'a str>, &'a str), StamError> {
        let name = if let Some('?') = querystring.chars().next() {
            Some(
                querystring[1..]
                    .split(QUERYSPLITCHARS)
                    .next()
                    .unwrap()
                    .trim_end_matches(';'),
            )
        } else {
            None
        };
        if let Some(name) = name {
            querystring = querystring[1 + name.len()..].trim_start();
        }
        Ok((name, querystring))
    }

    fn parse_offset<'b>(mut querystring: &'b str) -> Result<(Option<Offset>, &'b str), StamError> {
        if !Self::closed(querystring) && querystring.starts_with("OFFSET") {
            querystring = querystring["OFFSET".len()..].trim_start();
            let (arg, remainder, _) = get_arg(querystring)?;
            let begin = if arg == "WHOLE" || arg == "ALL" {
                Cursor::BeginAligned(0)
            } else {
                Cursor::try_from(arg).or_else(|_| {
                    Err(StamError::QuerySyntaxError(
                        format!(
                            "Expected integer for begin cursor after OFFSET, got '{}'",
                            arg
                        ),
                        "",
                    ))
                })?
            };
            querystring = remainder;
            let end = if Self::closed(querystring) {
                Cursor::EndAligned(0)
            } else {
                let (arg, remainder, _) = get_arg(querystring)?;
                querystring = remainder;
                Cursor::try_from(arg).or_else(|_| {
                    Err(StamError::QuerySyntaxError(
                        format!(
                            "Expected integer for end cursor, (optional) second parameter after OFFSET, got '{}'",
                            arg
                        ),
                        "",
                    ))
                })?
            };
            Ok((Some(Offset { begin, end }), querystring))
        } else {
            Ok((None, querystring))
        }
    }
}
