#![allow(unused_imports)]
#![allow(dead_code)]
use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdata::{AnnotationData, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::api::*;
use crate::datakey::DataKey;
use crate::datakey::DataKeyHandle;
use crate::error::StamError;
use crate::textselection::TextSelectionOperator;
use crate::{store::*, ResultTextSelection};
use crate::{types::*, DataOperator};
use crate::{TextResource, TextResourceHandle};

use smallvec::SmallVec;
use std::collections::HashMap;

use std::borrow::Cow;

const QUERYSPLITCHARS: &[char] = &[' ', '\n', '\r', '\t'];

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryType {
    Select,
}

impl QueryType {
    fn as_str(&self) -> &str {
        match self {
            QueryType::Select => "SELECT",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Query<'a> {
    /// The variable name
    name: Option<&'a str>,

    querytype: QueryType,
    resulttype: Option<Type>,
    constraints: Vec<Constraint<'a>>,
    subquery: Option<Box<Query<'a>>>,

    /// Context variables (external), if there are subqueries, they do NOT hold contextvars, only the root query does
    /// This allows binding variables programmatically, using the bind_*var() methods
    contextvars: HashMap<String, QueryResultItem<'a>>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// This is determines whether a query [`Constraint`] is applied normally or with a particular altered meaning.
pub enum SelectionQualifier {
    /// Normal behaviour, no changes.
    Normal,

    /// This corresponds to the TARGET keyword in STAMQL. It indicates that the item in the constrain is an explicit annotation TARGET. It causes the logic flow to go over methods like annotations_as_metadata() instead of annotations()
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
pub enum Constraint<'a> {
    Id(&'a str),

    /// ID of the annotation
    Annotation(&'a str, SelectionQualifier, AnnotationDepth),

    /// ID of a TextResource
    TextResource(&'a str, SelectionQualifier),
    DataSet(&'a str, SelectionQualifier),
    DataKey {
        set: &'a str,
        key: &'a str,
        qualifier: SelectionQualifier,
    },
    KeyVariable(&'a str, SelectionQualifier),
    DataVariable(&'a str, SelectionQualifier),
    DataSetVariable(&'a str, SelectionQualifier),

    ResourceVariable(&'a str, SelectionQualifier),
    TextVariable(&'a str),
    TextRelation {
        var: &'a str,
        operator: TextSelectionOperator,
    },
    KeyValue {
        set: &'a str,
        key: &'a str,
        operator: DataOperator<'a>,
        qualifier: SelectionQualifier,
    },
    Value(DataOperator<'a>, SelectionQualifier),
    KeyValueVariable(&'a str, DataOperator<'a>, SelectionQualifier),
    Text(&'a str),
    /// Disjunction
    Union(Vec<Constraint<'a>>),
    AnnotationVariable(&'a str, SelectionQualifier, AnnotationDepth),

    Annotations(Handles<'a, Annotation>, SelectionQualifier, AnnotationDepth),
    Data(Handles<'a, AnnotationData>, SelectionQualifier),
    Keys(Handles<'a, DataKey>, SelectionQualifier),
    Resources(Handles<'a, TextResource>, SelectionQualifier),
    TextSelections(Handles<'a, TextSelection>, SelectionQualifier),
}

impl<'a> Constraint<'a> {
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
            Self::Text { .. } | Self::TextVariable(..) => "TEXT",
            Self::AnnotationVariable(..) | Self::Annotation(..) => "ANNOTATION",
            Self::Union { .. } => "UNION",

            //not real keywords that can be parsed, only for debug printing purposes:
            Self::Annotations(..) => "ANNOTATIONS",
            Self::Data(..) => "DATA",
            Self::Resources(..) => "RESOURCES",
            Self::TextSelections(..) => "TEXTSELECTIONS",
            Self::Keys(..) => "KEYS",
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
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::TextVariable(&arg[1..])
                } else {
                    Self::Text(arg)
                }
            }
            Some("ANNOTATION") => {
                querystring = querystring["ANNOTATION".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, depth) = parse_qualifiers(arg, remainder)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::AnnotationVariable(&arg[1..], qualifier, depth)
                } else {
                    Self::Annotation(arg, qualifier, depth)
                }
            }
            Some("RESOURCE") => {
                querystring = querystring["RESOURCE".len()..].trim_start();
                let (arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, _) = parse_qualifiers(arg, remainder)?;
                querystring = remainder;
                if arg.starts_with("?") && arg.len() > 1 {
                    Self::ResourceVariable(&arg[1..], qualifier)
                } else {
                    Self::TextResource(arg, qualifier)
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
                let (mut arg, remainder, _) = get_arg(querystring)?;
                let (arg, remainder, qualifier, _) = parse_qualifiers(arg, remainder)?;
                querystring = remainder;
                if arg.starts_with("?") {
                    Self::DataVariable(&arg[1..], qualifier)
                } else {
                    let set = arg;
                    let (key, remainder, _) = get_arg(remainder)?;
                    querystring = remainder;
                    if querystring.is_empty() {
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
                    let (set, remainder, _) = get_arg(querystring)?;
                    let (key, remainder, _) = get_arg(remainder)?;
                    querystring = remainder;
                    Self::DataKey {
                        set,
                        key,
                        qualifier,
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

    /// Serialize the constraint to a STAMQL String
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
            Self::AnnotationVariable(varname, qualifier, depth) => {
                s += &format!(
                    "ANNOTATION{}{} ?{};",
                    qualifier.as_str(),
                    if depth == &AnnotationDepth::Max {
                        " RECURSIVE"
                    } else {
                        " "
                    },
                    varname
                );
            }
            Self::DataVariable(varname, qualifier) => {
                s += &format!("DATA{} ?{};", qualifier.as_str(), varname);
            }
            Self::DataSetVariable(varname, qualifier) => {
                s += &format!("DATASET{} ?{};", qualifier.as_str(), varname);
            }
            Self::ResourceVariable(varname, qualifier) => {
                s += &format!("RESOURCE{} ?{};", qualifier.as_str(), varname);
            }
            Self::TextVariable(varname) => {
                s += &format!("TEXT{};", varname);
            }
            Self::Annotation(id, qualifier, depth) => {
                s += &format!(
                    "ANNOTATION{}{} \"{}\";",
                    qualifier.as_str(),
                    if depth == &AnnotationDepth::Max {
                        " RECURSIVE"
                    } else {
                        " "
                    },
                    id
                );
            }
            Self::TextResource(id, qualifier) => {
                s += &format!("RESOURCE{} \"{}\";", qualifier.as_str(), id);
            }
            Self::DataSet(id, qualifier) => {
                s += &format!("DATASET{} \"{}\";", qualifier.as_str(), id);
            }
            Self::Text(text) => {
                s += &format!("TEXT \"{}\";", text);
            }
            Self::TextRelation { var, operator } => {
                s += &format!("RELATION ?{} {};", var, operator.as_str());
            }
            Self::Union(..) => {
                //TODO
                return Err(StamError::QuerySyntaxError(
                    "Query contains UNION constraint that can not yet be serialized to STAMQL (implementation still pending)"
                        .into(),
                    "Constraint::to_string()",
                ));
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
    pub fn new(querytype: QueryType, resulttype: Option<Type>, name: Option<&'a str>) -> Self {
        Self {
            name,
            querytype,
            resulttype,
            constraints: Vec::new(),
            subquery: None,
            contextvars: HashMap::new(),
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

    /// Returns the last subquery (if any), otherwise returns this query itself
    pub fn last_subquery(&self) -> &Query<'a> {
        let mut q = self;
        while q.subquery.is_some() {
            q = q.subquery.as_ref().unwrap();
        }
        q
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

    pub fn parse(mut querystring: &'a str) -> Result<(Self, &'a str), StamError> {
        let mut end: usize;
        querystring = querystring.trim();
        if let Some("SELECT") = querystring.split(QUERYSPLITCHARS).next() {
            end = 7;
        } else {
            return Err(StamError::QuerySyntaxError(
                format!(
                    "Expected SELECT, got '{}'",
                    querystring
                        .split(QUERYSPLITCHARS)
                        .next()
                        .unwrap_or("(empty string)"),
                ),
                "",
            ));
        }
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
                contextvars: HashMap::new(),
            },
            querystring,
        ))
    }

    pub fn with_annotationvar(
        mut self,
        name: impl Into<String>,
        annotation: ResultItem<'a, Annotation>,
    ) -> Self {
        self.contextvars
            .insert(name.into(), QueryResultItem::Annotation(annotation));
        self
    }

    pub fn bind_annotationvar(
        &mut self,
        name: impl Into<String>,
        annotation: ResultItem<'a, Annotation>,
    ) {
        self.contextvars
            .insert(name.into(), QueryResultItem::Annotation(annotation));
    }

    pub fn with_datavar(
        mut self,
        name: impl Into<String>,
        data: ResultItem<'a, AnnotationData>,
    ) -> Self {
        self.contextvars
            .insert(name.into(), QueryResultItem::AnnotationData(data));
        self
    }

    pub fn bind_datavar(&mut self, name: impl Into<String>, data: ResultItem<'a, AnnotationData>) {
        self.contextvars
            .insert(name.into(), QueryResultItem::AnnotationData(data));
    }

    pub fn with_keyvar(mut self, name: impl Into<String>, key: ResultItem<'a, DataKey>) -> Self {
        self.contextvars
            .insert(name.into(), QueryResultItem::DataKey(key));
        self
    }

    pub fn bind_keyvar(&mut self, name: impl Into<String>, key: ResultItem<'a, DataKey>) {
        self.contextvars
            .insert(name.into(), QueryResultItem::DataKey(key));
    }

    pub fn with_textvar(
        mut self,
        name: impl Into<String>,
        textselection: ResultTextSelection<'a>,
    ) -> Self {
        self.contextvars
            .insert(name.into(), QueryResultItem::TextSelection(textselection));
        self
    }

    pub fn bind_textvar(
        &mut self,
        name: impl Into<String>,
        textselection: ResultTextSelection<'a>,
    ) {
        self.contextvars
            .insert(name.into(), QueryResultItem::TextSelection(textselection));
    }

    pub fn with_resourcevar(
        mut self,
        name: impl Into<String>,
        resource: ResultItem<'a, TextResource>,
    ) -> Self {
        self.contextvars
            .insert(name.into(), QueryResultItem::TextResource(resource));
        self
    }

    pub fn bind_resourcevar(
        &mut self,
        name: impl Into<String>,
        resource: ResultItem<'a, TextResource>,
    ) {
        self.contextvars
            .insert(name.into(), QueryResultItem::TextResource(resource));
    }

    pub fn with_datasetvar(
        mut self,
        name: impl Into<String>,
        dataset: ResultItem<'a, AnnotationDataSet>,
    ) -> Self {
        self.contextvars
            .insert(name.into(), QueryResultItem::AnnotationDataSet(dataset));
        self
    }

    pub fn bind_datasetvar(
        &mut self,
        name: impl Into<String>,
        dataset: ResultItem<'a, AnnotationDataSet>,
    ) {
        self.contextvars
            .insert(name.into(), QueryResultItem::AnnotationDataSet(dataset));
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
        if let Some(subquery) = self.subquery() {
            s += "{\n";
            s += &subquery.to_string()?;
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

/// This type abstracts over all the main iterators
/// This abstraction uses dynamic dispatch so comes with a small performance cost
pub enum QueryResultIter<'store> {
    Annotations(Box<dyn Iterator<Item = ResultItem<'store, Annotation>> + 'store>),
    Data(Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>),
    Keys(Box<dyn Iterator<Item = ResultItem<'store, DataKey>> + 'store>),
    DataSets(Box<dyn Iterator<Item = ResultItem<'store, AnnotationDataSet>> + 'store>),
    TextSelections(Box<dyn Iterator<Item = ResultTextSelection<'store>> + 'store>),
    Resources(Box<dyn Iterator<Item = ResultItem<'store, TextResource>> + 'store>),
}

#[derive(Clone, Debug)]
pub enum QueryResultItem<'store> {
    None,
    TextSelection(ResultTextSelection<'store>),
    Annotation(ResultItem<'store, Annotation>),
    TextResource(ResultItem<'store, TextResource>),
    DataKey(ResultItem<'store, DataKey>),
    AnnotationData(ResultItem<'store, AnnotationData>),
    AnnotationDataSet(ResultItem<'store, AnnotationDataSet>),
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

    pub fn enumerate(&self) -> Vec<(usize, &str)> {
        let mut names = Vec::new();
        for (name, index) in self.0.iter() {
            names.push((*index, name.as_str()));
        }
        names.sort_unstable();
        names
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
            ///////////////////////////// target= RESOURCE ////////////////////////////////////////////
            Some(Type::TextResource) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, TextResource>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::Id(id)) | Some(&Constraint::TextResource(id,_)) => {
                            Box::new(Some(store.resource(id).or_fail()?).into_iter())
                        }
                        Some(&Constraint::Resources(ref handles,_)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store))
                        }
                        Some(&Constraint::Annotations(ref handles, SelectionQualifier::Normal, _)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store).resources())
                        }
                        Some(&Constraint::Annotations(ref handles, SelectionQualifier::Metadata, _)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store).resources_as_metadata())
                        }
                        Some(&Constraint::DataKey { set, key, qualifier: SelectionQualifier::Normal }) => Box::new(
                            store
                                .key(set, key).or_fail()?
                                .annotations()
                                .resources(),
                        ),
                        //KEY AS METADATA
                        Some(&Constraint::DataKey { set, key, qualifier: SelectionQualifier::Metadata }) => Box::new(
                            store
                                .key(set, key).or_fail()?
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
                        Some(&Constraint::KeyValueVariable(varname,
                            ref operator,
                            SelectionQualifier::Normal,
                        )) => {
                            let key = self.resolve_keyvar(varname)?;
                            Box::new(
                                 key.data().filter_value(operator.clone()).annotations().resources()
                            )
                        },
                        Some(&Constraint::KeyValueVariable(varname,
                            ref operator,
                            SelectionQualifier::Metadata,
                        )) => {
                            let key = self.resolve_keyvar(varname)?;
                            Box::new(
                                 key.data().filter_value(operator.clone()).annotations().resources_as_metadata()
                            )
                        },
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
                //secondary contraints for target RESOURCE
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::DataKey { set, key, qualifier: SelectionQualifier::Metadata } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_in_metadata(&key));
                        }
                        &Constraint::DataKey { set, key, qualifier: SelectionQualifier::Normal } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_on_text(&key));
                        }
                        &Constraint::KeyVariable(var, SelectionQualifier::Metadata) => {
                            let key = self.resolve_keyvar(var)?;
                            iter = Box::new(iter.filter_key_in_metadata(&key));
                        }
                        &Constraint::KeyVariable(var, SelectionQualifier::Normal) => {
                            let key = self.resolve_keyvar(var)?;
                            iter = Box::new(iter.filter_key_on_text(&key));
                        }
                        &Constraint::DataVariable(var, SelectionQualifier::Metadata) => {
                            let data = self.resolve_datavar(var)?;
                            iter = Box::new(iter.filter_annotationdata_in_metadata(&data));
                        }
                        &Constraint::DataVariable(var, SelectionQualifier::Normal) => {
                            let data = self.resolve_datavar(var)?;
                            iter = Box::new(iter.filter_annotationdata_on_text(&data));
                        }
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                            qualifier: SelectionQualifier::Metadata,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value_in_metadata(&key, operator.clone()));
                        }
                        &Constraint::KeyValueVariable(varname,
                            ref operator,
                            SelectionQualifier::Normal,
                        ) => {
                            let key = self.resolve_keyvar(varname)?;
                            iter = Box::new(
                                 iter.filter_key_value_on_text(&key, operator.clone())
                            )
                        },
                        &Constraint::KeyValueVariable(varname,
                            ref operator,
                            SelectionQualifier::Metadata,
                        ) => {
                            let key = self.resolve_keyvar(varname)?;
                            iter = Box::new(
                                 iter.filter_key_value_in_metadata(&key, operator.clone())
                            )
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
            ///////////////////////////// target= ANNOTATION ////////////////////////////////////////////
            Some(Type::Annotation) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, Annotation>>> =
                    //primary constraints for ANNOTATION
                    match constraintsiter.next() {
                        Some(&Constraint::Id(id)) => {
                            Box::new(Some(store.annotation(id).or_fail()?).into_iter())
                        }
                        Some(&Constraint::Annotations(ref handles,SelectionQualifier::Normal,AnnotationDepth::Zero)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store))
                        }
                        Some(&Constraint::Annotations(ref handles,SelectionQualifier::Normal,depth)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store).annotations_in_targets(depth))
                        }
                        Some(&Constraint::Annotations(ref handles,SelectionQualifier::Metadata, AnnotationDepth::One)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store).annotations())
                        }
                        Some(&Constraint::AnnotationVariable(var, SelectionQualifier::Normal, depth)) => {
                            let annotation = self.resolve_annotationvar(var)?;
                            Box::new(annotation.annotations_in_targets(depth))
                        }
                        Some(&Constraint::AnnotationVariable(var, SelectionQualifier::Metadata, AnnotationDepth::One)) => { //TODO LATER: handle Recursive variant?
                            let annotation = self.resolve_annotationvar(var)?;
                            Box::new(annotation.annotations())
                        }
                        Some(&Constraint::TextResource(res, SelectionQualifier::Normal)) => {
                            Box::new(store.resource(res).or_fail()?.annotations())
                        }
                        Some(&Constraint::TextResource(res, SelectionQualifier::Metadata)) => {
                            Box::new(store.resource(res).or_fail()?.annotations_as_metadata())
                        }
                        Some(&Constraint::ResourceVariable(var, SelectionQualifier::Normal)) => {
                            let resource = self.resolve_resourcevar(var)?;
                            Box::new(resource.annotations())
                        }
                        Some(&Constraint::ResourceVariable(var, SelectionQualifier::Metadata)) => {
                            let resource = self.resolve_resourcevar(var)?;
                            Box::new(resource.annotations_as_metadata())
                        }
                        Some(&Constraint::Annotation(annotation, SelectionQualifier::Normal, depth)) => {
                            Box::new(store.annotation(annotation).or_fail()?.annotations_in_targets(depth))
                        }
                        Some(&Constraint::Annotation(annotation, SelectionQualifier::Metadata, AnnotationDepth::One)) => {
                            Box::new(store.annotation(annotation).or_fail()?.annotations())
                        }
                        Some(&Constraint::DataKey { set, key, qualifier: SelectionQualifier::Normal }) => {
                            Box::new(store.key(set, key).or_fail()?.annotations())
                        }
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
                        Some(&Constraint::Value(
                            ref operator,
                            _
                        )) => Box::new(store.find_data(false, false, operator.clone()).annotations()),
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
                        Some(&Constraint::TextSelections(ref handles,SelectionQualifier::Normal)) => {
                            Box::new(ResultTextSelections::new(FromHandles::new(handles.clone().into_iter(), store)).annotations())
                        }
                        Some(&Constraint::KeyValueVariable(varname,
                            ref operator,
                            SelectionQualifier::Normal,
                        )) => {
                            let key = self.resolve_keyvar(varname)?;
                            Box::new(
                                 key.data().filter_value(operator.clone()).annotations()
                            )
                        },
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
                //secondary contraints for target ANNOTATION
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::TextResource(res, SelectionQualifier::Normal) => {
                            iter = Box::new(iter.filter_resource(&store.resource(res).or_fail()?));
                        }
                        &Constraint::TextResource(res, SelectionQualifier::Metadata) => {
                            iter = Box::new(
                                iter.filter_resource_as_metadata(&store.resource(res).or_fail()?),
                            );
                        }
                        &Constraint::ResourceVariable(varname, SelectionQualifier::Normal) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource(resource));
                        }
                        &Constraint::ResourceVariable(varname, SelectionQualifier::Metadata) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource_as_metadata(resource));
                        }
                        &Constraint::AnnotationVariable(
                            var,
                            SelectionQualifier::Normal,
                            AnnotationDepth::One,
                        ) => {
                            let annotation = self.resolve_annotationvar(var)?;
                            iter = Box::new(iter.filter_annotation(annotation));
                        }
                        &Constraint::AnnotationVariable(var, _, AnnotationDepth::Zero) => {
                            let annotation = self.resolve_annotationvar(var)?;
                            iter = Box::new(iter.filter_one(annotation));
                        }
                        &Constraint::AnnotationVariable(
                            var,
                            SelectionQualifier::Metadata,
                            depth,
                        ) => {
                            let annotation = self.resolve_annotationvar(var)?;
                            iter = Box::new(iter.filter_annotation_in_targets(annotation, depth));
                        }
                        &Constraint::DataVariable(varname, SelectionQualifier::Normal) => {
                            let data = self.resolve_datavar(varname)?;
                            iter = Box::new(iter.filter_annotationdata(data));
                        }
                        &Constraint::DataKey {
                            set,
                            key,
                            qualifier: SelectionQualifier::Normal,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key(&key));
                        }
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                            qualifier: SelectionQualifier::Normal,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()));
                        }
                        &Constraint::Value(ref operator, SelectionQualifier::Normal) => {
                            iter = Box::new(iter.filter_value(operator.clone()));
                        }
                        &Constraint::KeyValueVariable(
                            varname,
                            ref operator,
                            SelectionQualifier::Normal,
                        ) => {
                            let key = self.resolve_keyvar(varname)?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()))
                        }
                        &Constraint::Text(text) => {
                            iter = Box::new(iter.filter_text_byref(text, true, " "))
                        }
                        &Constraint::TextVariable(var) => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                iter =
                                    Box::new(iter.filter_any(tsel.annotations().to_handles(store)))
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                iter = Box::new(iter.filter_any(
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
                                iter = Box::new(iter.filter_any(
                                    tsel.related_text(operator).annotations().to_handles(store),
                                ))
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                iter = Box::new(
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
                                    format!(
                                        "Variable ?{} of type TEXT or ANNOTATION not found",
                                        var
                                    ),
                                    "",
                                ));
                            }
                        }
                        &Constraint::Annotation(
                            annotation,
                            SelectionQualifier::Normal,
                            AnnotationDepth::One,
                        ) => {
                            iter = Box::new(
                                iter.filter_annotation(&store.annotation(annotation).or_fail()?),
                            );
                        }
                        &Constraint::Annotation(annotation, _, AnnotationDepth::Zero) => {
                            iter =
                                Box::new(iter.filter_one(&store.annotation(annotation).or_fail()?));
                        }
                        &Constraint::Annotation(
                            annotation,
                            SelectionQualifier::Metadata,
                            depth,
                        ) => {
                            iter = Box::new(iter.filter_annotation_in_targets(
                                &store.annotation(annotation).or_fail()?,
                                depth,
                            ));
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
            /////////////////////////////////// target=TEXT //////////////////////////////////////
            Some(Type::TextSelection) => {
                let mut iter: Box<dyn Iterator<Item = ResultTextSelection<'store>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::TextSelections(ref handles,_)) => {
                            Box::new(ResultTextSelections::new(FromHandles::new(handles.clone().into_iter(), store)))
                        }
                        Some(&Constraint::TextVariable(varname)) => {
                            let textselection = self.resolve_textvar(varname)?;
                            Box::new(Some(textselection.clone()).into_iter())
                        }
                        Some(&Constraint::Annotations(ref handles,_,_)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store).textselections())
                        }
                        Some(&Constraint::TextResource(res,_)) => {
                            Box::new(store.resource(res).or_fail()?.textselections())
                        }
                        Some(&Constraint::ResourceVariable(varname,_)) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            Box::new(resource.textselections())
                        }
                        Some(&Constraint::Annotation(id,_,_)) => {
                            Box::new(store.annotation(id).or_fail()?.textselections())
                        }
                        Some(&Constraint::AnnotationVariable(varname,_,_)) => {
                            let annotation = self.resolve_annotationvar(varname)?;
                            Box::new(annotation.textselections())
                        }
                        Some(&Constraint::DataKey { set, key, qualifier: _ }) => Box::new(
                            store
                                .find_data(set, key, DataOperator::Any)
                                .annotations()
                                .textselections(),
                        ),
                        Some(&Constraint::DataVariable(varname,_)) => {
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
                        Some(&Constraint::KeyValueVariable(varname,
                            ref operator,
                            SelectionQualifier::Normal,
                        )) => {
                            let key = self.resolve_keyvar(varname)?;
                            Box::new(
                                 key.data().filter_value(operator.clone()).annotations().textselections()
                            )
                        },
                        Some(&Constraint::Value(
                            ref operator,
                            _
                        )) => Box::new(
                            store
                                .find_data(false, false, operator.clone())
                                .annotations()
                                .textselections(),
                        ),
                        Some(&Constraint::Text(text)) => Box::new(store.find_text(text)),
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
                                    "Constraint {} (primary) is not implemented for queries over TEXT selections",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                        None => Box::new(store.annotations().textselections()),
                    };
                //secondary contraints for target TEXT
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::TextResource(res,_) => {
                            iter = Box::new(iter.filter_resource(&store.resource(res).or_fail()?));
                        }
                        &Constraint::ResourceVariable(varname,_) => {
                            let resource = self.resolve_resourcevar(varname)?;
                            iter = Box::new(iter.filter_resource(&resource));
                        }
                        &Constraint::DataKey { set, key, qualifier: _ } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key(&key));
                        }
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                            qualifier: _,
                        } => {
                            let key = store.key(set, key).or_fail()?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()));
                        }
                        &Constraint::KeyValueVariable(
                            varname,
                            ref operator,
                            SelectionQualifier::Normal,
                        ) => {
                            let key = self.resolve_keyvar(varname)?;
                            iter = Box::new(iter.filter_key_value(&key, operator.clone()))
                        }
                        &Constraint::Value(
                            ref operator,
                            _
                        ) => {
                            iter = Box::new(iter.filter_value(operator.clone()));
                        }
                        &Constraint::Text(text) => {
                            iter = Box::new(iter.filter_text_byref(text, true))
                        }
                        &Constraint::TextRelation { var, operator } => {
                            if let Ok(tsel) = self.resolve_textvar(var) {
                                iter = Box::new(iter.filter_any(tsel.related_text(operator).filter_map(|x| x.as_resultitem().map(|x| x.clone())).to_handles(store)))
                            } else if let Ok(annotation) = self.resolve_annotationvar(var) {
                                iter = Box::new(iter.filter_any(
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
                                "Constraint {} (secondary) is not implemented for queries over TEXT selections",
                                c.keyword()
                            ),
                            "",
                        )),
                    }
                }
                Ok(QueryResultIter::TextSelections(iter))
            }
            ///////////////////////////////// target= DATA ////////////////////////////////////////
            Some(Type::AnnotationData) => {
                let mut iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::DataVariable(varname, SelectionQualifier::Normal)) => {
                            let data = self.resolve_datavar(varname)?;
                            Box::new(Some(data.clone()).into_iter())
                        }
                        Some(&Constraint::Data(ref handles, _)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store))
                        }
                        Some(&Constraint::Annotations(ref handles, _, _)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store).data())
                        }
                        Some(&Constraint::Annotation(id, SelectionQualifier::Normal, _)) => {
                            Box::new(store.annotation(id).or_fail()?.data())
                        }
                        Some(&Constraint::AnnotationVariable(
                            varname,
                            SelectionQualifier::Normal,
                            _,
                        )) => {
                            let annotation = self.resolve_annotationvar(varname)?;
                            Box::new(annotation.data())
                        }
                        Some(&Constraint::TextVariable(varname)) => {
                            let textselection = self.resolve_textvar(varname)?;
                            Box::new(textselection.annotations().data())
                        }
                        Some(&Constraint::TextSelections(
                            ref handles,
                            SelectionQualifier::Normal,
                        )) => Box::new(
                            ResultTextSelections::new(FromHandles::new(
                                handles.clone().into_iter(),
                                store,
                            ))
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
                    };
                //secondary contraints for target DATA
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        &Constraint::KeyValue {
                            set,
                            key,
                            ref operator,
                            qualifier: SelectionQualifier::Normal,
                        } => {
                            iter = Box::new(
                                iter.filter_any(
                                    store
                                        .find_data(set, key, operator.clone())
                                        .to_handles(store),
                                ),
                            );
                        }
                        &Constraint::Value(
                            ref operator,
                            SelectionQualifier::Normal,
                        ) => {
                            iter = Box::new(
                                iter.filter_value(operator.clone())
                            );
                        }
                        &Constraint::KeyValueVariable(
                            varname,
                            ref operator,
                            SelectionQualifier::Normal,
                        ) => {
                            let key = self.resolve_keyvar(varname)?;
                            iter = Box::new(iter.filter_key(&key).filter_value(operator.clone()))
                        }
                        &Constraint::AnnotationVariable(
                            varname,
                            SelectionQualifier::Normal,
                            _,
                        ) => {
                            let annotation = self.resolve_annotationvar(varname)?;
                            iter = Box::new(iter.filter_annotation(annotation));
                        }
                        c => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                    "Constraint {} (secondary) is not implemented for queries over DATA",
                                    c.keyword()
                                ),
                                "",
                            ))
                        }
                    }
                }
                Ok(QueryResultIter::Data(iter))
            }
            ///////////////////////////////// target= KEY ////////////////////////////////////////
            Some(Type::DataKey) => {
                let iter: Box<dyn Iterator<Item = ResultItem<'store, DataKey>>> =
                    match constraintsiter.next() {
                        Some(&Constraint::KeyVariable(varname, SelectionQualifier::Normal)) => {
                            let data = self.resolve_keyvar(varname)?;
                            Box::new(Some(data.clone()).into_iter())
                        }
                        Some(&Constraint::DataVariable(varname, SelectionQualifier::Normal)) => {
                            let data = self.resolve_datavar(varname)?;
                            Box::new(Some(data.key().clone()).into_iter())
                        }
                        Some(&Constraint::Keys(ref handles, _)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store))
                        }
                        Some(&Constraint::Annotations(ref handles, _, _)) => {
                            Box::new(FromHandles::new(handles.clone().into_iter(), store).keys())
                        }
                        Some(&Constraint::Annotation(id, SelectionQualifier::Normal, _)) => {
                            Box::new(store.annotation(id).or_fail()?.keys())
                        }
                        Some(&Constraint::AnnotationVariable(
                            varname,
                            SelectionQualifier::Normal,
                            _,
                        )) => {
                            let annotation = self.resolve_annotationvar(varname)?;
                            Box::new(annotation.keys())
                        }
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
                    };
                //secondary contraints for target KEY
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        c => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                "Constraint {} (secondary) is not implemented for queries over KEY",
                                c.keyword()
                            ),
                                "",
                            ))
                        }
                    }
                }
                Ok(QueryResultIter::Keys(iter))
            }
            ///////////////////////////////// target= DATASET ////////////////////////////////////////
            Some(Type::AnnotationDataSet) => {
                let iter: Box<dyn Iterator<Item = ResultItem<'store, AnnotationDataSet>>> =
                    match constraintsiter.next() {
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
                    };
                //secondary contraints for target KEY
                while let Some(constraint) = constraintsiter.next() {
                    match constraint {
                        c => {
                            return Err(StamError::QuerySyntaxError(
                                format!(
                                "Constraint {} (secondary) is not implemented for queries over DATASET",
                                c.keyword()
                            ),
                                "",
                            ))
                        }
                    }
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
                        QueryResultIter::Resources(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::TextResource(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::Keys(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::DataKey(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
                        QueryResultIter::DataSets(iter) => {
                            if let Some(result) = iter.next() {
                                state.result = QueryResultItem::AnnotationDataSet(result);
                                self.statestack.push(state);
                                return true;
                            } else {
                                break; //iterator depleted
                            }
                        }
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
        for query in self.queries.iter() {
            match query.contextvars.get(name) {
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
        }
        return Err(StamError::QuerySyntaxError(
            format!("Variable ?{} of type DATA not found", name),
            "",
        ));
    }

    fn resolve_keyvar(&self, name: &str) -> Result<&ResultItem<'store, DataKey>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self.queries.get(i).expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::DataKey(key) = &state.result {
                    return Ok(key);
                }
            }
        }
        for query in self.queries.iter() {
            match query.contextvars.get(name) {
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
        }
        return Err(StamError::QuerySyntaxError(
            format!(
                "Variable ?{} of type KEY not found - QUERY DEBUG: {:#?}",
                name, self.queries
            ),
            "",
        ));
    }

    fn resolve_datasetvar(
        &self,
        name: &str,
    ) -> Result<&ResultItem<'store, AnnotationDataSet>, StamError> {
        for (i, state) in self.statestack.iter().enumerate() {
            let query = self.queries.get(i).expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::AnnotationDataSet(dataset) = &state.result {
                    return Ok(dataset);
                }
            }
        }
        for query in self.queries.iter() {
            match query.contextvars.get(name) {
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
            let query = self.queries.get(i).expect("query must exist");
            if query.name() == Some(name) {
                if let QueryResultItem::Annotation(annotation) = &state.result {
                    return Ok(annotation);
                }
            }
        }
        for query in self.queries.iter() {
            match query.contextvars.get(name) {
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
        for query in self.queries.iter() {
            match query.contextvars.get(name) {
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
        for query in self.queries.iter() {
            match query.contextvars.get(name) {
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
    Null,
    Bool,
    Any,
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
        match s {
            "null" => ArgType::Null,
            "any" => ArgType::Any,
            "true" | "false" => ArgType::Bool,
            _ => ArgType::String,
        }
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
