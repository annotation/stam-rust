use std::ops::Deref;

use crate::annotation::Annotation;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::error::StamError;
use crate::resources::TextResource;
use crate::store::*;
use crate::textselection::{TextSelection, TextSelectionOperator, TextSelectionSet};
use crate::types::*;

type VarHandle = usize;

pub struct Query<'a> {
    name: Option<String>,
    querytype: QueryType,
    resulttype: Option<ResultType>,
    handle: Option<VarHandle>,
    constraints: Vec<(Constraint<'a>, Qualifier)>,
}

#[derive(Clone, Copy, Debug)]
pub enum Qualifier {
    None,
    All,
    Any,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Variable {
    Decoded(String),
    Encoded(ResultType, VarHandle),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryType {
    Select,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResultType {
    TextSelection,
    Annotation,
    TextResource,
}

pub enum Constraint<'a> {
    AnnotationData {
        set: RequestItem<'a, AnnotationDataSet>,
        key: RequestItem<'a, DataKey>,
        value: DataOperator<'a>,
    },
    TextResource(RequestItemSet<'a, TextResource>),
    TextResourceVariable(Variable),

    TextRelation(RequestItemSet<'a, TextSelection>, TextSelectionOperator),
    TextRelationVariable(Variable, TextSelectionOperator),
}

pub struct QueryState<'store, 'slf> {
    /// What query is being handled? (always procedes incrementally in the defined order)
    queryindex: usize,

    /// The iterator for the current query
    iterator: SubIter<'store, 'slf>,

    // note: this captures the result of the current state, in order to make it available for subsequent deeper iterators
    result: QueryResultItem<'store>,
}

pub struct QueryIter<'store, 'slf> {
    store: &'store AnnotationStore,

    queries: Vec<Query<'slf>>,

    statestack: Vec<QueryState<'store, 'slf>>,
}

/// Abstracts over different types of subiterators we may encounter during querying, the types are names after the type they return.
pub enum SubIter<'store, 'slf> {
    ResourceIter(Box<dyn Iterator<Item = ResultItemSet<'store, TextResource>> + 'slf>),
    AnnotationIter(Box<dyn Iterator<Item = ResultItemSet<'store, Annotation>> + 'slf>),
    TextSelectionIter(Box<dyn Iterator<Item = TextSelectionSet> + 'slf>),
}

pub enum QueryResultItem<'store> {
    None,
    TextSelection(TextSelectionSet),
    Annotation(ResultItemSet<'store, Annotation>),
    TextResource(ResultItemSet<'store, TextResource>),
}

impl<'a> Query<'a> {
    pub fn new(querytype: QueryType, resulttype: Option<ResultType>, name: Option<String>) -> Self {
        Self {
            name,
            querytype,
            resulttype,
            handle: None,
            constraints: Vec::new(),
        }
    }

    pub fn constrain(&mut self, constraint: Constraint<'a>, qualifier: Qualifier) -> &mut Self {
        self.constraints.push((constraint, qualifier));
        self
    }

    /// Iterates over all constraints in the Query
    pub fn iter(&self) -> std::slice::Iter<(Constraint<'a>, Qualifier)> {
        self.constraints.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> std::slice::IterMut<(Constraint<'a>, Qualifier)> {
        self.constraints.iter_mut()
    }
}

impl Variable {
    pub fn is_encoded(&self) -> bool {
        match self {
            Self::Encoded(..) => true,
            Self::Decoded(..) => false,
        }
    }
}

impl<'store> AnnotationStore {
    pub fn query<'a, I>(&'store self, queries: I) -> Result<QueryIter<'store, 'a>, StamError>
    where
        I: IntoIterator<Item = Query<'a>>,
    {
        let mut qi = QueryIter {
            store: self,
            queries: Vec::new(),
            statestack: Vec::new(),
        };
        for query in queries {
            qi.add_query(query)?;
        }
        Ok(qi)
    }
}

impl<'store, 'a> QueryIter<'store, 'a> {
    pub(crate) fn add_query(&mut self, mut query: Query<'a>) -> Result<(), StamError> {
        //Encode existing variables in the query
        for (constraint, qualifier) in query.iter_mut() {
            match constraint {
                Constraint::TextResourceVariable(mut var)
                | Constraint::TextRelationVariable(mut var, _) => {
                    if !var.is_encoded() {
                        var = self.resolve_variable(&var)?;
                    }
                }
                _ => continue,
            }
        }

        query.handle = Some(self.queries.len());
        self.queries.push(query);
        Ok(())
    }

    fn resolve_variable(&self, var: &Variable) -> Result<Variable, StamError> {
        match var {
            Variable::Decoded(name) => {
                for query in self.iter() {
                    if query.name.as_ref() == Some(name) {
                        if let (Some(resulttype), Some(var)) = (query.resulttype, query.handle) {
                            return Ok(Variable::Encoded(resulttype, var));
                        }
                    }
                }
                Err(StamError::UndefinedVariable(name.clone(), ""))
            }
            Variable::Encoded(..) => Ok(var.clone()),
        }
    }

    /// Iterates over all queries
    fn iter(&self) -> std::slice::Iter<Query<'a>> {
        self.queries.iter()
    }
}

impl<'store, 'slf> Iterator for QueryIter<'store, 'slf> {
    type Item = Vec<QueryResultItem<'store>>;
}
