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

pub struct QueryIterator<'store, 'a> {
    store: &'store AnnotationStore,

    queries: Vec<Query<'a>>,

    // note: these capture the current state of the search at any given point in time, they are not accumulators over all results!
    results_textselections: Vec<Option<TextSelectionSet>>,
    results_annotations: Vec<Option<ResultItemSet<'store, Annotation>>>,
    results_resources: Vec<Option<ResultItemSet<'store, TextResource>>>,
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
    pub fn query<'a, I>(&'store self, queries: I) -> Result<QueryIterator<'store, 'a>, StamError>
    where
        I: IntoIterator<Item = Query<'a>>,
    {
        let mut qi = QueryIterator {
            store: self,
            queries: Vec::new(),
            results_textselections: Vec::new(),
            results_annotations: Vec::new(),
            results_resources: Vec::new(),
        };
        for query in queries {
            qi.add_query(query)?;
        }
        Ok(qi)
    }
}

impl<'store, 'a> QueryIterator<'store, 'a> {
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

        //Obtain a handle for this query and reserve an output place
        if let Some(resulttype) = query.resulttype {
            match resulttype {
                ResultType::TextSelection => {
                    self.results_textselections.push(None);
                    query.handle = Some(self.results_textselections.len());
                }
                ResultType::Annotation => {
                    self.results_annotations.push(None);
                    query.handle = Some(self.results_annotations.len());
                }
                ResultType::TextResource => {
                    self.results_resources.push(None);
                    query.handle = Some(self.results_resources.len());
                }
            }
        }

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
