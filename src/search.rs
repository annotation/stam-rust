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
    /// The iterator for the current query
    iterator: SubIter<'store, 'slf>,

    // note: this captures the result of the current state, in order to make it available for subsequent deeper iterators
    result: QueryResultItem<'store>,
}

pub struct QueryIter<'store, 'slf> {
    store: &'store AnnotationStore,

    queries: Vec<Query<'slf>>,

    /// States in the stack hold iterators, each stack item corresponds to one further level of nesting
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

impl<'store, 'slf> QueryIter<'store, 'slf> {
    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    pub(crate) fn add_query(&mut self, mut query: Query<'slf>) -> Result<(), StamError> {
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
    fn iter(&self) -> std::slice::Iter<Query<'slf>> {
        self.queries.iter()
    }

    /// Initializes a new state
    pub fn init_state(&mut self, context: &QueryIter) -> Option<QueryState<'store, 'slf>> {
        let queryindex = context.queries.len();
        let query = context.queries.get(queryindex).expect("query must exist");
        let firstconstraint = query.iter().next();

        //create the subiterator based on the context
        let mut state = match query.resulttype {
            Some(ResultType::TextResource) => match firstconstraint {
                Some((Constraint::AnnotationData { set, key, value }, _qualifier)) => {
                    Some(QueryState {
                        iterator: SubIter::ResourceIter(Box::new(
                            context
                                .store()
                                .find_data(set.clone(), Some(key.clone()), value.clone())
                                .into_iter()
                                .flatten()
                                .map(|data| data.annotations(self.store))
                                .into_iter()
                                .flatten()
                                .flatten()
                                .map(|annotation| annotation.resources())
                                .flatten()
                                .map(|resource| resource.item.into()),
                        )),
                        result: QueryResultItem::None,
                    })
                }
                None => {
                    //unconstrained; all resources
                    Some(QueryState {
                        iterator: SubIter::ResourceIter(Box::new(
                            context.store().resources().map(|resource| resource.into()),
                        )),
                        result: QueryResultItem::None,
                    })
                }
            },
        };
        // Do the first iteration
        if let Some(state) = state.as_mut() {
            state.next(query, context);
            Some(*state)
        } else {
            //if tirst iteration fails, discard the entire state
            None
        }
    }
}

impl<'store, 'slf> QueryState<'store, 'slf> {
    ///Advances the query state, return true if a new result was obtained, false if the iterator ends without yielding a new result
    pub fn next(&mut self, query: &Query<'slf>, context: &QueryIter<'store, 'slf>) -> bool {
        loop {
            match self.iterator {
                SubIter::ResourceIter(mut iter) => {
                    if let Some(result) = iter.next() {
                        let mut constraints_met = true;
                        for (constraint, _qualifier) in query.iter() {
                            if !self.test_constraint(constraint, &result, query, context) {
                                constraints_met = false;
                                break;
                            }
                        }
                        if constraints_met {
                            self.result = QueryResultItem::TextResource(result);
                            return true;
                        }
                    }
                }
            }
        }
    }
}

trait TestConstraint<'store, 'slf, T> {
    fn test_constraint(
        &self,
        constraint: &Constraint<'slf>,
        itemset: &T,
        query: &Query<'slf>,
        context: &QueryIter<'store, 'slf>,
    ) -> bool;
}

impl<'store, 'slf> TestConstraint<'store, 'slf, ResultItemSet<'store, TextResource>>
    for QueryState<'store, 'slf>
{
    fn test_constraint(
        &self,
        constraint: &Constraint,
        itemset: &ResultItemSet<'store, TextResource>,
        query: &Query<'slf>,
        context: &QueryIter<'store, 'slf>,
    ) -> bool {
        //if a single item in an itemset matches, the itemset as a whole is valid
        for item in itemset.iter() {
            match constraint {
                Constraint::AnnotationData { set, key, value } => {
                    if let Some(iter) = item.annotations_metadata() {
                        for annotation in iter {
                            for data in annotation.data() {
                                if context
                                    .store()
                                    .wrap(data.set())
                                    .expect("wrap must succeed")
                                    .test(set)
                                    && data.test(Some(&key), &value)
                                {
                                    return true;
                                }
                            }
                        }
                    }
                }
                _ => unimplemented!(), //todo: remove
            }
        }
        false
    }
}

impl<'store, 'slf> Iterator for QueryIter<'store, 'slf> {
    type Item = Vec<QueryResultItem<'store>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while self.statestack.len() < self.queries.len() {
                if let Some(state) = self.init_state(self) {
                    self.statestack.push(state);
                } else {
                    //no state could be found, we're done
                    return None;
                }
            }

            // get the final/rightmost state (with subiterator) from the stack
            // if there is none: instantiate a new one on the left and call next() on the subiterator
            //    if that fails, there are no more results
            // if there is one: call next() and return the full results for the entire stack,
            //     if the iterator is depleted, remove it and call next() on the superiterator (if that is depleted too, move left)
        }
    }
}
