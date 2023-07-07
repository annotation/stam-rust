use smallvec::{smallvec, SmallVec};

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

pub struct Query<'q> {
    name: Option<String>,
    querytype: QueryType,
    resulttype: Option<ResultType>,
    handle: Option<VarHandle>,
    constraints: Vec<(Constraint<'q>, Qualifier)>,
}

#[derive(Clone, Copy, Debug)]
// TODO: implement handling of qualifiers
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

pub enum Constraint<'q> {
    AnnotationData {
        set: RequestItem<'q, AnnotationDataSet>,
        key: RequestItem<'q, DataKey>,
        value: DataOperator<'q>,
    },
    TextResource(RequestItemSet<'q, TextResource>),
    TextResourceVariable(Variable),

    TextRelation(RequestItemSet<'q, TextSelection>, TextSelectionOperator),
    TextRelationVariable(Variable, TextSelectionOperator),
}

pub struct QueryState<'store> {
    /// The iterator for the current query
    iterator: SubIter<'store>,

    // note: this captures the result of the current state, in order to make it available for subsequent deeper iterators
    result: QueryResultItem<'store>,
}

pub struct QueryIter<'store> {
    store: &'store AnnotationStore,

    queries: Vec<Query<'store>>,

    /// States in the stack hold iterators, each stack item corresponds to one further level of nesting
    statestack: Vec<QueryState<'store>>,

    /// Signals that we're done with the entire stack
    done: bool,
}

/// Abstracts over different types of subiterators we may encounter during querying, the types are names after the type they return.
pub enum SubIter<'store> {
    ResourceIter(Box<dyn Iterator<Item = ResultItemSet<'store, TextResource>> + 'store>),
    AnnotationIter(Box<dyn Iterator<Item = ResultItemSet<'store, Annotation>> + 'store>),
    TextSelectionIter(Box<dyn Iterator<Item = TextSelectionSet> + 'store>),
}

#[derive(Clone, Debug)]
pub enum QueryResultItem<'store> {
    None,
    TextSelection(TextSelectionSet),
    Annotation(ResultItemSet<'store, Annotation>),
    TextResource(ResultItemSet<'store, TextResource>),
}

/// Represents an entire result row, each result stems from a query
pub struct QueryResultItems<'store> {
    items: SmallVec<[QueryResultItem<'store>; 12]>,
}

impl<'q> Query<'q> {
    pub fn new(querytype: QueryType, resulttype: Option<ResultType>, name: Option<String>) -> Self {
        Self {
            name,
            querytype,
            resulttype,
            handle: None,
            constraints: Vec::new(),
        }
    }

    pub fn constrain(&mut self, constraint: Constraint<'q>, qualifier: Qualifier) -> &mut Self {
        self.constraints.push((constraint, qualifier));
        self
    }

    /// Iterates over all constraints in the Query
    pub fn iter(&self) -> std::slice::Iter<(Constraint<'q>, Qualifier)> {
        self.constraints.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> std::slice::IterMut<(Constraint<'q>, Qualifier)> {
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
    /// Instantiates a query, returns an iterator.
    /// No actual querying is done yet until you use the iterator.
    pub fn query<I>(&'store self, queries: I) -> Result<QueryIter<'store>, StamError>
    where
        I: IntoIterator<Item = Query<'store>>,
    {
        let mut qi = QueryIter {
            store: self,
            queries: Vec::new(),
            statestack: Vec::new(),
            done: false,
        };
        for query in queries {
            qi.add_query(query)?;
        }
        Ok(qi)
    }
}

impl<'store> QueryIter<'store> {
    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    pub(crate) fn add_query(&mut self, mut query: Query<'store>) -> Result<(), StamError> {
        //Encode existing variables in the query
        for (constraint, qualifier) in query.iter_mut() {
            match constraint {
                Constraint::TextResourceVariable(var)
                | Constraint::TextRelationVariable(var, _) => {
                    if !var.is_encoded() {
                        *var = self.resolve_variable(&var)?;
                    }
                }
                _ => continue,
            }
        }
        self.queries.push(query);
        Ok(())
    }

    fn resolve_variable(&self, var: &Variable) -> Result<Variable, StamError> {
        match var {
            Variable::Decoded(name) => {
                for (i, query) in self.iter().enumerate() {
                    if query.name.as_ref() == Some(name) {
                        if let Some(resulttype) = query.resulttype {
                            return Ok(Variable::Encoded(resulttype, i));
                        }
                    }
                }
                Err(StamError::UndefinedVariable(name.clone(), ""))
            }
            Variable::Encoded(..) => Ok(var.clone()),
        }
    }

    /// Iterates over all queries
    fn iter(&self) -> std::slice::Iter<Query<'store>> {
        self.queries.iter()
    }

    /// Initializes a new state
    pub fn init_state(&mut self) -> bool {
        let queryindex = self.queries.len();
        let query = self.queries.get(queryindex).expect("query must exist");
        let firstconstraint = query.iter().next();
        let store = self.store();

        //create the subiterator based on the context
        let state = match query.resulttype {
            Some(ResultType::TextResource) => match firstconstraint {
                Some((Constraint::AnnotationData { set, key, value }, _qualifier)) => {
                    //Get resources by annotationdata
                    Some(QueryState {
                        iterator: SubIter::ResourceIter(Box::new(
                            store
                                .find_data(set.clone(), Some(key.clone()), value.clone())
                                .into_iter()
                                .flatten()
                                .map(|data| data.annotations(store))
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
                            store.resources().map(|resource| resource.into()),
                        )),
                        result: QueryResultItem::None,
                    })
                }
                _ => unimplemented!("Constraint not implemented for target TextResource yet"),
            },
            Some(ResultType::TextSelection) => match firstconstraint {
                Some((Constraint::AnnotationData { set, key, value }, _qualifier)) => {
                    //Get text selections by annotationdata
                    Some(QueryState {
                        iterator: SubIter::TextSelectionIter(Box::new(
                            store
                                .find_data(set.clone(), Some(key.clone()), value.clone())
                                .into_iter()
                                .flatten()
                                .map(|data| data.annotations(store))
                                .into_iter()
                                .flatten()
                                .flatten()
                                .map(|annotation| annotation.textselections().collect()),
                        )),
                        result: QueryResultItem::None,
                    })
                }
                None => {
                    //unconstrained; all text selections in all resources (arbitrary order!)
                    Some(QueryState {
                        iterator: SubIter::TextSelectionIter(Box::new(
                            store
                                .resources()
                                .map(|resource| {
                                    resource
                                        .textselections()
                                        .map(|textselection| textselection.into())
                                })
                                .flatten(),
                        )),
                        result: QueryResultItem::None,
                    })
                }
                _ => unimplemented!("Constraint not implemented for target TextResource yet"),
            },
            Some(ResultType::Annotation) => todo!(),
            None => return false,
        };
        if let Some(state) = state {
            self.statestack.push(state);
        } else {
            return false;
        }
        // Do the first iteration (may remove this and other elements from the stack again if it fails)
        self.next_state()
    }

    /// Advances the query state on the stack, return true if a new result was obtained (stored in the state's result buffer),
    /// Pops items off the stack if they no longer yield result.
    /// If no result at all can be obtained anymore, false is returned.
    pub fn next_state(&mut self) -> bool {
        while !self.statestack.is_empty() {
            let queryindex = self.statestack.len() - 1;
            let store = self.store();
            if let Some(state) = self.statestack.last_mut() {
                let query = self.queries.get(queryindex).expect("query must exist");
                loop {
                    match &mut state.iterator {
                        SubIter::ResourceIter(ref mut iter) => {
                            if let Some(result) = iter.next() {
                                let mut constraints_met = true;
                                for (constraint, _qualifier) in query.iter() {
                                    if !constraint.test(store, &result) {
                                        constraints_met = false;
                                        break;
                                    }
                                }
                                if constraints_met {
                                    state.result = QueryResultItem::TextResource(result);
                                    return true;
                                }
                            } else {
                                break; //iterator depleted, break to pop state from stack
                            }
                        }
                        _ => unimplemented!("further iterators not implemented yet"), //TODO
                    }
                }
            }
            self.statestack.pop();
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
}

trait TestConstraint<'store, T> {
    fn test(&self, store: &'store AnnotationStore, itemset: &T) -> bool;
}

impl<'store> TestConstraint<'store, ResultItemSet<'store, TextResource>> for Constraint<'store> {
    fn test(
        &self,
        store: &'store AnnotationStore,
        itemset: &ResultItemSet<'store, TextResource>,
    ) -> bool {
        //if a single item in an itemset matches, the itemset as a whole is valid
        for item in itemset.iter() {
            match self {
                Constraint::AnnotationData { set, key, value } => {
                    if let Some(iter) = item.annotations_metadata() {
                        for annotation in iter {
                            for data in annotation.data() {
                                if store.wrap(data.set()).expect("wrap must succeed").test(set)
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

impl<'store> Iterator for QueryIter<'store> {
    type Item = QueryResultItems<'store>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            //iterator has been marked as done, do nothing else
            return None;
        }

        //populate the entire stack, producing a result at each level
        while self.statestack.len() < self.queries.len() {
            if !self.init_state() {
                //if we didn't succeed in preparing the next iteration, it means the entire stack is depleted and we're done
                return None;
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
    pub fn iter(&self) -> impl Iterator<Item = &QueryResultItem<'store>> {
        self.items.iter()
    }

    pub fn get(&self, index: usize) -> Option<&QueryResultItem<'store>> {
        self.items.get(index)
    }
}
