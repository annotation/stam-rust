use smallvec::{smallvec, SmallVec};
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

pub struct QueryState<'store, 'q> {
    /// The query this state pertains too
    queryindex: usize,

    /// The iterator for the current query
    iterator: SubIter<'store, 'q>,

    // note: this captures the result of the current state, in order to make it available for subsequent deeper iterators
    result: QueryResultItem<'store>,
}

pub struct QueryIter<'store, 'q> {
    store: &'store AnnotationStore,

    queries: Vec<Query<'q>>,

    /// States in the stack hold iterators, each stack item corresponds to one further level of nesting
    statestack: Vec<QueryState<'store, 'q>>,

    /// Signals that we're done with the entire stack
    done: bool,
}

/// Abstracts over different types of subiterators we may encounter during querying, the types are names after the type they return.
pub enum SubIter<'store, 'q> {
    ResourceIter(Box<dyn Iterator<Item = ResultItemSet<'store, TextResource>> + 'q>),
    AnnotationIter(Box<dyn Iterator<Item = ResultItemSet<'store, Annotation>> + 'q>),
    TextSelectionIter(Box<dyn Iterator<Item = TextSelectionSet> + 'q>),
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
    pub fn query<'a, I>(&'store self, queries: I) -> Result<QueryIter<'store, 'a>, StamError>
    where
        I: IntoIterator<Item = Query<'a>>,
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

impl<'store, 'q> QueryIter<'store, 'q> {
    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }

    pub(crate) fn add_query(&mut self, mut query: Query<'q>) -> Result<(), StamError> {
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
    fn iter(&self) -> std::slice::Iter<Query<'q>> {
        self.queries.iter()
    }

    /// Initializes a new state
    pub fn init_state(&mut self) -> Option<QueryState<'store, 'q>> {
        let queryindex = self.queries.len();
        let query = self.queries.get(queryindex).expect("query must exist");
        let firstconstraint = query.iter().next();

        //create the subiterator based on the context
        let mut state = match query.resulttype {
            Some(ResultType::TextResource) => match firstconstraint {
                Some((Constraint::AnnotationData { set, key, value }, _qualifier)) => {
                    //Get resources by annotationdata
                    Some(QueryState {
                        queryindex,
                        iterator: SubIter::ResourceIter(Box::new(
                            self.store()
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
                        queryindex,
                        iterator: SubIter::ResourceIter(Box::new(
                            self.store().resources().map(|resource| resource.into()),
                        )),
                        result: QueryResultItem::None,
                    })
                }
                _ => unimplemented!("Constraint not implemented for target TextResource yet"),
            },
            None => return None,
        };
        // Do the first iteration
        if let Some(mut state) = state {
            state.next(self);
            Some(state)
        } else {
            //if tirst iteration fails, discard the entire state
            None
        }
    }

    /// advance the rightmost iterator on the state
    /// remove it if it's depleted
    pub fn prepare_next(&mut self) -> bool {
        loop {
            if let Some(state) = self.statestack.last_mut() {
                //iterate the rightmost stack item and try again
                if state.next(self) {
                    return true;
                }
            } else {
                //stack is empty, and we have no results, we're done
                return false;
            }
            //remove the last item
            self.statestack.pop();
        }
    }

    pub(crate) fn build_result(&self) -> QueryResultItems<'store> {
        let mut items = SmallVec::new();
        for stackitem in self.statestack.iter() {
            items.push(stackitem.result.clone());
        }
        QueryResultItems { items }
    }
}

impl<'store, 'q> QueryState<'store, 'q> {
    pub fn query<'a>(&self, context: &'a QueryIter<'store, 'q>) -> &'a Query<'q> {
        context
            .queries
            .get(self.queryindex)
            .expect("queryindex must be valid")
    }

    ///Advances the query state, return true if a new result was obtained (stored in the state's result buffer),
    // and false if the iterator ends without yielding a new result
    pub fn next(&mut self, context: &QueryIter<'store, 'q>) -> bool {
        loop {
            match &mut self.iterator {
                SubIter::ResourceIter(ref mut iter) => {
                    if let Some(result) = iter.next() {
                        let mut constraints_met = true;
                        for (constraint, _qualifier) in self.query(context).iter() {
                            if !self.test_constraint(constraint, &result, context) {
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
                _ => unimplemented!("further iterators not implemented yet"), //TODO
            }
        }
    }
}

trait TestConstraint<'store, 'slf, T> {
    fn test_constraint(
        &self,
        constraint: &Constraint<'slf>,
        itemset: &T,
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

impl<'store, 'q> Iterator for QueryIter<'store, 'q> {
    type Item = QueryResultItems<'store>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            //iterator has been marked as done, do nothing else
            return None;
        }

        //populate the entire stack, producing a result at each level
        while self.statestack.len() < self.queries.len() {
            if let Some(state) = self.init_state() {
                self.statestack.push(state); //the state will hold a result for that level
            } else if !self.prepare_next() {
                //if we didn't succeed in preparing the next iteration, it means the entire stack is depleted and we're done
                return None;
            }
        }

        //read the result in the stack's result buffer
        let result = self.build_result();

        // prepare the result buffer for next iteration
        if !self.prepare_next() {
            //no results next time? mark as done (otherwise the iterator would restart from scratch again)
            self.done = true;
        }

        return Some(result);
    }
}

impl<'store> QueryResultItems<'store> {}
