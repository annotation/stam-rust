use crate::error::StamError;
use crate::types::*;
use crate::{
    Annotation, AnnotationData, AnnotationDataHandle, AnnotationDataSet, AnnotationDataSetHandle,
    AnnotationHandle, AnnotationStore, DataKey, DataKeyHandle, DataOperator, Item, Storable,
    TextResource, TextResourceHandle, TextSelection, TextSelectionHandle, TextSelectionOperator,
};
use smallvec::SmallVec;
use std::iter::IntoIterator;
use std::slice::Iter;

pub struct Query<'q> {
    subqueries: &'q [SelectQuery<'q>],

    /// subquery b depends on a (a goes first), this only lists the dependencies used for nesting
    nesting: Vec<(usize, usize)>,
}

impl<'q> Query<'q> {
    pub fn new(subqueries: &'q [SelectQuery<'q>]) -> Self {
        //TODO: compute nesting
        Self {
            subqueries,
            nesting: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SelectQueryType {
    Text,
    Data,
    Annotation,
    Resource,
}

pub struct SelectQuery<'q> {
    tp: SelectQueryType,

    /// Name for the Result Set
    varname: Option<&'q str>,

    //Constraints
    constraints: Vec<Constraint<'q>>,
}

impl<'q> SelectQuery<'q> {
    pub fn new(tp: SelectQueryType, varname: &'q str, constraints: Vec<Constraint<'q>>) -> Self {
        Self {
            tp,
            varname: None,
            constraints,
        }
    }
    pub fn constrain(&mut self, constraint: Constraint<'q>) -> &mut Self {
        self.constraints.push(constraint);
        self
    }
}

trait QueryEval<'store, 'q, T>
where
    T: Storable,
{
    /// Evaluate the Select Query (lazily!), returns an iterator
    fn eval(&self, context: &Context<'store, 'q>) -> Option<dyn Iterator<Item = T::HandleType>>;
}

impl<'store, 'q, T> QueryEval<'store, 'q, T> for SelectQuery<'q>
where
    T: Storable,
{
    fn eval(&self, context: &Context<'store, 'q>) -> Option<dyn Iterator<Item = T::HandleType>> {
        let mut iter = None;
        for constraint in self.constraints.iter() {
            iter = constraint.eval(iter, context);
            if iter.is_none() {
                return iter;
            }
        }
        iter
    }
}

trait ConstraintEval<'store, 'q, T>
where
    T: Storable,
{
    /// Evaluate the constraint (lazily!), returns an iterator derived from the input iterator
    fn eval(
        &self,
        iter: Option<dyn Iterator<Item = T::HandleType>>,
        context: &Context<'store, 'q>,
    ) -> Option<dyn Iterator<Item = T::HandleType>>;
}

impl<'store, 'q> ConstraintEval<'store, 'q, Annotation> for Constraint<'q> {
    fn eval(
        &self,
        iter: Option<dyn Iterator<Item = AnnotationHandle>>,
        context: &Context<'store, 'q>,
    ) -> Option<dyn Iterator<Item = AnnotationHandle>> {
        match self {
            Constraint::AnnotationData {
                dataset,
                key,
                operator,
            } => {
                if let Some(dataset) = context.store.annotationset(dataset) {
                    if iter.is_none() {
                        //first constraint, create iterator: AnnotationData -> Annotation
                        return dataset
                            .find_data(Some(key), operator)
                            .into_iter()
                            .flatten()
                            .map(|data| data.annotations(context.store).into_iter().flatten())
                            .flatten();
                    } else {
                        //filter existing iterator
                        if let Some(key) = dataset.key(key) {
                            return iter.into_iter().flatten().filter(|annotationhandle| {
                                if let Some(annotation) = context.store.annotation(annotationhandle)
                                {
                                    for data in annotation.data() {
                                        if data.key() == key.unwrap() {
                                            return data.test(operator);
                                        }
                                    }
                                }
                                false
                            });
                        }
                    }
                }
            }
        }
        return None;
    }
}

pub struct Context<'store, 'q> {
    store: &'store AnnotationStore,
    varnames: Vec<&'q str>,
    //v--- ID, resolved via varnames
    textselections: Vec<(usize, ResultSet<TextSelection>)>,
    annotations: Vec<(usize, ResultSet<Annotation>)>,
    data: Vec<(usize, ResultSet<AnnotationData>)>,
    resources: Vec<(usize, ResultSet<TextResource>)>,
    datasets: Vec<(usize, ResultSet<AnnotationDataSet>)>,
}

impl<'store, 'q> Context<'store, 'q> {
    pub fn new(store: &'store AnnotationStore) -> Self {
        Self {
            store: annotationstore,
            varnames: Vec::new(),
            textselections: Vec::new(),
            annotations: Vec::new(),
            data: Vec::new(),
            resources: Vec::new(),
            datasets: Vec::new(),
        }
    }
}

// Generic type for ResultSet
pub enum ResultSet<T>
where
    T: Storable,
{
    Empty,
    Vec(SmallVec<[T::HandleType; 8]>),
    Iterator(Iterator<Item = T::HandleType>),
    // MAYBE TODO: Let the selection set directly references a slice in the reverse index (there are no copies)?
    //Borrowed(&'a [T::HandleType]),
}

impl<T> ResultSet<T>
where
    T: Storable,
{
    /// Consumes the result set, if it was an iterator, no-op if it already was
    pub fn collect(mut self) -> Self {
        match self {
            Self::Vec(..) => self,
            Self::Iterator(iter) => Self::Vec(iter.collect()),
        }
    }

    pub fn is_empty(&self) -> bool {
        if let Self::Empty = self {
            true
        } else if Self::Vec(v) = self {
            v.is_empty()
        } else {
            false
        }
    }

    /// Add an item to the result set
    pub fn add<'q>(mut self, item: Item<'q, T>, store: &AnnotationStore) -> Self {
        self = self.collect();
        if let Some(handle) = item.to_handle(store) {
            self.0.push(handle);
        }
        self
    }
}

impl<T> ResultSet<T>
where
    T: Storable,
{
    pub fn iter(&self) -> Iter<T::HandleType> {
        match self {
            //Self::Borrowed(v) => v.iter(),
            Self::Owned(v) => v.iter(),
        }
    }
}

impl<'store, 'q> Iterator for Query<'q> {
    type Item = QueryResults<'store, 'q>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut popstack = false;
        let mut newstackitem: Option<SearchStack> = None;
        if let Some(stackitem) = self.stack.last() {
            match stackitem {
                SearchStack::Constraint(constraint) => {
                    //obtain an iterator for the constraint and add it to the stack
                    match constraint {
                        Constraint::Annotation(selectionset) => {
                            newstackitem = Some(SearchStack::AnnotationIter(selectionset.iter()));
                            popstack = true;
                        }
                    }
                }
                SearchStack::AnnotationIter(iter) => {
                    if let Some(annotationhandle) = iter.next() {
                        let annotation: &Annotation = self.store.get(annotationhandle);
                        self.store.textselections_by_annotation(annotationhandle);
                    } else {
                        //iterator is depleted
                        popstack = true;
                    }
                }
            }
        }
        if popstack {
            self.stack.pop();
        }
        if let Some(newstackitem) = newstackitem {
            self.stack.push(newstackitem);
            self.next() //recurse
        } else {
            None
        }
    }
}

pub enum Constraint<'q> {
    /// Constrains by annotations that have this associated data
    /// The reference value to test against is embedded within the [`DataOperator`].
    AnnotationData {
        dataset: Item<'q, AnnotationDataSet>,
        key: Item<'q, DataKey>,
        operator: DataOperator<'q>,
    },
    /*
    TextRelation(TextSelectionOperator, &'q TextSelectionSet),

    /// Constrain by annotation selection set
    ///
    /// * When applied to annotation, this matches all annotations that are referenced by an annotation in the set
    /// * When applied to resources, textselections, or datasets, it constrains the annotations that reference that resource/textselection
    Annotation(&'q AnnotationSelectionSet),

    AnnotationVar(&'q str),

    /// Constrain by annotation selection set
    ///
    /// * When applied to annotation, this match all annotations that point to an annotation in the set
    /// * When applied to resources, textselections, or datasets, the annotations point to BOTH the resource/textselection/dataset, as well as to any of these other annotations
    ToAnnotationSelection(&'q AnnotationSelectionSet),

    /// Constrain by resource selection set
    /// When applied to annotation, this match all annotations that reference a resource in the set
    Resource(&'q ResourceSelectionSet),

    ResourceVar(&'q str),

    /// Constrain by text selection set in subject position (known a priori, otherwise Query:TextSelections is used)
    TextSelection(&'q TextSelectionSet, Option<TextSelectionOperator>),
    /// Constrain by text selection set in object position (known a priori, otherwise Query:TextSelections is used)
    TextSelectionInv(Option<TextSelectionOperator>, &'q TextSelectionSet),

    /// Annotation Set
    AnnotationDataSet(Item<'q, AnnotationDataSet>),

    /*
    /// Text Selection
    /// (the actual TextSelectionSet is passed as part of the TextSelectionOperator)
    /// The first item of the pair may constrain to specific resources
    /// The second item of the pair may constrain to specific TextSelections, TextSelectionSets, mediated by a specific TextSelectionOperator.
    /// If both items are unset (None), then this will constrain to any annotations that references text (i.e. with a textselector)
    TextSelection {
        resources: Option<&'a ResourceSelectionSet<'a>>,
        operator: Option<TextSelectionOperator>,
    },
    */
    /// Constrains by annotations that use this key (regardless of value)
    DataKey {
        dataset: Item<'q, AnnotationDataSet>,
        key: Item<'q, DataKey>,
    },

    /// Constrains by exact text of the textselection
    Text(&'q str),

    ResourceSelector(Option<Item<'q, TextResource>>),

    And(Vec<Constraint<'q>>),
    Or(Vec<Constraint<'q>>),
    Not(Box<Constraint<'q>>),
    */
}

/*
impl<'a, T> IntoIterator for SelectionSet<'a, T> {
    type Item = T;
    type IntoIter = smallvec::IntoIter<[T; 8]>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Borrowed(v) => v.to_owned().into_iter(), //makes a copy!
            Self::Owned(v) => v.into_iter(),
        }
    }
}

impl AnnotationStore {
    pub fn query<'store, 'q>(&'store self, query: Query<'store, 'q>) -> QueryIter<'store, 'q> {
        QueryIter {
            store: self,
            stack: constraints
                .iter()
                .map(|c| SearchStack::Constraint(c))
                .collect(),
        }
    }
}

pub struct QueryIter<'store, 'q> {
    store: &'store AnnotationStore,

    stack: SmallVec<[SearchStack<'q>; 10]>,
}

enum SearchStack<'q> {
    Constraint(&'q Constraint<'q>),
    AnnotationIter(Iter<'q, AnnotationHandle>),
    TextSelectionIter(Iter<'q, TextSelection>),
}

pub enum QueryResult<'store, 'q> {
    Annotation(Option<&'q str>, &'store Annotation),
    Resource(Option<&'q str>, &'store Resource),
    TextSelection(Option<&'q str>, &'store TextSelection), //may need to be TextSelections with a SmallVec
}

pub struct QueryResults<'store, 'q> {
    row: Vec<QueryResult<'q, 'store>>,
}

impl<'q, 'store> QueryResults<'q, 'store> {
    pub fn annotation(var: &str) -> Option<&'store Annotation> {
        for field in self.row.iter() {
            if let QueryResult::Annotation(var2, annotation) = field {
                if var == var2 {
                    return Some(annotation);
                }
            }
        }
        None
    }
    pub fn textselection(var: &str) -> Option<&'store TextSelection> {}
    pub fn resource(var: &str) -> Option<&'store Resource> {}
}

impl<'store, 'q> Iterator for QueryIter<'store, 'q> {
    type Item = QueryResults<'store, 'q>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut popstack = false;
        let mut newstackitem: Option<SearchStack> = None;
        if let Some(stackitem) = self.stack.last() {
            match stackitem {
                SearchStack::Constraint(constraint) => {
                    //obtain an iterator for the constraint and add it to the stack
                    match constraint {
                        Constraint::Annotation(selectionset) => {
                            newstackitem = Some(SearchStack::AnnotationIter(selectionset.iter()));
                            popstack = true;
                        }
                    }
                }
                SearchStack::AnnotationIter(iter) => {
                    if let Some(annotationhandle) = iter.next() {
                        let annotation: &Annotation = self.store.get(annotationhandle);
                        self.store.textselections_by_annotation(annotationhandle);
                    } else {
                        //iterator is depleted
                        popstack = true;
                    }
                }
            }
        }
        if popstack {
            self.stack.pop();
        }
        if let Some(newstackitem) = newstackitem {
            self.stack.push(newstackitem);
            self.next() //recurse
        } else {
            None
        }
    }
}
*/
