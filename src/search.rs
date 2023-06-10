use crate::datavalue::DataOperator;
use crate::error::StamError;
use crate::types::*;
use crate::{
    AnnotationDataHandle, AnnotationDataSetHandle, AnnotationHandle, AnnotationStore,
    DataKeyHandle, TextResourceHandle, TextSelection, TextSelectionOperator,
    TextSelectionOperatorKind,
};
use smallvec::SmallVec;
use std::iter::IntoIterator;
use std::slice::Iter;

pub enum Query<'q> {
    /// Maps to to search_textselections call
    TextSelections(Option<TextSelectionOperator>, Vec<Constraint<'q>>),
    /// Maps to to search_annotations call
    Annotations(Vec<Constraint<'q>>),
    /// Maps to to search_resources call
    Resources(Vec<Constraint<'q>>),
}

pub enum Constraint<'q> {
    /// Constrain by annotation selection set
    ///
    /// * When applied to annotation, this matches all annotations that are referenced by an annotation in the set
    /// * When applied to resources, textselections, or datasets, it constrains the annotations that reference that resource/textselection
    Annotation(&'q AnnotationSelectionSet),

    /// Constrain by annotation selection set
    ///
    /// * When applied to annotation, this match all annotations that point to an annotation in the set
    /// * When applied to resources, textselections, or datasets, the annotations point to BOTH the resource/textselection/dataset, as well as to any of these other annotations
    ToAnnotationSelection(&'q AnnotationSelectionSet),

    /// Constrain by resource selection set
    /// When applied to annotation, this match all annotations that reference a resource in the set
    Resource(&'q ResourceSelectionSet),

    /// Constrain by text selection set (known a priori, otherwise Query:TextSelections is used
    TextSelection(Option<TextSelectionOperator>, &'q TextSelectionSet),

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
    /// Constrains by annotations that have this associated data
    /// The reference value to test against is embedded within the [`DataOperator`].
    AnnotationData {
        dataset: Item<'q, AnnotationDataSet>,
        key: Item<'q, DataKey>,
        operator: DataOperator<'q>,
    },

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
}

// Generic type for SelectionSets (not used by TextSelectionSet)
#[derive(Clone)]
pub enum SelectionSet<T>
where
    T: Storable,
{
    // TODO: Let the selection set directly references a slice in the reverse index (there are no copies)
    //Borrowed(&'a [T::HandleType]),
    Owned(SmallVec<[T::HandleType; 8]>),
}

type AnnotationSelectionSet = SelectionSet<Annotation>;
type DataSetSelectionSet = SelectionSet<AnnotationDataSet>;
type ResourceSelectionSet = SelectionSet<TextResource>;
//type DataSelectionSet<'a> = SelectionSet<'a, AnnotationData>;

impl<T> SelectionSet<T>
where
    T: Storable,
{
    /*
    // TODO: use proper ToOwned trait
    pub fn to_owned(mut self) -> Self {
        match self {
            Self::Borrowed(v) => self = Self::Owned(v.iter().map(|x| *x).collect()),
            Self::Owned(v) => Self::Owned(v.iter().map(|x| *x).collect()),,
        }
    }
    */

    pub fn add<'q, 'store>(&mut self, item: Item<'q, T>, store: &'store AnnotationStore) {
        if let Some(handle) = item.to_handle(store) {
            self.0.push(handle);
        }
    }
}

impl<T> SelectionSet<T>
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
*/

impl AnnotationStore {
    pub fn search_textselections<'store, 'q>(
        &'store self,
        constraints: &'q [Constraint<'q>],
    ) -> SearchTextSelectionIter<'store, 'q> {
        SearchTextSelectionIter {
            store: self,
            stack: constraints
                .iter()
                .map(|c| SearchStack::Constraint(c))
                .collect(),
        }
    }
}

pub struct SearchTextSelectionIter<'store, 'q> {
    store: &'store AnnotationStore,
    stack: SmallVec<[SearchStack<'q>; 10]>,
}

enum SearchStack<'q> {
    Constraint(&'q Constraint<'q>),
    AnnotationIter(Iter<'q, AnnotationHandle>),
    TextSelectionIter(Iter<'q, TextSelection>),
}

impl<'store, 'q> Iterator for SearchTextSelectionIter<'store, 'q> {
    type Item = (TextSelection, TextResourceHandle, AnnotationHandle);

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
