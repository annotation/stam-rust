use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::TextResource;
use crate::store::*;
use crate::textselection::TextSelection;
use crate::types::*;

pub struct SelectQuery<'store, 'q, T>
where
    T: Storable,
{
    store: &'store AnnotationStore,
    constraints: Vec<Constraint<'q>>,
    iterator: Option<Box<dyn Iterator<Item = WrappedItem<'store, T>>>>,
}

impl<'store, 'q, T> SelectQuery<'store, 'q, T>
where
    T: Storable,
{
    pub fn new(store: &'store AnnotationStore) -> Self {
        Self {
            store,
            constraints: Vec::new(),
            iterator: None,
        }
    }

    pub fn constrain(&mut self, constraint: Constraint<'q>) -> &mut Self {
        self.constraints.push(constraint);
        self
    }
}

pub trait SelectQueryIterator<'store>: Iterator
where
    Self::QueryItem: Storable,
{
    type QueryItem;

    /// Returns the iterator
    fn iterator(&self) -> Option<Box<dyn Iterator<Item = WrappedItem<'store, Self::QueryItem>>>>;
    fn init_iterator(&mut self);
}

pub enum Constraint<'q> {
    FilterData {
        set: Option<Item<'q, AnnotationDataSet>>,
        key: Option<Item<'q, DataKey>>,
        value: DataOperator<'q>,
    },
    Resource(Item<'q, TextResource>),
    TextRelation(ItemSet<TextSelection>, TextSelectionOperator),
    //for later:
    //AnnotationRelationIn(SelectionSet<Annotation>),
    //AnnotationRelationOut(SelectionSet<Annotation>),
    //DataSet(Item<'a, AnnotationDataSet>),
}

impl<'store, 'q, T> Iterator for SelectQuery<'store, 'q, T>
where
    T: Storable,
    Self: SelectQueryIterator<'store, QueryItem = T>,
{
    type Item = WrappedItemSet<'store, T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iterator().as_mut() {
            } else {
                self.init_iterator();
            }
        }
    }
}
