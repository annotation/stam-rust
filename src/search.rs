use std::ops::Deref;

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
    iterator: Option<Box<dyn Iterator<Item = WrappedItem<'store, T>> + 'store>>,
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

pub trait SelectQueryIterator: Iterator
where
    Self::QueryItem: Storable,
{
    type QueryItem;

    /// Initializes the iterator based on the first constraint
    fn init_iterator(&mut self);
}

impl<'store, 'q> SelectQueryIterator for SelectQuery<'store, 'q, TextResource>
where
    'q: 'store,
{
    type QueryItem = TextResource;

    /// Initializes the iterator based on the first constraint
    fn init_iterator(&mut self) {
        if let Some(constraint) = self.constraints.iter().next() {
            match constraint {
                Constraint::FilterData { set, key, value } => {
                    if let Some(iterator) =
                        self.store
                            .find_data(set.clone(), Some(key.clone()), value.clone())
                    //MAYBE TODO: optimize the clones out
                    {
                        let iterator = iterator
                            .map(|data| data.annotations(self.store))
                            .into_iter()
                            .flatten()
                            .flatten()
                            .map(|annotation| annotation.resources())
                            .flatten()
                            .map(|resource| resource.item);
                        self.iterator = Some(Box::new(iterator));
                    }
                }
                _ => !unimplemented!(),
            }
        } else {
            //unconstrained
            let iterator = self.store.resources();
            self.iterator = Some(Box::new(iterator));
        }
    }
}

pub enum Constraint<'q> {
    FilterData {
        set: Item<'q, AnnotationDataSet>,
        key: Item<'q, DataKey>,
        value: DataOperator<'q>,
    },
    Resource(Item<'q, TextResource>),
    //    TextRelation(ItemSet<TextSelection>, TextSelectionOperator),
    //for later:
    //AnnotationRelationIn(SelectionSet<Annotation>),
    //AnnotationRelationOut(SelectionSet<Annotation>),
    //DataSet(Item<'a, AnnotationDataSet>),
}

impl<'store, 'q, T> Iterator for SelectQuery<'store, 'q, T>
where
    T: Storable,
    Self: SelectQueryIterator<QueryItem = T>,
{
    type Item = WrappedItemSet<'store, T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iterator.as_mut() {
                if let Some(item) = iter.next() {
                    //process further constraints:
                    for constraint in self.constraints.iter().skip(1) {}
                } else {
                    return None;
                }
            } else {
                self.init_iterator();
            }
        }
    }
}
