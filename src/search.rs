use std::ops::Deref;

use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::TextResource;
use crate::store::*;
use crate::textselection::{TextSelection, TextSelectionOperator};
use crate::types::*;

pub struct SelectQuery<'store, T>
where
    T: Storable,
{
    store: &'store AnnotationStore,
    constraints: Vec<Constraint<'store>>,
    //                  v----- dyn: iterator can be of various types at run-time
    //              v--- needed to make things Sized, we have ownership over the iterator
    iterator: Option<Box<dyn Iterator<Item = WrappedItemSet<'store, T>> + 'store>>,
    //                                        ^---- We used a ItemSet instead of just an Item because we may want to retain groupings of results
}

impl<'store, T> SelectQuery<'store, T>
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

    pub fn constrain(&mut self, constraint: Constraint<'store>) -> &mut Self {
        self.constraints.push(constraint);
        self
    }

    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }
}

/// This internal trait abstracts over some common methods used by the Iterator implementation for SelectQuery
trait SelectQueryIterator<'store>
where
    Self::QueryItem: Storable,
{
    type QueryItem;

    /// Initializes the iterator based on the first constraint
    fn init_iterator(&mut self);

    /// Tests a retrieved item against remaining constraints
    fn test_itemset(
        &self,
        constraint: &Constraint,
        item: &WrappedItemSet<'store, Self::QueryItem>,
    ) -> bool;
}

pub enum Constraint<'a> {
    AnnotationData {
        set: Item<'a, AnnotationDataSet>,
        key: Item<'a, DataKey>,
        value: DataOperator<'a>,
    },
    TextResource(ItemSet<'a, TextResource>),
    TextRelation(ItemSet<'a, TextSelection>, TextSelectionOperator),
    //for later:
    //AnnotationRelationIn(SelectionSet<Annotation>),
    //AnnotationRelationOut(SelectionSet<Annotation>),
    //DataSet(Item<'a, AnnotationDataSet>),
}

impl<'store, T> Iterator for SelectQuery<'store, T>
where
    T: Storable,
    Self: SelectQueryIterator<'store, QueryItem = T>,
{
    type Item = WrappedItemSet<'store, T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iterator.as_mut() {
                if let Some(item) = iter.next() {
                    //process further constraints:
                    let mut constraints_met = true;
                    for constraint in self.constraints.iter().skip(1) {
                        if !self.test_itemset(constraint, &item) {
                            constraints_met = false;
                            break;
                        }
                    }
                    if constraints_met {
                        return Some(item.into());
                    }
                } else {
                    return None;
                }
            } else {
                self.init_iterator();
            }
        }
    }
}

impl<'store> SelectQueryIterator<'store> for SelectQuery<'store, TextResource> {
    type QueryItem = TextResource;

    /// Initializes the iterator based on the first constraint
    fn init_iterator(&mut self) {
        if let Some(constraint) = self.constraints.iter().next() {
            match constraint {
                Constraint::AnnotationData { set, key, value } => {
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
                            .map(|resource| WrappedItemSet::new(resource.item));
                        self.iterator = Some(Box::new(iterator));
                    }
                }
                _ => unimplemented!(), //TODO: remove
            }
        } else {
            //unconstrained: all resources
            let iterator = self
                .store
                .resources()
                .map(|resource| WrappedItemSet::new(resource));
            self.iterator = Some(Box::new(iterator));
        }
    }

    fn test_itemset(
        &self,
        constraint: &Constraint,
        itemset: &WrappedItemSet<TextResource>,
    ) -> bool {
        //if a single item in an itemset matches, the itemset as a whole is valid
        for item in itemset.iter() {
            match constraint {
                Constraint::AnnotationData { set, key, value } => {
                    if let Some(iter) = item.annotations_metadata() {
                        for annotation in iter {
                            for data in annotation.data() {
                                if self
                                    .store
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
                _ => unimplemented!(), //TODO: remove
            }
        }
        false
    }
}

impl<'store> SelectQueryIterator<'store> for SelectQuery<'store, TextSelection> {
    type QueryItem = TextSelection;

    /// Initializes the iterator based on the first constraint
    fn init_iterator(&mut self) {
        if let Some(constraint) = self.constraints.iter().next() {
            match constraint {
                Constraint::AnnotationData { set, key, value } => {
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
                            .map(|annotation| {
                                let itemset: WrappedItemSet<'store, TextSelection> =
                                    annotation.textselections().collect();
                                itemset
                            });
                        self.iterator = Some(Box::new(iterator));
                    }
                }
                Constraint::TextResource(itemset) => {
                    let iterator = self
                        .store
                        .resources_filtered(itemset.clone())
                        .map(|resource| {
                            resource
                                .unwrap()
                                .textselections()
                                .map(|textselection| WrappedItemSet::new(textselection))
                        })
                        .flatten();
                    self.iterator = Some(Box::new(iterator));
                    /*
                        .resources()
                        .filter(|resource| {
                            itemset
                                .iter()
                                .any(|x| x.to_handle(self.store) == resource.handle())
                        })
                    */
                }
                _ => unimplemented!(), //TODO: remove
            }
        } else {
            //unconstrained: all textselections in all resources (order not guaranteed!)
            let iterator = self
                .store
                .resources()
                .map(|resource| {
                    resource
                        .unwrap()
                        .textselections()
                        .map(|textselection| WrappedItemSet::new(textselection))
                })
                .flatten();
            self.iterator = Some(Box::new(iterator));
        }
    }

    fn test_itemset(
        &self,
        constraint: &Constraint,
        itemset: &WrappedItemSet<TextSelection>,
    ) -> bool {
        //if a single item in an itemset matches, the itemset as a whole is valid
        for item in itemset.iter() {
            match constraint {
                Constraint::AnnotationData { set, key, value } => {
                    if let Some(iter) = item.annotations(self.store) {
                        for annotation in iter {
                            for data in annotation.data() {
                                if self
                                    .store
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
                _ => unimplemented!(), //TODO: remove
            }
        }
        false
    }
}

impl AnnotationStore {
    pub fn select_resources<'a>(&'a self) -> SelectQuery<'a, TextResource> {
        SelectQuery::new(self)
    }

    pub fn select_text<'a>(&'a self) -> SelectQuery<'a, TextSelection> {
        SelectQuery::new(self)
    }
}
