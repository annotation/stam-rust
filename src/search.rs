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

    pub fn store(&self) -> &'store AnnotationStore {
        self.store
    }
}

pub trait SelectQueryIterator<'store>
where
    Self::QueryItem: Storable,
{
    type QueryItem;

    /// Initializes the iterator based on the first constraint
    fn init_iterator(&mut self);

    /// Tests a retrieved item against remaining constraints
    fn test_item(
        &self,
        constraint: &Constraint,
        item: &WrappedItem<'store, Self::QueryItem>,
    ) -> bool;
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
    Self: SelectQueryIterator<'store, QueryItem = T>,
    AnnotationStore: StoreFor<T>,
{
    type Item = WrappedItem<'store, T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.iterator.as_mut() {
                if let Some(item) = iter.next() {
                    //process further constraints:
                    let mut constraints_met = true;
                    for constraint in self.constraints.iter().skip(1) {
                        if !self.test_item(constraint, &item) {
                            constraints_met = false;
                            break;
                        }
                    }
                    if constraints_met {
                        return Some(item);
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

impl<'store, 'q> SelectQueryIterator<'store> for SelectQuery<'store, 'q, TextResource>
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
                _ => unimplemented!(),
            }
        } else {
            //unconstrained
            let iterator = self.store.resources();
            self.iterator = Some(Box::new(iterator));
        }
    }

    fn test_item(&self, constraint: &Constraint, item: &WrappedItem<TextResource>) -> bool {
        match constraint {
            Constraint::FilterData { set, key, value } => {
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
                return false;
            }
            _ => unimplemented!(),
        }
    }
}

impl AnnotationStore {
    pub fn select_resources<'a>(&'a self) -> SelectQuery<'a, '_, TextResource> {
        SelectQuery::new(self)
    }
}
