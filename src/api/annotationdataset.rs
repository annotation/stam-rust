/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the high-level API for [`AnnotationDataSet`]. This API is implemented on
//! [`ResultItem<AnnotationDataSet>`].

use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::api::*;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::store::*;
use crate::substore::AnnotationSubStore;

use rayon::iter::IntoParallelIterator;

impl<'store> FullHandle<AnnotationDataSet> for ResultItem<'store, AnnotationDataSet> {
    fn fullhandle(&self) -> <AnnotationDataSet as Storable>::FullHandleType {
        self.handle()
    }
}

impl<'store> ResultItem<'store, AnnotationDataSet> {
    /// Returns an iterator over all data in this set.
    pub fn data(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, AnnotationData>>> {
        let store = self.as_ref();
        let rootstore = self.rootstore();
        ResultIter::new_sorted(
            self.as_ref()
                .data()
                .map(|data| data.as_resultitem(store, rootstore)),
        )
    }

    /// Returns an iterator over all keys in this set
    pub fn keys(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, DataKey>>> {
        let store = self.as_ref();
        let rootstore = self.rootstore();
        ResultIter::new_sorted(
            self.as_ref()
                .keys()
                .map(|item| item.as_resultitem(store, rootstore)),
        )
    }

    /// Retrieve a [`DataKey`] in this set
    pub fn key(&self, key: impl Request<DataKey>) -> Option<ResultItem<'store, DataKey>> {
        self.as_ref()
            .get(key)
            .map(|x| x.as_resultitem(self.as_ref(), self.rootstore()))
            .ok()
    }

    /// Retrieve a single [`AnnotationData`] in this set
    pub fn annotationdata(
        &self,
        annotationdata: impl Request<AnnotationData>,
    ) -> Option<ResultItem<'store, AnnotationData>> {
        self.as_ref()
            .get(annotationdata)
            .map(|x| x.as_resultitem(self.as_ref(), self.rootstore()))
            .ok()
    }

    /// Returns an iterator over annotations that directly point at the dataset, i.e. are metadata for it (via a DataSetSelector).
    pub fn annotations(&self) -> ResultIter<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let store = self.store();
        if let Some(annotations) = self.store().annotations_by_dataset_metadata(self.handle()) {
            ResultIter::new_sorted(FromHandles::new(annotations.iter().copied(), store))
        } else {
            ResultIter::new_empty()
        }
    }

    /// Finds the [`AnnotationData`] in the annotation dataset. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use [`Self::test_data()`] instead.
    ///
    /// If you pass an empty string literal or boolean to `key`, all keys will be searched.
    ///
    /// If you already have a [`ResultItem<DataKey>`] , use [`ResultItem<DataKey>.find_data()`] instead, it'll be much more efficient.
    ///
    /// Value is a [`DataOperator`], it is not wrapped in an Option but can be set to [`DataOperator::Any`] to return all values.
    ///
    /// ## Example
    ///
    /// ```
    /// # use stam::*;
    /// # fn main() -> Result<(),StamError> {
    /// # let store = AnnotationStore::default()
    /// #   .with_id("example")
    /// #   .add(TextResource::from_string(
    /// #       "myresource",
    /// #       "Hello world",
    /// #       Config::default(),
    /// #   ))?
    /// #   .add(AnnotationDataSet::new(Config::default()).with_id("mydataset"))?
    /// #   .with_annotation(
    /// #       AnnotationBuilder::new()
    /// #           .with_id("A1")
    /// #           .with_target(SelectorBuilder::textselector(
    /// #               "myresource",
    /// #               Offset::simple(6, 11),
    /// #           ))
    /// #           .with_data_with_id("mydataset", "part-of-speech", "noun", "D1"),
    /// #   )?;
    /// //in this store we have a single annotation, and single annotation data with key 'part-of-speech' and value 'noun':
    /// let dataset = store.dataset("mydataset").or_fail()?;
    /// for annotationdata in dataset.find_data("part-of-speech", DataOperator::Equals("noun")) {
    ///     assert_eq!(annotationdata.id(), Some("D1"));
    ///     assert_eq!(annotationdata.value(), "noun");
    /// }
    /// #    Ok(())
    /// # }
    /// ```
    pub fn find_data<'q>(
        &self,
        key: impl Request<DataKey>,
        value: DataOperator<'q>,
    ) -> Box<dyn Iterator<Item = ResultItem<'store, AnnotationData>> + 'store>
    where
        'q: 'store,
    {
        if !key.any() {
            if let Some(key) = self.key(key) {
                if let DataOperator::Any = value {
                    return Box::new(key.data());
                } else {
                    return Box::new(key.data().filter_value(value));
                }
            } else {
                //requested key doesn't exist, bail out early, we won't find anything at all
                return Box::new(std::iter::empty());
            }
        };

        //any key
        if let DataOperator::Any = value {
            Box::new(self.data())
        } else {
            Box::new(self.data().filter_value(value))
        }
    }

    /// Tests if the dataset has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// If you pass an empty string literal or boolean to `key`, all keys will be searched.
    /// Value is a DataOperator, it can be set set to [`DataOperator::Any`] to return all values.
    pub fn test_data<'a>(&self, key: impl Request<DataKey>, value: DataOperator<'a>) -> bool {
        self.find_data(key, value).next().is_some()
    }

    /// Tests whether two AnnotationDataSets are the same
    pub fn test(&self, other: impl Request<AnnotationDataSet>) -> bool {
        Some(self.handle()) == other.to_handle(self.store())
    }

    /// Returns an iterator over all substores ([`AnnotationSubStore`] instances) this dataset is a part of.
    pub fn substores<'a>(
        &'a self,
    ) -> ResultIter<impl Iterator<Item = ResultItem<'a, AnnotationSubStore>>> {
        let handle = self.handle();
        let store = self.store();
        ResultIter::new_sorted(
            store
                .dataset_substore_map
                .get(handle)
                .into_iter()
                .flatten()
                .map(|substorehandle| store.substore(*substorehandle).expect("handle must exist")),
        )
    }
}

/// Holds a collection of [`AnnotationDataSet`] (by reference to an [`AnnotationStore`] and handles). This structure is produced by calling
/// [`ToHandles::to_handles()`], which is available on all iterators over keys ([`ResultItem<AnnotationDataSet>`]).
pub type AnnotationDataSets<'store> = Handles<'store, AnnotationDataSet>;

impl<'store, I> FullHandleToResultItem<'store, AnnotationDataSet>
    for FromHandles<'store, AnnotationDataSet, I>
where
    I: Iterator<Item = AnnotationDataSetHandle>,
{
    fn get_item(
        &self,
        handle: AnnotationDataSetHandle,
    ) -> Option<ResultItem<'store, AnnotationDataSet>> {
        self.store.dataset(handle)
    }
}

impl<'store, I> FullHandleToResultItem<'store, AnnotationDataSet>
    for FilterAllIter<'store, AnnotationDataSet, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationDataSet>>,
{
    fn get_item(
        &self,
        handle: AnnotationDataSetHandle,
    ) -> Option<ResultItem<'store, AnnotationDataSet>> {
        self.store.dataset(handle)
    }
}

/// Trait for iteration over datasets ([`ResultItem<AnnotationDataSet>`]; encapsulation over
/// [`AnnotationDataSet`]). Implements numerous filter methods to further constrain the iterator, as well
/// as methods to map from keys to other items.
pub trait DataSetIterator<'store>: Iterator<Item = ResultItem<'store, AnnotationDataSet>>
where
    Self: Sized,
{
    fn parallel(self) -> rayon::vec::IntoIter<ResultItem<'store, AnnotationDataSet>> {
        let datasets: Vec<_> = self.collect();
        datasets.into_par_iter()
    }

    fn filter_handle(self, set: AnnotationDataSetHandle) -> FilteredDataSets<'store, Self> {
        FilteredDataSets {
            inner: self,
            filter: Filter::AnnotationDataSet(set, SelectionQualifier::Normal),
        }
    }

    fn filter_any(self, datasets: AnnotationDataSets<'store>) -> FilteredDataSets<'store, Self> {
        FilteredDataSets {
            inner: self,
            filter: Filter::DataSets(datasets, FilterMode::Any, SelectionQualifier::Normal),
        }
    }

    fn filter_any_byref(
        self,
        datasets: &'store AnnotationDataSets<'store>,
    ) -> FilteredDataSets<'store, Self> {
        FilteredDataSets {
            inner: self,
            filter: Filter::BorrowedDataSets(datasets, FilterMode::Any, SelectionQualifier::Normal),
        }
    }

    /// Constrain this iterator to filter on *all* of the mentioned data sets, that is.
    /// If not all of the items in the parameter exist in the iterator, the iterator returns nothing.
    fn filter_all(
        self,
        datasets: AnnotationDataSets<'store>,
        store: &'store AnnotationStore,
    ) -> FilterAllIter<'store, AnnotationDataSet, Self> {
        FilterAllIter::new(self, datasets, store)
    }

    fn filter_one(
        self,
        data: &ResultItem<'store, AnnotationData>,
    ) -> FilteredDataSets<'store, Self> {
        FilteredDataSets {
            inner: self,
            filter: Filter::AnnotationData(
                data.set().handle(),
                data.handle(),
                SelectionQualifier::Normal,
            ),
        }
    }
}

impl<'store, I> DataSetIterator<'store> for I
where
    I: Iterator<Item = ResultItem<'store, AnnotationDataSet>>,
{
    //blanket implementation
}

/// An iterator that applies a filter to constrain keys.
/// This iterator implements [`KeyIterator`]
/// and is itself produced by the various `filter_*()` methods on that trait.
pub struct FilteredDataSets<'store, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationDataSet>>,
{
    inner: I,
    filter: Filter<'store>,
}

impl<'store, I> Iterator for FilteredDataSets<'store, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationDataSet>>,
{
    type Item = ResultItem<'store, AnnotationDataSet>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.inner.next() {
                if self.test_filter(&item) {
                    return Some(item);
                }
            } else {
                return None;
            }
        }
    }
}

impl<'store, I> FilteredDataSets<'store, I>
where
    I: Iterator<Item = ResultItem<'store, AnnotationDataSet>>,
{
    fn test_filter(&self, _dataset: &ResultItem<'store, AnnotationDataSet>) -> bool {
        match &self.filter {
            Filter::DataSets(_, FilterMode::All, _) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            Filter::BorrowedDataSets(_, FilterMode::All, _) => {
                unreachable!("not handled by this iterator but by FilterAllIter")
            }
            _ => unreachable!(
                "Filter {:?} not implemented for FilteredDataSets",
                self.filter
            ),
        }
    }
}
