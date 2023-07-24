use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::error::*;
use crate::resources::TextResource;
use crate::selector::Offset;
use crate::store::*;
use crate::textselection::{
    ResultTextSelection, ResultTextSelectionSet, TextSelection, TextSelectionHandle,
    TextSelectionOperator, TextSelectionSet,
};

use std::cmp::Ordering;
use std::collections::BTreeSet;

impl<'store> ResultItem<'store, TextSelection> {
    pub fn wrap(self) -> ResultTextSelection<'store> {
        ResultTextSelection::Bound(self)
    }
    pub fn begin(&self) -> usize {
        self.as_ref().begin()
    }

    pub fn end(&self) -> usize {
        self.as_ref().end()
    }

    pub fn resource(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> ResultItem<'store, TextResource> {
        self.store().as_resultitem(annotationstore)
    }

    /// Iterates over all annotations that reference this TextSelection, if any.
    /// Note that you need to explicitly specify the `AnnotationStore` for this method.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        annotationstore
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
            .map(|v| {
                v.into_iter()
                    .map(|a_handle| annotationstore.annotation(*a_handle).unwrap())
            })
            .into_iter()
            .flatten()
    }

    /// Returns the number of annotations that reference this text selection
    pub fn annotations_len(&self, annotationstore: &'store AnnotationStore) -> usize {
        if let Some(vec) = annotationstore
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
        {
            vec.len()
        } else {
            0
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let tset: TextSelectionSet = self.clone().into();
        self.resource(annotationstore).related_text(operator, tset)
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultItem<TextSelection>`]).
    /// If you are interested in the text selections only, use [`Self.find_textselections()`] instead.
    pub fn annotations_by_related_text(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        let tset: TextSelectionSet = self.clone().into();
        self.resource(annotationstore)
            .related_text(operator, tset)
            .filter_map(|tsel| tsel.annotations(annotationstore))
            .flatten()
    }
}

impl<'store> ResultTextSelection<'store> {
    /// Return a reference to the inner textselection.
    /// This works in all cases but will have a limited lifetime.
    /// Use [`Self.as_ref()`] instead if you have bound item.
    pub fn inner(&self) -> &TextSelection {
        match self {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, item) => item,
        }
    }

    /// Return a reference to the textselection in the store.
    /// Only works on bound items.
    /// Use [`Self.inner()`] instead if
    pub fn as_ref(&self) -> Option<&'store TextSelection> {
        match self {
            Self::Bound(item) => Some(item.as_ref()),
            Self::Unbound(..) => None,
        }
    }

    /// Return a reference to the textselection in the store.
    /// Only works on bound items.
    /// Use [`Self.inner()`] instead if
    pub fn as_resultitem(&self) -> Option<&ResultItem<'store, TextSelection>> {
        match self {
            Self::Bound(item) => Some(item),
            Self::Unbound(..) => None,
        }
    }

    /// Return the begin position (unicode points)
    pub fn begin(&self) -> usize {
        match self {
            Self::Bound(item) => item.as_ref().begin(),
            Self::Unbound(_, item) => item.begin(),
        }
    }

    /// Return the end position (non-inclusive) in unicode points
    pub fn end(&self) -> usize {
        match self {
            Self::Bound(item) => item.as_ref().end(),
            Self::Unbound(_, item) => item.end(),
        }
    }

    /// Returns the begin cursor of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_begin(&self, container: &ResultTextSelection<'store>) -> Option<usize> {
        if self.store() != container.store() {
            None
        } else {
            let container = match container {
                Self::Bound(item) => item.as_ref(),
                Self::Unbound(_, item) => &item,
            };
            match self {
                Self::Bound(item) => item.as_ref().relative_begin(container),
                Self::Unbound(_, item) => item.relative_begin(container),
            }
        }
    }

    /// Returns the end cursor (begin-aligned) of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_end(&self, container: &ResultTextSelection<'store>) -> Option<usize> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_end(container),
            Self::Unbound(_, item) => item.relative_end(container),
        }
    }

    /// Returns the offset of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_offset(&self, container: &ResultTextSelection<'store>) -> Option<Offset> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_offset(container),
            Self::Unbound(_, item) => item.relative_offset(container),
        }
    }

    pub fn store(&self) -> &'store TextResource {
        match self {
            Self::Bound(item) => item.store(),
            Self::Unbound(store, ..) => store,
        }
    }

    pub fn resource(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> ResultItem<'store, TextResource> {
        self.store().as_resultitem(annotationstore)
    }

    pub fn handle(&self) -> Option<TextSelectionHandle> {
        match self {
            Self::Bound(item) => Some(item.handle()),
            Self::Unbound(..) => None,
        }
    }

    pub fn take(self) -> Result<TextSelection, StamError> {
        match self {
            Self::Bound(_) => Err(StamError::AlreadyBound(
                "Item is bound, can't be taken out!",
            )),
            Self::Unbound(_store, item) => Ok(item),
        }
    }

    /// Iterates over all annotations that are referenced by this TextSelection, if any.
    /// Note that you need to explicitly specify the `AnnotationStore` for this method.
    pub fn annotations(
        &self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        match self {
            Self::Bound(item) => Some(item.annotations(annotationstore)),
            Self::Unbound(..) => None,
        }
    }

    /// Returns the number of annotations that reference this text selection
    pub fn annotations_len(&self, annotationstore: &'store AnnotationStore) -> usize {
        match self {
            Self::Bound(item) => item.annotations_len(annotationstore),
            Self::Unbound(..) => 0,
        }
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.annotations_by_related_text()`] instead.
    pub fn related_text(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, textselection) => textselection.clone(),
        });
        self.resource(annotationstore).related_text(operator, tset)
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultItem<TextSelection>`]).
    /// If you are interested in the text selections only, use [`Self.related_text()`] instead.
    /// The annotations are returned in textual order.
    pub fn annotations_by_related_text(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> BTreeSet<ResultItem<'store, Annotation>> {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, textselection) => textselection.clone(),
        });
        self.resource(annotationstore)
            .related_text(operator, tset)
            .filter_map(|tsel| tsel.annotations(annotationstore))
            .flatten()
            .collect()
    }

    /// Search for data *about* this text, i.e. data on annotations that refer to this text.
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn find_data_about<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
        store: &'store AnnotationStore,
    ) -> Option<
        impl Iterator<
                Item = (
                    ResultItem<'store, AnnotationData>,
                    ResultItem<'store, Annotation>,
                ),
            > + 'store,
    >
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) = store.find_data_request_resolver(set, key)
        {
            Some(
                self.annotations(store)
                    .into_iter()
                    .flatten()
                    .map(move |annotation| {
                        annotation
                            .find_data(test_set_handle, test_key_handle, value)
                            .into_iter()
                            .flatten()
                            .map(move |data| (data, annotation.clone()))
                    })
                    .flatten(),
            )
        } else {
            None
        }
    }

    /// Shortcut method to get all data *about* this text, i.e. data on annotations that refer to this text
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn data_about(
        &self,
        store: &'store AnnotationStore,
    ) -> Option<
        impl Iterator<
                Item = (
                    ResultItem<'store, AnnotationData>,
                    ResultItem<'store, Annotation>,
                ),
            > + 'store,
    > {
        self.find_data_about(false, false, &DataOperator::Any, store)
    }

    /// Test data *about* this text, i.e. data on annotations that refer to this text
    pub fn test_data_about<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
        store: &'store AnnotationStore,
    ) -> bool
    where
        'a: 'store,
    {
        match self.find_data_about(set, key, value, store) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }
}

impl<'store> ResultTextSelectionSet<'store> {
    pub fn rootstore(&self) -> &'store AnnotationStore {
        self.rootstore
    }

    pub fn resource(&self) -> ResultItem<'store, TextResource> {
        self.rootstore()
            .resource(self.tset.resource())
            .expect("resource must exist")
    }

    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current text selection set. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    /// This variant consumes the TextSelectionSet, use `find_textselections_ref()` for a borrowed version.
    pub fn related_text(
        self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let resource = self.resource();
        resource
            .as_ref()
            .textselections_by_operator(operator, self.tset)
            .map(move |ts_handle| {
                let textselection: &'store TextSelection = resource
                    .as_ref()
                    .get(ts_handle)
                    .expect("textselection handle must be valid");
                textselection.as_resultitem(resource.as_ref()).into()
            })
    }

    /*
    /// Applies a [`TextSelectionOperator`] to find all other text selections that
    /// are in a specific relation with the current text selection set. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultItem<TextSelection>`]).
    /// If you are interested in the annotations associated with the found text selections, then use [`Self.find_annotations()`] instead.
    /// This variant borrows the TextSelectionSet, use `find_textselections()` for an owned version that consumes the set.
    pub fn find_textselections_ref(
        &self,
        operator: TextSelectionOperator,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = ResultItem<'store, TextSelection>> + 'store> {
        if let Some(resource) = annotationstore.resource(self.resource()) {
            Some(
                resource
                    .as_ref()
                    .textselections_by_operator_ref(operator, self)
                    .map(move |ts_handle| {
                        let textselection: &'store TextSelection = resource
                            .as_ref()
                            .get(ts_handle)
                            .expect("textselection handle must be valid");
                        textselection.as_resultitem(resource.as_ref())
                    }),
            )
        } else {
            None
        }
    }
    */
}

pub trait SortTextualOrder<T>
where
    T: PartialOrd,
{
    /// Sorts items in the iterator in textual order, items that do not relate to text at all will be put at the end with arbitrary sorting
    /// This method allocates and returns a buffer to do the sorting, it also takes care to remove any duplicates
    fn textual_order(&mut self) -> Vec<T>;
}

impl<'store, I> SortTextualOrder<ResultItem<'store, Annotation>> for I
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    fn textual_order(&mut self) -> Vec<ResultItem<'store, Annotation>> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| {
            let tset_a: TextSelectionSet = a.textselections().collect();
            let tset_b: TextSelectionSet = b.textselections().collect();
            if tset_a.is_empty() && tset_b.is_empty() {
                //compare by handle
                a.handle().cmp(&b.handle())
            } else if tset_a.is_empty() {
                Ordering::Greater
            } else if tset_b.is_empty() {
                Ordering::Less
            } else {
                tset_a
                    .partial_cmp(&tset_b)
                    .expect("textual_order() can only be applied if annotations reference text!")
                //should never occur because I tested for this already
            }
        });
        v.dedup();
        v
    }
}

impl<'store, I> SortTextualOrder<ResultTextSelection<'store>> for I
where
    I: Iterator<Item = ResultTextSelection<'store>>,
{
    fn textual_order(&mut self) -> Vec<ResultTextSelection<'store>> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| {
            a.partial_cmp(b)
                .expect("PartialOrd must work for ResultTextSelection")
        });
        v
    }
}

impl<'store, I> SortTextualOrder<ResultTextSelectionSet<'store>> for I
where
    I: Iterator<Item = ResultTextSelectionSet<'store>>,
{
    fn textual_order(&mut self) -> Vec<ResultTextSelectionSet<'store>> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| {
            if a.tset.is_empty() && b.tset.is_empty() {
                Ordering::Equal
            } else if a.tset.is_empty() {
                Ordering::Greater
            } else if b.tset.is_empty() {
                Ordering::Less
            } else {
                a.partial_cmp(b)
                    .expect("PartialOrd must work for ResultTextSelectionSet")
            }
        });
        v
    }
}

impl<'store, I> SortTextualOrder<TextSelectionSet> for I
where
    I: Iterator<Item = TextSelectionSet>,
{
    fn textual_order(&mut self) -> Vec<TextSelectionSet> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| {
            if a.is_empty() && b.is_empty() {
                Ordering::Equal
            } else if a.is_empty() {
                Ordering::Greater
            } else if b.is_empty() {
                Ordering::Less
            } else {
                a.partial_cmp(b)
                    .expect("PartialOrd must work for TextSelectionSet")
            }
        });
        v
    }
}

impl<'store, I> SortTextualOrder<TextSelection> for I
where
    I: Iterator<Item = TextSelection>,
{
    fn textual_order(&mut self) -> Vec<TextSelection> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(|a, b| a.cmp(b));
        v
    }
}
