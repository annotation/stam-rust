use crate::annotation::Annotation;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::error::*;
use crate::resources::{TextResource, TextResourceHandle};
use crate::selector::{Offset, OffsetMode};
use crate::store::*;
use crate::textselection::{
    ResultTextSelection, ResultTextSelectionSet, TextSelection, TextSelectionHandle,
    TextSelectionOperator, TextSelectionSet,
};
use crate::IntersectionIter;
use std::borrow::Cow;
use std::cmp::Ordering;

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

    pub fn resource(&self) -> ResultItem<'store, TextResource> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
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
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let tset: TextSelectionSet = self.clone().into();
        self.resource().related_text(operator, tset)
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one. Returns an iterator over the [`TextSelection`] instances.
    /// (as [`ResultItem<TextSelection>`]).
    /// If you are interested in the text selections only, use [`Self.find_textselections()`] instead.
    pub fn annotations_by_related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> {
        let tset: TextSelectionSet = self.clone().into();
        self.resource()
            .related_text(operator, tset)
            .filter_map(|tsel| tsel.annotations())
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
            Self::Unbound(_, _, item) => item,
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
            Self::Unbound(_, _, item) => item.begin(),
        }
    }

    /// Return the end position (non-inclusive) in unicode points
    pub fn end(&self) -> usize {
        match self {
            Self::Bound(item) => item.as_ref().end(),
            Self::Unbound(_, _, item) => item.end(),
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
                Self::Unbound(_, _, item) => &item,
            };
            match self {
                Self::Bound(item) => item.as_ref().relative_begin(container),
                Self::Unbound(_, _, item) => item.relative_begin(container),
            }
        }
    }

    /// Returns the end cursor (begin-aligned) of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_end(&self, container: &ResultTextSelection<'store>) -> Option<usize> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, _, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_end(container),
            Self::Unbound(_, _, item) => item.relative_end(container),
        }
    }

    /// Returns the offset of this text selection in another. Returns None if they are not embedded.
    /// This also checks whether the textselections pertain to the same resource. Returns None otherwise.
    pub fn relative_offset(
        &self,
        container: &ResultTextSelection<'store>,
        offsetmode: OffsetMode,
    ) -> Option<Offset> {
        let container = match container {
            Self::Bound(item) => item.as_ref(),
            Self::Unbound(_, _, item) => &item,
        };
        match self {
            Self::Bound(item) => item.as_ref().relative_offset(container, offsetmode),
            Self::Unbound(_, _, item) => item.relative_offset(container, offsetmode),
        }
    }

    pub(crate) fn store(&self) -> &'store TextResource {
        match self {
            Self::Bound(item) => item.store(),
            Self::Unbound(_, store, ..) => store,
        }
    }

    pub fn rootstore(&self) -> &'store AnnotationStore {
        match self {
            Self::Bound(item) => item.rootstore(),
            Self::Unbound(rootstore, ..) => rootstore,
        }
    }

    pub fn resource(&self) -> ResultItem<'store, TextResource> {
        let rootstore = self.rootstore();
        self.store().as_resultitem(rootstore, rootstore)
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
            Self::Unbound(_, _, item) => Ok(item),
        }
    }

    /// Iterates over all annotations that are referenced by this TextSelection, if any.
    pub fn annotations(&self) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>>> {
        let annotationstore = self.rootstore();
        match self {
            Self::Bound(item) => Some(item.annotations(annotationstore)),
            Self::Unbound(..) => None,
        }
    }

    /// Returns the number of annotations that reference this text selection
    pub fn annotations_len(&self) -> usize {
        let annotationstore = self.rootstore();
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
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, _, textselection) => textselection.clone(),
        });
        self.resource().related_text(operator, tset)
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one. Returns a set of annotations.
    /// If you also want to filter based on the data, use [`Self.annotations_by_related_text_matching_data()`]
    /// If you are interested in the text selections only, use [`Self.related_text()`] instead.
    /// The annotations are returned in textual order.
    pub fn annotations_by_related_text(
        &self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, _, textselection) => textselection.clone(),
        });
        self.resource()
            .related_text(operator, tset)
            .filter_map(|tsel| tsel.annotations())
            .flatten()
    }

    /// Applies a [`TextSelectionOperator`] to find *annotations* referencing other text selections that
    /// are in a specific relation with the current one *and* that match specific data. Returns a set of annotations.
    /// If you also want to filter based on the data, use [`Self.annotations_by_related_text_matching_data()`]
    /// If you are interested in the text selections only, use [`Self.related_text()`] instead.
    /// The annotations are returned in textual order.
    pub fn annotations_by_related_text_and_data<'a>(
        &self,
        operator: TextSelectionOperator,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + 'store>
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) =
            self.rootstore().find_data_request_resolver(set, key)
        {
            Some(
                self.related_text(operator)
                    .map(move |tsel| {
                        tsel.find_data_about(test_set_handle, test_key_handle, value)
                            .into_iter()
                            .flatten()
                            .map(|(_data, annotation)| annotation)
                    })
                    .flatten(),
            )
        } else {
            None
        }
    }

    /// Search for data *about* this text, i.e. data on annotations that refer to this text.
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn find_data_about<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
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
        if let Some((test_set_handle, test_key_handle)) =
            self.rootstore().find_data_request_resolver(set, key)
        {
            Some(
                self.annotations()
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
    ) -> Option<
        impl Iterator<
                Item = (
                    ResultItem<'store, AnnotationData>,
                    ResultItem<'store, Annotation>,
                ),
            > + 'store,
    > {
        self.find_data_about(false, false, &DataOperator::Any)
    }

    /// Test data *about* this text, i.e. data on annotations that refer to this text
    pub fn test_data_about<'a>(
        &self,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> bool
    where
        'a: 'store,
    {
        match self.find_data_about(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }

    /// Search for annotations *about* this textselection, satisfying certain exact data that is already known.
    /// For a higher-level variant, see `find_data_about`, this method is more efficient.
    /// Both the matching data as well as the matching annotation will be returned in an iterator.
    pub fn annotations_by_data_about(
        &self,
        data: ResultItem<'store, AnnotationData>,
    ) -> impl Iterator<Item = ResultItem<'store, Annotation>> + 'store {
        self.annotations()
            .into_iter()
            .flatten()
            .filter(move |annotation| annotation.has_data(&data))
    }

    /// Tests if the textselection has certain data in annotatations that reference this textselection, returns a boolean.
    /// If you don't have a data instance yet, use `test_data_about()` instead.
    /// This method is much more efficient than `test_data_about()`.
    pub fn has_data_about(&self, data: ResultItem<'store, AnnotationData>) -> bool {
        self.annotations_by_data_about(data).next().is_some()
    }

    /// This selects text in a specific relation to the text of the current annotation, where that has text has certain data describing it.
    /// It returns both the matching text and for each also the matching annotation data and matching annotation
    /// If you do not wish to return the data, but merely test for it, then use [`Self.related_text_test_data()`] instead.
    /// It effectively combines `related_text()` with `find_data_about()` on its results, into a single method.
    /// See these methods for further parameter explanation.
    pub fn related_text_with_data<'a>(
        &self,
        operator: TextSelectionOperator,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<
        impl Iterator<
            Item = (
                ResultTextSelection<'store>,
                Vec<(
                    ResultItem<'store, AnnotationData>,
                    ResultItem<'store, Annotation>,
                )>,
            ),
        >,
    >
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) =
            self.rootstore().find_data_request_resolver(set, key)
        {
            Some(self.related_text(operator).filter_map(move |tsel| {
                if let Some(iter) = tsel.find_data_about(test_set_handle, test_key_handle, value) {
                    let data: Vec<_> = iter.collect();
                    if data.is_empty() {
                        None
                    } else {
                        Some((tsel.clone(), data))
                    }
                } else {
                    None
                }
            }))
        } else {
            None
        }
    }

    /// This selects text in a specific relation to the text of the current annotation, where that has text has certain data describing it.
    /// This returns the matching text, not the data. Use [`Self.related_text_with_data()`] if you want the data as well.
    /// It effectively combines `related_text()` with `test_data_about()` on its results, into a single method.
    /// See these methods for further parameter explanation.
    pub fn related_text_test_data<'a>(
        &self,
        operator: TextSelectionOperator,
        set: impl Request<AnnotationDataSet>,
        key: impl Request<DataKey>,
        value: &'a DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultTextSelection<'store>>>
    where
        'a: 'store,
    {
        if let Some((test_set_handle, test_key_handle)) =
            self.rootstore().find_data_request_resolver(set, key)
        {
            Some(self.related_text(operator).filter_map(move |tsel| {
                if tsel.test_data_about(test_set_handle, test_key_handle, value) {
                    Some(tsel)
                } else {
                    None
                }
            }))
        } else {
            None
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
    pub fn related_text(
        self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = ResultTextSelection<'store>> {
        let rootstore = self.rootstore();
        let resource = self.resource();
        resource
            .as_ref()
            .textselections_by_operator(operator, self.tset)
            .map(move |ts_handle| {
                let textselection: &'store TextSelection = resource
                    .as_ref()
                    .get(ts_handle)
                    .expect("textselection handle must be valid");
                textselection
                    .as_resultitem(resource.as_ref(), rootstore)
                    .into()
            })
    }
}

pub trait SortTextualOrder<T>
where
    T: PartialOrd,
{
    /// Sorts items in the iterator in textual order, items that do not relate to text at all will be put at the end with arbitrary sorting
    /// This method allocates and returns a buffer to do the sorting, it also takes care to remove any duplicates
    fn textual_order(&mut self) -> Vec<T>;
}

fn compare_annotation<'store>(
    a: &ResultItem<'store, Annotation>,
    b: &ResultItem<'store, Annotation>,
) -> Ordering {
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
}

impl<'store, I> SortTextualOrder<ResultItem<'store, Annotation>> for I
where
    I: Iterator<Item = ResultItem<'store, Annotation>>,
{
    fn textual_order(&mut self) -> Vec<ResultItem<'store, Annotation>> {
        let mut v: Vec<_> = self.collect();
        v.sort_unstable_by(compare_annotation);
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

pub struct TextSelectionsIter<'store> {
    data: Vec<ResultTextSelection<'store>>,
    cursor: usize,
    store: &'store AnnotationStore,
}

impl<'store> Iterator for TextSelectionsIter<'store> {
    type Item = ResultTextSelection<'store>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.data.get(self.cursor) {
            self.cursor += 1;
            return Some(item.clone());
        }
        None
    }
}

//// delete the below:

/*
pub struct TextSelectionsIter<'store> {
    iter: Option<IntersectionIter<'store, (TextResourceHandle, TextSelectionHandle)>>,
    cursor: usize,
    store: &'store AnnotationStore,

    //for optimisations:
    last_resource_handle: Option<TextResourceHandle>,
    last_resource: Option<ResultItem<'store, TextResource>>,
}

impl<'store> TextSelectionsIter<'store> {
    pub(crate) fn new(
        iter: IntersectionIter<'store, (TextResourceHandle, TextSelectionHandle)>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            cursor: 0,
            iter: Some(iter),
            store,
            last_resource_handle: None,
            last_resource: None,
        }
    }

    pub(crate) fn new_empty(store: &'store AnnotationStore) -> Self {
        Self {
            cursor: 0,
            iter: None,
            store,
            last_resource_handle: None,
            last_resource: None,
        }
    }

    /// Constrain the iterator to return only the text selections that are used by the specified annotation
    pub fn filter_annotation(mut self, annotation: &ResultItem<'store, Annotation>) -> Self {
        let textselections = self
            .store
            .textselections_by_selector(annotation.as_ref().target())
            .into_iter()
            .map(|(resource, textselection)| {
                (
                    resource.handle().expect("must have handle"),
                    textselection.handle().expect("must have handle"),
                )
            })
            .collect();

        if let Some(iter) = self.iter.as_mut() {
            *iter = iter.with(Cow::Owned(textselections), true);
        }
        self
    }

    pub fn filter_annotations(mut self, annotations: AnnotationsIter<'store>) -> Self {
        todo!("implement");
    }

    pub fn filter_textselections(mut self, textselections: TextSelectionsIter<'store>) -> Self {
        self.iter.merge(textselections.iter);
        self
    }

    /// Iterate over the annotations that make use of data in this iterator
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        todo!("implement");
    }
}

impl<'store> Iterator for TextSelectionsIter<'store> {
    type Item = ResultTextSelection<'store>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.iter.as_mut() {
            if let Some((res_handle, tsel_handle)) = iter.next() {
                //optimisation so we don't have to grab the same resource over and over:
                let resource = if Some(res_handle) == self.last_resource_handle {
                    self.last_resource.unwrap()
                } else {
                    self.last_resource_handle = Some(res_handle);
                    self.last_resource = Some(
                        self.store
                            .resource(res_handle)
                            .expect("resource must exist"),
                    );
                    self.last_resource.unwrap()
                };
                let tsel: &TextSelection = resource
                    .as_ref()
                    .get(tsel_handle)
                    .expect("text selection must exist");
                return Some(ResultTextSelection::Bound(
                    tsel.as_resultitem(resource.as_ref(), self.store),
                ));
            }
        }
        None
    }
}
*/
