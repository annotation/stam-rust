use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::annotationstore::AnnotationStore;
use crate::api::annotation::AnnotationsIter;
use crate::datakey::DataKey;
use crate::datavalue::DataOperator;
use crate::resources::{TextResource, TextResourceHandle};
use crate::selector::{Offset, OffsetMode};
use crate::store::*;
use crate::text::Text;
use crate::textselection::{
    FindTextSelectionsIter, ResultTextSelection, ResultTextSelectionSet, TextSelection,
    TextSelectionHandle, TextSelectionOperator, TextSelectionSet,
};
use crate::IntersectionIter;
use crate::{error::*, FindText};
use smallvec::SmallVec;
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
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        if let Some(annotations) = self
            .rootstore()
            .annotations_by_textselection(self.store().handle().unwrap(), self.as_ref())
        {
            AnnotationsIter::new(
                IntersectionIter::new(Cow::Borrowed(annotations), true),
                self.rootstore(),
            )
        } else {
            //dummy iterator that yields nothing
            AnnotationsIter::new_empty(self.rootstore())
        }
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
    pub fn annotations(&self) -> AnnotationsIter<'store> {
        match self {
            Self::Bound(item) => item.annotations(),
            Self::Unbound(..) => AnnotationsIter::new_empty(self.rootstore()),
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
    pub fn related_text(&self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        let mut tset: TextSelectionSet =
            TextSelectionSet::new(self.store().handle().expect("resource must have handle"));
        tset.add(match self {
            Self::Bound(item) => item.as_ref().clone().into(),
            Self::Unbound(_, _, textselection) => textselection.clone(),
        });
        self.resource().related_text(operator, tset)
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
    pub fn related_text(self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        let resource = self.resource();
        let store = self.rootstore();
        TextSelectionsIter::new_with_iterator(
            resource
                .as_ref()
                .textselections_by_operator(operator, self.tset),
            store,
        )
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

/// Source for TextSelectionsIter
pub(crate) enum TextSelectionsSource<'store> {
    HighVec(Vec<ResultTextSelection<'store>>),
    LowVec(SmallVec<[(TextResourceHandle, TextSelectionHandle); 2]>), //used with AnnotationStore.textselections_by_selector
    FindIter(FindTextSelectionsIter<'store>), //used with textselections_by_operator()
}

/// Iterator over TextSelections (yields [`ResultTextSelection`] instances)
/// offering high-level API methods.
pub struct TextSelectionsIter<'store> {
    source: TextSelectionsSource<'store>,
    cursor: usize,
    store: &'store AnnotationStore,
}

impl<'store> Iterator for TextSelectionsIter<'store> {
    type Item = ResultTextSelection<'store>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.source {
            TextSelectionsSource::HighVec(data) => {
                if let Some(item) = data.get(self.cursor) {
                    self.cursor += 1;
                    return Some(item.clone());
                }
            }
            TextSelectionsSource::LowVec(data) => {
                if let Some((res_handle, tsel_handle)) = data.get(self.cursor) {
                    let resource = self
                        .store
                        .resource(*res_handle)
                        .expect("resource must exist");
                    let tsel: &TextSelection = resource
                        .as_ref()
                        .get(*tsel_handle)
                        .expect("text selection must exist");
                    self.cursor += 1;
                    return Some(ResultTextSelection::Bound(
                        tsel.as_resultitem(resource.as_ref(), self.store),
                    ));
                }
            }
            TextSelectionsSource::FindIter(iter) => {
                if let Some(tsel_handle) = iter.next() {
                    let resource = iter.resource();
                    let tsel: &TextSelection = resource
                        .get(tsel_handle)
                        .expect("text selection must exist");
                    self.cursor += 1; //not really used in this context
                    return Some(ResultTextSelection::Bound(
                        tsel.as_resultitem(resource, self.store),
                    ));
                }
            }
        }
        None
    }
}

impl<'store> TextSelectionsIter<'store> {
    pub(crate) fn new(
        data: Vec<ResultTextSelection<'store>>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::HighVec(data),
            store,
            cursor: 0,
        }
    }

    pub(crate) fn new_lowlevel(
        data: SmallVec<[(TextResourceHandle, TextSelectionHandle); 2]>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::LowVec(data),
            store,
            cursor: 0,
        }
    }

    pub(crate) fn new_with_iterator(
        iter: FindTextSelectionsIter<'store>,
        store: &'store AnnotationStore,
    ) -> Self {
        Self {
            source: TextSelectionsSource::FindIter(iter),
            store,
            cursor: 0,
        }
    }

    /// Iterate over the annotations that make use of text selections in this iterator. No duplicates are returned and results are in chronological order.
    pub fn annotations(self) -> AnnotationsIter<'store> {
        let mut annotations: Vec<AnnotationHandle> = Vec::new();
        let store = self.store;
        for textselection in self {
            if let Some(moreannotations) = store.annotations_by_textselection(
                textselection.resource().handle(),
                textselection.inner(),
            ) {
                annotations.extend(moreannotations);
            }
        }
        annotations.sort_unstable();
        annotations.dedup();
        AnnotationsIter::new(IntersectionIter::new(Cow::Owned(annotations), true), store)
    }

    /// Find all text selections that are related to any text selections in this iterator, the operator
    /// determines the type of the relation.
    pub fn related_text(self, operator: TextSelectionOperator) -> TextSelectionsIter<'store> {
        let mut textselections: Vec<ResultTextSelection<'store>> = Vec::new();
        let store = self.store;
        for textselection in self {
            textselections.extend(textselection.related_text(operator))
        }
        textselections.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        textselections.dedup();
        TextSelectionsIter::new(textselections, store)
    }

    /// Iterates over all text slices in this iterator
    pub fn text(self) -> impl Iterator<Item = &'store str> {
        self.map(|textselection| textselection.text())
    }

    /// Returns all underlying text concatenated into a single String
    pub fn text_join(self, delimiter: &str) -> String {
        let mut s = String::new();
        for textselection in self {
            let text = textselection.text();
            if !s.is_empty() {
                s += delimiter;
            }
            s += text;
        }
        s
    }

    /// If this acollections refers to a single simple text slice,
    /// this returns it. If it contains no text or multiple text references, it returns None.
    pub fn text_simple(self) -> Option<&'store str> {
        let mut iter = self.text();
        let text = iter.next();
        if let None = iter.next() {
            return text;
        } else {
            None
        }
    }
}
