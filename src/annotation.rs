use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;
use std::slice::Iter;

use datasize::DataSize;
use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::annotationdata::{
    AnnotationData, AnnotationDataBuilder, AnnotationDataHandle, AnnotationDataRefWithSet,
};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::config::{Config, Configurable};
use crate::datakey::{DataKey, DataKeyHandle};
use crate::datavalue::{DataOperator, DataValue};
use crate::error::*;
use crate::file::*;
use crate::resources::TextResource;
use crate::selector::{
    AncestorVec, Offset, Selector, SelectorBuilder, SelectorIter, SelectorIterItem, SelfSelector,
    WrappedSelector,
};
use crate::store::*;
use crate::text::Text;
use crate::textselection::ResultTextSelection;
use crate::types::*;

type DataVec = Vec<(AnnotationDataSetHandle, AnnotationDataHandle)>; // I also tried SmallVec, makes no noticable difference in memory size or performance

/// `Annotation` represents a particular *instance of annotation* and is the central
/// concept of the model. They can be considered the primary nodes of the graph model. The
/// instance of annotation is strictly decoupled from the *data* or key/value of the
/// annotation ([`AnnotationData`]). After all, multiple instances can be annotated
/// with the same label (multiple annotations may share the same annotation data).
/// Moreover, an `Annotation` can have multiple annotation data associated.
/// The result is that multiple annotations with the exact same content require less storage
/// space, and searching and indexing is facilitated.  
#[derive(Clone, Debug, DataSize, Encode, Decode)]
pub struct Annotation {
    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership,
    /// encapsulated by a handle type
    #[n(0)] //these macros are field index numbers for cbor binary (de)serialisation
    intid: Option<AnnotationHandle>,

    /// Public identifier for this annotation
    #[n(1)]
    id: Option<String>,

    /// Reference to the annotation data (may be multiple) that describe(s) this annotation, the first ID refers to an AnnotationDataSet as owned by the AnnotationStore, the second to an AnnotationData instance as owned by that set
    #[n(2)]
    data: DataVec,

    /// Determines selection target
    #[n(3)]
    target: Selector, //note: Boxing this didn't reduce overall memory footprint, even though Annotation became smaller, probably due to allocator overhead
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, DataSize, Encode, Decode)]
#[cbor(transparent)]
pub struct AnnotationHandle(#[n(0)] u32);

#[sealed]
impl Handle for AnnotationHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl<'a> Request<Annotation> for AnnotationHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<AnnotationHandle>
    where
        S: StoreFor<Annotation>,
    {
        Some(*self)
    }
}

// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> From<&AnnotationHandle> for BuildItem<'a, Annotation> {
    fn from(handle: &AnnotationHandle) -> Self {
        BuildItem::Handle(*handle)
    }
}
impl<'a> From<Option<&AnnotationHandle>> for BuildItem<'a, Annotation> {
    fn from(handle: Option<&AnnotationHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(*handle)
        } else {
            BuildItem::None
        }
    }
}
impl<'a> From<AnnotationHandle> for BuildItem<'a, Annotation> {
    fn from(handle: AnnotationHandle) -> Self {
        BuildItem::Handle(handle)
    }
}
impl<'a> From<Option<AnnotationHandle>> for BuildItem<'a, Annotation> {
    fn from(handle: Option<AnnotationHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(handle)
        } else {
            BuildItem::None
        }
    }
}

#[sealed]
impl TypeInfo for Annotation {
    fn typeinfo() -> Type {
        Type::Annotation
    }
}

#[sealed]
impl Storable for Annotation {
    type HandleType = AnnotationHandle;
    type StoreType = AnnotationStore;

    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
    fn handle(&self) -> Option<Self::HandleType> {
        self.intid
    }
    fn set_handle(&mut self, handle: Self::HandleType) {
        self.intid = Some(handle);
    }

    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    fn carries_id() -> bool {
        true
    }

    fn set_id(&mut self, id: Option<String>) {
        self.id = id;
    }
}

impl PartialEq<Annotation> for Annotation {
    fn eq(&self, other: &Annotation) -> bool {
        self.id.is_some()
            && self.id == other.id
            && self.target == other.target
            && self.data == other.data
    }
}
/// This is the builder that builds [`Annotation`]. The actual building is done by passing this structure to [`AnnotationStore::annotate()`], there is no `build()` method for this builder.
#[derive(Deserialize, Debug)]
#[serde(tag = "Annotation")]
#[serde(from = "AnnotationJson")]
pub struct AnnotationBuilder<'a> {
    pub(crate) id: BuildItem<'a, Annotation>,
    pub(crate) data: Vec<AnnotationDataBuilder<'a>>,
    pub(crate) target: WithAnnotationTarget<'a>,
}

#[derive(Debug)]
pub(crate) enum WithAnnotationTarget<'a> {
    Unset,
    FromSelector(Selector),
    FromSelectorBuilder(SelectorBuilder<'a>),
}

impl<'a> From<Selector> for WithAnnotationTarget<'a> {
    fn from(other: Selector) -> Self {
        Self::FromSelector(other)
    }
}

impl<'a> From<SelectorBuilder<'a>> for WithAnnotationTarget<'a> {
    fn from(other: SelectorBuilder<'a>) -> Self {
        Self::FromSelectorBuilder(other)
    }
}

impl<'a> Default for AnnotationBuilder<'a> {
    fn default() -> Self {
        Self {
            id: BuildItem::None,
            target: WithAnnotationTarget::Unset,
            data: Vec::new(),
        }
    }
}

impl Annotation {
    /// Writes an Annotation to one big STAM JSON string, with appropriate formatting
    pub fn to_json_string(&self, store: &AnnotationStore) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        let wrapped: ResultItem<Self> = ResultItem::new(self, store)?;
        serde_json::to_string_pretty(&wrapped).map_err(|e| {
            StamError::SerializationError(format!("Writing annotation to string: {}", e))
        })
    }
}

impl<'a> AnnotationBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = BuildItem::Id(id.into());
        self
    }

    /// Sets the annotation target using an already existing selector
    /// Use [`Self.with_target()`] or one of the `target_` shortcut methods instead if you still need to build the target selector
    pub fn with_selector(mut self, selector: Selector) -> Self {
        self.target = WithAnnotationTarget::FromSelector(selector);
        self
    }

    /// Set the target to be a [`crate::resources::TextResource`]. Creates a [`Selector::ResourceSelector`]
    /// Sets the annotation target. Instantiates a new selector. Use [`Self.with_target()`] instead if you already have
    /// a selector. Under the hood, this will invoke `select()` to obtain a selector.
    pub fn with_target(mut self, selector: SelectorBuilder<'a>) -> Self {
        self.target = WithAnnotationTarget::FromSelectorBuilder(selector);
        self
    }

    /// Associate data with the annotation.
    /// If you provide a public key ID that does not exist yet, it's [`crate::DataKey`] will be created.
    ///
    /// You may use this (and similar methods) multiple times.
    /// Do note that multiple data associated with the same annotation is considered *inter-dependent*,
    /// use multiple annotations instead if each it interpretable independent of the others.
    pub fn with_data(
        self,
        annotationset: impl Into<BuildItem<'a, AnnotationDataSet>>,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue>,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            annotationset: annotationset.into(),
            key: key.into(),
            value: value.into(),
            ..Default::default()
        })
    }

    /// Use this method instead of [`Self::with_data()`]  if you want to assign a public identifier (last argument)
    pub fn with_data_with_id(
        self,
        dataset: impl Into<BuildItem<'a, AnnotationDataSet>>,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue>,
        id: impl Into<BuildItem<'a, AnnotationData>>,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            id: id.into(),
            annotationset: dataset.into(),
            key: key.into(),
            value: value.into(),
        })
    }

    /// This references existing [`AnnotationData`], in a particular [`AnnotationDataSet'], by Id.
    /// Useful if you have an Id or reference instance already.
    pub fn with_existing_data(
        self,
        dataset: impl Into<BuildItem<'a, AnnotationDataSet>>,
        annotationdata: impl Into<BuildItem<'a, AnnotationData>>,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            id: annotationdata.into(),
            annotationset: dataset.into(),
            ..Default::default()
        })
    }

    /// Lower level method if you want to create and pass [`AnnotationDataBuilder'] yourself rather than use the other ``with_data_*()`` shortcut methods.
    pub fn with_data_builder(mut self, builder: AnnotationDataBuilder<'a>) -> Self {
        self.data.push(builder);
        self
    }

    /// Reads a single annotation in STAM JSON from file
    pub fn from_json_file(filename: &str, config: &Config) -> Result<Self, StamError> {
        let reader = open_file_reader(filename, config)?;
        let deserializer = &mut serde_json::Deserializer::from_reader(reader);
        let result: Result<Self, _> = serde_path_to_error::deserialize(deserializer);
        result.map_err(|e| {
            StamError::JsonError(e, filename.to_string(), "Reading annotation from file")
        })
    }

    /// Reads a single annotation in STAM JSON from string
    pub fn from_json_str(string: &str) -> Result<Self, StamError> {
        let deserializer = &mut serde_json::Deserializer::from_str(string);
        let result: Result<Self, _> = serde_path_to_error::deserialize(deserializer);
        result.map_err(|e| {
            StamError::JsonError(e, string.to_string(), "Reading annotation from string")
        })
    }
}

impl<'a> Serialize for ResultItem<'a, Annotation> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Annotation", 2)?;
        state.serialize_field("@type", "Annotation")?;
        state.serialize_field("@id", &self.id())?;
        // we need to wrap the selector in a smart pointer so it has a reference to the annotation store
        // only as a smart pointer can it be serialized (because it needs to look up the resource IDs)
        let target = WrappedSelector::new(&self.as_ref().target(), &self.store());
        state.serialize_field("target", &target)?;
        let wrappeddata = AnnotationDataRefSerializer { annotation: &self };
        state.serialize_field("data", &wrappeddata)?;
        state.end()
    }
}

//Helper for serialising annotationdata under annotations
struct AnnotationDataRefSerializer<'a, 'b> {
    annotation: &'b ResultItem<'a, Annotation>,
}

struct AnnotationDataRef<'a> {
    id: &'a str,
    set: &'a str,
}

impl<'a> Serialize for AnnotationDataRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AnnotationDataRef", 3)?;
        state.serialize_field("@type", "AnnotationData")?;
        state.serialize_field("@id", &self.id)?;
        state.serialize_field("set", &self.set)?;
        state.end()
    }
}

//This implementation serializes the AnnotationData references under Annotation
impl<'a, 'b> Serialize for AnnotationDataRefSerializer<'a, 'b> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.annotation.as_ref().data.len()))?;
        for (datasethandle, datahandle) in self.annotation.as_ref().data.iter() {
            let store: &AnnotationStore = self.annotation.store();
            let annotationset: &AnnotationDataSet = store
                .get(*datasethandle)
                .map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
            let annotationdata: &AnnotationData = annotationset
                .get(*datahandle)
                .map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
            if annotationdata.id().is_none() {
                //AnnotationData has no ID, we can't make a reference, therefore we serialize the whole thing (may lead to redundancy in the output)
                //                v--- this is just a newtype wrapper around WrappedStorable<'a, AnnotationData, AnnotationDataSet>, with a distinct
                //                     serialize implementation so it also outputs the set
                let wrappeddata = AnnotationDataRefWithSet(
                    annotationdata
                        .as_resultitem(annotationset)
                        .map_err(|e| serde::ser::Error::custom(format!("{}", e)))?,
                );
                seq.serialize_element(&wrappeddata)?;
            } else {
                seq.serialize_element(&AnnotationDataRef {
                    id: annotationdata.id().unwrap(),
                    set: annotationset.id().ok_or_else(|| {
                        serde::ser::Error::custom(
                            "AnnotationDataSet must have a public ID if it is to be serialized",
                        )
                    })?,
                })?;
            }
        }
        seq.end()
    }
}

impl AnnotationStore {
    /// Builds and adds an annotation
    pub fn with_annotation(mut self, builder: AnnotationBuilder) -> Result<Self, StamError> {
        self.annotate(builder)?;
        Ok(self)
    }

    /// Builds and adds multiple annotations
    pub fn with_annotations(mut self, builders: Vec<AnnotationBuilder>) -> Result<Self, StamError> {
        self.annotate_from_iter(builders)?;
        Ok(self)
    }

    /// Builds and inserts an AnnotationData item
    pub fn insert_data(
        &mut self,
        dataitem: AnnotationDataBuilder,
    ) -> Result<(AnnotationDataSetHandle, AnnotationDataHandle), StamError> {
        debug(self.config(), || {
            format!("AnnotationStore.insert_data: dataitem={:?}", dataitem)
        });
        // Obtain the dataset for this data item
        let dataset: &mut AnnotationDataSet =
            if let Ok(dataset) = self.get_mut(&dataitem.annotationset) {
                dataset
            } else {
                // this data referenced a dataset that does not exist yet, create it
                let dataset_id: String = match dataitem.annotationset {
                    BuildItem::Id(dataset_id) => dataset_id,
                    BuildItem::IdRef(dataset_id) => dataset_id.to_string(),
                    _ =>
                    // if no dataset was specified at all, we create one named 'default-annotationset'
                    // the name is not prescribed by the STAM spec, the fact that we
                    // handle a missing set, however, is.
                    {
                        "default-annotationset".into()
                    }
                };
                let inserted_intid =
                    self.insert(AnnotationDataSet::new(self.config().clone()).with_id(dataset_id))?;
                self.get_mut(inserted_intid)
                    .expect("must exist after insertion")
            };

        // Insert the data into the dataset
        let data_handle = dataset.insert_data(dataitem.id, dataitem.key, dataitem.value, true)?;
        Ok((dataset.handle_or_err()?, data_handle))
    }

    /// Builds and inserts an annotation
    /// In a builder pattenr, use [`Self.with_annotation()`] instead
    pub fn annotate(&mut self, builder: AnnotationBuilder) -> Result<AnnotationHandle, StamError> {
        debug(self.config(), || {
            format!("AnnotationStore.annotate: builder={:?}", builder)
        });
        // Create the target selector if needed
        // If the selector fails, the annotate() fails with an error
        let target = match builder.target {
            WithAnnotationTarget::Unset => {
                //No target was set at all! An annotation must have a target
                return Err(StamError::NoTarget(""));
            }
            WithAnnotationTarget::FromSelector(s) => {
                //We are given a selector directly, good
                s
            }
            WithAnnotationTarget::FromSelectorBuilder(s) => {
                //We are given a builder for a selector, obtain the actual selector
                self.selector(s)?
            }
        };

        // Convert AnnotationDataBuilder into AnnotationData that is ready to be stored
        let mut data = DataVec::with_capacity(builder.data.len());
        for dataitem in builder.data {
            let (datasethandle, datahandle) = self.insert_data(dataitem)?;
            data.push((datasethandle, datahandle));
        }

        // Has the caller set a public ID for this annotation?
        let public_id: Option<String> = match builder.id {
            BuildItem::Id(id) => Some(id),
            _ => None,
        };

        // Add the annotation to the store
        self.insert(Annotation::new(public_id, target, data))
    }

    /// Builds and inserts using multiple annotation builders
    pub fn annotate_from_iter<'a, I>(&mut self, builders: I) -> Result<(), StamError>
    where
        I: IntoIterator<Item = AnnotationBuilder<'a>>,
    {
        for builder in builders {
            self.annotate(builder)?;
        }
        Ok(())
    }
}

impl<'a> Annotation {
    /// Create a new unbounded Annotation instance, you will likely want to use BuildAnnotation::new() instead and pass it to AnnotationStore.build()
    fn new(id: Option<String>, target: Selector, data: DataVec) -> Self {
        Annotation {
            id,
            data,
            target,
            intid: None, //unbounded upon first instantiation
        }
    }

    /// Returns an Annotation builder to build new annotations
    pub fn builder() -> AnnotationBuilder<'a> {
        AnnotationBuilder::default()
    }

    /// Iterate over the annotation data, returns tuples of internal IDs for (annotationset,annotationdata)
    /// For a higher-level method, use `WrappedItem<Annotation>::data()` instead.
    pub fn data(&'a self) -> Iter<'a, (AnnotationDataSetHandle, AnnotationDataHandle)> {
        self.data.iter()
    }

    /// Low-level method that returns raw data (handles) at specified index
    pub fn data_by_index(
        &self,
        index: usize,
    ) -> Option<&(AnnotationDataSetHandle, AnnotationDataHandle)> {
        self.data.get(index)
    }

    /// Returns the number of annotation data items
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns a reference to the selector that selects the target of this annotation
    pub fn target(&self) -> &Selector {
        &self.target
    }
}

impl<'store, 'slf> ResultItem<'store, Annotation> {
    /// Iterate over the annotation data, returns [`WrappedItem<AnnotationData>`].
    pub fn data(&'slf self) -> impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'slf {
        //                               this was the magic bit I needed to make it work ---^
        self.as_ref().data().map(|(dataset_handle, data_handle)| {
            let annotationset: &'store AnnotationDataSet = self
                .store()
                .get(*dataset_handle)
                .expect("dataset must exist");
            let annotationdata: ResultItem<'store, AnnotationData> = annotationset
                .annotationdata(*data_handle)
                .expect("data must exist");
            annotationdata
        })
    }

    /// Iterates over the resources this annotation points to
    pub fn resources(&'slf self) -> TargetIter<'store, TextResource> {
        let selector_iter: SelectorIter<'store> = self
            .as_ref() // <-- deref() doesn't work here, need 'store lifetime
            .target()
            .iter(self.store(), true, true);
        //                                                                         ^ -- we track ancestors because it is needed to resolve relative offsets
        TargetIter {
            store: self.store(),
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterates over all the annotations this annotation points to directly (i.e. via a [`Selector::AnnotationSelector'])
    /// Use [`Self.annotations_reverse()'] if you want to find the annotations this resource is pointed by.
    pub fn annotations(
        &'slf self,
        recursive: bool,
        track_ancestors: bool,
    ) -> TargetIter<'store, Annotation> {
        let selector_iter: SelectorIter<'store> = self
            .as_ref() // <-- deref() doesn't work here, need 'store lifetime
            .target()
            .iter(self.store(), recursive, track_ancestors);
        TargetIter {
            store: self.store(),
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterates over all the annotations that reference this annotation, if any
    pub fn annotations_reverse(
        &'slf self,
    ) -> Option<impl Iterator<Item = ResultItem<'store, Annotation>> + 'slf> {
        if let Some(v) = self
            .store()
            .annotations_by_annotation_reverse(self.handle())
        {
            Some(v.iter().map(|a_handle| {
                self.store()
                    .annotation(*a_handle)
                    .expect("annotation handle must be valid")
            }))
        } else {
            None
        }
    }

    /// Iterates over the annotation data sets this annotation points to (only the ones it points to directly using DataSetSelector, i.e. as metadata)
    pub fn annotationsets(&'slf self) -> TargetIter<'store, AnnotationDataSet> {
        let selector_iter: SelectorIter<'store> = self
            .as_ref() // <-- deref() doesn't work here, need 'store lifetime
            .target()
            .iter(self.store(), true, false);
        TargetIter {
            store: self.store(),
            iter: selector_iter,
            _phantomdata: PhantomData,
        }
    }

    /// Iterate over all resources with text selections this annotation refers to (i.e. via [`Selector::TextSelector`])
    pub fn textselections(&'slf self) -> impl Iterator<Item = ResultTextSelection<'store>> + 'slf {
        self.resources().filter_map(|targetitem| {
            //process offset relative offset
            //MAYBE TODO: rewrite AnnotationStore::textselection to something in Textual trait
            self.store()
                .textselection(
                    targetitem.selector(),
                    Some(targetitem.ancestors().iter().map(|x| x.as_ref())),
                )
                .ok() //ignores errors!
        })
    }

    /// Iterates over all text slices this annotation refers to
    pub fn text(&'slf self) -> impl Iterator<Item = &'store str> + 'slf {
        self.textselections()
            .map(|textselection| textselection.text())
    }

    /// Returns the resource the annotation points to. Only works for TextSelector,
    /// ResourceSelector and AnnotationSelector, and not for complex selectors.
    /// Returns a WrongSelectorType error if the annotation does not point at any resource.
    pub fn resource(&'slf self) -> Option<ResultItem<'store, TextResource>> {
        match self.as_ref().target() {
            Selector::TextSelector(res_id, _) | Selector::ResourceSelector(res_id) => {
                self.store().resource(*res_id)
            }
            Selector::AnnotationSelector(a_id, _) => {
                if let Some(annotation) = self.store().annotation(*a_id) {
                    annotation.resource()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Finds the [`AnnotationData'] in the annotation. Returns an iterator over all matches.
    /// If you're not interested in returning the results but merely testing their presence, use `test_data` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn find_data<'a>(
        &'slf self,
        set: Option<impl Request<AnnotationDataSet>>,
        key: Option<impl Request<DataKey>>,
        value: DataOperator<'a>,
    ) -> Option<impl Iterator<Item = ResultItem<'store, AnnotationData>> + 'slf>
    where
        'a: 'slf,
    {
        let mut set_handle: Option<AnnotationDataSetHandle> = None; //None means 'any' in this context
        let mut key_handle: Option<DataKeyHandle> = None; //idem

        if let Some(set) = set {
            if let Ok(set) = self.store().get(set) {
                set_handle = Some(set.handle().expect("set must have handle"));
                if let Some(key) = key {
                    key_handle = key.to_handle(set);
                    if key_handle.is_none() {
                        //requested key doesn't exist, bail out early, we won't find anything at all
                        return None;
                    }
                }
            } else {
                //requested set doesn't exist, bail out early, we won't find anything at all
                return None;
            }
        }

        Some(self.data().filter_map(move |annotationdata| {
            if (set_handle.is_none() || set_handle == annotationdata.set().handle())
                && (key_handle.is_none() || key_handle.unwrap() == annotationdata.key().handle())
                && annotationdata.as_ref().value().test(&value)
            {
                Some(annotationdata)
            } else {
                None
            }
        }))
    }

    /// Tests if the annotation has certain data, returns a boolean.
    /// If you want to actually retrieve the data, use `find_data()` instead.
    ///
    /// Provide `set` and `key`  as Options, if set to `None`, all sets and keys will be searched.
    /// Value is a DataOperator, it is not wrapped in an Option but can be set to `DataOperator::Any` to return all values.
    /// Note: If you pass a `key` you must also pass `set`, otherwise the key will be ignored.
    pub fn test_data<'a>(
        &'slf self,
        set: Option<BuildItem<AnnotationDataSet>>,
        key: Option<BuildItem<DataKey>>,
        value: DataOperator<'a>,
    ) -> bool {
        match self.find_data(set, key, value) {
            Some(mut iter) => iter.next().is_some(),
            None => false,
        }
    }
}

/// Helper structure for deserialisation
#[derive(Deserialize)]
pub(crate) struct AnnotationJson<'a> {
    #[serde(rename = "@id")]
    id: Option<String>,
    data: Vec<AnnotationDataBuilder<'a>>,
    target: SelectorBuilder<'a>,
}

#[derive(Deserialize)]
pub(crate) struct AnnotationsJson<'a>(pub(crate) Vec<AnnotationJson<'a>>);

impl<'a> From<AnnotationJson<'a>> for AnnotationBuilder<'a> {
    fn from(helper: AnnotationJson<'a>) -> Self {
        Self {
            id: helper.id.into(),
            data: helper.data,
            target: WithAnnotationTarget::FromSelectorBuilder(helper.target),
        }
    }
}

impl SelfSelector for Annotation {
    /// Returns a selector to this resource
    fn selector(&self) -> Result<Selector, StamError> {
        if let Some(handle) = self.handle() {
            Ok(Selector::AnnotationSelector(
                handle,
                Some(Offset::default()),
            ))
        } else {
            Err(StamError::Unbound("Annotation::self_selector()"))
        }
    }
}

pub struct TargetIter<'a, T>
where
    T: Storable,
{
    pub(crate) store: &'a T::StoreType,
    pub(crate) iter: SelectorIter<'a>,
    _phantomdata: PhantomData<T>,
}

impl<'a, T> TargetIter<'a, T>
where
    T: Storable,
{
    pub fn new(store: &'a T::StoreType, iter: SelectorIter<'a>) -> Self {
        Self {
            store,
            iter,
            _phantomdata: PhantomData,
        }
    }
}

pub struct TargetIterItem<'store, T>
where
    T: Storable,
{
    pub(crate) item: ResultItem<'store, T>,
    pub(crate) selectoriteritem: SelectorIterItem<'store>,
}

impl<'a, T> Deref for TargetIterItem<'a, T>
where
    T: Storable,
{
    type Target = ResultItem<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl<'store, T> TargetIterItem<'store, T>
where
    T: Storable,
{
    pub fn depth(&self) -> usize {
        self.selectoriteritem.depth()
    }
    pub fn selector<'b>(&'b self) -> &'b Cow<'store, Selector> {
        self.selectoriteritem.selector()
    }
    pub fn ancestors<'b>(&'b self) -> &'b AncestorVec<'store> {
        self.selectoriteritem.ancestors()
    }
    pub fn is_leaf(&self) -> bool {
        self.selectoriteritem.is_leaf()
    }

    // some copied methods from ResultItem:
    pub fn as_ref(&self) -> &'store T {
        self.item.as_ref()
    }
    pub fn handle(&self) -> T::HandleType {
        self.item.handle()
    }
    pub fn id(&self) -> Option<&'store str> {
        self.item.id()
    }
}

impl<'a> Iterator for TargetIter<'a, TextResource> {
    type Item = TargetIterItem<'a, TextResource>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.selector().as_ref() {
                    Selector::TextSelector(res_id, _) | Selector::ResourceSelector(res_id) => {
                        let resource: &TextResource =
                            self.iter.store.get(*res_id).expect("Resource must exist");
                        return Some(TargetIterItem {
                            item: resource
                                .as_resultitem(self.store)
                                .expect("wrap must succeed"),
                            selectoriteritem: selectoritem,
                        });
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a> Iterator for TargetIter<'a, AnnotationDataSet> {
    type Item = TargetIterItem<'a, AnnotationDataSet>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.selector().as_ref() {
                    Selector::DataSetSelector(set_id) => {
                        let annotationset: &AnnotationDataSet =
                            self.iter.store.get(*set_id).expect("Dataset must exist");
                        return Some(TargetIterItem {
                            item: annotationset
                                .as_resultitem(self.store)
                                .expect("wrap must succeed"),
                            selectoriteritem: selectoritem,
                        });
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a> Iterator for TargetIter<'a, Annotation> {
    type Item = TargetIterItem<'a, Annotation>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.selector().as_ref() {
                    Selector::AnnotationSelector(a_id, _) => {
                        let annotation: &Annotation =
                            self.iter.store.get(*a_id).expect("Annotation must exist");
                        return Some(TargetIterItem {
                            item: annotation
                                .as_resultitem(self.store)
                                .expect("wrap must succeed"),
                            selectoriteritem: selectoritem,
                        });
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}
