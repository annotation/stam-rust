/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the low-level API for [`Annotation`]. It defines and implements the
//! struct, the handle, and things like serialisation, deserialisation to STAM JSON.
//!
//! It also implements [`TargetIter`], an iterator to iterator over the targets of an annotation.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::slice::Iter;

use datasize::DataSize;
use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

use crate::annotationdata::{AnnotationData, AnnotationDataBuilder, AnnotationDataHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::config::{Config, Configurable};
use crate::datakey::DataKey;
use crate::datavalue::DataValue;
use crate::error::*;
use crate::file::*;
use crate::resources::{TextResource, TextResourceHandle};
use crate::selector::{
    OffsetMode, Selector, SelectorBuilder, SelectorIter, SelfSelector, WrappedSelector,
};
use crate::store::*;
use crate::types::*;

use smallvec::SmallVec;

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
    pub(crate) id: Option<String>,

    /// Reference to the annotation data (may be multiple) that describe(s) this annotation, the first ID refers to an AnnotationDataSet as owned by the AnnotationStore, the second to an AnnotationData instance as owned by that set
    #[n(2)]
    data: DataVec,

    /// Determines selection target
    #[n(3)]
    target: Selector, //note: Boxing this didn't reduce overall memory footprint, even though Annotation became smaller, probably due to allocator overhead
}

/// [Handle] to an instance of [`Annotation`] in the store.
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
    type StoreHandleType = ();
    type FullHandleType = Self::HandleType;
    type StoreType = AnnotationStore;

    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
    fn handle(&self) -> Option<Self::HandleType> {
        self.intid
    }
    fn with_handle(mut self, handle: Self::HandleType) -> Self {
        self.intid = Some(handle);
        self
    }
    fn carries_id() -> bool {
        true
    }
    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    fn fullhandle(
        _parenthandle: Self::StoreHandleType,
        handle: Self::HandleType,
    ) -> Self::FullHandleType {
        handle
    }

    fn merge(&mut self, _other: Self) -> Result<(), StamError> {
        return Err(StamError::OtherError("Can not merge annotations"));
    }

    fn unbind(mut self) -> Self {
        self.intid = None;
        self
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
/// This is the builder that builds [`Annotation`]. The actual building is done by passing this
/// structure to [`AnnotationStore::annotate()`], there is no `build()` method for this builder.
///
/// See the top-level documentation for [`AnnotationStore`] for a complete usage example.
#[derive(Deserialize, Debug)]
#[serde(tag = "Annotation")]
#[serde(from = "AnnotationJson")]
pub struct AnnotationBuilder<'a> {
    pub(crate) id: BuildItem<'a, Annotation>,
    pub(crate) data: Vec<AnnotationDataBuilder<'a>>,
    pub(crate) target: Option<SelectorBuilder<'a>>,
}

impl<'a> Default for AnnotationBuilder<'a> {
    fn default() -> Self {
        Self {
            id: BuildItem::None,
            target: None,
            data: Vec::new(),
        }
    }
}

impl Annotation {
    /// Writes an Annotation to one big STAM JSON string, with appropriate formatting
    pub fn to_json_string(&self, store: &AnnotationStore) -> Result<String, StamError> {
        //note: this function is not invoked during regular serialisation via the store
        let wrapped: ResultItem<Self> = ResultItem::new_partial(self, store);
        serde_json::to_string_pretty(&wrapped).map_err(|e| {
            StamError::SerializationError(format!("Writing annotation to string: {}", e))
        })
    }
}

impl<'a> AnnotationBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn data(&self) -> &Vec<AnnotationDataBuilder<'a>> {
        &self.data
    }

    pub fn target(&self) -> Option<&SelectorBuilder<'a>> {
        self.target.as_ref()
    }

    /// Set an explicit ID. If you want to generate a random one, pass the result of `generate_id()` to the first parameter.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = BuildItem::Id(id.into());
        self
    }

    /// Set the target to be a [`crate::resources::TextResource`]. Creates a [`Selector::ResourceSelector`]
    /// Sets the annotation target. Instantiates a new selector. Use [`Self::with_target()`] instead if you already have
    /// a selector. Under the hood, this will invoke `select()` to obtain a selector.
    pub fn with_target(mut self, selector: SelectorBuilder<'a>) -> Self {
        self.target = Some(selector);
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
        dataset: impl Into<BuildItem<'a, AnnotationDataSet>>,
        key: impl Into<BuildItem<'a, DataKey>>,
        value: impl Into<DataValue>,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            dataset: dataset.into(),
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
            dataset: dataset.into(),
            key: key.into(),
            value: value.into(),
        })
    }

    /// This references existing [`AnnotationData`], in a particular [`AnnotationDataSet`], by Id.
    /// Useful if you have an Id or reference instance already.
    pub fn with_existing_data(
        self,
        dataset: impl Into<BuildItem<'a, AnnotationDataSet>>,
        annotationdata: impl Into<BuildItem<'a, AnnotationData>>,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            id: annotationdata.into(),
            dataset: dataset.into(),
            ..Default::default()
        })
    }

    /// Lower level method if you want to create and pass [`AnnotationDataBuilder`] yourself rather than use the other ``with_data_*()`` shortcut methods.
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
        if let Some(id) = self.id() {
            state.serialize_field("@id", id)?;
        } else {
            state.serialize_field(
                "@id",
                &self.as_ref().temp_id().expect("temp_id must succeed"),
            )?;
        }
        // we need to wrap the selector in a smart pointer so it has a reference to the annotation store
        // only as a smart pointer can it be serialized (because it needs to look up the resource IDs)
        let target = WrappedSelector::new(&self.as_ref().target(), &self.store());
        state.serialize_field("target", &target)?;
        let wrappeddata = AnnotationDataRefSerializer { annotation: &self };
        state.serialize_field("data", &wrappeddata)?;
        state.end()
    }
}

/// Helper for serialising annotationdata under annotations
struct AnnotationDataRefSerializer<'a, 'b> {
    annotation: &'b ResultItem<'a, Annotation>,
}

/// Helper structure for serialisation only
struct AnnotationDataRef<'a> {
    id: Cow<'a, str>,
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
            /*
            if annotationdata.id().is_none() {
                //AnnotationData has no ID, we can't make a reference, therefore we serialize the whole thing (may lead to redundancy in the output)
                //                v--- this is just a newtype wrapper around WrappedStorable<'a, AnnotationData, AnnotationDataSet>, with a distinct
                //                     serialize implementation so it also outputs the set
                let wrappeddata =
                    AnnotationDataRefImpliedSet(annotationdata.as_resultitem(annotationset, store));
                seq.serialize_element(&wrappeddata)?;
            } else {*/

            seq.serialize_element(&AnnotationDataRef {
                id: if let Some(id) = annotationdata.id() {
                    Cow::Borrowed(id)
                } else {
                    Cow::Owned(annotationdata.temp_id().expect("temp_id must succeed"))
                },
                set: annotationset.id().ok_or_else(|| {
                    serde::ser::Error::custom(
                        "AnnotationDataSet must have a public ID if it is to be serialized",
                    )
                })?,
            })?;

            //}
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
    pub fn with_annotations<'a, I>(mut self, builders: I) -> Result<Self, StamError>
    where
        I: IntoIterator<Item = AnnotationBuilder<'a>>,
    {
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
        let dataset: &mut AnnotationDataSet = if let Ok(dataset) = self.get_mut(&dataitem.dataset) {
            dataset
        } else {
            // this data referenced a dataset that does not exist yet, create it
            let dataset_id: String = match dataitem.dataset {
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

    /// Builds and inserts an annotation.
    /// If you're instantiating an annotation store from scratch a builder pattern, then you can use [`Self::with_annotation()`] instead.
    ///
    /// ## Example
    /// ```
    /// # use stam::*;
    /// # fn main() -> Result<(),StamError> {
    /// //instantiate a store
    /// let mut store = AnnotationStore::new(Config::default())
    ///     .with_id("example")
    ///     .with_resource(
    ///         TextResourceBuilder::new()
    ///             .with_id("myresource")
    ///             .with_text("Hello world")
    ///     )?
    ///     .with_dataset(
    ///         AnnotationDataSetBuilder::new()
    ///             .with_id("mydataset")
    ///     )?;
    ///
    /// //do some other stuff in the middle (otherwise you could have just as well used with_annotation())
    ///
    /// //and then annotate:
    /// store.annotate(
    ///     AnnotationBuilder::new()
    ///         .with_id("A1")
    ///         .with_target(SelectorBuilder::textselector(
    ///             "myresource",
    ///             Offset::simple(6, 11),
    ///         ))
    ///         .with_data_with_id("mydataset", "part-of-speech", "noun", "D1"),
    ///     )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn annotate(&mut self, builder: AnnotationBuilder) -> Result<AnnotationHandle, StamError> {
        debug(self.config(), || {
            format!("AnnotationStore.annotate: builder={:?}", builder)
        });
        // Create the target selector if needed
        // If the selector fails, the annotate() fails with an error
        if builder.target.is_none() {
            return Err(StamError::NoTarget("(AnnotationStore.annotate)"));
        }
        let target = self.selector(builder.target.unwrap()).map_err(|err| {
            StamError::BuildError(
                Box::new(err),
                "Getting target selector failed (AnnotationStore.annotate)",
            )
        })?;

        // Convert AnnotationDataBuilder into AnnotationData that is ready to be stored
        let mut data = DataVec::with_capacity(builder.data.len());
        for dataitem in builder.data {
            let (datasethandle, datahandle) = self.insert_data(dataitem).map_err(|err| {
                StamError::BuildError(
                    Box::new(err),
                    "Inserting dataitem failed (AnnotationStore.annotate)",
                )
            })?;
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

    /// Builds and inserts using multiple annotation builders. Returns the handles in a vector.
    pub fn annotate_from_iter<'a, I>(
        &mut self,
        builders: I,
    ) -> Result<Vec<AnnotationHandle>, StamError>
    where
        I: IntoIterator<Item = AnnotationBuilder<'a>>,
    {
        let mut handles = Vec::new();
        for builder in builders {
            handles.push(self.annotate(builder)?);
        }
        Ok(handles)
    }
}

impl Annotation {
    /// Create a new unbounded Annotation instance, you will likely want to use BuildAnnotation::new() instead and pass it to AnnotationStore.build()
    fn new(id: Option<String>, target: Selector, data: DataVec) -> Self {
        Annotation {
            id,
            data,
            target,
            intid: None, //unbounded upon first instantiation
        }
    }

    /// Iterate over the annotation data, returns tuples of internal IDs for (annotationset,annotationdata)
    /// For a higher-level method, use [`ResultItem<Annotation>::data()`] instead.
    pub fn data(&self) -> Iter<(AnnotationDataSetHandle, AnnotationDataHandle)> {
        self.data.iter()
    }

    /// Provides access to the raw underlying data
    pub fn raw_data(&self) -> &[(AnnotationDataSetHandle, AnnotationDataHandle)] {
        &self.data
    }

    /// Removes data from the annotation, does not update any reverse indices!
    pub(crate) fn remove_data(&mut self, set: AnnotationDataSetHandle, data: AnnotationDataHandle) {
        self.data.retain(|(s, d)| (*s != set && *d != data));
    }

    /// Low-level method that returns raw data (handles) at specified index
    pub fn data_by_index(
        &self,
        index: usize,
    ) -> Option<&(AnnotationDataSetHandle, AnnotationDataHandle)> {
        self.data.get(index)
    }

    pub fn has_data(&self, set: AnnotationDataSetHandle, handle: AnnotationDataHandle) -> bool {
        self.data.contains(&(set, handle))
    }

    // Low-level method, to add data, it's generally **discouraged** to use this once an annotation is published,
    // because they should be immutable. Note that this does not check for duplicates NOR update reverse indices!
    pub(crate) fn add_data(&mut self, set: AnnotationDataSetHandle, handle: AnnotationDataHandle) {
        self.data.push((set, handle));
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

/// Helper structure for JSON deserialisation
#[derive(Deserialize)]
pub(crate) struct AnnotationJson<'a> {
    #[serde(rename = "@id")]
    id: Option<String>,
    data: Vec<AnnotationDataBuilder<'a>>,
    target: SelectorBuilder<'a>,
}

/// Helper structure for JSON deserialisation
#[derive(Deserialize)]
pub(crate) struct AnnotationsJson<'a>(pub(crate) Vec<AnnotationJson<'a>>);

impl<'a> From<AnnotationJson<'a>> for AnnotationBuilder<'a> {
    fn from(helper: AnnotationJson<'a>) -> Self {
        Self {
            id: helper.id.into(),
            data: helper.data,
            target: Some(helper.target),
        }
    }
}

impl SelfSelector for Annotation {
    /// Returns a selector to this resource
    fn to_selector(&self) -> Result<Selector, StamError> {
        if let Some(handle) = self.handle() {
            if let (Some(res_handle), Some(tsel_handle)) = (
                self.target().resource_handle(),
                self.target().textselection_handle(),
            ) {
                Ok(Selector::AnnotationSelector(
                    handle,
                    Some((res_handle, tsel_handle, OffsetMode::default())),
                ))
            } else {
                Ok(Selector::AnnotationSelector(handle, None))
            }
        } else {
            Err(StamError::Unbound("Annotation::self_selector()"))
        }
    }
}

/// Iterator over the targets (T) for an annotation. This builds upon [`SelectorIter`] and returns atomic handles.
pub struct TargetIter<'a, T>
where
    T: Storable,
{
    pub(crate) iter: SelectorIter<'a>,
    pub(crate) history: SmallVec<[T::HandleType; 3]>, //only used for certain types
    pub(crate) _phantomdata: PhantomData<T>,
}

impl<'a, T> TargetIter<'a, T>
where
    T: Storable,
{
    pub fn new(iter: SelectorIter<'a>) -> Self {
        Self {
            iter,
            history: SmallVec::new(),
            _phantomdata: PhantomData,
        }
    }
}

impl<'a> Iterator for TargetIter<'a, TextResource> {
    type Item = TextResourceHandle;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.as_ref() {
                    Selector::TextSelector(res_id, _, _)
                    | Selector::ResourceSelector(res_id)
                    | Selector::AnnotationSelector(_, Some((res_id, _, _))) => {
                        if self.history.contains(res_id) {
                            continue;
                        }
                        self.history.push(*res_id);
                        return Some(*res_id);
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
    type Item = AnnotationDataSetHandle;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.as_ref() {
                    Selector::DataSetSelector(set_id) => {
                        return Some(*set_id);
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
    type Item = AnnotationHandle;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let selectoritem = self.iter.next();
            if let Some(selectoritem) = selectoritem {
                match selectoritem.as_ref() {
                    Selector::AnnotationSelector(a_id, _) => {
                        if self.iter.recurse_annotation {
                            if self.history.contains(a_id) {
                                continue;
                            }
                            self.history.push(*a_id);
                        }
                        return Some(*a_id);
                    }
                    _ => continue,
                }
            } else {
                return None;
            }
        }
    }
}
