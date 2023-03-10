use std::slice::Iter;

use sealed::sealed;
use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::annotationdata::{
    AnnotationData, AnnotationDataBuilder, AnnotationDataHandle, AnnotationDataRefWithSet,
};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKeyHandle;
use crate::datavalue::DataValue;
use crate::error::*;
use crate::resources::TextResourceHandle;
use crate::selector::{Offset, Selector, SelectorBuilder, SelfSelector, WrappedSelector};
use crate::types::*;

/// `Annotation` represents a particular *instance of annotation* and is the central
/// concept of the model. They can be considered the primary nodes of the graph model. The
/// instance of annotation is strictly decoupled from the *data* or key/value of the
/// annotation ([`AnnotationData`]). After all, multiple instances can be annotated
/// with the same label (multiple annotations may share the same annotation data).
/// Moreover, an `Annotation` can have multiple annotation data associated.
/// The result is that multiple annotations with the exact same content require less storage
/// space, and searching and indexing is facilitated.  
#[derive(Clone)]
pub struct Annotation {
    /// Public identifier for this annotation
    id: Option<String>,

    /// Reference to the annotation data (may be multiple) that describe(s) this annotation, the first ID refers to an AnnotationDataSet as owned by the AnnotationStore, the second to an AnnotationData instance as owned by that set
    data: Vec<(AnnotationDataSetHandle, AnnotationDataHandle)>,

    /// Determines selection target
    target: Selector,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership,
    /// encapsulated by a handle type
    intid: Option<AnnotationHandle>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AnnotationHandle(u32);
#[sealed]
impl Handle for AnnotationHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn unwrap(&self) -> usize {
        self.0 as usize
    }
}

#[sealed]
impl Storable for Annotation {
    type HandleType = AnnotationHandle;

    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
    fn handle(&self) -> Option<Self::HandleType> {
        self.intid
    }
    fn set_handle(&mut self, handle: Self::HandleType) {
        self.intid = Some(handle);
    }
}

/// This is the build recipe for `Annotation`. It contains public IDs or handles that will be resolved
/// when the actual Annotation is built. The building is done by passing this to [`AnnotationStore::annotate()`].
#[serde_as]
#[derive(Deserialize, Debug)]
#[serde(tag = "Annotation")]
#[serde(from = "AnnotationJson")]
pub struct AnnotationBuilder {
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    id: AnyId<AnnotationHandle>,
    data: Vec<AnnotationDataBuilder>,
    target: WithAnnotationTarget,
}

#[derive(Debug)]
pub enum WithAnnotationTarget {
    Unset,
    FromSelector(Selector),
    FromSelectorBuilder(SelectorBuilder),
}

impl From<Selector> for WithAnnotationTarget {
    fn from(other: Selector) -> Self {
        Self::FromSelector(other)
    }
}

impl From<SelectorBuilder> for WithAnnotationTarget {
    fn from(other: SelectorBuilder) -> Self {
        Self::FromSelectorBuilder(other)
    }
}

impl Default for AnnotationBuilder {
    fn default() -> Self {
        Self {
            id: AnyId::None,
            target: WithAnnotationTarget::Unset,
            data: Vec::new(),
        }
    }
}

impl AnnotationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: String) -> Self {
        self.id = AnyId::Id(id);
        self
    }

    /// Sets the annotation target using an already existing selector
    /// Use [`Self.with_target()`] or one of the `target_` shortcut methods instead if you still need to build the target selector
    pub fn with_selector(mut self, selector: Selector) -> Self {
        self.target = WithAnnotationTarget::FromSelector(selector);
        self
    }

    /// Set the target to be a [`crate::resources::TextResource`]. Creates a [`Selector::ResourceSelector`]
    pub fn target_resource(self, resource: AnyId<TextResourceHandle>) -> Self {
        self.with_target(SelectorBuilder::ResourceSelector(resource))
    }

    /// Set the target to be a text slice inside a [`crate::resources::TextResource`]. Creates a [`Selector::TextSelector`]
    pub fn target_text(self, resource: AnyId<TextResourceHandle>, offset: Offset) -> Self {
        self.with_target(SelectorBuilder::TextSelector(resource, offset))
    }

    /// Sets the annotation target. Instantiates a new selector. Use [`Self.with_target()`] instead if you already have
    /// a selector. Under the hood, this will invoke `select()` to obtain a selector.
    pub fn with_target(mut self, selector: SelectorBuilder) -> Self {
        self.target = WithAnnotationTarget::FromSelectorBuilder(selector);
        self
    }

    /// Associate data with the annotation.
    /// If you provide a public key ID that does not exist yet, it's ([`crate::DataKey`] will be created).
    ///
    /// You may use this (and similar methods) multiple times.
    /// Do note that multiple data associated with the same annotation is considered *inter-dependent*,
    /// use multiple annotations instead if each it interpretable independent of the others.
    pub fn with_data(
        self,
        annotationset: AnyId<AnnotationDataSetHandle>,
        key: AnyId<DataKeyHandle>,
        value: DataValue,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            annotationset,
            key,
            value,
            ..Default::default()
        })
    }

    /// Use this method instead of [`Self.with_data()`]  if you want to assign a public identifier (last argument)
    pub fn with_data_with_id(
        self,
        dataset: AnyId<AnnotationDataSetHandle>,
        key: AnyId<DataKeyHandle>,
        value: DataValue,
        id: AnyId<AnnotationDataHandle>,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            id,
            annotationset: dataset,
            key,
            value,
        })
    }

    /// This references existing [`AnnotationData`], in a particular [`AnnotationDataSet'], by Id.
    /// Useful if you have an Id or reference instance already.
    pub fn with_data_by_id(
        self,
        dataset: AnyId<AnnotationDataSetHandle>,
        id: AnyId<AnnotationDataHandle>,
    ) -> Self {
        self.with_data_builder(AnnotationDataBuilder {
            id,
            annotationset: dataset,
            ..Default::default()
        })
    }

    /// Lower level method if you want to create and pass [`AnnotationDataBuilder'] yourself.
    pub fn with_data_builder(mut self, builder: AnnotationDataBuilder) -> Self {
        self.data.push(builder);
        self
    }
}

impl<'a> Serialize for WrappedStorable<'a, Annotation, AnnotationStore> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Annotation", 2)?;
        state.serialize_field("@type", "Annotation")?;
        state.serialize_field("@id", &self.id())?;
        // we need to wrap the selector in a smart pointer so it has a reference to the annotation store
        // only as a smart pointer can it be serialized (because it needs to look up the resource IDs)
        let target = WrappedSelector::new(&self.target, &self.store());
        state.serialize_field("target", &target)?;
        let wrappeddata = AnnotationDataRefSerializer { annotation: &self };
        state.serialize_field("data", &wrappeddata)?;
        state.end()
    }
}

//Helper for serialising annotationdata under to annotations
struct AnnotationDataRefSerializer<'a, 'b> {
    annotation: &'b WrappedStorable<'a, Annotation, AnnotationStore>,
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
        state.serialize_field("id", &self.id)?;
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
        let mut seq = serializer.serialize_seq(Some(self.annotation.data.len()))?;
        for (datasethandle, datahandle) in self.annotation.data.iter() {
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
                    annotationset
                        .wrap(annotationdata)
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

    /// Builds an inserts an AnnotationData item
    pub fn insert_data(
        &mut self,
        dataitem: AnnotationDataBuilder,
    ) -> Result<(AnnotationDataSetHandle, AnnotationDataHandle), StamError> {
        // Obtain the dataset for this data item
        let dataset: &mut AnnotationDataSet =
            if let Some(dataset) = self.get_mut_by_anyid(&dataitem.annotationset) {
                dataset
            } else {
                // this data referenced a dataset that does not exist yet, create it
                let dataset_id: String = if let AnyId::Id(dataset_id) = dataitem.annotationset {
                    dataset_id
                } else {
                    // if no dataset was specified at all, we create one named 'default'
                    // the name is not prescribed by the STAM spec, the fact that we
                    // handle a missing set, however, is.
                    "default".into()
                };
                let inserted_intid = self.insert(AnnotationDataSet::new().with_id(dataset_id))?;
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
        let mut data = Vec::with_capacity(builder.data.len());
        for dataitem in builder.data {
            let (datasethandle, datahandle) = self.insert_data(dataitem)?;
            data.push((datasethandle, datahandle));
        }

        // Has the caller set a public ID for this annotation?
        let public_id: Option<String> = match builder.id {
            AnyId::Id(id) => Some(id),
            _ => None,
        };

        // Add the annotation to the store
        self.insert(Annotation::new(public_id, target, data))
    }
}

impl<'a> Annotation {
    /// Create a new unbounded Annotation instance, you will likely want to use BuildAnnotation::new() instead and pass it to AnnotationStore.build()
    pub fn new(
        id: Option<String>,
        target: Selector,
        data: Vec<(AnnotationDataSetHandle, AnnotationDataHandle)>,
    ) -> Self {
        Annotation {
            id,
            data,
            target,
            intid: None, //unbounded upon first instantiation
        }
    }

    /// Returns an Annotation builder to build new annotations
    pub fn builder() -> AnnotationBuilder {
        AnnotationBuilder::default()
    }

    /// Iterate over the annotation data, returns tuples of internal IDs for (annotationset,annotationdata)
    pub fn data(&'a self) -> Iter<'a, (AnnotationDataSetHandle, AnnotationDataHandle)> {
        self.data.iter()
    }

    /// Returns data at specified index
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

/// Helper structure for deserialisation
#[serde_as]
#[derive(Deserialize)]
struct AnnotationJson {
    #[serde(rename = "@id")]
    id: Option<String>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    data: Vec<AnnotationDataBuilder>,
    target: SelectorBuilder,
}

impl From<AnnotationJson> for AnnotationBuilder {
    fn from(helper: AnnotationJson) -> Self {
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
