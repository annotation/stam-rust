use datasize::{data_size, DataSize};
use minicbor::{Decode, Encode};
use sealed::sealed;
use serde::de::DeserializeSeed;
use serde::ser::{SerializeStruct, Serializer};
use serde::Serialize;
use std::path::PathBuf;

use crate::annotation::{Annotation, AnnotationHandle};
use crate::annotationdataset::{AnnotationDataSet, AnnotationDataSetHandle};
use crate::annotationstore::AnnotationStore;
use crate::error::StamError;
use crate::file::*;
use crate::json::{FromJson, ToJson};
use crate::resources::TextResource;
use crate::resources::TextResourceHandle;
use crate::store::*;
use crate::types::*;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, DataSize, Encode, Decode)]
#[cbor(transparent)]
pub struct AnnotationSubStoreHandle(#[n(0)] u16);

#[sealed]
impl Handle for AnnotationSubStoreHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u16)
    }
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

/// A substore is a sub-collection of annotations that is serialised as an independent AnnotationStore,
/// The actual contents are still defined and kept by the parent AnnotationStore.
/// This structure only holds references used for serialisation purposes.
#[derive(Debug, Encode, Decode, Default, PartialEq, Clone)]
pub struct AnnotationSubStore {
    ///Internal numeric ID, corresponds with the index in the AnnotationStore::substores that has the ownership
    #[n(0)]
    intid: Option<AnnotationSubStoreHandle>,

    //these macros are field index numbers for cbor binary (de)serialisation, which itself does not allow stand-off files!
    #[n(1)]
    pub(crate) id: Option<String>,

    /// path associated with this substore
    #[n(2)]
    pub(crate) filename: Option<PathBuf>,

    #[n(3)]
    /// Refers to an index in substores which is the parent of the curent substore. This allows for deeper nesting, it is set to None if this is a first level substore
    pub(crate) parent: Option<AnnotationSubStoreHandle>,

    #[n(4)]
    pub(crate) annotations: Vec<AnnotationHandle>,
    #[n(5)]
    pub(crate) annotationsets: Vec<AnnotationDataSetHandle>,
    #[n(6)]
    pub(crate) resources: Vec<TextResourceHandle>,
}

impl AnnotationSubStore {
    /// Returns the ID of the annotation store (if any)
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Returns the filename of the annotation store (if any)
    pub fn filename(&self) -> Option<&PathBuf> {
        self.filename.as_ref()
    }
}

#[sealed]
impl TypeInfo for AnnotationSubStore {
    fn typeinfo() -> Type {
        Type::AnnotationSubStore
    }
}

impl AnnotationSubStore {
    fn with_parent(mut self, index: Option<AnnotationSubStoreHandle>) -> Self {
        self.parent = index;
        self
    }
}

//these I couldn't solve nicely using generics:

impl<'a> Request<AnnotationSubStore> for AnnotationSubStoreHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<AnnotationSubStoreHandle>
    where
        S: StoreFor<AnnotationSubStore>,
    {
        Some(*self)
    }
}

#[sealed]
impl Storable for AnnotationSubStore {
    type HandleType = AnnotationSubStoreHandle;
    type StoreHandleType = ();
    type FullHandleType = Self::HandleType;
    type StoreType = AnnotationStore;

    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
    fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    fn handle(&self) -> Option<Self::HandleType> {
        self.intid
    }

    fn with_handle(mut self, handle: AnnotationSubStoreHandle) -> Self {
        self.intid = Some(handle);
        self
    }
    fn carries_id() -> bool {
        true
    }

    fn fullhandle(
        _storehandle: Self::StoreHandleType,
        handle: Self::HandleType,
    ) -> Self::FullHandleType {
        handle
    }

    fn merge(&mut self, _other: Self) -> Result<(), StamError> {
        Ok(())
    }

    fn unbind(mut self) -> Self {
        self.intid = None;
        self
    }
}

impl AnnotationStore {
    /// Adds another AnnotationStore as a stand-off dependency (uses the @include mechanism in STAM JSON)
    pub fn add_substore(&mut self, filename: &str) -> Result<(), StamError> {
        if !self.substores.is_empty() {
            // check if the substore is already loaded (it may be referenced from multiple places)
            // in that case we don't need to process it again
            let foundpath = Some(get_filepath(filename, self.config.workdir())?);
            for substore in <Self as StoreFor<AnnotationSubStore>>::iter(self) {
                if substore.filename == foundpath {
                    return Ok(());
                }
            }
        }
        let new_index = self.substores.len();
        let parent_index = if new_index == 0 {
            None
        } else {
            Some(new_index - 1)
        };
        let handle = self.insert(
            AnnotationSubStore::default()
                .with_parent(parent_index.map(|x| AnnotationSubStoreHandle::new(x))),
        )?; //this data will be modified whilst parsing
        self.push_current_substore(handle);
        self.merge_json_file(filename)?;
        self.pop_current_substore();
        Ok(())
    }

    /// used to add a substore to the path, indicating which substore is currently being parsed
    fn push_current_substore(&mut self, index: AnnotationSubStoreHandle) {
        self.config.current_substore_path.push(index);
    }

    /// used to add a substore to the path, indicating which substore is currently being parsed
    fn pop_current_substore(&mut self) -> bool {
        self.config.current_substore_path.pop().is_some()
    }
}

pub trait AssignToSubStore<T>
where
    T: Storable,
{
    /// Assigns an item to a substore.
    /// Depending on the type of item, this can be either an exclusive assignment (one-to-one) or allow multiple (one-to-many)
    /// Annotations are always exclusive (one-to-one), Resources and datasets can be one-to-many if
    /// and only if they are stand-off (i.e. they have an associated filename and use the @include
    /// mechanism).
    /// If this is called on exclusive items, they old substore will be unassigned before the new one is assigned.
    fn assign_substore(
        &mut self,
        item: impl Request<T>,
        substore: impl Request<AnnotationSubStore>,
    ) -> Result<(), StamError>;
}

impl AssignToSubStore<Annotation> for AnnotationStore {
    fn assign_substore(
        &mut self,
        item: impl Request<Annotation>,
        substore: impl Request<AnnotationSubStore>,
    ) -> Result<(), StamError> {
        if let Some(handle) = item.to_handle(self) {
            //check if the item is already assigned to a substore
            //as this is an exclusive relation (unlike resources/datasets that use @include)
            if let Some(substore_handle) = self.annotation_substore_map.get(handle) {
                //then first remove it from the substore
                let substore = self.get_mut(substore_handle)?;
                if let Some(pos) = substore.annotations.iter().position(|x| *x == handle) {
                    substore.annotations.remove(pos);
                }
                self.annotation_substore_map.remove_all(handle);
            }

            let substore = self.get_mut(substore)?;
            let substore_handle = substore.handle().expect("substore must have handle");
            substore.annotations.push(handle);
            self.annotation_substore_map.insert(handle, substore_handle);
            Ok(())
        } else {
            Err(StamError::NotFoundError(Type::Annotation, "Not found"))
        }
    }
}

impl AssignToSubStore<TextResource> for AnnotationStore {
    fn assign_substore(
        &mut self,
        item: impl Request<TextResource>,
        substore: impl Request<AnnotationSubStore>,
    ) -> Result<(), StamError> {
        if let Some(handle) = item.to_handle(self) {
            let resource = self.get(handle)?;
            if resource.filename().is_some() {
                //the resource is not stand-off, so the relation is exclusive
                //check if the item is already assigned to a substore
                if let Some(substore_handles) = self.resource_substore_map.get(handle) {
                    let substore_handles: Vec<_> = substore_handles.clone();
                    for substore_handle in substore_handles {
                        //then first remove it from the substore
                        let substore = self.get_mut(substore_handle)?;
                        if let Some(pos) = substore.resources.iter().position(|x| *x == handle) {
                            substore.resources.remove(pos);
                        }
                    }
                    self.resource_substore_map.remove_all(handle);
                }
            }

            let substore = self.get_mut(substore)?;
            let substore_handle = substore.handle().expect("substore must have handle");
            if !substore.resources.contains(&handle) {
                substore.resources.push(handle);
            }
            self.resource_substore_map.insert(handle, substore_handle);
            Ok(())
        } else {
            Err(StamError::NotFoundError(Type::Annotation, "Not found"))
        }
    }
}

impl AssignToSubStore<AnnotationDataSet> for AnnotationStore {
    fn assign_substore(
        &mut self,
        item: impl Request<AnnotationDataSet>,
        substore: impl Request<AnnotationSubStore>,
    ) -> Result<(), StamError> {
        if let Some(handle) = item.to_handle(self) {
            let dataset = self.get(handle)?;
            if dataset.filename().is_some() {
                //the dataset is not stand-off, so the relation is exclusive
                //check if the item is already assigned to a substore
                if let Some(substore_handles) = self.dataset_substore_map.get(handle) {
                    let substore_handles: Vec<_> = substore_handles.clone();
                    for substore_handle in substore_handles {
                        //then first remove it from the substore
                        let substore = self.get_mut(substore_handle)?;
                        if let Some(pos) = substore.annotationsets.iter().position(|x| *x == handle)
                        {
                            substore.annotationsets.remove(pos);
                        }
                    }
                    self.dataset_substore_map.remove_all(handle);
                }
            }

            let substore = self.get_mut(substore)?;
            let substore_handle = substore.handle().expect("substore must have handle");
            if !substore.annotationsets.contains(&handle) {
                substore.annotationsets.push(handle);
            }
            self.dataset_substore_map.insert(handle, substore_handle);
            Ok(())
        } else {
            Err(StamError::NotFoundError(Type::Annotation, "Not found"))
        }
    }
}
