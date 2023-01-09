use std::slice::Iter;
use std::borrow::Cow;

use crate::types::*;
use crate::error::*;
use crate::annotationdata::AnnotationData;
use crate::annotationdataset::AnnotationDataSet;
use crate::datakey::DataKey;
use crate::datavalue::DataValue;
use crate::annotationstore::AnnotationStore;
use crate::selector::{Selector,Offset,SelectorBuilder};

/// `Annotation` represents a particular *instance of annotation* and is the central
/// concept of the model. They can be considered the primary nodes of the graph model. The
/// instance of annotation is strictly decoupled from the *data* or key/value of the
/// annotation ([`AnnotationData`]). After all, multiple instances can be annotated
/// with the same label (multiple annotations may share the same annotation data).
/// Moreover, an `Annotation` can have multiple annotation data associated. 
/// The result is that multiple annotations with the exact same content require less storage
/// space, and searching and indexing is facilitated.  
pub struct Annotation {
    /// Public identifier for this annotation
    id: Option<String>,

    /// Reference to the annotation data (may be multiple) that describe(s) this annotation, the first ID refers to an AnnotationDataSet as owned by the AnnotationStore, the second to an AnnotationData instance as owned by that set
    data: Vec<(IntId,IntId)>,

    /// Determines selection target
    pub target: Selector,

    ///Internal numeric ID for this AnnotationData, corresponds with the index in the AnnotationDataSet::data that has the ownership. 
    intid: Option<IntId>,
    ///Referers to internal IDs of Annotations (as owned by an AnnotationStore) that reference this Annotation (via an AnnotationSelector)
    referenced_by: Vec<IntId>
}

impl Storable for Annotation {
    fn get_id(&self) -> Option<&str> { 
        self.id.as_ref().map(|x| &**x)
    }
    fn get_intid(&self) -> Option<IntId> { 
        self.intid
    }
}

impl MutableStorable for Annotation {
    fn set_intid(&mut self, intid: IntId) {
        self.intid = Some(intid);
    }
}

/// This is the build recipe for `Annotation`. It contains references to public IDs that will be resolved
/// when the actual AnnotationData is build. The building is done by passing the `BuildAnnotation` to [`AnnotationDataSet::build()`].
pub struct AnnotationBuilder<'a> {
    ///Refers to the key by id, the keys are stored in the AnnotationDataSet that holds this AnnotationData
    id: AnyId<'a>,
    data: Vec<AnnotationDataBuilder<'a>>,
    target: WithAnnotationTarget<'a>,
}

pub struct AnnotationDataBuilder<'a> {
    id: AnyId<'a>,
    dataset: AnyId<'a>,
    key: AnyId<'a>,
    value: DataValue,
}

enum WithAnnotationTarget<'a> {
    Unset,
    FromSelector(Selector),
    FromSelectorBuilder(SelectorBuilder<'a>),
}

impl<'a> Default for AnnotationBuilder<'a> {
    fn default() -> Self {
        Self {
            id: AnyId::None,
            target: WithAnnotationTarget::Unset,
            data: Vec::new(),
        }
    }
}

impl<'a> Default for AnnotationDataBuilder<'a> {
    fn default() -> Self {
        Self {
            id: AnyId::None,
            dataset: AnyId::None,
            key: AnyId::None,
            value: DataValue::Null,
        }
    }
}


impl<'a> AnnotationBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: String) -> Self {
        self.id = AnyId::Id(Cow::Owned(id));
        self
    }

    /// Sets the annotation target using an already existing selector
    /// Use [`with_target()`] or one of the `target_` shortcut methods instead if you still need to build the target selector
    pub fn with_selector(mut self, selector: Selector) -> Self {
        self.target = WithAnnotationTarget::FromSelector(selector);
        self
    }

    pub fn target_resource(self, resource: AnyId<'a>) -> Self {
        self.with_target(SelectorBuilder::ResourceSelector(resource))
    }

    pub fn target_text(self, resource: AnyId<'a>, offset: Offset) -> Self {
        self.with_target(SelectorBuilder::TextSelector{ resource, offset })
    }

    /// Sets the annotation target. Instantiates a new selector. Use [`with_target()`] instead if you already have
    /// a selector. Under the hood, this will invoke `select()` to obtain a selector.
    pub fn with_target(mut self, selector: SelectorBuilder<'a>) -> Self {
        self.target = WithAnnotationTarget::FromSelectorBuilder(selector);
        self
    }

    /// Associate data with the annotation. 
    /// If you provide a public key ID that does not exist yet, it ([`DataKey`] will be created).
    ///
    /// You may use this (and similar methods) multiple times. 
    /// Do note that multiple data associated with the same annotation is considered *inter-dependent*,
    /// use multiple annotations instead if each it interpretable independent of the others.
    pub fn with_data(self, dataset: AnyId<'a>, key: AnyId<'a>, value: DataValue) -> Self {
        self.with_data_builder(
            AnnotationDataBuilder {
                dataset,
                key,
                value,
                ..Default::default()
            }
        )
    }

    /// Use this method instead of [`with_data()`]  if you want to assign a public identifier (last argument)
    pub fn with_data_with_id(self, dataset: AnyId<'a>, key: AnyId<'a>, value: DataValue, id: AnyId<'a>) -> Self {
        self.with_data_builder(
            AnnotationDataBuilder {
                id,
                dataset,
                key,
                value,
                ..Default::default()
            }
        )
    }

    /// This references existing [`AnnotationData`], in a particular [`AnnotationDataSet'], by Id.
    /// Useful if you have an Id or reference instance already.
    pub fn with_data_by_id(self, dataset: AnyId<'a>, id: AnyId<'a>) -> Self {
        self.with_data_builder(
            AnnotationDataBuilder {
                id,
                dataset,
                ..Default::default()
            }
        )
    }

    /// Lower level method if you want to create and pass [`AnnotationDataBuilder'] yourself.
    pub fn with_data_builder(mut self, builder: AnnotationDataBuilder<'a>) -> Self {
        self.data.push(builder);
        self
    }
}

impl<'a> AnnotationStore {
    /// Builds and adds an annotation
    pub fn with_annotation(mut self, builder: AnnotationBuilder<'a>) -> Result<Self,StamError> {
        self.annotate(builder)?;
        Ok(self)
    }

    /// Builds and inserts an annotation
    /// In a builder pattenr, use [`with_annotation()`] instead
    pub fn annotate(&mut self, builder: AnnotationBuilder<'a>) -> Result<IntId,StamError> {

        // Create the target selector if needed
        // If the selector fails, the annotate() fails with an error
        let target = match builder.target {
            WithAnnotationTarget::Unset => {
                //No target was set at all! An annotation must have a target
                return Err(StamError::NoTarget(""));
            },
            WithAnnotationTarget::FromSelector(s) => {
                //We are given a selector directly, good 
                s
            },
            WithAnnotationTarget::FromSelectorBuilder(s) => {
                //We are given a builder for a selector, obtain the actual selector
                self.selector(s)?
            }
        };

        // Convert AnnotationDataBuilder into AnnotationData that is ready to be stored
        let mut data = Vec::with_capacity(builder.data.len());
        for dataitem in builder.data {
            // Obtain the dataset for this data item
            let dataset: &mut AnnotationDataSet = if let Some(dataset) = self.get_mut_by_anyid(&dataitem.dataset) {
                dataset
            } else {
                // this data referenced a dataset that does not exist yet, create it
                let dataset_id: String = if let AnyId::Id(dataset_id) = dataitem.dataset {
                    dataset_id.into()
                } else {
                    // if no dataset was specified at all, we create one named 'default'
                    // the name is not prescribed by the STAM spec, the fact that we 
                    // handle a missing set, however, is.
                    "default".into()
                };
                let inserted_intid = self.insert(AnnotationDataSet::new().with_id(dataset_id))?;
                self.get_mut(inserted_intid).expect("must exist after insertion")
            };

            // Insert the data into the dataset 
            let found_intid = match dataset.insert_data(dataitem.id, dataitem.key, dataitem.value, true) {
                Err(StamError::AlreadyExists(intid, _)) => intid,
                Ok(intid) => intid,
                Err(x) => return Err(x)
            };
            data.push((dataset.get_intid_or_err()?, found_intid));
        }

        // Has the caller set a public ID for this annotation?
        let public_id: Option<String> = match builder.id {
            AnyId::Id(id) => Some(id.into()),
            _ => None
        };

        // Add the annotation to the store
        self.insert(
            Annotation::new(public_id, target, data)
        )
    }
}

impl<'a> Annotation {
    /// Create a new unbounded Annotation instance, you will likely want to use BuildAnnotation::new() instead and pass it to AnnotationStore.build()
    pub fn new(id: Option<String>, target: Selector, data: Vec<(IntId,IntId)>) -> Self {
        Annotation {
            id,
            data,
            target,
            intid: None,
            referenced_by: Vec::new(),
        }
    }

    /// Returns an Annotation builder to build new annotations
    pub fn builder() -> AnnotationBuilder<'a> {
        AnnotationBuilder::default()
    }
}

impl AnnotationStore {
    /// Iterate over the data for the specified annotation
    pub fn iter_data<'a>(&'a self, annotation: &'a Annotation) -> AnnotationDataIter<'a>  {
        AnnotationDataIter {
            store: self,
            iter: annotation.data.iter()
        }
    }
}

pub struct AnnotationDataIter<'a> {
    store: &'a AnnotationStore,
    iter: Iter<'a, (IntId,IntId)>
}


impl<'a> Iterator for AnnotationDataIter<'a> {
    type Item = (&'a DataKey, &'a AnnotationData, &'a AnnotationDataSet);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some((dataset_intid, annotationdata_intid)) => {
                let dataset: &AnnotationDataSet = self.store.get(*dataset_intid).expect("Getting dataset for annotation");
                let annotationdata: &AnnotationData = dataset.get(*annotationdata_intid).expect("Getting annotationdata for annotation");
                let datakey = annotationdata.get_key(dataset).expect("Getting datakey for annotation");
                Some((datakey, annotationdata, dataset))
            },
            None => None
        }
    }
}
