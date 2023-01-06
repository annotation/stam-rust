use std::borrow::Cow;

use crate::types::*;
use crate::error::*;
use crate::resources::*;
use crate::annotation::*;
use crate::selector::*;
use crate::annotationstore::*;
use crate::annotationdata::*;

#[cfg(test)]

#[test]
fn instantiation_naive() -> Result<(),StamError> {
    let mut store = AnnotationStore::new().with_id("test".to_string());

    let res_intid = store.add(
        TextResource::from_string("testres".to_string(),"Hello world".to_string())
    );

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".to_string());
    dataset.add(DataKey::new("pos".to_string(), false))?;
    store.add(dataset)?;

    Ok(())
}

#[test]
fn sanity_check() -> Result<(),StamError> {
    let mut store = AnnotationStore::new().with_id("test".to_string());

    let res_intid = store.add(
        TextResource::from_string("testres".to_string(),"Hello world".to_string())
    );

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".to_string());
    dataset.add(DataKey::new("pos".to_string(), false))?;

    let dataset_intid = store.add(dataset)?;

    //get by intid
    let dataset: &AnnotationDataSet = store.get(dataset_intid)?;

    //get by id
    let resource: &TextResource = store.get_by_id("testres")?;
    //store.annotate(TextSelector::new(&resource), &dataset, &key, )
    Ok(())
}

pub fn setup_example_1() -> Result<AnnotationStore,StamError> {
    //instantiate with builder pattern
    let mut store = AnnotationStore::new().with_id("test".to_string())
        .store(TextResource::from_string("testres".to_string(),"Hello world".to_string()))?
        .store(AnnotationDataSet::new().with_id("testdataset".to_string())
               .store(DataKey::new("pos".to_string(), false))?
               .build_and_store( BuildAnnotationData::new("D1", "pos", DataValue::from("noun")))?
        )?
        .build_and_store( BuildAnnotation::new("A1", 
                             BuildSelector::TextSelector { resource: "testres", offset: Offset::simple(6,11) }
                          ).with_data("testdataset","D1"))?;
    Ok(store)
}

#[test]
fn instantiation_with_builder_pattern() -> Result<(),StamError> {
    let mut store = setup_example_1()?;
    Ok(())
}

#[test]
fn sanity_check_get_by_intid() -> Result<(),StamError> {
    let store = setup_example_1()?;

    //test by internal ID
    let _resource: &TextResource = store.get(0)?;
    let dataset: &AnnotationDataSet = store.get(0)?;
    let _datakey: &DataKey = dataset.get(0)?;
    let _annotationdata: &AnnotationData = dataset.get(0)?;
    let _annotation: &Annotation = store.get(0)?;
    Ok(())
}

#[test]
fn sanity_check_get_by_id() -> Result<(),StamError> {
    let store = setup_example_1()?;

    //test by internal ID
    let _resource: &TextResource = store.get_by_id("testres")?;
    let dataset: &AnnotationDataSet = store.get_by_id("testdataset")?;
    let _datakey: &DataKey = dataset.get_by_id("pos")?;
    let _annotationdata: &AnnotationData = dataset.get_by_id("D1")?;
    let _annotation: &Annotation = store.get_by_id("A1")?;
    Ok(())
}
