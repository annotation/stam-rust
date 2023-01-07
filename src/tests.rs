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

    let res_intid = store.insert(
        TextResource::from_string("testres".to_string(),"Hello world".to_string())
    );

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".to_string());
    dataset.insert(DataKey::new("pos".to_string(), false))?;
    store.insert(dataset)?;

    Ok(())
}

#[test]
fn sanity_check() -> Result<(),StamError> {
    let mut store = AnnotationStore::new().with_id("test".to_string());

    let res_intid = store.insert(
        TextResource::from_string("testres".to_string(),"Hello world".to_string())
    );

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".to_string());
    dataset.insert(DataKey::new("pos".to_string(), false))?;

    let dataset_intid = store.insert(dataset)?;

    //get by intid
    let dataset: &AnnotationDataSet = store.get(dataset_intid)?;

    //get by id
    let resource: &TextResource = store.get_by_id("testres")?;
    //store.annotate(TextSelector::new(&resource), &dataset, &key, )
    Ok(())
}

pub fn setup_example_1() -> Result<AnnotationStore,StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new().with_id("test".to_string())
        .add(TextResource::from_string("testres".to_string(),"Hello world".to_string()))?
        .add(AnnotationDataSet::new().with_id("testdataset".to_string())
               .add(DataKey::new("pos".to_string(), false))?
               .add( NewAnnotationData::new("D1", "pos", DataValue::from("noun")))?
        )?
        .add( NewAnnotation::new("A1", 
                             NewSelector::TextSelector { resource: "testres", offset: Offset::simple(6,11) }
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

    //test by public ID
    let _resource: &TextResource = store.get_by_id("testres")?;
    let dataset: &AnnotationDataSet = store.get_by_id("testdataset")?;
    let _datakey: &DataKey = dataset.get_by_id("pos")?;
    let _annotationdata: &AnnotationData = dataset.get_by_id("D1")?;
    let _annotation: &Annotation = store.get_by_id("A1")?;
    Ok(())
}

#[test]
fn sanity_check_iter_data() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let annotation: &Annotation = store.get_by_id("A1")?;

    let mut count = 0;
    for (datakey, annotationdata, dataset) in store.iter_data(annotation) {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(datakey.get_id(), Some("pos"));
        assert_eq!(datakey.as_str(), "pos"); //shortcut for the same as above
        assert_eq!(datakey, "pos"); //shortcut for the same as above
        assert_eq!(annotationdata.get_value(), &DataValue::String("noun".to_string())); 
        assert_eq!(annotationdata.get_value(), "noun"); //shortcut for the same as above (and more efficient without heap allocated string)
        assert_eq!(annotationdata.get_id(), Some("D1"));
        assert_eq!(dataset.get_id(), Some("testdataset"));
    }
    assert_eq!(count,1);
    
    Ok(())
}
