use std::borrow::Cow;

use crate::types::*;
use crate::resources::*;
use crate::annotationstore::*;
use crate::annotationdata::*;

#[cfg(test)]
#[test]
fn instantiation() {
    let mut store = AnnotationStore::new().with_id("test".to_string());

    let res_intid = store.add(
        TextResource::from_string("testres".to_string(),"Hello world".to_string())
    );

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".to_string());
    dataset.add(DataKey::new("pos".to_string(), false));
    let dataset_intid = store.add(dataset).expect("add failed");

    //get by intid
    let dataset: &AnnotationDataSet = store.get(dataset_intid).expect("get failed");
    //get by id
    let resource: &TextResource = store.get_by_id("testres").unwrap();
    let selector = resource.select_resource(); 

    //store.annotate(TextSelector::new(&resource), &dataset, &key, )

}

fn instantiation_build() {
    let mut store = AnnotationStore::new().with_id("test".to_string());

    /*
    store
        .build(TextResource::from_string("testres".to_string(),"Hello world".to_string()))
        .build(AnnotationDataSet::new().with_id("testdataset".to_string())
            .build(DataKey::new("pos".to_string(), false))
            .build_annotationdata("D1", "pos", DataValue::String("noun")))
        .build("A1", BuildSelector::TextSelector("testres",6,11), vec!["A1"]);
    */

    //store.annotate(TextSelector::new(&resource), &dataset, &key, )
    store
        .store(TextResource::from_string("testres".to_string(),"Hello world".to_string()))
        .store(AnnotationDataSet::new().with_id("testdataset".to_string())
            .store(DataKey::new("pos".to_string(), false))
            .build( BuildAnnotationData::new("D1", "pos", DataValue::from("noun"))));
//        .build( BuildAnnotation::new("A1", BuildSelector::TextSelector("testres",6,11), vec!["A1"]));

}
