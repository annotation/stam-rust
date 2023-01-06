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

    let selector = resource.new_selector(); 

    //store.annotate(TextSelector::new(&resource), &dataset, &key, )

}

#[test]
fn instantiation_store() -> Result<(),StamError> {
    let mut store = AnnotationStore::new().with_id("test".to_string());
    store
        .store(TextResource::from_string("testres".to_string(),"Hello world".to_string()))?
        .store(AnnotationDataSet::new().with_id("testdataset".to_string())
               .store(DataKey::new("pos".to_string(), false))?
               .build_and_store( BuildAnnotationData::new("D1", "pos", DataValue::from("noun")))?
        )?
        .build_and_store( BuildAnnotation::new("A1", 
                             BuildSelector::TextSelector { resource: "testres", offset: Offset::simple(6,11) }
                          ).with_data("testdataset","D1"))?;
    Ok(())
}
