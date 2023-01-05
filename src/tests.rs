use crate::types::*;
use crate::resources::*;
use crate::annotationstore::*;
use crate::annotationdata::*;

#[cfg(test)]
#[test]
fn instantiation() {
    let mut store = AnnotationStore::new().with_id("test".to_string());

    let resource = TextResource::from_string("testres".to_string(),"Hello world".to_string());
    let res_intid = store.add(resource);

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
