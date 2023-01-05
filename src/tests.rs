use crate::types::*;
use crate::resources::*;
use crate::annotationstore::*;
use crate::annotationdata::*;

#[cfg(test)]
#[test]
fn instantiation() {
    let mut store = AnnotationStore::new().with_id("test".to_string());

    let resource = TextResource::from_string("testres".to_string(),"Hello world".to_string());
    store.add(resource);

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".to_string());
    dataset.get_or_add(DataKey::new("pos".to_string(), false));
    store.add(dataset);

    let dataset = store.get_dataset("testdataset").unwrap();
    let resource = store.get_resource("testres").unwrap();
    let selector = resource.select_resource(); 

    //store.annotate(TextSelector::new(&resource), &dataset, &key, )

}
