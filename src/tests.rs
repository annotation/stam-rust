use crate::types::*;
use crate::resources::*;
use crate::annotationstore::*;
use crate::annotationdata::*;

#[cfg(test)]
#[test]
fn instantiation() {
    let mut store = AnnotationStore::new().with_id("test".to_string());
    let mut dataset = AnnotationDataSet::new().with_id("testdataset".to_string());
    let mut resource = TextResource::from_string("testres".to_string(),"Hello world".to_string());
}
