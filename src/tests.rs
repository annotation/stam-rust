use crate::types::*;
use crate::annotationstore::*;
use crate::annotationdata::*;

#[cfg(test)]
#[test]
fn annotationstore() {
    let mut store = AnnotationStore::new().with_id("test".to_string());
    let mut dataset = AnnotationDataSet::new().with_id("test".to_string());


}
