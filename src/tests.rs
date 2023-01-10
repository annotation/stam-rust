use crate::types::*;
use crate::error::*;
use crate::resources::*;
use crate::annotation::*;
use crate::selector::*;
use crate::annotationstore::*;
use crate::annotationdata::*;
use crate::annotationdataset::*;
use crate::datakey::*;
use crate::datavalue::*;

use serde::Deserialize;
use serde_json::Value;

#[cfg(test)]

#[test]
fn instantiation_naive() -> Result<(),StamError> {
    let mut store = AnnotationStore::new().with_id("test".into());

    let _res_intid = store.insert(
        TextResource::from_string("testres".into(),"Hello world".into())
    );

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".into());
    dataset.insert(DataKey::new("pos".into()))?;
    store.insert(dataset)?;

    Ok(())
}

#[test]
fn sanity_check() -> Result<(),StamError> {
    let mut store = AnnotationStore::new().with_id("test".into());

    let _res_intid = store.insert(
        TextResource::from_string("testres".into(),"Hello world".into())
    );

    let mut dataset = AnnotationDataSet::new().with_id("testdataset".into());
    dataset.insert(DataKey::new("pos".into()))?;

    let dataset_intid = store.insert(dataset)?;

    //get by intid
    let _dataset: &AnnotationDataSet = store.get(dataset_intid)?;

    //get by id
    let _resource: &TextResource = store.get_by_id("testres")?;
    Ok(())
}

pub fn setup_example_1() -> Result<AnnotationStore,StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new().with_id("test".into())
        .add( TextResource::from_string("testres".into(), "Hello world".into()))?
        .add( AnnotationDataSet::new().with_id("testdataset".into())
               .add( DataKey::new("pos".into()))?
               .with_data("D1".into(), "pos".into() , "noun".into())?
        )?
        .with_annotation( Annotation::builder() 
                .with_id("A1".into())
                .target_text( "testres".into(), Offset::simple(6,11)) 
                .with_data_by_id("testdataset".into(), "D1".into()) )?;
    Ok(store)
}

pub fn setup_example_2() -> Result<AnnotationStore,StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new().with_id("test".into())
        .add( TextResource::from_string("testres".to_string(),"Hello world".into()))?
        .add( AnnotationDataSet::new().with_id("testdataset".into()))?
        .with_annotation( Annotation::builder()
                .with_id("A1".into())
                .with_target( SelectorBuilder::TextSelector( "testres".into(), Offset::simple(6,11) ))
                .with_data_with_id("testdataset".into(),"pos".into(),"noun".into(),"D1".into())
        )?;
    Ok(store)
}

#[test]
fn instantiation_with_builder_pattern() -> Result<(),StamError> {
    setup_example_1()?;
    Ok(())
}

#[test]
fn store_resolve_id() -> Result<(),StamError> {
    let store = setup_example_1()?;

    //this is a bit too contrived
    let pointer: AnnotationPointer = <AnnotationStore as StoreFor<Annotation>>::resolve_id(&store, "A1")?;
    let _annotation: &Annotation = store.get(pointer)?;

    //This is the shortcut if you want an intermediate pointer
    let pointer = store.resolve_annotation_id("A1")?;
    let _annotation: &Annotation = store.get(pointer)?;

    //this is the direct method without intermediate pointer (still used internally but no longer exposed)
    let _annotation: &Annotation = store.get_by_id("A1")?;
    Ok(())
}

#[test]
fn store_get_by_id() -> Result<(),StamError> {
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
fn store_get_by_id_2() -> Result<(),StamError> {
    let store = setup_example_2()?;

    //test by public ID
    let _resource: &TextResource = store.get_by_id("testres")?;
    let dataset: &AnnotationDataSet = store.get_by_id("testdataset")?;
    let _datakey: &DataKey = dataset.get_by_id("pos")?;
    let _annotationdata: &AnnotationData = dataset.get_by_id("D1")?;
    let _annotation: &Annotation = store.get_by_id("A1")?;
    Ok(())
}

#[test]
fn store_get_by_anyid() -> Result<(),StamError> {
    let store = setup_example_1()?;

    let anyid: AnyId<AnnotationPointer> = "A1".into();
    let _annotation: &Annotation = store.get_by_anyid_or_err(&anyid)?;
    Ok(())
}

#[test]
fn store_iter_data() -> Result<(),StamError> {
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

#[test]
fn store_get_text() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let resource: &TextResource = store.get_by_id("testres")?;
    let text = resource.get_text();
    assert_eq!(text,"Hello world");
    Ok(())
}

#[test]
fn store_get_text_slice() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let resource: &TextResource = store.get_by_id("testres")?;
    let text = resource.get_text_slice( &Offset::new(Cursor::BeginAligned(0),Cursor::BeginAligned(5)))?;
    assert_eq!(text,"Hello");
    let text = resource.get_text_slice( &Offset::simple(0,5))?; //same as above, shorthand
    assert_eq!(text,"Hello");
    let text = resource.get_text_slice( &Offset::simple(6,11))?;
    assert_eq!(text,"world");
    let text = resource.get_text_slice( &Offset::new(Cursor::EndAligned(-5),Cursor::EndAligned(0)))?;
    assert_eq!(text,"world");
    let text = resource.get_text_slice( &Offset::new(Cursor::EndAligned(-11),Cursor::EndAligned(0)))?;
    assert_eq!(text,"Hello world");
    let text = resource.get_text_slice( &Offset::new(Cursor::EndAligned(-11),Cursor::EndAligned(-6)))?;
    assert_eq!(text,"Hello");
    //these should produce an InvalidOffset error (begin >= end)
    assert!( resource.get_text_slice( &Offset::simple(11,7)).is_err() );
    assert!( resource.get_text_slice( &Offset::new(Cursor::EndAligned(-9),Cursor::EndAligned(-11))).is_err() );
    Ok(())
}

#[test]
fn text_selector() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let annotation: &Annotation = store.get_by_id("A1")?;
    let resource: &TextResource = store.select(&annotation.target)?;
    let text: &str = resource.select(&annotation.target)?;
    assert_eq!(text,"world");
    let text: &str = store.select(&annotation.target)?; //shortcut form of the two previous steps combined in one
    assert_eq!(text,"world");
    Ok(())
}

#[test]
fn annotate() -> Result<(),StamError> {
    let mut store = setup_example_1()?;
    store.annotate( AnnotationBuilder::new()
               .with_target( SelectorBuilder::TextSelector( "testres".into(), Offset::simple(0,5) ) )
               .with_data("tokenset".into(),"word".into(), DataValue::Null)
             )?;
    store.annotate( AnnotationBuilder::new()
               .with_target( SelectorBuilder::TextSelector( "testres".into(), Offset::simple(6,11) ) )
               .with_data("tokenset".into(),"word".into(), DataValue::Null)
             )?;
    Ok(())
}



#[test]
fn add_after_borrow() -> Result<(),StamError> {
    let mut store = setup_example_2()?;
    let annotation: &Annotation = store.get_by_id("A1".into())?;
    let mut count = 0;
    for (_datakey, _annotationdata, _dataset) in store.iter_data(annotation) {
        count += 1;
    }
    assert_eq!(count,1);
    store.annotate( AnnotationBuilder::new()
               .with_target( SelectorBuilder::TextSelector("testres".into(), Offset::simple(6,11) ) )
               .with_data("tokenset".into(),"word".into(), DataValue::Null)
             )?;
    Ok(())
}

#[test]
fn add_during_borrowproblem() -> Result<(),StamError> {
    let mut store = setup_example_2()?;
    let annotation: &Annotation = store.get_by_id("A1".into())?;
    //                                 V---- here we clone the annotation to prevent a borrow problem (cannot borrow `store` as mutable because it is also borrowed as immutable (annotation)), this is relatively low-cost
    for (dataset, data) in annotation.clone().iter_data() {
        store.annotate( AnnotationBuilder::new()
                   .with_target( SelectorBuilder::TextSelector( "testres".into(), Offset::simple(6,11) ) )
                   .with_data_by_id(dataset.into(), data.into())
                 )?;
    }
    Ok(())
}


#[test]
fn parse_json_datakey() {
    let data = r#"{ 
        "@type": "DataKey",
        "@id": "pos"
    }"#;

    let v: serde_json::Value = serde_json::from_str(data).unwrap();
    let key: DataKey = serde_json::from_value(v).unwrap();
    assert_eq!(key.get_id().unwrap(), "pos");
}

#[test]
fn parse_json_annotationdata() -> Result<(), std::io::Error> {
    let data = r#"{ 
        "@type": "AnnotationData",
        "@id": "A2",
        "key": "pos",
        "value": {
            "@type": "String",
            "value": "verb"
        }
    }"#;

    let v: serde_json::Value = serde_json::from_str(data)?;
    let _data: AnnotationDataBuilder = serde_json::from_value(v)?;
    Ok(())
}

#[test]
fn parse_json_annotationdata2() -> Result<(), std::io::Error> {
    let data = r#"{ 
        "@type": "AnnotationData",
        "@id": "A2",
        "set": "testset"
    }"#;

    let v: serde_json::Value = serde_json::from_str(data)?;
    let _data: AnnotationDataBuilder = serde_json::from_value(v)?;
    Ok(())
}


/*
#[test]
fn parse_json_anyid() {
    let data = r#""test-id""#;

    let v: serde_json::Value = serde_json::from_str(data).unwrap();
    let id: AnyId = serde_json::from_value(v).unwrap();
    assert_eq!(id.to_string().unwrap(), "test-id");
}
*/
