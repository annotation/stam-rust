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
    // Instantiate the store
    let mut store = AnnotationStore::new().with_id("test".into());

    // Insert a text resource into the store
    let _res_pointer = store.insert(
        TextResource::from_string("testres".into(),"Hello world".into())
    );

    // Create a dataset with one key and insert it into the store
    let mut dataset = AnnotationDataSet::new().with_id("testdataset".into());
    dataset.insert(DataKey::new("pos".into()))?;  //returns a DataKeyPointer, not further used in this test
    let dataset_pointer = store.insert(dataset)?;

    //get by pointer (internal id)
    let dataset: &AnnotationDataSet = store.get(dataset_pointer)?;
    assert_eq!(dataset.get_id(), Some("testdataset"));

    //get by directly by id
    let resource: &TextResource = store.get_by_id("testres")?;
    assert_eq!(resource.get_id(), Some("testres"));
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
    let json = r#"{ 
        "@type": "DataKey",
        "@id": "pos"
    }"#;

    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    let key: DataKey = serde_json::from_value(v).unwrap();
    assert_eq!(key.get_id().unwrap(), "pos");
}

#[test]
fn parse_json_annotationdata() -> Result<(), std::io::Error> {
    let json = r#"{ 
        "@type": "AnnotationData",
        "@id": "D2",
        "key": "pos",
        "value": {
            "@type": "String",
            "value": "verb"
        }
    }"#;

    let v: serde_json::Value = serde_json::from_str(json)?;
    let data: AnnotationDataBuilder = serde_json::from_value(v)?;

    assert_eq!(data.id, AnyId::Id("D2".into()));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, AnyId::Id("pos".into()));
    assert_eq!(data.key, "pos");
    assert_eq!(data.value, DataValue::String("verb".into()));
    assert_eq!(data.value, "verb");  //shorter version

    let mut store = setup_example_2().unwrap();
    let dataset: &mut AnnotationDataSet = store.get_mut_by_id("testdataset").unwrap();
    let datapointer = dataset.build_insert_data(data,true).unwrap();

    let data: &AnnotationData = dataset.get(datapointer).unwrap();

    assert_eq!(data.get_id(), Some("D2")); //can also be compared with &str etc
    assert_eq!(data.get_key(&dataset).unwrap().get_id() , Some("pos"));
    assert_eq!(data.get_value(), &DataValue::String("verb".into()));
    assert_eq!(data.get_value(), "verb");  //shorter version

    Ok(())
}

#[test]
fn parse_json_annotationdata2() -> Result<(), std::io::Error> {
    let data = r#"{ 
        "@type": "AnnotationData",
        "@id": "D1",
        "set": "testdataset"
    }"#;

    let v: serde_json::Value = serde_json::from_str(data)?;
    let data: AnnotationDataBuilder = serde_json::from_value(v)?;

    assert_eq!(data.id, AnyId::Id("D1".into()));
    assert_eq!(data.id, "D1"); //can also be compared with &str etc
    assert_eq!(data.dataset, AnyId::Id("testdataset".into()));
    assert_eq!(data.dataset, "testdataset");

    let mut store = setup_example_2().unwrap();
    let dataset: &mut AnnotationDataSet = store.get_mut_by_id("testdataset").unwrap();
    //we alreayd had this annotation, check prior to insert
    let datapointer1: AnnotationDataPointer = dataset.resolve_data_id("D1").unwrap();
    //insert (which doesn't really insert in this case) but returns the same existing pointer
    let datapointer2 = dataset.build_insert_data(data,true).unwrap();
    assert_eq!(datapointer1, datapointer2);

    let data: &AnnotationData = dataset.get(datapointer2).unwrap();

    assert_eq!(data.get_id(), Some("D1")); //can also be compared with &str etc
    assert_eq!(data.get_key(&dataset).unwrap().get_id() , Some("pos"));
    assert_eq!(data.get_value(), &DataValue::String("noun".into()));
    assert_eq!(data.get_value(), "noun");  //shorter version

    Ok(())
}

#[test]
fn parse_json_cursor() -> Result<(), std::io::Error> {
    let data = r#"{
                "@type": "BeginAlignedCursor",
                "value": 0
    }"#;

    let v: serde_json::Value = serde_json::from_str(data)?;
    let cursor: Cursor = serde_json::from_value(v)?;
    assert_eq!( cursor, Cursor::BeginAligned(0) );
    Ok(())
}

#[test]
fn parse_json_offset() -> Result<(), std::io::Error> {
    let data = r#"{
        "begin": {
            "@type": "BeginAlignedCursor",
            "value": 0
        },
        "end": {
            "@type": "BeginAlignedCursor",
            "value": 5
        }
    }"#;

    let v: serde_json::Value = serde_json::from_str(data)?;
    let offset: Offset = serde_json::from_value(v)?;
    assert_eq!( offset, Offset::simple(0,5) );
    Ok(())
}

#[test]
fn parse_json_textselector() -> Result<(), std::io::Error> {
    let data = r#"{ 
        "@type": "TextSelector",
        "resource": "testres",
        "offset": {
            "begin": {
                "@type": "BeginAlignedCursor",
                "value": 0
            },
            "end": {
                "@type": "BeginAlignedCursor",
                "value": 5
            }
        }
    }"#;

    let v: serde_json::Value = serde_json::from_str(data)?;
    let builder: SelectorBuilder = serde_json::from_value(v)?;

    let mut store = setup_example_2().unwrap();
    let selector = store.selector(builder).unwrap(); 
    assert_eq!( selector, Selector::TextSelector(store.resolve_resource_id("testres").unwrap(), Offset::simple(0,5)) );
    Ok(())
}

#[test]
fn parse_json_annotation() -> Result<(), std::io::Error> {
    let data = r#"{ 
        "@type": "Annotation",
        "@id": "A2",
        "target": {
            "@type": "TextSelector",
            "resource": "testres",
            "offset": {
                "begin": {
                    "@type": "BeginAlignedCursor",
                    "value": 0
                },
                "end": {
                    "@type": "BeginAlignedCursor",
                    "value": 5
                }
            }
        },
        "data": [{
            "@type": "AnnotationData",
            "key": "pos",
            "set": "testdataset",
            "value": {
                "@type": "String",
                "value": "interjection"
            }
        }]
    }"#;

    let v: serde_json::Value = serde_json::from_str(data)?;
    let builder: AnnotationBuilder = serde_json::from_value(v)?;
    let mut store = setup_example_2().unwrap();
    let annotationpointer = store.annotate(builder).unwrap();
    let annotation: &Annotation = store.get(annotationpointer).unwrap();
    
    assert_eq!(annotation.get_id(), Some("A2"));
    let mut count = 0;
    for (datakey, annotationdata, dataset) in store.iter_data(annotation) {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(datakey.get_id(), Some("pos"));
        assert_eq!(datakey.as_str(), "pos"); //shortcut for the same as above
        assert_eq!(datakey, "pos"); //shortcut for the same as above
        assert_eq!(annotationdata.get_value(), &DataValue::String("interjection".to_string())); 
        assert_eq!(annotationdata.get_value(), "interjection"); //shortcut for the same as above (and more efficient without heap allocated string)
        assert_eq!(annotationdata.get_id(), None); //No public ID was assigned
        assert_eq!(dataset.get_id(), Some("testdataset"));
    }
    assert_eq!(count,1);

    Ok(())
}

#[test]
fn parse_json_dataset() -> Result<(), std::io::Error> {
    let json = r#"{ 
        "@type": "AnnotationDataSet",
        "@id": "https://purl.org/dc",
        "keys": [
            {
              "@type": "DataKey",
              "@id": "http://purl.org/dc/terms/creator"
            },
            {
              "@type": "DataKey",
              "@id": "http://purl.org/dc/terms/created"
            },
            {
              "@type": "DataKey",
              "@id": "http://purl.org/dc/terms/generator"
            }
        ],
        "data": [
            {
                "@type": "AnnotationData",
                "@id": "D1",
                "key": "http://purl.org/dc/terms/creator",
                "value": {
                    "@type": "String",
                    "value": "proycon"
                }
            }
        ]
    }"#;

    let v: serde_json::Value = serde_json::from_str(json)?;
    let builder: AnnotationDataSetBuilder = serde_json::from_value(v)?;
    let dataset: AnnotationDataSet = builder.try_into().expect("conversion to dataset");


    let mut store = setup_example_2().unwrap();
    let setpointer = store.insert(dataset).unwrap();

    let dataset: &AnnotationDataSet = store.get(setpointer).unwrap();
    assert_eq!(dataset.get_id(), Some("https://purl.org/dc"));

    let mut count = 0;
    let mut firstkeypointer: Option<DataKeyPointer> = None;
    for key in dataset.iter_keys() {
        count += 1;
        if count == 1 {
            assert_eq!(key.get_id(), Some("http://purl.org/dc/terms/creator"));
            firstkeypointer = key.get_pointer();
        }
    }
    assert_eq!(count,3);

    count = 0;
    for data in dataset.iter_data() {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(data.get_id(), Some("D1"));
        assert_eq!(data.get_key_pointer(), firstkeypointer.unwrap()); //shortcut for the same as above
        assert_eq!(data.get_value(), "proycon");
    }
    assert_eq!(count,1);

    
    Ok(())
}
