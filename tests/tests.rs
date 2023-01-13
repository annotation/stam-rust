use std::io::prelude::*;
use std::fs::File;

use stam::*;


#[test]
fn instantiation_naive() -> Result<(),StamError> {
    let mut store = AnnotationStore::new().with_id("test".into());

    let _res_intid = store.insert(
        TextResource::from_string("testres".into(),"Hello world".into())
    );

    let mut annotationset = AnnotationDataSet::new().with_id("testdataset".into());
    annotationset.insert(DataKey::new("pos".into()))?;
    store.insert(annotationset)?;

    Ok(())
}

#[test]
fn sanity_check() -> Result<(),StamError> {
    // Instantiate the store
    let mut store = AnnotationStore::new().with_id("test".into());

    // Insert a text resource into the store
    let _res_handle = store.insert(
        TextResource::from_string("testres".into(),"Hello world".into())
    );

    // Create a dataset with one key and insert it into the store
    let mut annotationset = AnnotationDataSet::new().with_id("testdataset".into());
    annotationset.insert(DataKey::new("pos".into()))?;  //returns a DataKeyHandle, not further used in this test
    let set_handle = store.insert(annotationset)?;

    //get by handle (internal id)
    let annotationset: &AnnotationDataSet = store.get(set_handle)?;
    assert_eq!(annotationset.id(), Some("testdataset"));

    //get by directly by id
    let resource: &TextResource = store.get_by_id("testres")?;
    assert_eq!(resource.id(), Some("testres"));
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
    let handle: AnnotationHandle = <AnnotationStore as StoreFor<Annotation>>::resolve_id(&store, "A1")?;
    let _annotation: &Annotation = store.get(handle)?;

    //This is the shortcut if you want an intermediate handle
    let handle = store.resolve_annotation_id("A1")?;
    let _annotation: &Annotation = store.get(handle)?;

    //this is the direct method without intermediate handle (still used internally but no longer exposed)
    let _annotation: &Annotation = store.get_by_id("A1")?;
    Ok(())
}

#[test]
fn store_get_by_id() -> Result<(),StamError> {
    let store = setup_example_1()?;

    //test by public ID
    let _resource: &TextResource = store.get_by_id("testres")?;
    let annotationset: &AnnotationDataSet = store.get_by_id("testdataset")?;
    let _datakey: &DataKey = annotationset.get_by_id("pos")?;
    let _annotationdata: &AnnotationData = annotationset.get_by_id("D1")?;
    let _annotation: &Annotation = store.get_by_id("A1")?;
    Ok(())
}

#[test]
fn store_get_by_id_2() -> Result<(),StamError> {
    let store = setup_example_2()?;

    //test by public ID
    let _resource: &TextResource = store.get_by_id("testres")?;
    let annotationset: &AnnotationDataSet = store.get_by_id("testdataset")?;
    let _datakey: &DataKey = annotationset.get_by_id("pos")?;
    let _annotationdata: &AnnotationData = annotationset.get_by_id("D1")?;
    let _annotation: &Annotation = store.get_by_id("A1")?;
    Ok(())
}

#[test]
fn store_get_by_anyid() -> Result<(),StamError> {
    let store = setup_example_1()?;

    let anyid: AnyId<AnnotationHandle> = "A1".into();
    let _annotation: &Annotation = store.get_by_anyid_or_err(&anyid)?;
    Ok(())
}

#[test]
fn store_iter_data() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let annotation: &Annotation = store.get_by_id("A1")?;

    let mut count = 0;
    for (datakey, annotationdata, annotationset) in store.data(annotation) {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(datakey.id(), Some("pos"));
        assert_eq!(datakey.as_str(), "pos"); //shortcut for the same as above
        assert_eq!(datakey, "pos"); //shortcut for the same as above
        assert_eq!(annotationdata.value(), &DataValue::String("noun".to_string())); 
        assert_eq!(annotationdata.value(), "noun"); //shortcut for the same as above (and more efficient without heap allocated string)
        assert_eq!(annotationdata.id(), Some("D1"));
        assert_eq!(annotationset.id(), Some("testdataset"));
    }
    assert_eq!(count,1);
    
    Ok(())
}

#[test]
fn store_get_text() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let resource: &TextResource = store.get_by_id("testres")?;
    let text = resource.text();
    assert_eq!(text,"Hello world");
    Ok(())
}

#[test]
fn store_get_text_slice() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let resource: &TextResource = store.get_by_id("testres")?;
    let text = resource.text_slice( &Offset::new(Cursor::BeginAligned(0),Cursor::BeginAligned(5)))?;
    assert_eq!(text,"Hello");
    let text = resource.text_slice( &Offset::simple(0,5))?; //same as above, shorthand
    assert_eq!(text,"Hello");
    let text = resource.text_slice( &Offset::simple(6,11))?;
    assert_eq!(text,"world");
    let text = resource.text_slice( &Offset::new(Cursor::EndAligned(-5),Cursor::EndAligned(0)))?;
    assert_eq!(text,"world");
    let text = resource.text_slice( &Offset::new(Cursor::EndAligned(-11),Cursor::EndAligned(0)))?;
    assert_eq!(text,"Hello world");
    let text = resource.text_slice( &Offset::new(Cursor::EndAligned(-11),Cursor::EndAligned(-6)))?;
    assert_eq!(text,"Hello");
    //these should produce an InvalidOffset error (begin >= end)
    assert!( resource.text_slice( &Offset::simple(11,7)).is_err() );
    assert!( resource.text_slice( &Offset::new(Cursor::EndAligned(-9),Cursor::EndAligned(-11))).is_err() );
    Ok(())
}

#[test]
fn text_selector() -> Result<(),StamError> {
    let store = setup_example_1()?;
    let annotation: &Annotation = store.get_by_id("A1")?;
    let resource: &TextResource = store.select(&annotation.target())?;
    let text: &str = resource.select(&annotation.target())?;
    assert_eq!(text,"world");
    let text: &str = store.select(&annotation.target())?; //shortcut form of the two previous steps combined in one
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
fn annotate_existing_data() -> Result<(),StamError> {
    let mut store = setup_example_2()?;
    store.annotate( AnnotationBuilder::new()
               .with_target( SelectorBuilder::TextSelector( "testres".into(), Offset::simple(0,5) ) )
               .with_data("testdataset".into(),"pos".into(), DataValue::String("noun".to_string()))  //this one already exists so should not be recreated but found and referenced intead
             )?;

    //check if the dataset still contains only one key
    let dataset: &AnnotationDataSet = store.get_by_id("testdataset")?;
    assert_eq!( dataset.keys().count(), 1);
    assert_eq!( dataset.data().count(), 1);

    Ok(())
}



#[test]
fn add_after_borrow() -> Result<(),StamError> {
    let mut store = setup_example_2()?;
    let annotation: &Annotation = store.get_by_id("A1".into())?;
    let mut count = 0;
    for (_datakey, _annotationdata, _dataset) in store.data(annotation) {
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
    for (dataset, data) in annotation.clone().data() {
        store.annotate( AnnotationBuilder::new()
                   .with_target( SelectorBuilder::TextSelector( "testres".into(), Offset::simple(6,11) ) )
                   .with_data_by_id(dataset.into(), data.into())
                 )?;
    }
    Ok(())
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

    let data: AnnotationDataBuilder = serde_json::from_str(json)?;

    assert_eq!(data.id, AnyId::Id("D2".into()));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, AnyId::Id("pos".into()));
    assert_eq!(data.key, "pos");
    assert_eq!(data.value, DataValue::String("verb".into()));
    assert_eq!(data.value, "verb");  //shorter version

    let mut store = setup_example_2().unwrap();
    let dataset: &mut AnnotationDataSet = store.get_mut_by_id("testdataset").unwrap();
    let datahandle = dataset.build_insert_data(data,true).unwrap();

    let data: &AnnotationData = dataset.get(datahandle).unwrap();
    let key: &DataKey = dataset.get(data.key()).unwrap();

    assert_eq!(data.id(), Some("D2")); //can also be compared with &str etc
    assert_eq!(key.id() , Some("pos"));
    assert_eq!(data.value(), &DataValue::String("verb".into()));
    assert_eq!(data.value(), "verb");  //shorter version

    Ok(())
}

#[test]
fn parse_json_annotationdata2() -> Result<(), std::io::Error> {
    let data = r#"{ 
        "@type": "AnnotationData",
        "@id": "D1",
        "set": "testdataset"
    }"#;

    let data: AnnotationDataBuilder = serde_json::from_str(data)?;

    assert_eq!(data.id, AnyId::Id("D1".into()));
    assert_eq!(data.id, "D1"); //can also be compared with &str etc
    assert_eq!(data.annotationset, AnyId::Id("testdataset".into()));
    assert_eq!(data.annotationset, "testdataset");

    let mut store = setup_example_2().unwrap();
    let dataset: &mut AnnotationDataSet = store.get_mut_by_id("testdataset").unwrap();
    //we alreayd had this annotation, check prior to insert
    let datahandle1: AnnotationDataHandle = dataset.resolve_data_id("D1").unwrap();
    //insert (which doesn't really insert in this case) but returns the same existing handle
    let datahandle2 = dataset.build_insert_data(data,true).unwrap();
    assert_eq!(datahandle1, datahandle2);

    let data: &AnnotationData = dataset.get(datahandle2).unwrap();
    let key: &DataKey = dataset.get(data.key()).unwrap();

    assert_eq!(data.id(), Some("D1")); //can also be compared with &str etc
    assert_eq!(key.id() , Some("pos"));
    assert_eq!(data.value(), &DataValue::String("noun".into()));
    assert_eq!(data.value(), "noun");  //shorter version

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

    let builder: SelectorBuilder = serde_json::from_str(data)?;

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

    let builder: AnnotationBuilder = serde_json::from_str(data)?;
    let mut store = setup_example_2().unwrap();
    let annotationhandle = store.annotate(builder).unwrap();
    let annotation: &Annotation = store.get(annotationhandle).unwrap();
    
    assert_eq!(annotation.id(), Some("A2"));
    let mut count = 0;
    for (datakey, annotationdata, dataset) in store.data(annotation) {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(datakey.id(), Some("pos"));
        assert_eq!(datakey.as_str(), "pos"); //shortcut for the same as above
        assert_eq!(datakey, "pos"); //shortcut for the same as above
        assert_eq!(annotationdata.value(), &DataValue::String("interjection".to_string())); 
        assert_eq!(annotationdata.value(), "interjection"); //shortcut for the same as above (and more efficient without heap allocated string)
        assert_eq!(annotationdata.id(), None); //No public ID was assigned
        assert_eq!(dataset.id(), Some("testdataset"));
    }
    assert_eq!(count,1);

    Ok(())
}

#[test]
fn parse_json_annotationset() -> Result<(), std::io::Error> {
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

    let builder: AnnotationDataSetBuilder = serde_json::from_str(json)?;
    let annotationset: AnnotationDataSet = builder.try_into().expect("conversion to dataset");


    let mut store = setup_example_2().unwrap();
    let sethandle = store.insert(annotationset).unwrap();

    let annotationset: &AnnotationDataSet = store.get(sethandle).unwrap();
    assert_eq!(annotationset.id(), Some("https://purl.org/dc"));

    let mut count = 0;
    let mut firstkeyhandle: Option<DataKeyHandle> = None;
    for key in annotationset.keys() {
        count += 1;
        if count == 1 {
            assert_eq!(key.id(), Some("http://purl.org/dc/terms/creator"));
            firstkeyhandle = key.handle();
        }
    }
    assert_eq!(count,3);

    count = 0;
    for data in annotationset.data() {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(data.id(), Some("D1"));
        assert_eq!(data.key(), firstkeyhandle.unwrap()); //shortcut for the same as above
        assert_eq!(data.value(), "proycon");
    }
    assert_eq!(count,1);

    
    Ok(())
}

const EXAMPLE_3: &'static str = r#"{ 
        "@type": "AnnotationStore",
        "annotationsets": {
            "@type": "AnnotationDataSet",
            "@id": "testdataset",
            "keys": [
                {
                  "@type": "DataKey",
                  "@id": "pos"
                }
            ],
            "data": [
                {
                    "@type": "AnnotationData",
                    "@id": "D1",
                    "key": "pos",
                    "value": {
                        "@type": "String",
                        "value": "noun"
                    }
                }
            ]
        },
        "resources": [{
            "@id": "testres",
            "text": "Hello world"
        }],
        "annotations": [{
            "@type": "Annotation",
            "@id": "A1",
            "target": {
                "@type": "TextSelector",
                "resource": "testres",
                "offset": {
                    "begin": {
                        "@type": "BeginAlignedCursor",
                        "value": 6
                    },
                    "end": {
                        "@type": "BeginAlignedCursor",
                        "value": 11
                    }
                }
            },
            "data": [{
                "@type": "AnnotationData",
                "@id": "D1",
                "set": "testdataset"
            }]
        }]
    }"#;

fn example_3_common_tests(store: &AnnotationStore) -> Result<(),StamError> {
    //repeat some common tests
    let _resource: &TextResource = store.get_by_id("testres")?;
    let annotationset: &AnnotationDataSet = store.get_by_id("testdataset")?;


    let _datakey: &DataKey = annotationset.get_by_id("pos")?;
    let _annotationdata: &AnnotationData = annotationset.get_by_id("D1")?;
    let _annotation: &Annotation = store.get_by_id("A1")?;

    for key in annotationset.keys() {
        //there is only one so we can test in loop body
        assert_eq!(key.id(), Some("pos"));
    }

    for data in annotationset.data() {
        //there is only one so we can test in loop body
        assert_eq!(data.id(), Some("D1"));
    }
    Ok(())
}

#[test]
fn parse_json_annotationstore() -> Result<(), StamError> {
    let builder: AnnotationStoreBuilder = serde_json::from_str(EXAMPLE_3).expect("Parsing json");

    let store = AnnotationStore::build_new(builder).expect("Building store");
    example_3_common_tests(&store)?;

    Ok(())
}

#[test]
fn parse_json_annotationstore_from_file() -> Result<(), StamError> {
    //write the test file
    let mut f = File::create("/tmp/test.stam.json").expect("opening test file for writing");
    write!(f, "{}", EXAMPLE_3).expect("writing test file");


    //read the test file
    let store = AnnotationStore::from_file("/tmp/test.stam.json")?;
    example_3_common_tests(&store)?;

    Ok(())
}

#[test]
fn wrapped() -> Result<(), StamError> {
    let store = setup_example_2()?;

    let annotation: &Annotation = store.get_by_id("A1")?;
    let wrappedannotation = store.wrap(annotation)?; //alternative we could have used wrap_in() directly on the previous line, but that would require more complex type annotations

    assert_eq!(wrappedannotation.id(), Some("A1"));
    let _store2 = wrappedannotation.store();

    Ok(())
}

#[test]
fn serialize_annotationset() -> Result<(),StamError> {
    let store = setup_example_2()?;
    let annotationset: &AnnotationDataSet = store.get_by_id("testdataset")?;
    serde_json::to_string(&annotationset).expect("serialization");
    Ok(())
}

#[test]
fn serialize_annotationstore() -> Result<(),StamError> {
    let store = setup_example_2()?;
    serde_json::to_string(&store).expect("serialization");
    Ok(())
}

#[test]
fn serialize_annotationstore_to_file() -> Result<(),StamError> {
    let store = setup_example_2()?;
    store.to_file("/tmp/testoutput.stam.json")
}
