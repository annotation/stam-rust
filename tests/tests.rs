use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::ops::Deref;

use stam::*;

const CARGO_MANIFEST_DIR: &'static str = env!("CARGO_MANIFEST_DIR");

#[test]
fn instantiation_naive() -> Result<(), StamError> {
    let mut store = AnnotationStore::new().with_id("test");

    let _res_intid = store.insert(TextResource::from_string(
        "testres",
        "Hello world",
        Config::default(),
    ));

    let mut annotationset = AnnotationDataSet::new(Config::default()).with_id("testdataset");
    annotationset.insert(DataKey::new("pos"))?;
    store.insert(annotationset)?;

    Ok(())
}

#[test]
fn sanity_check() -> Result<(), StamError> {
    // Instantiate the store
    let mut store = AnnotationStore::new().with_id("test");

    // Insert a text resource into the store
    let _res_handle = store.insert(TextResource::from_string(
        "testres",
        "Hello world",
        Config::default(),
    ));

    // Create a dataset with one key and insert it into the store
    let mut annotationset = AnnotationDataSet::new(Config::default()).with_id("testdataset");
    annotationset.insert(DataKey::new("pos"))?; //returns a DataKeyHandle, not further used in this test
    let set_handle = store.insert(annotationset)?;

    //get by handle (internal id)
    let annotationset: &AnnotationDataSet = store.get(set_handle)?;
    assert_eq!(annotationset.id(), Some("testdataset"));

    //get by directly by id
    let resource: &TextResource = store.get("testres")?;
    assert_eq!(resource.id(), Some("testres"));
    assert_eq!(resource.textlen(), 11);
    Ok(())
}

pub fn setup_example_1() -> Result<AnnotationStore, StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new()
        .with_config(Config::default().with_debug(true))
        .with_id("test")
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(
            AnnotationDataSet::new(Config::default())
                .with_id("testdataset")
                .add(DataKey::new("pos"))?
                .with_data_with_id("pos", "noun", "D1")?,
        )?
        .with_annotation(
            Annotation::builder()
                .with_id("A1")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(6, 11),
                ))
                .with_existing_data("testdataset", "D1"),
        )?;
    Ok(store)
}

pub fn setup_example_2() -> Result<AnnotationStore, StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new()
        .with_id("test")
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
        .with_annotation(
            Annotation::builder()
                .with_id("A1")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(6, 11),
                ))
                .with_data_with_id("testdataset", "pos", "noun", "D1"),
        )?;
    Ok(store)
}

pub fn setup_example_4() -> Result<AnnotationStore, StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new()
        .with_id("test")
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
        .with_annotation(
            Annotation::builder()
                .with_id("A1")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(6, 11),
                ))
                .with_data_with_id("testdataset", "pos", "noun", "D1"),
        )?
        .with_annotation(
            Annotation::builder()
                .with_id("A2")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(0, 5),
                ))
                .with_data_with_id("testdataset", "pos", "interjection", "D2"),
        )?;
    Ok(store)
}

pub fn setup_example_3() -> Result<AnnotationStore, StamError> {
    //this example includes a higher-order annotation with relative offset
    let store = AnnotationStore::new()
        .with_id("test")
        .add(TextResource::from_string(
            "testres",
            "I have no special talent. I am only passionately curious. -- Albert Einstein",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
        .with_annotation(
            Annotation::builder()
                .with_id("sentence2")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(26, 57),
                ))
                .with_data("testdataset", "type", "sentence"),
        )?
        .with_annotation(
            Annotation::builder()
                .with_id("sentence2word2")
                .with_target(SelectorBuilder::AnnotationSelector(
                    "sentence2".into(),
                    Some(Offset::simple(2, 4)),
                ))
                .with_data("testdataset", "type", "word"),
        )?;
    Ok(store)
}

#[test]
fn instantiation_with_builder_pattern() -> Result<(), StamError> {
    setup_example_1()?;
    Ok(())
}

#[test]
fn store_resolve_id() -> Result<(), StamError> {
    let store = setup_example_1()?;

    //this is a bit too contrived
    let handle: AnnotationHandle =
        <AnnotationStore as StoreFor<Annotation>>::resolve_id(&store, "A1")?;
    let _annotation: &Annotation = store.get(handle)?;

    //This is the shortcut if you want an intermediate handle
    let handle = store.resolve_annotation_id("A1")?;
    let _annotation: &Annotation = store.get(handle)?;

    //this is the direct method without intermediate handle (still used internally but no longer exposed)
    let _annotation: &Annotation = store.get("A1")?;
    Ok(())
}

#[test]
fn store_get_by_id() -> Result<(), StamError> {
    let store = setup_example_1()?;

    //test by public ID
    let _resource: &TextResource = store.get("testres")?;
    let annotationset: &AnnotationDataSet = store.get("testdataset")?;
    let _datakey: &DataKey = annotationset.get("pos")?;
    let _annotationdata: &AnnotationData = annotationset.get("D1")?;
    let _annotation: &Annotation = store.get("A1")?;
    Ok(())
}

#[test]
fn store_get_by_id_2() -> Result<(), StamError> {
    let store = setup_example_2()?;

    //test by public ID
    let _resource: &TextResource = store.get("testres")?;
    let annotationset: &AnnotationDataSet = store.get("testdataset")?;
    let _datakey: &DataKey = annotationset.get("pos")?;
    let _annotationdata: &AnnotationData = annotationset.get("D1")?;
    let _annotation: &Annotation = store.get("A1")?;
    Ok(())
}

#[test]
fn store_iter_data() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let annotation = store.annotation("A1").unwrap();

    let mut count = 0;
    for data in annotation.data() {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(data.key().id(), Some("pos"));
        assert_eq!(data.key().as_str(), "pos"); //shortcut for the same as above
        assert_eq!(data.value(), &DataValue::String("noun".to_string()));
        assert_eq!(data.value(), "noun"); //shortcut for the same as above (and more efficient without heap allocated string)
        assert_eq!(data.id(), Some("D1"));
        assert_eq!(data.set().id(), Some("testdataset"));
    }
    assert_eq!(count, 1);

    Ok(())
}

#[test]
fn resource_text() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let resource: &TextResource = store.get("testres")?;
    let text = resource.text();
    assert_eq!(text, "Hello world");
    Ok(())
}

#[test]
fn store_get_text_slice() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let resource: &TextResource = store.get("testres")?;
    let text = resource.text_by_offset(&Offset::new(
        Cursor::BeginAligned(0),
        Cursor::BeginAligned(5),
    ))?;
    assert_eq!(text, "Hello", "testing slice 0:5 (1)");
    let text = resource.text_by_offset(&Offset::simple(0, 5))?; //same as above, shorthand
    assert_eq!(text, "Hello", "testing slice 0:5 (2)");
    let text = resource.text_by_offset(&Offset::simple(6, 11))?;
    assert_eq!(text, "world", "testing slice 6:11");
    let text =
        resource.text_by_offset(&Offset::new(Cursor::EndAligned(-5), Cursor::EndAligned(0)))?;
    assert_eq!(text, "world", "testing slice -5:-0");
    let text =
        resource.text_by_offset(&Offset::new(Cursor::EndAligned(-11), Cursor::EndAligned(0)))?;
    assert_eq!(text, "Hello world", "testing slice -11:-0");
    let text = resource.text_by_offset(&Offset::new(
        Cursor::EndAligned(-11),
        Cursor::EndAligned(-6),
    ))?;
    assert_eq!(text, "Hello", "testing slice -11:-6");
    //these should produce an InvalidOffset error (begin >= end)
    assert!(
        resource.text_by_offset(&Offset::simple(11, 7)).is_err(),
        "testing invalid slice 11:7"
    );
    assert!(
        resource
            .text_by_offset(&Offset::new(
                Cursor::EndAligned(-9),
                Cursor::EndAligned(-11)
            ))
            .is_err(),
        "testing invalid end aligned slice -9:-11"
    );
    Ok(())
}

#[test]
fn store_get_text_selection() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let resource: &TextResource = store.get("testres")?;
    let textselection = resource.textselection(&Offset::new(
        Cursor::BeginAligned(0),
        Cursor::BeginAligned(5),
    ))?;
    assert_eq!(textselection.begin(), 0, "testing begin offset");
    assert_eq!(textselection.end(), 5, "testing end offset");
    assert_eq!(
        resource.utf8byte(textselection.begin())?,
        0,
        "testing utf-8 begin byte"
    ); //bytes and unicode point happen to correspond in this simple, example, but this is not necessarily the case
    assert_eq!(
        resource.utf8byte(textselection.end())?,
        5,
        "testing utf-8 end byte"
    );
    assert_eq!(textselection.text(), "Hello");
    Ok(())
}

#[test]
fn text_selector() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let annotation = store.annotation("A1").unwrap();
    let _resource = annotation.resources().next().unwrap();
    let text: &str = annotation.text().next().unwrap();
    assert_eq!(text, "world");
    Ok(())
}

#[test]
fn annotate() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::TextSelector(
                "testres".into(),
                Offset::simple(0, 5),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::TextSelector(
                "testres".into(),
                Offset::simple(6, 11),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    Ok(())
}

#[test]
fn annotate_existing_data() -> Result<(), StamError> {
    let mut store = setup_example_2()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::TextSelector(
                "testres".into(),
                Offset::simple(0, 5),
            ))
            .with_data("testdataset", "pos", "noun"), //this one already exists so should not be recreated but found and referenced intead
    )?;

    //check if the dataset still contains only one key
    let dataset: &AnnotationDataSet = store.get("testdataset")?;
    assert_eq!(dataset.keys().count(), 1);
    assert_eq!(dataset.data().count(), 1);

    Ok(())
}

#[test]
fn add_after_borrow() -> Result<(), StamError> {
    let mut store = setup_example_2()?;
    let annotation = store.annotation("A1").unwrap();
    let mut count = 0;
    for _data in annotation.data() {
        count += 1;
    }
    assert_eq!(count, 1);
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::TextSelector(
                "testres".into(),
                Offset::simple(6, 11),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    Ok(())
}

#[test]
fn add_during_borrowproblem() -> Result<(), StamError> {
    let mut store = setup_example_2()?;
    let annotation: &Annotation = store.get("A1")?;
    //                                 V---- here we clone the annotation to prevent a borrow problem (cannot borrow `store` as mutable because it is also borrowed as immutable (annotation)), this is relatively low-cost
    for (dataset, data) in annotation.clone().data() {
        store.annotate(
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(6, 11),
                ))
                .with_existing_data(dataset, data),
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

    assert_eq!(data.id(), &BuildItem::from("D2"));
    assert_eq!(data.id(), "D2"); //can also be compared with &str etc
    assert_eq!(data.key(), &BuildItem::from("pos"));
    assert_eq!(data.key(), "pos");
    assert_eq!(data.value(), &DataValue::String("verb".into()));
    assert_eq!(data.value(), "verb"); //shorter version

    let mut store = setup_example_2().unwrap();
    let dataset: &mut AnnotationDataSet = store.get_mut("testdataset").unwrap();
    let datahandle = dataset.build_insert_data(data, true).unwrap();

    let data: &AnnotationData = dataset.get(datahandle).unwrap();
    let key: &DataKey = dataset.get(data.key()).unwrap();

    assert_eq!(data.id(), Some("D2")); //can also be compared with &str etc
    assert_eq!(key.id(), Some("pos"));
    assert_eq!(data.value(), &DataValue::String("verb".into()));
    assert_eq!(data.value(), "verb"); //shorter version

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

    assert_eq!(data.id(), &BuildItem::from("D1"));
    assert_eq!(data.id(), "D1"); //can also be compared with &str etc
    assert_eq!(data.annotationset(), &BuildItem::from("testdataset"));
    assert_eq!(data.annotationset(), "testdataset");

    let mut store = setup_example_2().unwrap();
    let dataset: &mut AnnotationDataSet = store.get_mut("testdataset").unwrap();
    //we already had this annotation, check prior to insert
    let datahandle1: AnnotationDataHandle = dataset.annotationdata("D1").unwrap().handle();
    //insert (which doesn't really insert in this case) but returns the same existing handle
    let datahandle2 = dataset.build_insert_data(data, true).unwrap();
    assert_eq!(datahandle1, datahandle2);

    let data: &AnnotationData = dataset.get(datahandle2).unwrap();
    let key: &DataKey = dataset.get(data.key()).unwrap();

    assert_eq!(data.id(), Some("D1")); //can also be compared with &str etc
    assert_eq!(key.id(), Some("pos"));
    assert_eq!(data.value(), &DataValue::String("noun".into()));
    assert_eq!(data.value(), "noun"); //shorter version

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
    assert_eq!(
        selector,
        Selector::TextSelector(
            store.resolve_resource_id("testres").unwrap(),
            Offset::simple(0, 5)
        )
    );
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
    let annotation = store.annotation(annotationhandle).unwrap();

    assert_eq!(annotation.id(), Some("A2"));
    let mut count = 0;
    for data in annotation.data() {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(data.key().id(), Some("pos"));
        assert_eq!(data.key().as_str(), "pos"); //shortcut for the same as above
        assert_eq!(data.value(), &DataValue::String("interjection".to_string()));
        assert_eq!(data.value(), "interjection"); //shortcut for the same as above (and more efficient without heap allocated string)
        assert_eq!(data.set().id(), Some("testdataset"));
    }
    assert_eq!(count, 1);

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
            firstkeyhandle = Some(key.handle());
        }
    }
    assert_eq!(count, 3);

    count = 0;
    for data in annotationset.data() {
        //there should be only one so we can safely test in the loop body
        count += 1;
        assert_eq!(data.id(), Some("D1"));
        assert_eq!(data.as_ref().key(), firstkeyhandle.unwrap());
        assert_eq!(data.value(), "proycon");
    }
    assert_eq!(count, 1);

    Ok(())
}

const EXAMPLE_3: &'static str = r#"{ 
        "@type": "AnnotationStore",
        "annotationsets": [{
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
        }],
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

fn example_3_common_tests(store: &AnnotationStore) -> Result<(), StamError> {
    //repeat some common tests
    let _resource: &TextResource = store.get("testres")?;
    let annotationset: &AnnotationDataSet = store.get("testdataset")?;

    let _datakey: &DataKey = annotationset.get("pos")?;
    let _annotationdata: &AnnotationData = annotationset.get("D1")?;
    let _annotation: &Annotation = store.get("A1")?;

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

    let store: AnnotationStore = builder.build().expect("Building store");

    example_3_common_tests(&store)?;

    Ok(())
}

#[test]
fn parse_json_annotationstore_from_file() -> Result<(), StamError> {
    //write the test file
    let mut f = File::create("/tmp/test.stam.json").expect("opening test file for writing");
    write!(f, "{}", EXAMPLE_3).expect("writing test file");

    //read the test file
    let store = AnnotationStore::from_file("/tmp/test.stam.json", Config::default())?;
    example_3_common_tests(&store)?;

    Ok(())
}

#[test]
fn wrapped() -> Result<(), StamError> {
    let store = setup_example_2()?;

    let annotation: &Annotation = store.get("A1")?;
    let wrappedannotation = store.wrap(annotation)?; //alternative we could have used wrap_in() directly on the previous line, but that would require more complex type annotations

    assert_eq!(wrappedannotation.id(), Some("A1"));
    let _store2 = wrappedannotation.store();

    Ok(())
}

#[test]
fn serialize_annotationset() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotationset: &AnnotationDataSet = store.get("testdataset")?;
    serde_json::to_string(&annotationset).expect("serialization");
    Ok(())
}

#[test]
fn serialize_annotationstore() -> Result<(), StamError> {
    let store = setup_example_2()?;
    serde_json::to_string(&store).expect("serialization");
    Ok(())
}

#[test]
fn serialize_annotationstore_to_file() -> Result<(), StamError> {
    let store = setup_example_2()?;
    store.to_json_file("/tmp/testoutput.stam.json", &Config::default())
}

#[test]
fn loop_annotations() -> Result<(), StamError> {
    let store = setup_example_2()?;
    for annotation in store.annotations() {
        let id = annotation.id().unwrap_or("");
        for data in annotation.data() {
            // get the text to which this annotation refers (if any)
            let text: &str = annotation.text().next().unwrap();
            print!(
                "{}\t{}\t{}\t{}",
                id,
                data.key().id().unwrap(),
                data.value(),
                text
            );
        }
    }
    Ok(())
}

#[test]
fn textselection() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let sentence = store.annotation("sentence2").unwrap();
    for textselection in sentence.textselections() {
        assert_eq!(textselection.text(), "I am only passionately curious.")
    }
    Ok(())
}

#[test]
fn selectoriter() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let word: &Annotation = store.get("sentence2word2")?;
    for (i, selectoritem) in word.target().iter(&store, true, true).enumerate() {
        match i {
            0 => assert!(selectoritem.ancestors().is_empty()),
            1 => {
                assert_eq!(selectoritem.ancestors().len(), 1);
                assert_eq!(
                    selectoritem.ancestors().get(0).unwrap().kind(),
                    SelectorKind::AnnotationSelector
                );
            }
            _ => panic!("expected only two iterations!"),
        }
    }
    Ok(())
}

#[test]
fn textselection_relative() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let word = store.annotation("sentence2word2").unwrap();
    for textselection in word.textselections() {
        assert_eq!(textselection.text(), "am")
    }
    Ok(())
}

#[test]
fn textselection_relative_endaligned() -> Result<(), StamError> {
    let store = setup_example_3()?.with_annotation(
        Annotation::builder()
            .with_id("sentence2lastword")
            .with_target(SelectorBuilder::AnnotationSelector(
                "sentence2".into(),
                Some(Offset::new(Cursor::EndAligned(-8), Cursor::EndAligned(-1))),
            ))
            .with_data("testdataset", "type", "word"),
    )?;
    let word = store.annotation("sentence2lastword").unwrap();
    for textselection in word.textselections() {
        assert_eq!(textselection.text(), "curious")
    }
    Ok(())
}

#[test]
fn existing_textselection() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let resource = store
        .resource("testres")
        .expect("test: resource must exist");
    let textselection = resource.textselection(&Offset::simple(6, 11))?;

    assert_eq!(textselection.begin(), 6);
    assert_eq!(textselection.end(), 11);
    assert!(
        textselection.handle().is_some(),
        "testing whether TextSelection has a handle"
    );

    Ok(())
}

#[test]
fn annotations_by_offset() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let res_handle = store.resolve_resource_id("testres")?;
    let v = store
        .annotations_by_offset(res_handle, &Offset::simple(6, 11))
        .expect("test: offset should exist");
    let a_ref = store.resolve_annotation_id("A1")?;
    assert_eq!(v, &vec!(a_ref));
    Ok(())
}

#[test]
fn textselections_by_annotation() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotation = store.annotation("A1").unwrap();
    let _reference_res_handle = store.resolve_resource_id("testres")?;
    let mut count = 0;
    for textselection in annotation.textselections() {
        count += 1;
        assert_eq!(textselection.begin(), 6);
        assert_eq!(textselection.end(), 11);
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn textselections_by_resource_unsorted() -> Result<(), StamError> {
    let store = setup_example_4()?;
    let resource: &TextResource = store.get("testres")?;
    let v: Vec<_> = resource.textselections().collect();
    assert_eq!(v[0].begin(), 6);
    assert_eq!(v[0].end(), 11);
    assert_eq!(v[1].begin(), 0);
    assert_eq!(v[1].end(), 5);
    assert_eq!(v.len(), 2);
    Ok(())
}

#[test]
fn textselections_by_resource_sorted() -> Result<(), StamError> {
    let store = setup_example_4()?;
    let resource: &TextResource = store.get("testres")?;
    let v: Vec<_> = resource.iter().collect();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].begin(), 0);
    assert_eq!(v[0].end(), 5);
    Ok(())
}

#[test]
fn positionindex() -> Result<(), StamError> {
    let store = setup_example_4()?;
    let resource: &TextResource = store.get("testres")?;
    let v: Vec<&usize> = resource.positions(PositionMode::Both).collect();
    assert_eq!(v.len(), 4);
    assert_eq!(v[0], &0);
    assert_eq!(v[1], &5);
    assert_eq!(v[2], &6);
    assert_eq!(v[3], &11);
    let v2: Vec<_> = resource
        .position(0)
        .unwrap()
        .iter_begin2end()
        .collect::<Vec<_>>();
    assert_eq!(v2.len(), 1);
    assert_eq!(v2[0].0, 5);
    let v2: Vec<_> = resource
        .position(6)
        .unwrap()
        .iter_begin2end()
        .collect::<Vec<_>>();
    assert_eq!(v2.len(), 1);
    assert_eq!(v2[0].0, 11);
    Ok(())
}

#[test]
fn positionindex_mode_begins() -> Result<(), StamError> {
    let store = setup_example_4()?;
    let resource: &TextResource = store.get("testres")?;
    let v: Vec<&usize> = resource.positions(PositionMode::Begin).collect();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0], &0);
    assert_eq!(v[1], &6);
    let v2: Vec<_> = resource
        .position(0)
        .unwrap()
        .iter_begin2end()
        .collect::<Vec<_>>();
    assert_eq!(v2.len(), 1);
    assert_eq!(v2[0].0, 5);
    let v2: Vec<_> = resource
        .position(6)
        .unwrap()
        .iter_begin2end()
        .collect::<Vec<_>>();
    assert_eq!(v2.len(), 1);
    assert_eq!(v2[0].0, 11);
    Ok(())
}

#[test]
fn textselections_by_resource_range() -> Result<(), StamError> {
    let store = setup_example_4()?;
    let resource: &TextResource = store.get("testres")?;
    let v: Vec<_> = resource.range(6, 11).collect();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].begin(), 6);
    assert_eq!(v[0].end(), 11);
    Ok(())
}

#[test]
fn text_by_annotation() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotation = store.annotation("A1").unwrap();
    let mut count = 0;
    for text in annotation.text() {
        count += 1;
        assert_eq!(text, "world");
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn data_by_value_low() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotationset: &AnnotationDataSet = store.get("testdataset")?;
    let annotationdata: Option<&AnnotationData> =
        annotationset.data_by_value("pos", &"noun".into());
    assert!(annotationdata.is_some());
    assert_eq!(annotationdata.unwrap().id(), Some("D1"));
    Ok(())
}

#[test]
fn data_by_value_high() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotationset = store.annotationset("testdataset").unwrap();
    let annotationdata = annotationset.data_by_value("pos", &"noun".into());
    assert!(annotationdata.is_some());
    assert_eq!(annotationdata.unwrap().id(), Some("D1"));
    Ok(())
}

#[test]
fn find_data_exact() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotationset = store.annotationset("testdataset").unwrap();
    let mut count = 0;
    for annotationdata in annotationset
        .find_data(Some("pos"), DataOperator::Equals("noun"))
        .into_iter()
        .flatten()
    {
        count += 1;
        assert_eq!(annotationdata.id(), Some("D1"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn find_data_by_key() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let annotationset = store.annotationset("testdataset").unwrap();
    let mut count = 0;
    for _ in annotationset
        .find_data(Some("type"), DataOperator::Any)
        .into_iter()
        .flatten()
    {
        count += 1;
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn find_data_all() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let annotationset = store.annotationset("testdataset").unwrap();
    let mut count = 0;
    let key: Option<DataKeyHandle> = None;
    for _ in annotationset
        .find_data(key, DataOperator::Any)
        .into_iter()
        .flatten()
    {
        count += 1;
    }
    assert_eq!(count, 2);
    Ok(())
}

pub fn setup_example_multiselector() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::new()
        .with_id("test")
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
        .with_annotation(
            Annotation::builder()
                .with_id("WordAnnotation")
                .with_target(SelectorBuilder::MultiSelector(vec![
                    SelectorBuilder::TextSelector("testres".into(), Offset::simple(0, 5)),
                    SelectorBuilder::TextSelector("testres".into(), Offset::simple(6, 11)),
                ]))
                .with_data_with_id("testdataset", "type", "word", "WordAnnotationData"),
        )?;
    Ok(store)
}

#[test]
fn test_multiselector_creation() -> Result<(), StamError> {
    let _store = setup_example_multiselector()?;
    Ok(())
}

#[test]
fn test_multiselector_iter() -> Result<(), StamError> {
    let store = setup_example_multiselector()?;
    let annotation = store.annotation("WordAnnotation").unwrap();
    let result: Vec<&str> = annotation.text().collect();
    assert_eq!(result[0], "Hello");
    assert_eq!(result[1], "world");
    Ok(())
}

pub fn setup_example_multiselector2() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::new()
        .with_id("test")
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
        .with_annotation(
            Annotation::builder()
                .with_id("A1")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(6, 11),
                ))
                .with_data_with_id("testdataset", "pos", "noun", "D1"),
        )?
        .with_annotation(
            Annotation::builder()
                .with_id("A2")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(0, 5),
                ))
                .with_data_with_id("testdataset", "pos", "interjection", "D2"),
        )?
        .with_annotation(
            Annotation::builder()
                .with_id("WordAnnotation")
                .with_target(SelectorBuilder::MultiSelector(vec![
                    SelectorBuilder::TextSelector("testres".into(), Offset::simple(0, 5)),
                    SelectorBuilder::TextSelector("testres".into(), Offset::simple(6, 11)),
                ]))
                .with_data_with_id("testdataset", "type", "word", "WordAnnotationData"),
        )?
        .with_annotation(
            Annotation::builder()
                .with_id("AllPosAnnotation")
                .with_target(SelectorBuilder::MultiSelector(vec![
                    SelectorBuilder::AnnotationSelector("A1".into(), Some(Offset::whole())),
                    SelectorBuilder::AnnotationSelector("A2".into(), Some(Offset::whole())),
                ]))
                .with_data_with_id("testdataset", "hastype", "pos", "AllPosAnnotationData"),
        )?;
    Ok(store)
}

#[test]
fn test_multiselector2_creation_and_sanity() -> Result<(), StamError> {
    let mut store = setup_example_multiselector2()?;

    //annotate existing data (just to test sanity)
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::TextSelector(
                "testres".into(),
                Offset::simple(0, 5),
            ))
            .with_data(
                "testdataset",
                "pos",
                DataValue::String("greeting".to_string()),
            ),
    )?;

    //sanity check
    let resource: &TextResource = store.get("testres")?;
    let v: Vec<&usize> = resource.positions(PositionMode::Begin).collect();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0], &0);
    assert_eq!(v[1], &6);
    let v2: Vec<_> = resource
        .position(0)
        .unwrap()
        .iter_begin2end()
        .collect::<Vec<_>>();
    assert_eq!(v2.len(), 1);
    assert_eq!(v2[0].0, 5);
    let v2: Vec<_> = resource
        .position(6)
        .unwrap()
        .iter_begin2end()
        .collect::<Vec<_>>();
    assert_eq!(v2.len(), 1);
    assert_eq!(v2[0].0, 11);

    let v3: Vec<_> = resource.textselections().collect();
    assert_eq!(v3.len(), 2);

    Ok(())
}

#[test]
fn test_multiselector2_iter() -> Result<(), StamError> {
    let store = setup_example_multiselector2()?;
    let annotation = store.annotation("WordAnnotation").unwrap();
    let result: Vec<&str> = annotation.text().collect();
    assert_eq!(result[0], "Hello");
    assert_eq!(result[1], "world");
    Ok(())
}

#[test]
fn test_read_single() -> Result<(), StamError> {
    AnnotationStore::from_file(
        &format!("{}/tests/singletest.store.stam.json", CARGO_MANIFEST_DIR),
        Config::default(),
    )?;
    Ok(())
}

#[test]
fn test_read_include() -> Result<(), StamError> {
    let store = AnnotationStore::from_file(
        "tests/test.store.stam.json",
        Config::default().with_debug(true),
    )?;
    test_example_a_sanity(&store)?;
    Ok(())
}

#[test]
fn test_find_text() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string("Hello world");
    let mut count = 0;
    for result in resource.find_text("world") {
        count += 1;
        assert_eq!(result.begin(), 6);
        assert_eq!(result.end(), 11);
        assert_eq!(result.text(), "world");
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_find_text_nocase() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string("Hello world");
    let mut count = 0;
    for result in resource.find_text_nocase("hello") {
        count += 1;
        assert_eq!(result.begin(), 0);
        assert_eq!(result.end(), 5);
        assert_eq!(result.text(), "Hello");
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_find_text_sequence() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string("Hello world");
    let results = resource
        .find_text_sequence(&["Hello", "world"], |c| !c.is_alphabetic(), true)
        .expect("results must be found");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].begin(), 0);
    assert_eq!(results[0].end(), 5);
    assert_eq!(results[0].text(), "Hello");
    assert_eq!(results[1].begin(), 6);
    assert_eq!(results[1].end(), 11);
    assert_eq!(results[1].text(), "world");
    Ok(())
}

#[test]
fn test_find_text_sequence2() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string("Hello, world!");
    let results = resource
        .find_text_sequence(&["Hello", "world"], |c| !c.is_alphabetic(), true)
        .expect("results must be found");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].begin(), 0);
    assert_eq!(results[0].end(), 5);
    assert_eq!(results[0].text(), "Hello");
    assert_eq!(results[1].begin(), 7);
    assert_eq!(results[1].end(), 12);
    assert_eq!(results[1].text(), "world");
    Ok(())
}

#[test]
fn test_find_text_sequence_nocase() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string("Hello world");
    let results = resource
        .find_text_sequence(&["hello", "world"], |c| !c.is_alphabetic(), false)
        .expect("results must be found");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].begin(), 0);
    assert_eq!(results[0].end(), 5);
    assert_eq!(results[0].text(), "Hello");
    assert_eq!(results[1].begin(), 6);
    assert_eq!(results[1].end(), 11);
    assert_eq!(results[1].text(), "world");
    Ok(())
}

#[test]
fn test_find_text_sequence_nomatch() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string("Hello world");
    let results =
        resource.find_text_sequence(&["hello", "world", "hi"], |c| !c.is_alphabetic(), false);
    assert!(results.is_none());
    Ok(())
}

#[test]
fn test_split_text() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string("Hello world");
    let mut count = 0;
    for result in resource.split_text(" ") {
        count += 1;
        if count == 1 {
            assert_eq!(result.begin(), 0);
            assert_eq!(result.end(), 5);
            assert_eq!(result.text(), "Hello");
        } else {
            assert_eq!(result.begin(), 6);
            assert_eq!(result.end(), 11);
            assert_eq!(result.text(), "world");
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn test_search_text_regex_single() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    );
    let mut count = 0;
    for result in resource.find_text_regex(&[Regex::new(r"eavesdropping").unwrap()], None, true)? {
        count += 1;
        assert_eq!(result.textselections().len(), 1);
        assert_eq!(result.textselections()[0].begin(), 25);
        assert_eq!(result.textselections()[0].end(), 38);
        assert_eq!(result.as_str(), Some("eavesdropping"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_search_text_regex_single2() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    );
    let mut count = 0;
    for result in resource.find_text_regex(&[Regex::new(r"\b\w{13}\b").unwrap()], None, true)? {
        count += 1;
        if count == 1 {
            assert_eq!(result.textselections().len(), 1);
            assert_eq!(result.textselections()[0].begin(), 2);
            assert_eq!(result.textselections()[0].end(), 15);
            assert_eq!(result.as_str(), Some("categorically"));
        } else {
            assert_eq!(result.textselections().len(), 1);
            assert_eq!(result.textselections()[0].begin(), 25);
            assert_eq!(result.textselections()[0].end(), 38);
            assert_eq!(result.as_str(), Some("eavesdropping"));
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn test_search_text_regex_single_multiexpr() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    );
    let mut count = 0;
    for result in resource.find_text_regex(
        &[
            Regex::new(r"eavesdropping").unwrap(),
            Regex::new(r"\.").unwrap(),
        ],
        None,
        true,
    )? {
        count += 1;
        if count == 1 {
            assert_eq!(result.textselections().len(), 1);
            assert_eq!(result.textselections()[0].begin(), 25);
            assert_eq!(result.textselections()[0].end(), 38);
            assert_eq!(result.as_str(), Some("eavesdropping"));
        } else {
            assert_eq!(result.textselections().len(), 1);
            assert_eq!(result.textselections()[0].begin(), 86);
            assert_eq!(result.textselections()[0].end(), 87);
            assert_eq!(result.as_str(), Some("."));
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn test_search_text_regex_single_multiexpr2() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    );
    let mut count = 0;
    for result in resource.find_text_regex(
        &[
            Regex::new(r"triskaidekaphobia").unwrap(),
            Regex::new(r"\.").unwrap(),
        ],
        None,
        true,
    )? {
        count += 1;
        if count == 1 {
            assert_eq!(result.textselections().len(), 1);
            assert_eq!(result.textselections()[0].begin(), 69);
            assert_eq!(result.textselections()[0].end(), 86);
            assert_eq!(result.as_str(), Some("triskaidekaphobia"));
        } else {
            assert_eq!(result.textselections().len(), 1);
            assert_eq!(result.textselections()[0].begin(), 86);
            assert_eq!(result.textselections()[0].end(), 87);
            assert_eq!(result.as_str(), Some("."));
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn test_search_text_regex_single_capture() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    );
    let mut count = 0;
    for result in resource.find_text_regex(
        &[Regex::new(r"deny\s(\w+)\seavesdropping").unwrap()],
        None,
        true,
    )? {
        count += 1;
        assert_eq!(result.textselections().len(), 1);
        assert_eq!(result.textselections()[0].begin(), 21);
        assert_eq!(result.textselections()[0].end(), 24);
        assert_eq!(result.as_str(), Some("any"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_search_text_regex_double_capture() -> Result<(), StamError> {
    let resource = TextResource::new("testres", Config::default()).with_string(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    );
    let mut count = 0;
    for result in resource.find_text_regex(
        &[Regex::new(r"deny\s(\w+)\seavesdropping\s(on\s\w+)\b").unwrap()],
        None,
        true,
    )? {
        count += 1;
        assert_eq!(result.textselections().len(), 2);
        assert_eq!(result.textselections()[0].begin(), 21);
        assert_eq!(result.textselections()[0].end(), 24);
        assert_eq!(result.textselections()[1].begin(), 39);
        assert_eq!(result.textselections()[1].end(), 45);
        assert_eq!(result.text(), vec!("any", "on you"));
    }
    assert_eq!(count, 1);
    Ok(())
}

fn test_example_a_sanity(store: &AnnotationStore) -> Result<(), StamError> {
    // Instantiate the store
    let resource: &TextResource = store.get("hello.txt")?;
    assert_eq!(resource.text(), "Hallå världen\n");

    let annotation = store.annotation("A1").unwrap();
    assert_eq!(annotation.text().next().unwrap(), "Hallå");
    for data in annotation.data() {
        assert_eq!(data.key().id(), Some("pos"));
        assert_eq!(data.id(), Some("PosInterjection"));
        assert_eq!(data.set().id(), Some("https://example.org/test/"));
        assert_eq!(data.value(), "interjection");
    }

    let annotation = store.annotation("A2").unwrap();
    assert_eq!(annotation.text().next().unwrap(), "världen");
    for data in annotation.data() {
        assert_eq!(data.key().id(), Some("pos"));
        assert_eq!(data.id(), Some("PosNoun"));
        assert_eq!(data.set().id(), Some("https://example.org/test/"));
        assert_eq!(data.value(), "noun");
    }

    Ok(())
}

#[test]
fn serialize_csv() -> Result<(), StamError> {
    let mut store = AnnotationStore::from_file(
        "tests/test.store.stam.json",
        Config::default().with_debug(true),
    )?;
    store.set_filename("tests/test.store.stam.csv");
    store.save()?;
    Ok(())
}

#[test]
fn parse_csv() -> Result<(), StamError> {
    let store = AnnotationStore::from_file(
        "tests/test.store.stam.csv",
        Config::default().with_debug(true),
    )?;
    test_example_a_sanity(&store)?;
    Ok(())
}

const EXAMPLE5_TEXT: &str = "
Article 1

All human beings are born free and equal in dignity and rights. They are endowed with reason and conscience and should act towards one another in a spirit of brotherhood.

Article 2

Everyone is entitled to all the rights and freedoms set forth in this Declaration, without distinction of any kind, such as race, colour, sex, language, religion, political or other opinion, national or social origin, property, birth or other status. Furthermore, no distinction shall be made on the basis of the political, jurisdictional or international status of the country or territory to which a person belongs, whether it be independent, trust, non-self-governing or under any other limitation of sovereignty.

Article 3

Everyone has the right to life, liberty and security of person.

Article 4

No one shall be held in slavery or servitude; slavery and the slave trade shall be prohibited in all their forms.
";

pub fn setup_example_5() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::new()
        .with_id("example5")
        .add(TextResource::from_string(
            "humanrights",
            EXAMPLE5_TEXT,
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?;
    Ok(store)
}

pub fn setup_example_6() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::new()
        .with_id("example6")
        .add(TextResource::from_string(
            "humanrights",
            "All human beings are born free and equal in dignity and rights.",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
        .with_annotation(AnnotationBuilder::new().with_id("Sentence1").with_target(
            SelectorBuilder::TextSelector("humanrights".into(), Offset::whole()),
        ))?
        .with_annotation(AnnotationBuilder::new().with_id("Phrase1").with_target(
            SelectorBuilder::TextSelector(
                "humanrights".into(),
                Offset::simple(17, 40), //"are born free and equal"
            ),
        ))?;

    Ok(store)
}

pub fn setup_example_6b(store: &mut AnnotationStore) -> Result<AnnotationHandle, StamError> {
    store.annotate(AnnotationBuilder::new().with_id("Phrase2").with_target(
        SelectorBuilder::TextSelector("humanrights".into(), Offset::simple(4, 25)), //"human beings are born",
    ))?;
    store.annotate(AnnotationBuilder::new().with_id("Phrase3").with_target(
        SelectorBuilder::TextSelector("humanrights".into(), Offset::simple(44, 62)), //"dignity and rights",
    ))
}

pub fn annotate_regex(store: &mut AnnotationStore) -> Result<(), StamError> {
    let resource = store.resource("humanrights").unwrap();
    store.annotate_builders(
        resource
            .find_text_regex(&[Regex::new(r"Article \d").unwrap()], None, true)?
            .into_iter()
            .map(|foundmatch| {
                let offset: Offset = foundmatch.textselections().first().unwrap().deref().into();
                AnnotationBuilder::new()
                    .with_target(SelectorBuilder::TextSelector(
                        resource.handle().into(),
                        offset,
                    ))
                    .with_data("myset", "type", "header")
            })
            .collect(),
    )?;
    Ok(())
}

#[test]
fn test_annotate_regex_single2() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex(&mut store)?;

    let set = store.annotationset("myset").unwrap();
    let data = set.data_by_value("type", &"header".into()).unwrap();
    let vec = store.annotations_by_data(set.handle(), data.handle());
    assert_eq!(vec.unwrap().len(), 4);
    Ok(())
}

#[test]
fn test_lifetime_sanity_resources() -> Result<(), StamError> {
    let store = setup_example_5()?;
    //Gather references to resources in a vec, to ensure their lifetimes remain valid after the iterator
    let result: Vec<&TextResource> = store
        .resources()
        .map(|resource| resource.as_ref())
        .collect();
    assert_eq!(result.len(), 1);
    Ok(())
}

#[test]
fn test_lifetime_sanity_textselections() -> Result<(), StamError> {
    let store = setup_example_5()?;
    //Gather references to resources and their textselections in a vec, to ensure their lifetimes remain valid after the iterator
    let result: Vec<(&TextResource, Vec<&TextSelection>)> = store
        .resources()
        .map(|resource| {
            (
                resource.as_ref(),
                resource
                    .as_ref()
                    .textselections()
                    .map(|textselection| textselection.as_ref())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(result.len(), 1);
    Ok(())
}

#[test]
fn test_lifetime_sanity_annotationdata() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex(&mut store)?;
    //Gather references to annotations and their data in a vec, to ensure their lifetimes remain valid after the iterator
    let result: Vec<(&Annotation, Vec<&AnnotationData>)> = store
        .annotations()
        .map(|annotation| {
            (
                annotation.as_ref(),
                annotation.data().map(|data| data.as_ref()).collect(),
            )
        })
        .collect();
    assert!(!result.is_empty());
    Ok(())
}

#[test]
fn test_lifetime_sanity_keys() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex(&mut store)?;
    let result: Vec<(&AnnotationDataSet, Vec<&DataKey>)> = store
        .annotationsets()
        .map(|annotationset| {
            (
                annotationset.as_ref(),
                annotationset
                    .as_ref()
                    .keys()
                    .map(|key| key.as_ref())
                    .collect(),
            )
        })
        .collect();
    assert!(!result.is_empty());
    Ok(())
}

#[test]
fn test_lifetime_sanity_annotationdata2() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex(&mut store)?;
    //Gather references to annotations and their data in a vec, to ensure their lifetimes remain valid after the iterator
    let mut result: Vec<(&Annotation, Vec<&AnnotationData>)> = Vec::new();
    for annotation in store.annotations() {
        let mut subresult: Vec<&AnnotationData> = Vec::new();
        for data in annotation.data() {
            subresult.push(data.as_ref())
        }
        result.push((annotation.as_ref(), subresult));
    }
    assert!(!result.is_empty());
    Ok(())
}

#[test]
fn test_annotations_by_textselection() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex(&mut store)?;
    let resource = store.resource("humanrights").unwrap();
    let mut count = 0;
    let mut count_annotations = 0;
    for textselection in resource.textselections() {
        count += 1;
        if let Some(iter) = textselection.annotations(&store) {
            count_annotations += 1;
            for annotation in iter {
                eprintln!("{} {:?}", textselection.text(), annotation.id());
            }
        }
    }
    assert_eq!(count, resource.textselections_len());
    assert_ne!(count, 0);
    assert_eq!(count_annotations, store.annotations_len());
    assert_ne!(count_annotations, 0);
    Ok(())
}

#[test]
fn test_example6_sanity() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let resource = store.resource("humanrights").unwrap();
    assert_eq!(
        resource.text(),
        "All human beings are born free and equal in dignity and rights."
    );
    let sentence = store.annotation("Sentence1").unwrap();
    assert_eq!(
        sentence.text().next().unwrap(),
        "All human beings are born free and equal in dignity and rights."
    );

    Ok(())
}

#[test]
fn test_annotations_by_textselection_none() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let resource = store.resource("humanrights").unwrap();
    let textselection = resource.textselection(&Offset::simple(1, 14))?; //no annotations for this random selection
    let v: Vec<_> = textselection
        .annotations(&store)
        .into_iter()
        .flatten()
        .collect();
    assert!(v.is_empty());
    Ok(())
}

#[test]
fn test_find_textselections_embedded() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let phrase1 = store.annotation("Phrase1").unwrap();
    let mut count = 0;
    for reftextsel in phrase1.textselections() {
        for textsel in reftextsel.find_textselections(TextSelectionOperator::Embedded {
            all: false,
            negate: false,
        }) {
            count += 1;
            assert_eq!(textsel.begin(), 0);
            assert_eq!(textsel.end(), 63);
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_textselectioniter_range_exact() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let resource = store.resource("humanrights").unwrap();
    let mut count = 0;
    for textsel in resource.range(17, 40) {
        count += 1;
        assert_eq!(textsel.begin(), 17);
        assert_eq!(textsel.end(), 40);
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_textselectioniter_range_bigger() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let resource = store.resource("humanrights").unwrap();
    let mut count = 0;
    for textsel in resource.range(1, 50) {
        count += 1;
        assert_eq!(textsel.begin(), 17);
        assert_eq!(textsel.end(), 40);
    }
    assert_eq!(count, 1);
    count = 0;
    for _textsel in resource.range(0, 64) {
        count += 1;
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn test_find_textselections_embeds() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence1 = store.annotation("Sentence1").unwrap();
    let mut count = 0;
    for reftextsel in sentence1.textselections() {
        for textsel in reftextsel.find_textselections(TextSelectionOperator::Embeds {
            all: false,
            negate: false,
        }) {
            count += 1;
            assert_eq!(textsel.begin(), 17);
            assert_eq!(textsel.end(), 40);
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_find_annotations_embeds() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence1 = store.annotation("Sentence1").unwrap();
    let mut count = 0;
    for reftextsel in sentence1.textselections() {
        for annotation in reftextsel.find_annotations(
            TextSelectionOperator::Embeds {
                all: false,
                negate: false,
            },
            &store,
        ) {
            count += 1;
            assert_eq!(annotation.id(), Some("Phrase1"));
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_find_annotations_embeds_2() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence = store.annotation("Sentence1").unwrap();
    let mut count = 0;
    for annotation in sentence
        .find_annotations(TextSelectionOperator::Embeds {
            all: false,
            negate: false,
        })
        .unwrap()
    {
        count += 1;
        assert_eq!(annotation.id(), Some("Phrase1"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn test_find_annotations_overlaps() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    setup_example_6b(&mut store)?;
    let phrase = store.annotation("Phrase1").unwrap();
    let annotations: Vec<_> = phrase
        .find_annotations(TextSelectionOperator::Overlaps {
            all: false,
            negate: false,
        })
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(annotations.len(), 2);
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Phrase2"));
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Sentence1"));
    Ok(())
}

#[test]
fn test_find_annotations_overlaps_2() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    setup_example_6b(&mut store)?;
    let phrase = store.annotation("Phrase2").unwrap();
    let annotations: Vec<_> = phrase
        .find_annotations(TextSelectionOperator::Overlaps {
            all: false,
            negate: false,
        })
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(annotations.len(), 2);
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Phrase1"));
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Sentence1"));
    Ok(())
}

#[test]
fn test_find_annotations_precedes() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    setup_example_6b(&mut store)?;
    let phrase = store.annotation("Phrase2").unwrap();
    let annotations: Vec<_> = phrase
        .find_annotations(TextSelectionOperator::Precedes {
            all: false,
            negate: false,
        })
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(annotations.len(), 1);
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Phrase3"));
    Ok(())
}

#[test]
fn test_find_annotations_succeeds() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    setup_example_6b(&mut store)?;
    let phrase = store.annotation("Phrase3").unwrap();
    let annotations: Vec<_> = phrase
        .find_annotations(TextSelectionOperator::Succeeds {
            all: false,
            negate: false,
        })
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(annotations.len(), 2);
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Phrase1"));
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Phrase2"));
    Ok(())
}

#[test]
fn test_textselections_relative_offset() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence1 = store
        .annotation("Sentence1")
        .unwrap()
        .textselections()
        .next()
        .unwrap();
    let phrase1 = store
        .annotation("Phrase1")
        .unwrap()
        .textselections()
        .next()
        .unwrap();
    let offset = phrase1.relative_offset(&sentence1).unwrap();
    assert_eq!(offset.begin, Cursor::BeginAligned(17)); //happens to be the same as absolute offset since Sentence1 covers all..
    assert_eq!(offset.end, Cursor::BeginAligned(40));
    Ok(())
}

pub fn setup_example_7(n: usize) -> Result<(AnnotationStore, TextResourceHandle), StamError> {
    let mut store = AnnotationStore::new();
    let mut text = String::with_capacity(n);
    for _ in 0..n {
        text.push('x');
    }
    let resource_handle =
        store.insert(TextResource::new("dummy", Config::default()).with_string(text))?;

    let dataset_handle = store.insert(
        AnnotationDataSet::new(Config::default()).with_data_with_id("type", "bigram", "D1")?,
    )?;

    for x in 0..n - 2 {
        store.annotate(
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::TextSelector(
                    resource_handle.into(),
                    Offset::simple(x, x + 2),
                ))
                .with_existing_data(BuildItem::Handle(dataset_handle), "D1"),
        )?;
    }
    Ok((store, resource_handle))
}

#[test]
fn test_textselections_scale_unsorted_iter() -> Result<(), StamError> {
    let n = 100000;
    let (store, resource_handle) = setup_example_7(n)?;

    let resource = store
        .resource(resource_handle)
        .expect("resource must exist");

    //iterate over all textselections unsorted
    let mut count = 0;
    for _ in resource.textselections() {
        count += 1;
    }
    assert_eq!(count, n - 2);

    Ok(())
}

#[test]
fn test_textselections_scale_sorted_iter() -> Result<(), StamError> {
    let n = 100000;
    let (store, resource_handle) = setup_example_7(n)?;

    let resource = store
        .resource(resource_handle)
        .expect("resource must exist");

    //iterate over all textselections unsorted
    let mut count = 0;
    for _ in resource.iter() {
        count += 1;
    }
    assert_eq!(count, n - 2);

    Ok(())
}

#[test]
fn test_textselections_scale_test_overlap() -> Result<(), StamError> {
    let n = 100000;
    let (store, resource_handle) = setup_example_7(n)?;

    let resource = store
        .resource(resource_handle)
        .expect("resource must exist");

    let selection = resource.textselection(&Offset::simple(100, 105))?;
    //iterate over all textselections unsorted

    let mut count = 0;
    for _textselection in selection.find_textselections(TextSelectionOperator::Overlaps {
        all: false,
        negate: false,
    }) {
        count += 1;
    }

    assert_eq!(count, 6);

    Ok(())
}
