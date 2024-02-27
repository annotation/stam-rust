use std::env;
use std::fs::File;
use std::io::prelude::*;

mod common;
use crate::common::*;

use stam::*;

const CARGO_MANIFEST_DIR: &'static str = env!("CARGO_MANIFEST_DIR");

#[test]
fn instantiation_naive() -> Result<(), StamError> {
    let mut store = AnnotationStore::default().with_id("test");

    store.insert(TextResource::from_string(
        "testres",
        "Hello world",
        Config::default(),
    ))?;

    let mut dataset = AnnotationDataSet::new(Config::default()).with_id("testdataset");
    dataset.insert(DataKey::new("pos"))?;
    store.insert(dataset)?;

    Ok(())
}

#[test]
fn sanity_check() -> Result<(), StamError> {
    // Instantiate the store
    let mut store = AnnotationStore::default().with_id("test");

    // Insert a text resource into the store
    store.insert(TextResource::from_string(
        "testres",
        "Hello world",
        Config::default(),
    ))?;

    // Create a dataset with one key and insert it into the store
    let mut dataset = AnnotationDataSet::new(Config::default()).with_id("testdataset");
    dataset.insert(DataKey::new("pos"))?; //returns a DataKeyHandle, not further used in this test
    let set_handle = store.insert(dataset)?;

    //get by handle (internal id)
    let dataset = store.dataset(set_handle).or_fail()?;
    assert_eq!(dataset.id(), Some("testdataset"));

    //get by directly by id
    let resource = store.resource("testres").or_fail()?;
    assert_eq!(resource.id(), Some("testres"));
    assert_eq!(resource.textlen(), 11);
    Ok(())
}

#[test]
fn instantiation_with_builder_pattern() -> Result<(), StamError> {
    setup_example_1()?;
    Ok(())
}

#[test]
fn annotation_iter_data() -> Result<(), StamError> {
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
    let resource = store.resource("testres").or_fail()?;
    let text = resource.text();
    assert_eq!(text, "Hello world");
    Ok(())
}

#[test]
fn resource_textselection() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let resource = store.resource("testres").unwrap();
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
fn annotation_text() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let annotation = store.annotation("A1").unwrap();
    let resource = annotation.resources().next().unwrap();
    assert_eq!(resource.id(), Some("testres"));
    let text: &str = annotation.text().next().unwrap();
    assert_eq!(text, "world");
    Ok(())
}

#[test]
fn store_annotate() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
                Offset::simple(0, 5),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
                Offset::simple(6, 11),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    Ok(())
}

#[test]
fn store_annotate_existing_data() -> Result<(), StamError> {
    let mut store = setup_example_2()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
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
fn store_annotate_add_after_borrow() -> Result<(), StamError> {
    let mut store = setup_example_2()?;
    let annotation = store.annotation("A1").unwrap();
    let mut count = 0;
    for _data in annotation.data() {
        count += 1;
    }
    assert_eq!(count, 1);
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
                Offset::simple(6, 11),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    Ok(())
}

#[test]
fn store_annotate_add_during_borrowproblem() -> Result<(), StamError> {
    let mut store = setup_example_2()?;
    let annotation: &Annotation = store.get("A1")?;
    //this is an antipattern!
    //                                 V---- here we clone the annotation to prevent a borrow problem (cannot borrow `store` as mutable because it is also borrowed as immutable (annotation)), this is relatively low-cost
    for (dataset, data) in annotation.clone().data() {
        store.annotate(
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(6, 11),
                ))
                .with_existing_data(dataset, data),
        )?;
    }
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
fn parse_json_dataset() -> Result<(), StamError> {
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

    let annotationset = AnnotationDataSet::from_json_str(json, Config::default())?;

    let mut store = setup_example_2()?;
    let sethandle = store.insert(annotationset)?;

    let dataset = store.dataset(sethandle).unwrap();
    assert_eq!(dataset.id(), Some("https://purl.org/dc"));

    let mut count = 0;
    let mut firstkeyhandle: Option<DataKeyHandle> = None;
    for key in dataset.keys() {
        count += 1;
        if count == 1 {
            assert_eq!(key.id(), Some("http://purl.org/dc/terms/creator"));
            firstkeyhandle = Some(key.handle());
        }
    }
    assert_eq!(count, 3);

    count = 0;
    for data in dataset.data() {
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

const EXAMPLE_3_TEMP_ID: &'static str = r#"{ 
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
                    "@id": "!D0",
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
            "@id": "!A0",
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
                "@id": "!D0",
                "set": "testdataset"
            }]
        }]
    }"#;

fn example_3_common_tests(store: &AnnotationStore) -> Result<(), StamError> {
    //repeat some common tests
    let _resource = store.resource("testres").or_fail()?;
    let annotationset = store.dataset("testdataset").or_fail()?;

    let _datakey = annotationset.key("pos").or_fail()?;
    let _annotationdata = annotationset.annotationdata("D1").or_fail()?;
    let _annotation = store.annotation("A1").or_fail()?;

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
    let store = AnnotationStore::from_json_str(EXAMPLE_3, Config::default())?;

    example_3_common_tests(&store)?;

    Ok(())
}

#[test]
fn parse_json_annotationstore_temp_id() -> Result<(), StamError> {
    let store = AnnotationStore::from_json_str(EXAMPLE_3_TEMP_ID, Config::default())?;

    //repeat some common tests
    let _resource: &TextResource = store.get("testres")?;
    let annotationset: &AnnotationDataSet = store.get("testdataset")?;

    let _datakey: &DataKey = annotationset.get("pos")?;
    let _annotationdata: &AnnotationData = annotationset.get("!D0")?;
    let annotation: &Annotation = store.get("!A0")?;
    assert_eq!(annotation.id(), None); //we have no public ID

    for key in annotationset.keys() {
        //there is only one so we can test in loop body
        assert_eq!(key.id(), Some("pos"));
    }

    for data in annotationset.data() {
        //there is only one so we can test in loop body
        assert_eq!(data.id(), None); //we have no public ID
    }
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
fn annotation_textselections() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let sentence = store.annotation("sentence2").unwrap();
    for textselection in sentence.textselections() {
        assert_eq!(textselection.text(), "I am only passionately curious.")
    }
    Ok(())
}

#[test]
fn annotation_textselections_relative() -> Result<(), StamError> {
    let store = setup_example_3()?;
    //this annotation uses relative position
    let word = store.annotation("sentence2word2").unwrap();
    for textselection in word.textselections() {
        assert_eq!(textselection.text(), "am")
    }
    Ok(())
}

#[test]
fn annotation_textselections_relative_endaligned() -> Result<(), StamError> {
    let store = setup_example_3()?.with_annotation(
        AnnotationBuilder::new()
            .with_id("sentence2lastword")
            .with_target(SelectorBuilder::annotationselector(
                "sentence2",
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
fn resource_textselection_existing() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let resource = store.resource("testres").or_fail()?;
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
    let resource = store.resource("testres").or_fail()?;
    let textselection = resource.textselection(&Offset::simple(6, 11))?;
    let v: Vec<_> = textselection.annotations().collect();
    let a_ref = store.annotation("A1").unwrap();
    assert_eq!(v, vec!(a_ref));
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
    let resource = store.resource("testres").or_fail()?;
    let v: Vec<_> = resource.as_ref().textselections_unsorted().collect(); //lower-level method
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
    let resource = store.resource("testres").or_fail()?;
    let v: Vec<_> = resource.textselections().collect();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].begin(), 0);
    assert_eq!(v[0].end(), 5);
    Ok(())
}

#[test]
fn text_by_annotation() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotation = store.annotation("A1").or_fail()?;
    let mut count = 0;
    for text in annotation.text() {
        count += 1;
        assert_eq!(text, "world");
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn data_by_value() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let dataset = store.dataset("testdataset").or_fail()?;
    let key = dataset.key("pos").unwrap();
    let annotationdata = key.data().filter_value(DataOperator::Equals("noun")).next();
    assert!(annotationdata.is_some());
    assert_eq!(annotationdata.unwrap().id(), Some("D1"));
    Ok(())
}

#[test]
fn find_data_exact() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let dataset = store.dataset("testdataset").or_fail()?;
    let mut count = 0;
    for annotationdata in dataset.find_data("pos", DataOperator::Equals("noun")) {
        count += 1;
        assert_eq!(annotationdata.id(), Some("D1"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn find_data_by_key() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let dataset = store.dataset("testdataset").or_fail()?;
    let count = dataset.find_data("type", DataOperator::Any).count();
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn data_by_key() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let dataset = store.dataset("testdataset").or_fail()?;
    let key = dataset.key("type").unwrap();
    let count = key.data().count();
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn find_data_all() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let annotationset = store.dataset("testdataset").or_fail()?;
    let count = annotationset.find_data(false, DataOperator::Any).count();
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn find_data_all_2() -> Result<(), StamError> {
    let store = setup_example_3()?;
    let annotationset = store.dataset("testdataset").or_fail()?;
    let mut count = 0;
    for _ in annotationset.data() {
        count += 1;
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn multiselector_creation_notranged() -> Result<(), StamError> {
    let _store = setup_example_multiselector_notranged()?;
    Ok(())
}

#[test]
fn multiselector_creation_ranged() -> Result<(), StamError> {
    let _store = setup_example_multiselector_ranged()?;
    Ok(())
}

#[test]
fn multiselector_iter_notranged() -> Result<(), StamError> {
    let store = setup_example_multiselector_notranged()?;
    let annotation = store.annotation("WordAnnotation").unwrap();
    let result: Vec<&str> = annotation.text().collect();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "Hello");
    assert_eq!(result[1], "world");
    Ok(())
}

#[test]
fn multiselector_iter_ranged() -> Result<(), StamError> {
    let store = setup_example_multiselector_ranged()?;
    let annotation = store.annotation("WordAnnotation").or_fail()?;
    let result: Vec<&str> = annotation.text().collect();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "Hello");
    assert_eq!(result[1], "world");
    Ok(())
}

#[test]
fn multiselector2_iter() -> Result<(), StamError> {
    let store = setup_example_multiselector_2()?;
    let annotation = store.annotation("WordAnnotation").or_fail()?;
    let result: Vec<&str> = annotation.text().collect();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "Hello");
    assert_eq!(result[1], "world");
    Ok(())
}

#[test]
fn read_single() -> Result<(), StamError> {
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
fn find_text() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world")
            .build()?,
    )?;
    let resource = store.resource("testres").unwrap();
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
fn find_text_nocase() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world")
            .build()?,
    )?;
    let resource = store.resource("testres").or_fail()?;
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
fn find_text_sequence() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world")
            .build()?,
    )?;
    let resource = store.resource("testres").or_fail()?;
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
fn find_text_sequence2() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello, world")
            .build()?,
    )?;
    let resource = store.resource("testres").or_fail()?;
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
fn find_text_sequence_nocase() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world")
            .build()?,
    )?;
    let resource = store.resource("testres").or_fail()?;
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
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world")
            .build()?,
    )?;
    let resource = store.resource("testres").or_fail()?;
    let results =
        resource.find_text_sequence(&["hello", "world", "hi"], |c| !c.is_alphabetic(), false);
    assert!(results.is_none());
    Ok(())
}

#[test]
fn test_split_text() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world")
            .build()?,
    )?;
    let resource = store.resource("testres").or_fail()?;
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
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
        ).build()?
    )?;
    let resource = store.resource("testres").or_fail()?;
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
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
        ).build()?
    )?;
    let resource = store.resource("testres").or_fail()?;
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
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
        ).build()?
    )?;
    let resource = store.resource("testres").or_fail()?;
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
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
        ).build()?
    )?;
    let resource = store.resource("testres").or_fail()?;
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
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
        ).build()?
    )?;
    let resource = store.resource("testres").or_fail()?;
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
    let mut store = AnnotationStore::default();
    store.insert(
        TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
        ).build()?
    )?;
    let resource = store.resource("testres").or_fail()?;
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

    let annotation = store.annotation("A1").or_fail()?;
    assert_eq!(annotation.text().next().unwrap(), "Hallå");
    for data in annotation.data() {
        assert_eq!(data.key().id(), Some("pos"));
        assert_eq!(data.id(), Some("PosInterjection"));
        assert_eq!(data.set().id(), Some("https://example.org/test/"));
        assert_eq!(data.value(), "interjection");
    }

    let annotation = store.annotation("A2").or_fail()?;
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

#[test]
fn parse_old_csv() -> Result<(), StamError> {
    let store = AnnotationStore::from_file(
        "tests/oldtest.store.stam.csv", //this lacks two optional columns that were added later
        Config::default().with_debug(true),
    )?;
    test_example_a_sanity(&store)?;
    Ok(())
}

#[test]
fn annotate_regex_single2() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex_for_example_6(&mut store)?;

    let data: Vec<_> = store
        .find_data("myset", "type", DataOperator::Equals("header"))
        .annotations()
        .collect();
    assert_eq!(data.len(), 4);
    Ok(())
}

#[test]
fn annotations_by_textselection() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex_for_example_6(&mut store)?;
    let resource = store.resource("humanrights").unwrap();
    let mut count = 0;
    let mut count_annotations = 0;
    for textselection in resource.textselections() {
        count += 1;
        for annotation in textselection.annotations() {
            count_annotations += 1;
            eprintln!("{} {:?}", textselection.text(), annotation.id());
        }
    }
    assert_eq!(count, resource.textselections_len());
    assert_ne!(count, 0);
    assert_eq!(count_annotations, store.annotations_len());
    assert_ne!(count_annotations, 0);
    Ok(())
}

#[test]
fn annotations_by_textselection_2() -> Result<(), StamError> {
    let mut store = setup_example_5()?;
    annotate_regex_for_example_6(&mut store)?;
    let resource = store.resource("humanrights").unwrap();
    let count_annotations = resource.textselections().annotations().count();
    assert_eq!(count_annotations, store.annotations_len());
    assert_ne!(count_annotations, 0);
    Ok(())
}

#[test]
fn test_example_6_full() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    annotate_phrases_for_example_6(&mut store)?;
    annotate_words(&mut store, "humanrights")?;
    let resource = store.resource("humanrights").or_fail()?;
    assert_eq!(
        resource.text(),
        "All human beings are born free and equal in dignity and rights."
    );
    let sentence = store.annotation("Sentence1").or_fail()?;
    assert_eq!(
        sentence.text_simple(),
        Some("All human beings are born free and equal in dignity and rights.")
    );
    let phrase1 = store.annotation("Phrase1").or_fail()?;
    assert_eq!(phrase1.text_simple(), Some("are born free and equal"));
    let phrase2 = store.annotation("Phrase2").or_fail()?;
    assert_eq!(phrase2.text_simple(), Some("human beings are born"));
    let phrase3 = store.annotation("Phrase3").or_fail()?;
    assert_eq!(phrase3.text_simple(), Some("dignity and rights"));
    Ok(())
}

#[test]
fn test_example_6b() -> Result<(), StamError> {
    let store = setup_example_6b()?;
    let resource = store.resource("humanrights").or_fail()?;
    assert_eq!(
        resource.text(),
        "All human beings are born free and equal in dignity and rights."
    );
    let sentence = store.annotation("Sentence1").or_fail()?;
    assert_eq!(
        sentence.text_simple(),
        Some("All human beings are born free and equal in dignity and rights.")
    );
    let phrase1 = store.annotation("Phrase1").or_fail()?;
    assert_eq!(phrase1.text_simple(), Some("are born free and equal"));
    let phrase2 = store.annotation("Phrase2").or_fail()?;
    assert_eq!(phrase2.text_simple(), Some("human beings are born"));
    let phrase3 = store.annotation("Phrase3").or_fail()?;
    assert_eq!(phrase3.text_simple(), Some("dignity and rights"));
    Ok(())
}

#[test]
fn test_example_6c() -> Result<(), StamError> {
    let store = setup_example_6c()?;
    let resource = store.resource("humanrights").or_fail()?;
    assert_eq!(
        resource.text(),
        "All human beings are born free and equal in dignity and rights."
    );
    let sentence = store.annotation("Sentence1").or_fail()?;
    assert_eq!(
        sentence.text_join(" "),
        "All human beings are born free and equal in dignity and rights ." // <-- note the tokenisation!
    );
    let phrase1 = store.annotation("Phrase1").or_fail()?;
    assert_eq!(phrase1.text_join(" "), "are born free and equal");
    let phrase2 = store.annotation("Phrase2").or_fail()?;
    assert_eq!(phrase2.text_join(" "), "human beings are born");
    let phrase3 = store.annotation("Phrase3").or_fail()?;
    assert_eq!(phrase3.text_join(" "), "dignity and rights");
    Ok(())
}

#[test]
fn annotations_by_textselection_none() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let resource = store.resource("humanrights").or_fail()?;
    let textselection = resource.textselection(&Offset::simple(1, 14))?; //no annotations for this random selection
    let v: Vec<_> = textselection.annotations().collect();
    assert!(v.is_empty());
    Ok(())
}

#[test]
fn related_text_embedded() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let phrase1 = store.annotation("Phrase1").or_fail()?;
    let mut count = 0;
    for reftextsel in phrase1.textselections() {
        for textsel in reftextsel.related_text(TextSelectionOperator::embedded()) {
            count += 1;
            assert_eq!(textsel.begin(), 0);
            assert_eq!(textsel.end(), 63);
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn textselections_in_range_exact() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let resource = store.resource("humanrights").or_fail()?;
    let mut count = 0;
    for textsel in resource.textselections_in_range(17, 40) {
        count += 1;
        assert_eq!(textsel.begin(), 17);
        assert_eq!(textsel.end(), 40);
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn textselections_in_range_bigger() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let resource = store.resource("humanrights").or_fail()?;
    let mut count = 0;
    for textsel in resource.textselections_in_range(1, 50) {
        count += 1;
        assert_eq!(textsel.begin(), 17);
        assert_eq!(textsel.end(), 40);
    }
    assert_eq!(count, 1);
    count = 0;
    for _textsel in resource.textselections_in_range(0, 64) {
        count += 1;
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn related_text_embeds() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence = store.annotation("Sentence1").or_fail()?;
    let mut count = 0;
    for reftextsel in sentence.textselections() {
        for textsel in reftextsel.related_text(TextSelectionOperator::embeds()) {
            count += 1;
            assert_eq!(textsel.begin(), 17);
            assert_eq!(textsel.end(), 40);
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn related_text_embeds_shortcut() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence = store.annotation("Sentence1").or_fail()?;
    let mut count = 0;
    for textsel in sentence.related_text(TextSelectionOperator::embeds()) {
        count += 1;
        assert_eq!(textsel.begin(), 17);
        assert_eq!(textsel.end(), 40);
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn annotations_by_related_text_embeds() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence = store.annotation("Sentence1").or_fail()?;
    let mut count = 0;
    for annotation in sentence
        .related_text(TextSelectionOperator::embeds())
        .annotations()
    {
        count += 1;
        assert_eq!(annotation.id(), Some("Phrase1"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn annotations_by_related_text_embeds_2() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence = store.annotation("Sentence1").or_fail()?;
    let mut count = 0;
    for annotation in sentence
        .textselections()
        .related_text(TextSelectionOperator::embeds())
        .annotations()
    {
        count += 1;
        assert_eq!(annotation.id(), Some("Phrase1"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn annotations_by_related_text_overlaps() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    annotate_phrases_for_example_6(&mut store)?;
    let phrase = store.annotation("Phrase1").or_fail()?;
    let annotations: Vec<_> = phrase
        .related_text(TextSelectionOperator::overlaps())
        .annotations()
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
fn annotations_by_related_text_before() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    annotate_phrases_for_example_6(&mut store)?;
    let phrase = store.annotation("Phrase2").or_fail()?;
    let annotations: Vec<_> = phrase
        .related_text(TextSelectionOperator::before())
        .annotations()
        .collect();
    assert_eq!(annotations.len(), 1);
    assert!(annotations
        .iter()
        .any(|annotation| annotation.id().unwrap() == "Phrase3"));
    Ok(())
}

#[test]
fn annotations_by_related_text_after() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    annotate_phrases_for_example_6(&mut store)?;
    let phrase = store.annotation("Phrase3").or_fail()?;
    let annotations: Vec<_> = phrase
        .related_text(TextSelectionOperator::after())
        .annotations()
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
fn related_text_with_data() -> Result<(), StamError> {
    // Given annotation type=phrase, get the text of all annotations type=word embedded in that phrase

    let mut store = setup_example_6()?;
    annotate_phrases_for_example_6(&mut store)?;
    annotate_words(&mut store, "humanrights")?; //simple tokeniser in tests/common
    let phrase = store.annotation("Phrase3").or_fail()?; // "dignity and rights"
    let key_type = store.key("myset", "type").or_fail()?;
    let text: Vec<_> = phrase
        .related_text(TextSelectionOperator::embeds())
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("word"))
        .text()
        .collect();
    /*
    let text: Vec<_> = phrase
        .related_text_with_data(
            TextSelectionOperator::embeds(),
            "myset",
            "type",
            &DataOperator::Equals("word"),
        )
        .into_iter() //work away the Option<>
        .flatten() // --^
        .map(|(text, _)| text.text()) //grab only the textual content
        .collect();
    */
    assert_eq!(text, &["dignity", "and", "rights"]);
    Ok(())
}

#[test]
fn related_text_with_data_2() -> Result<(), StamError> {
    // Given annotation type=phrase, get the text of all annotations type=word that come before in that phrase

    let mut store = setup_example_6()?;
    annotate_phrases_for_example_6(&mut store)?;
    annotate_words(&mut store, "humanrights")?; //simple tokeniser in tests/common
    let phrase = store.annotation("Phrase3").or_fail()?; // "dignity and rights"
    let key_type = store.key("myset", "type").or_fail()?;
    let text: Vec<_> = phrase
        .related_text(TextSelectionOperator::after()) //phrase AFTER result, so result before phrase
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("word"))
        .text()
        .collect();
    /*
    let text: Vec<_> = phrase
        .related_text_with_data(
            TextSelectionOperator::after(), //phrase AFTER result, so result before phrase
            "myset",
            "type",
            &DataOperator::Equals("word"),
        )
        .into_iter() //work away the Option<>
        .flatten() // --^
        .map(|(text, _)| text.text()) //grab only the textual content
        .collect();
    */
    assert_eq!(
        text,
        &["All", "human", "beings", "are", "born", "free", "and", "equal", "in"]
    );
    Ok(())
}

#[test]
fn related_text_with_data_3() -> Result<(), StamError> {
    // Given annotation type=phrase, get the text of all annotations type=word that come before in that phrase

    let store = setup_example_6b()?; //<--- used different example! rest of code is the same
    let phrase = store.annotation("Phrase3").or_fail()?; // "dignity and rights"
    let key_type = store.key("myset", "type").or_fail()?;
    let text: Vec<_> = phrase
        .related_text(TextSelectionOperator::after()) //phrase AFTER result, so result before phrase
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("word"))
        .text()
        .collect();
    assert_eq!(
        text,
        &["All", "human", "beings", "are", "born", "free", "and", "equal", "in"]
    );
    Ok(())
}

#[test]
fn related_text_with_data_4() -> Result<(), StamError> {
    // Given annotation type=phrase, get the text of all annotations type=word that come before in that phrase

    let store = setup_example_6c()?; //<--- used different example! rest of code is the same
    let phrase = store.annotation("Phrase3").or_fail()?; // "dignity and rights"
    let key_type = store.key("myset", "type").or_fail()?;
    let text: Vec<_> = phrase
        .related_text(TextSelectionOperator::after()) //phrase AFTER result, so result before phrase
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("word"))
        .text()
        .collect();
    assert_eq!(
        text,
        &["All", "human", "beings", "are", "born", "free", "and", "equal", "in"]
    );
    Ok(())
}

#[test]
fn annotations_in_targets() -> Result<(), StamError> {
    //Get the 2nd word in the sentence

    let store = setup_example_6c()?; //<--- used different example! rest of code is the same
    let sentence = store.annotation("Sentence1").or_fail()?;
    //get the 2nd word in the sentence
    let secondword = sentence
        .annotations_in_targets(AnnotationDepth::default())
        .nth(1)
        .unwrap();
    assert_eq!(secondword.text_simple(), Some("human"));
    Ok(())
}

#[test]
fn annotations_in_targets_len() -> Result<(), StamError> {
    //test the number of targets

    let store = setup_example_6c()?; //<--- used different example! rest of code is the same
    let sentence = store.annotation("Sentence1").or_fail()?;
    let words: Vec<_> = sentence
        .annotations_in_targets(AnnotationDepth::default())
        .collect();
    assert_eq!(words.len(), 12, "number of targets returned"); //the final punctuation is not included because it's not selected via AnnotationSelector but via a TextSelector
    Ok(())
}

#[test]
fn related_text_with_data_5() -> Result<(), StamError> {
    //Get the 2nd word in the sentence

    let mut store = setup_example_6()?;
    annotate_phrases_for_example_6(&mut store)?;
    annotate_words(&mut store, "humanrights")?; //simple tokeniser in tests/common
    let sentence = store.annotation("Sentence1").or_fail()?;
    let key_type = store.key("myset", "type").or_fail()?;

    let words: Vec<_> = sentence
        .related_text(TextSelectionOperator::embeds())
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("word"))
        .textual_order(); //we could omit this if we were sure word annotations were added in sequence

    let secondword = words.iter().nth(1).unwrap();

    assert_eq!(secondword.text_simple(), Some("human"));
    Ok(())
}

#[test]
fn find_data_about() -> Result<(), StamError> {
    let store = setup_example_6c()?;
    let sentence = store.annotation("Sentence1").or_fail()?;
    //get the 2nd word in the sentence
    let secondword = sentence
        .annotations_in_targets(AnnotationDepth::default())
        .nth(1)
        .unwrap();
    assert_eq!(
        secondword.text_simple(),
        Some("human"),
        "testing the second word in the sentence"
    );

    //now find the phrase this word belongs to:
    let key_type = store.key("myset", "type").or_fail()?;
    let mut count = 0;
    for phrase in secondword
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("phrase"))
    {
        count += 1;
        //we can test in body because we only have one:
        assert_eq!(
            phrase.id(),
            Some("Phrase2"),
            "testing whether we got the right phrase"
        );
        assert_eq!(phrase.text_join(" "), "human beings are born");
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn annotations_by_related_text_matching_data() -> Result<(), StamError> {
    let mut store = setup_example_6()?; //note: not the same as the previous two!
    annotate_phrases_for_example_6(&mut store)?;
    annotate_words(&mut store, "humanrights")?; //simple tokeniser in tests/common
    let sentence = store.annotation("Sentence1").or_fail()?;
    let key_type = store.key("myset", "type").or_fail()?;

    let words: Vec<_> = sentence
        .related_text(TextSelectionOperator::embeds())
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("word"))
        .textual_order(); //we could omit this if we were sure word annotations were added in sequence

    let secondword = words.iter().nth(1).unwrap();

    assert_eq!(secondword.text_simple(), Some("human"));

    //now find the phrase this word belongs to:
    let mut count = 0;
    for phrase in secondword
        .related_text(TextSelectionOperator::embedded())
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("phrase"))
    {
        count += 1;
        //we can test in body because we only have one:
        assert_eq!(phrase.id(), Some("Phrase2"));
        assert_eq!(phrase.text_simple(), Some("human beings are born"));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn annotations_by_related_text_relative_offset() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let sentence1 = store
        .annotation("Sentence1")
        .or_fail()?
        .textselections()
        .next()
        .unwrap();
    let phrase1 = store
        .annotation("Phrase1")
        .or_fail()?
        .textselections()
        .next()
        .unwrap();
    let offset = phrase1
        .relative_offset(&sentence1, OffsetMode::default())
        .unwrap();
    assert_eq!(offset.begin, Cursor::BeginAligned(17)); //happens to be the same as absolute offset since Sentence1 covers all..
    assert_eq!(offset.end, Cursor::BeginAligned(40));
    Ok(())
}

#[test]
fn ex7_resource_textselections_scale_unsorted_iter() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    //iterate over all textselections unsorted
    let mut count = 0;
    for _ in resource.textselections() {
        count += 1;
    }
    assert!(count >= n - 2);

    Ok(())
}

#[test]
fn ex7_resource_textselections_scale_sorted_iter() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    //iterate over all textselections unsorted
    let mut count = 0;
    for _ in resource.as_ref().textselections_unsorted() {
        count += 1;
    }
    assert!(count >= n - 2);

    Ok(())
}

#[test]
fn ex7_resource_textselection() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    resource.textselection(&Offset::simple(100, 105))?;

    Ok(())
}

#[test]
fn ex7_textselections_scale_test_overlap() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    let selection = resource.textselection(&Offset::simple(100, 105))?;
    //iterate over all textselections unsorted

    let mut count = 0;
    for textselection in selection.related_text(TextSelectionOperator::overlaps()) {
        eprintln!("Overlaps: {:?}", textselection);
        count += 1;
    }

    assert_eq!(count, 11);

    Ok(())
}

#[test]
fn ex7_textselections_scale_test_embeds() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    let selection = resource.textselection(&Offset::simple(100, 105))?;
    //iterate over all textselections unsorted

    let mut count = 0;
    for textselection in selection.related_text(TextSelectionOperator::embeds()) {
        eprintln!("Embeds: {:?}", textselection);
        count += 1;
    }

    assert_eq!(count, 9);

    Ok(())
}

#[test]
fn ex7_textselections_scale_test_embedded() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    let selection = resource.textselection(&Offset::simple(100, 101))?;
    //iterate over all textselections unsorted

    let mut count = 0;
    for textselection in selection.related_text(TextSelectionOperator::embedded()) {
        eprintln!("Embedded: {:?}", textselection);
        count += 1;
    }

    assert_eq!(count, 2);

    Ok(())
}

#[test]
fn ex7_textselections_scale_test_precedes() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    let selection = resource.textselection(&Offset::simple(100, 101))?;
    //iterate over all textselections unsorted

    let mut count = 0;
    for textselection in selection.related_text(TextSelectionOperator::precedes()) {
        eprintln!("Precedes: {:?}", textselection);
        count += 1;
    }

    assert_eq!(count, 2);

    Ok(())
}

#[test]
fn ex7_textselections_scale_test_succeeds() -> Result<(), StamError> {
    let n = 10000;
    let store = setup_example_7(n)?;

    let resource = store.resource("testres").or_fail()?;

    let selection = resource.textselection(&Offset::simple(100, 101))?;
    //iterate over all textselections unsorted

    let mut count = 0;
    for textselection in selection.related_text(TextSelectionOperator::succeeds()) {
        eprintln!("Succeeds: {:?}", textselection);
        count += 1;
    }

    assert_eq!(count, 2);

    Ok(())
}

#[test]
fn query_parse() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA set key = value;";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for constraint in query.iter() {
        count += 1;
        if let Constraint::KeyValue {
            set,
            key,
            operator,
            qualifier: _,
        } = constraint
        {
            assert_eq!(*set, "set");
            assert_eq!(*key, "key");
            assert_eq!(*operator, DataOperator::Equals("value"));
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse_quoted() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = \"value\";";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for constraint in query.iter() {
        count += 1;
        if let Constraint::KeyValue {
            set,
            key,
            operator,
            qualifier: _,
        } = constraint
        {
            assert_eq!(*set, "set");
            assert_eq!(*key, "key");
            assert_eq!(*operator, DataOperator::Equals("value"));
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse_numeric() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = 5;";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for constraint in query.iter() {
        count += 1;
        if let Constraint::KeyValue {
            set,
            key,
            operator,
            qualifier: _,
        } = constraint
        {
            assert_eq!(*set, "set");
            assert_eq!(*key, "key");
            assert_eq!(*operator, DataOperator::EqualsInt(5));
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse_data_short() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\";";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for constraint in query.iter() {
        count += 1;
        if let Constraint::DataKey {
            set,
            key,
            qualifier: _,
        } = constraint
        {
            assert_eq!(*set, "set");
            assert_eq!(*key, "key");
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse2() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE TEXT blah;";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for constraint in query.iter() {
        count += 1;
        if let Constraint::Text(value, _) = constraint {
            assert_eq!(*value, "blah");
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse_subquery() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = \"value\"; { SELECT ANNOTATION WHERE RELATION ?a SUCCEEDS; }";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    let subquery = query.subquery().expect("expected subquery");
    for constraint in subquery.iter() {
        count += 1;
        if let Constraint::TextRelation { var, operator } = constraint {
            assert_eq!(*var, "a");
            assert_eq!(*operator, TextSelectionOperator::succeeds());
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let query: Query = "SELECT ANNOTATION ?a WHERE DATA myset type = phrase;".try_into()?;
    let mut count = 0;
    let refdata = store
        .find_data("myset", "type", DataOperator::Equals("phrase"))
        .next()
        .expect("reference data must exist");
    for results in store.query(query) {
        for result in results.iter() {
            match result {
                QueryResultItem::Annotation(annotation) => {
                    count += 1;
                    assert!(annotation.has_data(&refdata));
                }
                _ => assert!(false, "wrong return type"),
            }
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_by_name() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let query: Query = "SELECT ANNOTATION ?a WHERE DATA myset type = phrase;".try_into()?;
    let mut count = 0;
    let refdata = store
        .find_data("myset", "type", DataOperator::Equals("phrase"))
        .next()
        .expect("reference data must exist");
    let iter = store.query(query);
    let names = iter.names();
    for results in iter {
        if let Ok(result) = results.get_by_name(&names, "a") {
            match result {
                QueryResultItem::Annotation(annotation) => {
                    count += 1;
                    assert!(annotation.has_data(&refdata));
                }
                _ => assert!(false, "wrong return type"),
            }
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_subquery() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let query: Query = "SELECT ANNOTATION ?sentence WHERE DATA myset type = sentence; { SELECT ANNOTATION ?phrase WHERE RELATION ?sentence EMBEDS; DATA myset type = phrase; }".try_into()?;
    let mut count = 0;
    let refdata = store
        .find_data("myset", "type", DataOperator::Equals("phrase"))
        .next()
        .expect("reference data must exist");
    let refdata2 = store
        .find_data("myset", "type", DataOperator::Equals("sentence"))
        .next()
        .expect("reference data must exist");
    let queryresults = store.query(query);
    let names = queryresults.names();
    for results in queryresults {
        count += 1;
        if let QueryResultItem::Annotation(annotation) = results.get_by_name(&names, "phrase")? {
            assert!(annotation.has_data(&refdata));
        } else {
            assert!(false, "did not get phrase");
        }
        if let QueryResultItem::Annotation(annotation) = results.get_by_name(&names, "sentence")? {
            assert!(annotation.has_data(&refdata2));
        } else {
            assert!(false, "did not get sentence");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
#[cfg(feature = "transpose")]
fn transposition_simple() -> Result<(), StamError> {
    let store = setup_example_8()?;
    let annotation = store.annotation("SimpleTransposition1").or_fail()?;
    assert_eq!(annotation.text().count(), 2);
    let mut iter = annotation.text();
    assert_eq!(iter.next(), Some("human beings are born"));
    assert_eq!(iter.next(), Some("human beings are born"));
    Ok(())
}

#[test]
#[cfg(feature = "transpose")]
fn transposition_complex() -> Result<(), StamError> {
    let store = setup_example_8b()?;
    let phrase1 = store.annotation("A1").or_fail()?;
    assert_eq!(phrase1.text_simple(), Some("human beings are born"));
    let phrase2 = store.annotation("A2").or_fail()?;
    assert_eq!(phrase2.text_simple(), Some("human beings are born"));
    let annotation = store.annotation("SimpleTransposition1").or_fail()?;
    assert_eq!(
        annotation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2
    );
    Ok(())
}
