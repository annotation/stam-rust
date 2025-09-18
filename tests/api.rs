use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::process::Command;

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
fn store_reannotate_add_data() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    let handle = store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
                Offset::simple(0, 5),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    let annotations = store.annotations_len();
    store.reannotate(
        AnnotationBuilder::new().with_handle(handle).with_data(
            "someset",
            "revision",
            DataValue::Int(1),
        ),
        ReannotateMode::Add,
    )?;
    assert_eq!(
        store.annotations_len(),
        annotations,
        "Checking that we did not get extra annotations"
    );
    let annotation = store.annotation(handle).or_fail()?;
    assert_eq!(annotation.text_simple(), Some("Hello"));
    let mut count = 0;
    for data in annotation.data() {
        count += 1;
        if count == 1 {
            assert_eq!(data.key().id(), Some("word"));
            assert_eq!(data.set().id(), Some("tokenset"));
        } else if count == 2 {
            assert_eq!(data.key().id(), Some("revision"));
            assert_eq!(data.set().id(), Some("someset"));
            assert_eq!(data.value(), &DataValue::Int(1));
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn store_reannotate_replace_data() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    let handle = store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
                Offset::simple(0, 5),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    let annotations = store.annotations_len();
    store.reannotate(
        AnnotationBuilder::new().with_handle(handle).with_data(
            "someset",
            "revision",
            DataValue::Int(1),
        ),
        ReannotateMode::Replace,
    )?;
    assert_eq!(
        store.annotations_len(),
        annotations,
        "Checking that we did not get extra annotations"
    );
    let annotation = store.annotation(handle).or_fail()?;
    assert_eq!(annotation.text_simple(), Some("Hello"));
    let mut count = 0;
    for data in annotation.data() {
        count += 1;
        assert_eq!(data.key().id(), Some("revision"));
        assert_eq!(data.set().id(), Some("someset"));
        assert_eq!(data.value(), &DataValue::Int(1));
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn store_reannotate_replace_target() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    let handle = store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
                Offset::simple(0, 5),
            ))
            .with_data("tokenset", "word", DataValue::Null),
    )?;
    let annotations = store.annotations_len();
    store.reannotate(
        AnnotationBuilder::new()
            .with_handle(handle)
            .with_target(SelectorBuilder::textselector(
                "testres",
                Offset::simple(6, 11),
            )),
        ReannotateMode::Add, //no data, no effect
    )?;
    assert_eq!(
        store.annotations_len(),
        annotations,
        "Checking that we did not get extra annotations"
    );
    let annotation = store.annotation(handle).or_fail()?;
    assert_eq!(annotation.text_simple(), Some("world")); //different target
    let mut count = 0;
    //data unchanged
    for data in annotation.data() {
        count += 1;
        assert_eq!(data.key().id(), Some("word"));
        assert_eq!(data.set().id(), Some("tokenset"));
    }
    assert_eq!(count, 1);
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
    let annotationdata = key
        .data()
        .filter_value(DataOperator::Equals("noun".into()))
        .next();
    assert!(annotationdata.is_some());
    assert_eq!(annotationdata.unwrap().id(), Some("D1"));
    Ok(())
}

#[test]
fn find_data_exact() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let dataset = store.dataset("testdataset").or_fail()?;
    let mut count = 0;
    for annotationdata in dataset.find_data("pos", DataOperator::Equals("noun".into())) {
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

#[cfg(not(target_os = "windows"))]
#[test]
fn write_single() -> Result<(), StamError> {
    let mut store = AnnotationStore::from_file(
        &format!("{}/tests/singletest.store.stam.json", CARGO_MANIFEST_DIR),
        Config::default(),
    )?;
    store.set_filename("/tmp/singletest.store.stam.json");
    store.save()?;
    let output = Command::new("diff")
        .arg("-w")
        .arg(format!(
            "{}/tests/singletest.store.stam.json",
            CARGO_MANIFEST_DIR
        ))
        .arg("/tmp/singletest.store.stam.json")
        .output()
        .expect("failed to run diff");
    assert!(output.status.success());
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

#[cfg(not(target_os = "windows"))]
#[test]
fn test_write_include() -> Result<(), StamError> {
    let mut store = AnnotationStore::from_file(
        &format!("{}/tests/test.store.stam.json", CARGO_MANIFEST_DIR),
        Config::default().with_debug(true),
    )?;
    store.set_filename("/tmp/test.store.stam.json");
    store.save()?;
    let output = Command::new("diff")
        .arg("-w")
        .arg(format!("{}/tests/test.store.stam.json", CARGO_MANIFEST_DIR))
        .arg("/tmp/test.store.stam.json")
        .output()
        .expect("failed to run diff");
    assert!(output.status.success());
    Ok(())
}

#[test]
fn test_read_include_annotationstore() -> Result<(), StamError> {
    let store = AnnotationStore::from_file(
        "tests/includetest.store.stam.json",
        Config::default().with_debug(true),
    )?;
    test_example_a_sanity(&store)?;

    //annotation in main store
    let annotation = store.annotation("A3").or_fail()?;
    assert_eq!(annotation.text().next().unwrap(), "Hallå");
    for data in annotation.data() {
        assert_eq!(data.key().id(), Some("pos"));
        assert_eq!(data.id(), Some("PosInterjection"));
        assert_eq!(data.set().id(), Some("https://example.org/test/"));
        assert_eq!(data.value(), "interjection");
    }

    let mut count = 0;
    for substore in store.substores() {
        count += 1;
        assert_eq!(substore.id(), Some("Example A"));
        assert_eq!(
            substore.annotations().count(),
            2,
            "Two annotations in substore"
        )
    }
    assert_eq!(count, 1);

    Ok(())
}

#[cfg(not(target_os = "windows"))]
#[test]
fn test_write_include_annotationstore() -> Result<(), StamError> {
    let mut store = AnnotationStore::from_file(
        "tests/includetest.store.stam.json",
        Config::default().with_debug(true),
    )?;
    store.set_filename("/tmp/includetest.store.stam.json");
    store.save()?;
    let output = Command::new("diff")
        .arg("-w")
        .arg(format!(
            "{}/tests/includetest.store.stam.json",
            CARGO_MANIFEST_DIR
        ))
        .arg("/tmp/includetest.store.stam.json")
        .output()
        .expect("failed to run diff");
    assert!(output.status.success());
    Ok(())
}

#[test]
fn find_text() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world"),
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
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world"),
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
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world"),
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
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello, world"),
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
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world"),
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
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world"),
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
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world"),
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
    store.add_resource(TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    ))?;
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
    store.add_resource(TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    ))?;
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
    store.add_resource(TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    ))?;
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
    store.add_resource(TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    ))?;
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
    store.add_resource(TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    ))?;
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
    store.add_resource(TextResourceBuilder::new().with_id("testres").with_text(
        "I categorically deny any eavesdropping on you and hearing about your triskaidekaphobia.",
    ))?;
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
    store.set_filename("/tmp/test.store.stam.csv");
    store.save()?;

    //and parse again:
    eprintln!("\n\nTesting deserialisation of serialised result:");
    let store = AnnotationStore::from_file(
        "/tmp/test.store.stam.csv",
        Config::default().with_debug(true),
    )?;
    test_example_a_sanity(&store)?;
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
        .find_data("myset", "type", DataOperator::Equals("header".into()))
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
        .filter_key_value(&key_type, DataOperator::Equals("word".into()))
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
        .filter_key_value(&key_type, DataOperator::Equals("word".into()))
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
        .filter_key_value(&key_type, DataOperator::Equals("word".into()))
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
        .filter_key_value(&key_type, DataOperator::Equals("word".into()))
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
        .filter_key_value(&key_type, DataOperator::Equals("word".into()))
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
        .filter_key_value(&key_type, DataOperator::Equals("phrase".into()))
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
        .filter_key_value(&key_type, DataOperator::Equals("word".into()))
        .textual_order(); //we could omit this if we were sure word annotations were added in sequence

    let secondword = words.iter().nth(1).unwrap();

    assert_eq!(secondword.text_simple(), Some("human"));

    //now find the phrase this word belongs to:
    let mut count = 0;
    for phrase in secondword
        .related_text(TextSelectionOperator::embedded())
        .annotations()
        .filter_key_value(&key_type, DataOperator::Equals("phrase".into()))
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
            assert_eq!(*operator, DataOperator::Equals("value".into()));
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
            assert_eq!(*operator, DataOperator::Equals("value".into()));
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
fn query_parse_nonquoted_disjunction() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = value|value2|value3;";
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
            if let DataOperator::Or(v) = operator {
                assert_eq!(v.len(), 3);
                assert_eq!(v.get(0), Some(&DataOperator::Equals("value".into())));
                assert_eq!(v.get(1), Some(&DataOperator::Equals("value2".into())));
                assert_eq!(v.get(2), Some(&DataOperator::Equals("value3".into())));
            } else {
                assert!(false, "Expected OR constraint");
            }
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse_nonquoted_disjunction_numeric() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = 3|4|5;";
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
            if let DataOperator::Or(v) = operator {
                assert_eq!(v.len(), 3);
                assert_eq!(v.get(0), Some(&DataOperator::EqualsInt(3)));
                assert_eq!(v.get(1), Some(&DataOperator::EqualsInt(4)));
                assert_eq!(v.get(2), Some(&DataOperator::EqualsInt(5)));
            } else {
                assert!(false, "Expected OR constraint");
            }
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse_quoted_disjunction() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = \"value|value2|value3\";";
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
            if let DataOperator::Or(v) = operator {
                assert_eq!(v.len(), 3);
                assert_eq!(v.get(0), Some(&DataOperator::Equals("value".into())));
                assert_eq!(v.get(1), Some(&DataOperator::Equals("value2".into())));
                assert_eq!(v.get(2), Some(&DataOperator::Equals("value3".into())));
            } else {
                assert!(false, "Expected OR constraint");
            }
        } else {
            assert!(false, "Constraint not as expected");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_parse_attributes() -> Result<(), StamError> {
    let querystring =
        "@a @b SELECT ANNOTATION ?a WHERE @blah @blieh=bloeh DATA \"set\" \"key\" = \"value\";";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    assert_eq!(query.attributes().len(), 2);
    assert_eq!(query.attributes().nth(0), Some("@a").as_ref());
    assert_eq!(query.attributes().nth(1), Some("@b").as_ref());
    for (constraint, attributes) in query.constraints_with_attributes() {
        count += 1;
        assert_eq!(attributes.len(), 2);
        assert_eq!(attributes.get(0), Some("@blah").as_ref());
        assert_eq!(attributes.get(1), Some("@blieh=bloeh").as_ref());
        if let Constraint::KeyValue {
            set,
            key,
            operator,
            qualifier: _,
        } = constraint
        {
            assert_eq!(*set, "set");
            assert_eq!(*key, "key");
            assert_eq!(*operator, DataOperator::Equals("value".into()));
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
    let subquery = query.subqueries().next().expect("expected subquery");
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
fn query_parse_optional_subquery() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = \"value\"; { SELECT OPTIONAL ANNOTATION WHERE RELATION ?a SUCCEEDS; }";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    let subquery = query.subqueries().next().expect("expected subquery");
    assert_eq!(subquery.qualifier(), QueryQualifier::Optional);
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
fn query_parse_multiple_subqueries() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = \"value\"; { SELECT ANNOTATION WHERE RELATION ?a SUCCEEDS; | SELECT ANNOTATION WHERE RELATION ?a PRECEDES; }";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for subquery in query.subqueries() {
        count += 1;
        for constraint in subquery.iter() {
            if count == 1 {
                if let Constraint::TextRelation { var, operator } = constraint {
                    assert_eq!(*var, "a");
                    assert_eq!(*operator, TextSelectionOperator::succeeds());
                } else {
                    assert!(false, "Constraint not as expected");
                }
            } else if count == 2 {
                if let Constraint::TextRelation { var, operator } = constraint {
                    assert_eq!(*var, "a");
                    assert_eq!(*operator, TextSelectionOperator::precedes());
                } else {
                    assert!(false, "Constraint not as expected");
                }
            }
        }
    }
    assert_eq!(count, 2);
    Ok(())
}
#[test]

fn query_parse_names() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\" = \"value\"; { SELECT ANNOTATION ?b WHERE RELATION ?a SUCCEEDS; | SELECT ANNOTATION ?c WHERE RELATION ?a PRECEDES; }";
    let query: Query = querystring.try_into()?;
    let names = query.names();
    assert_eq!(names.get(0), Some("a").as_ref());
    assert_eq!(names.get(1), Some("b").as_ref());
    assert_eq!(names.get(2), Some("c").as_ref());
    Ok(())
}

#[test]
fn query_parse_union() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE [ DATA \"set\" \"key\" = \"value\" OR DATA \"set\" \"key\" = \"value\" ];";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for constraint in query.iter() {
        count += 1;
        if let Constraint::Union(subconstraints) = constraint {
            assert_eq!(subconstraints.len(), 2, "Subconstraint count");
            for subconstraint in subconstraints {
                if let Constraint::KeyValue {
                    set,
                    key,
                    operator,
                    qualifier: _,
                } = subconstraint
                {
                    assert_eq!(*set, "set");
                    assert_eq!(*key, "key");
                    assert_eq!(*operator, DataOperator::Equals("value".into()));
                } else {
                    assert!(false, "Subconstraint not as expected");
                }
            }
        }
    }
    assert_eq!(count, 1, "Number of constraints");
    Ok(())
}

#[test]
fn query_parse_union_unquoted() -> Result<(), StamError> {
    let querystring =
        "SELECT ANNOTATION ?a WHERE [ DATA set key = value OR DATA set key = value ];";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Select);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for constraint in query.iter() {
        count += 1;
        if let Constraint::Union(subconstraints) = constraint {
            assert_eq!(subconstraints.len(), 2, "Subconstraint count");
            for subconstraint in subconstraints {
                if let Constraint::KeyValue {
                    set,
                    key,
                    operator,
                    qualifier: _,
                } = subconstraint
                {
                    assert_eq!(*set, "set");
                    assert_eq!(*key, "key");
                    assert_eq!(*operator, DataOperator::Equals("value".into()));
                } else {
                    assert!(false, "Subconstraint not as expected");
                }
            }
        }
    }
    assert_eq!(count, 1, "Number of constraints");
    Ok(())
}

#[test]
fn query_parse_union2() -> Result<(), StamError> {
    let querystring = "SELECT ANNOTATION ?a WHERE DATA \"set\" \"key\"; [ DATA \"set\" \"key\" = \"value\" OR DATA \"set\" \"key\" = \"value\" ]; RESOURCE \"x\";";
    let query: Query = querystring.try_into()?;
    let mut count = 0;
    for _ in query.iter() {
        count += 1;
    }
    assert_eq!(count, 3, "Number of constraints");
    Ok(())
}

#[test]
fn query_parse_add() -> Result<(), StamError> {
    let querystring = "ADD ANNOTATION ?a WITH DATA \"set\" \"key\" \"value\"; TARGET ?x; { SELECT ANNOTATION ?x WHERE ID \"A1\"; }";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Add);
    assert_eq!(query.querytype().readonly(), false);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    let mut count = 0;
    for _assignment in query.assignments() {
        count += 1;
    }
    assert_eq!(count, 2, "Number of assignments");
    Ok(())
}

#[test]
fn query_parse_delete() -> Result<(), StamError> {
    let querystring = "DELETE ANNOTATION ?a { SELECT ANNOTATION ?a WHERE ID \"A1\"; }";
    let query: Query = querystring.try_into()?;
    assert_eq!(query.name(), Some("a"));
    assert_eq!(query.querytype(), QueryType::Delete);
    assert_eq!(query.querytype().readonly(), false);
    assert_eq!(query.resulttype(), Some(Type::Annotation));
    Ok(())
}

#[test]
fn query() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let query: Query = "SELECT ANNOTATION ?a WHERE DATA myset type = phrase;".try_into()?;
    let mut count = 0;
    let refdata = store
        .find_data("myset", "type", DataOperator::Equals("phrase".into()))
        .next()
        .expect("reference data must exist");
    for results in store.query(query)? {
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
        .find_data("myset", "type", DataOperator::Equals("phrase".into()))
        .next()
        .expect("reference data must exist");
    let iter = store.query(query)?;
    for results in iter {
        if let Ok(result) = results.get_by_name("a") {
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
fn query_limit_first() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let query: Query = "SELECT ANNOTATION ?a WHERE LIMIT 1;".try_into()?;
    let mut count = 0;
    let iter = store.query(query)?;
    for results in iter {
        if let Ok(result) = results.get_by_name("a") {
            match result {
                QueryResultItem::Annotation(annotation) => {
                    count += 1;
                    assert_eq!(annotation.id(), Some("Sentence1"));
                }
                _ => assert!(false, "wrong return type"),
            }
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_limit_last() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let query: Query = "SELECT ANNOTATION ?a WHERE LIMIT -1;".try_into()?;
    let mut count = 0;
    let iter = store.query(query)?;
    for results in iter {
        if let Ok(result) = results.get_by_name("a") {
            match result {
                QueryResultItem::Annotation(annotation) => {
                    count += 1;
                    assert_eq!(annotation.id(), Some("Phrase1"));
                }
                _ => assert!(false, "wrong return type"),
            }
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_limit_range() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let query: Query = "SELECT ANNOTATION ?a WHERE LIMIT 0 1;".try_into()?;
    let mut count = 0;
    let iter = store.query(query)?;
    for results in iter {
        if let Ok(result) = results.get_by_name("a") {
            match result {
                QueryResultItem::Annotation(annotation) => {
                    count += 1;
                    assert_eq!(annotation.id(), Some("Sentence1"));
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
        .find_data("myset", "type", DataOperator::Equals("phrase".into()))
        .next()
        .expect("reference data must exist");
    let refdata2 = store
        .find_data("myset", "type", DataOperator::Equals("sentence".into()))
        .next()
        .expect("reference data must exist");
    let queryresults = store.query(query)?;
    for results in queryresults {
        count += 1;
        if let QueryResultItem::Annotation(annotation) = results.get_by_name("phrase")? {
            assert!(annotation.has_data(&refdata));
        } else {
            assert!(false, "did not get phrase");
        }
        if let QueryResultItem::Annotation(annotation) = results.get_by_name("sentence")? {
            assert!(annotation.has_data(&refdata2));
        } else {
            assert!(false, "did not get sentence");
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_multiple_subqueries() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(21, 25), //"born"
            ))
            .with_data("myset", "type", "word"),
    )?;
    let query: Query = "SELECT ANNOTATION ?sentence WHERE DATA myset type = sentence; { SELECT ANNOTATION ?phrase WHERE RELATION ?sentence EMBEDS; DATA myset type = phrase; | SELECT ANNOTATION ?word WHERE RELATION ?sentence EMBEDS; DATA myset type = word;}".try_into()?;
    let mut count = 0;
    let refdata = store
        .find_data("myset", "type", DataOperator::Equals("phrase".into()))
        .next()
        .expect("reference data must exist");
    let refdata2 = store
        .find_data("myset", "type", DataOperator::Equals("sentence".into()))
        .next()
        .expect("reference data must exist");
    let refdata3 = store
        .find_data("myset", "type", DataOperator::Equals("word".into()))
        .next()
        .expect("reference data must exist");
    let queryresults = store.query(query)?;
    for results in queryresults {
        count += 1;
        if let QueryResultItem::Annotation(annotation) = results.get_by_name("sentence")? {
            assert!(annotation.has_data(&refdata2));
        }
        if count == 1 {
            if let QueryResultItem::Annotation(annotation) = results.get_by_name("phrase")? {
                assert!(annotation.has_data(&refdata));
            }
        } else if count == 2 {
            if let QueryResultItem::Annotation(annotation) = results.get_by_name("word")? {
                assert!(annotation.has_data(&refdata3));
            }
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn query_multiple_optional_subqueries() -> Result<(), StamError> {
    let mut store = setup_example_6()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(21, 25), //"born"
            ))
            .with_data("myset", "type", "word"),
    )?;
    let query: Query = "SELECT ANNOTATION ?sentence WHERE DATA myset type = sentence; { SELECT OPTIONAL ANNOTATION ?phrase WHERE RELATION ?sentence EMBEDS; DATA myset type = phrase; | SELECT OPTIONAL ANNOTATION ?word WHERE RELATION ?sentence EMBEDS; DATA myset type = word; | SELECT OPTIONAL ANNOTATION ?nonexistant WHERE RELATION ?sentence EMBEDS; DATA myset type = nonexistant; }".try_into()?;
    let mut count = 0;
    let refdata = store
        .find_data("myset", "type", DataOperator::Equals("phrase".into()))
        .next()
        .expect("reference data must exist");
    let refdata2 = store
        .find_data("myset", "type", DataOperator::Equals("sentence".into()))
        .next()
        .expect("reference data must exist");
    let refdata3 = store
        .find_data("myset", "type", DataOperator::Equals("word".into()))
        .next()
        .expect("reference data must exist");
    let queryresults = store.query(query)?;
    for results in queryresults {
        count += 1;
        if let QueryResultItem::Annotation(annotation) = results.get_by_name("sentence")? {
            assert!(annotation.has_data(&refdata2));
        }
        if count == 1 {
            if let QueryResultItem::Annotation(annotation) = results.get_by_name("phrase")? {
                assert!(annotation.has_data(&refdata));
            }
        } else if count == 2 {
            if let QueryResultItem::Annotation(annotation) = results.get_by_name("word")? {
                assert!(annotation.has_data(&refdata3));
            }
        } else if count == 3 {
            assert!(results.get_by_name("word").is_err());
            assert!(results.get_by_name("phrase").is_err());
        }
    }
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn query_subquery_deep() -> Result<(), StamError> {
    let store = setup_example_9()?;
    let querystring = r#" 
    SELECT ANNOTATION ?det WHERE
        DATA testdataset pos = det;
    {
        SELECT ANNOTATION ?adj WHERE
            RELATION ?det PRECEDES;
            DATA testdataset pos = adj;
        {
            SELECT ANNOTATION ?n WHERE
                RELATION ?adj PRECEDES;
                DATA testdataset pos = n;
        }
    }
    "#;
    let query: Query = querystring.try_into()?;
    let iter = store.query(query)?;
    let mut count = 0;
    for results in iter {
        count += 1;
        assert!(
            results.get_by_name("det").is_ok(),
            "checking det in round {} of 2",
            count
        );
        assert!(
            results.get_by_name("adj").is_ok(),
            "checking adj in round {} of 2",
            count
        );
        assert!(
            results.get_by_name("n").is_ok(),
            "checking n in round {} of 2",
            count
        );
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn query_subquery_optional() -> Result<(), StamError> {
    let store = setup_example_9()?;
    let querystring = r#" 
    SELECT ANNOTATION ?n WHERE
        DATA testdataset pos = n;
    {
        SELECT OPTIONAL ANNOTATION ?v WHERE
            RELATION ?n PRECEDES;
            DATA testdataset pos = v;
    }
    "#;
    let query: Query = querystring.try_into()?;
    let iter = store.query(query)?;
    let mut count = 0;
    for results in iter {
        count += 1;
        if count == 1 {
            assert!(
                results.get_by_name("n").is_ok(),
                "checking n in round {} of 2",
                count
            );
            assert!(
                results.get_by_name("v").is_ok(),
                "checking v in round {} of 2",
                count
            );
        } else if count == 2 {
            assert!(
                results.get_by_name("n").is_ok(),
                "checking n in round {} of 2",
                count
            );
            assert!(
                results.get_by_name("v").is_err(),
                "checking no v in round {} of 2",
                count
            );
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn query_subquery_optional_nonexistant() -> Result<(), StamError> {
    let store = setup_example_9()?;
    let querystring = r#" 
    SELECT ANNOTATION ?n WHERE
        DATA testdataset pos = n;
    {
        SELECT OPTIONAL ANNOTATION ?v WHERE
            RELATION ?n PRECEDES;
            DATA testdataset pos = nonexistant;
    }
    "#;
    let query: Query = querystring.try_into()?;
    let iter = store.query(query)?;
    let mut count = 0;
    for results in iter {
        count += 1;
        assert!(
            results.get_by_name("n").is_ok(),
            "checking n in round {} of 2",
            count
        );
        assert!(
            results.get_by_name("v").is_err(),
            "checking no v in round {} of 2",
            count
        );
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_union() -> Result<(), StamError> {
    let store = setup_example_6()?;
    //this example could have been just a DataOperator::And but we want to test the UNION construction:
    let query: Query =
        "SELECT ANNOTATION ?a WHERE [ DATA myset type = phrase OR DATA myset type = sentence ];"
            .try_into()?;
    let mut count = 0;
    let refdata = store
        .find_data("myset", "type", DataOperator::Equals("phrase".into()))
        .next()
        .expect("reference data must exist");
    let refdata2 = store
        .find_data("myset", "type", DataOperator::Equals("sentence".into()))
        .next()
        .expect("reference data must exist");
    let iter = store.query(query)?;
    for results in iter {
        if let Ok(result) = results.get_by_name("a") {
            match result {
                QueryResultItem::Annotation(annotation) => {
                    count += 1;
                    assert!(annotation.has_data(&refdata) || annotation.has_data(&refdata2));
                }
                _ => assert!(false, "wrong return type"),
            }
        }
    }
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn query_add_annotation_textselector() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    let query: Query =
        "ADD ANNOTATION ?a WITH DATA \"testdataset\" \"type\" \"phrase\"; TARGET ?target; 
            { SELECT TEXT ?target WHERE RESOURCE \"testres\" OFFSET 0 11; }"
            .try_into()?;
    let iter = store.query_mut(query)?;
    let mut count = 0;
    for results in iter {
        if let Ok(result) = results.get_by_name("a") {
            match result {
                QueryResultItem::Annotation(_annotation) => {
                    count += 1;
                }
                _ => assert!(false, "wrong return type"),
            }
        }
    }
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn query_delete_annotation() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    let query: Query =
        "DELETE ANNOTATION ?a { SELECT ANNOTATION ?a WHERE ID \"A1\"; }".try_into()?;
    store.query_mut(query)?;
    assert_eq!(store.annotation("A1"), None);
    Ok(())
}

#[test]
fn query_map() -> Result<(), StamError> {
    let store = setup_example_10()?;
    let query: Query =
        "SELECT ANNOTATION ?a WHERE DATA \"testdataset\" \"person\" . \"lastname\" = \"Einstein\";"
            .try_into()?;
    let iter = store.query(query)?;
    let mut count = 0;
    for results in iter {
        if let Ok(result) = results.get_by_name("a") {
            match result {
                QueryResultItem::Annotation(annotation) => {
                    count += 1;
                    assert_eq!(annotation.id(), Some("einstein"));
                }
                _ => assert!(false, "wrong return type"),
            }
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
    let annotation = store.annotation("ComplexTransposition1").or_fail()?;
    assert_eq!(
        annotation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2
    );
    Ok(())
}

#[test]
#[cfg(feature = "transpose")]
fn transpose_over_simple_transposition() -> Result<(), StamError> {
    let mut store = setup_example_8()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "species", "homo sapiens")
            .with_target(SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(4, 9),
            )),
    )?;
    let transposition = store.annotation("SimpleTransposition1").or_fail()?;
    let source = store.annotation("A3").or_fail()?;
    assert_eq!(
        source.text_simple(),
        Some("human"),
        "sanity check for source annotation"
    );
    let config = TransposeConfig {
        transposition_id: Some("NewTransposition".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        ..Default::default()
    };
    let targets =
        store.annotate_from_iter(source.transpose(&transposition, config)?.into_iter())?;
    assert_eq!(targets.len(), 2);
    let new_transposition = store.annotation("NewTransposition").or_fail()?;
    assert_eq!(
        new_transposition
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "new transposition must have two target annotations (source annotation and transposed annotation)"
    );
    let source = store.annotation("A3").or_fail()?; //reobtain (otherwise borrow checker complains after mutation)
    let transposed = store.annotation("A3t").or_fail()?;
    assert_eq!(
        transposed.text_simple(),
        source.text_simple(),
        "transposed annotation must reference same text as source"
    );
    let tsel = transposed.textselections().next().unwrap();
    assert_eq!(
        tsel.resource().id(),
        Some("warhol"),
        "transposed annotation must reference the target resource"
    );
    assert_eq!(tsel.begin(), 0, "transposed offset check");
    assert_eq!(tsel.end(), 5, "transposed offset check");
    Ok(())
}

#[test]
#[cfg(feature = "transpose")]
fn transpose_over_simple_transposition_invalid() -> Result<(), StamError> {
    let mut store = setup_example_8()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "species", "homo sapiens")
            .with_target(SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(0, 9), //"all human"
            )),
    )?;
    let transposition = store.annotation("SimpleTransposition1").or_fail()?;
    let source = store.annotation("A3").or_fail()?;
    assert_eq!(
        source.text_simple(),
        Some("all human"),
        "sanity check for source annotation"
    );
    let config = TransposeConfig {
        transposition_id: Some("NewTransposition".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        ..Default::default()
    };
    assert!(source.transpose(&transposition, config).is_err());
    Ok(())
}

#[test]
#[cfg(feature = "transpose")]
fn transpose_over_complex_transposition() -> Result<(), StamError> {
    let mut store = setup_example_8b()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "species", "homo sapiens")
            .with_target(SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(4, 9),
            )),
    )?;
    let transposition = store.annotation("ComplexTransposition1").or_fail()?;
    let source = store.annotation("A3").or_fail()?;
    assert_eq!(
        source.text_simple(),
        Some("human"),
        "sanity check for source annotation"
    );
    let config = TransposeConfig {
        transposition_id: Some("NewTransposition".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        ..Default::default()
    };
    let targets =
        store.annotate_from_iter(source.transpose(&transposition, config)?.into_iter())?;
    assert_eq!(targets.len(), 2);
    let new_transposition = store.annotation("NewTransposition").or_fail()?;
    assert_eq!(
        new_transposition
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "new transposition must have two target annotations (source annotation and transposed annotation)"
    );
    let source = store.annotation("A3").or_fail()?; //reobtain (otherwise borrow checker complains after mutation)
    let transposed = store.annotation("A3t").or_fail()?;
    assert_eq!(
        transposed.text_simple(),
        source.text_simple(),
        "transposed annotation must reference same text as source"
    );
    let tsel = transposed.textselections().next().unwrap();
    assert_eq!(
        tsel.resource().id(),
        Some("warhol"),
        "transposed annotation must reference the target resource"
    );
    assert_eq!(tsel.begin(), 0, "transposed offset check");
    assert_eq!(tsel.end(), 5, "transposed offset check");
    Ok(())
}

#[test]
#[cfg(feature = "transpose")]
fn transpose_over_complex_transposition_with_resegmentation() -> Result<(), StamError> {
    let mut store = setup_example_8c()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "species", "homo sapiens")
            .with_target(SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(4, 9),
            )),
    )?;
    let transposition = store.annotation("ComplexTransposition1").or_fail()?;
    let source = store.annotation("A3").or_fail()?;
    assert_eq!(
        source.text_join(""),
        "human",
        "sanity check for source annotation"
    );
    let config = TransposeConfig {
        transposition_id: Some("NewTransposition".to_string()),
        resegmentation_id: Some("NewResegmentation".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        ..Default::default()
    };
    let targets =
        store.annotate_from_iter(source.transpose(&transposition, config)?.into_iter())?;
    assert_eq!(targets.len(), 4, "Test the number of generated annotations");
    let new_transposition = store.annotation("NewTransposition").or_fail()?;
    assert_eq!(
        new_transposition
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "new transposition must have two target annotations (source annotation and transposed annotation)"
    );
    assert!(
        new_transposition
            .annotations_in_targets(AnnotationDepth::One)
            .all(|a| a.id() != Some("A3")),
        "the source annotation may NOT be a direct part of the transposition in this case"
    );
    let transposed = store.annotation("A3t").or_fail()?;
    let tsels: ResultTextSelectionSet = transposed.textselections().collect();
    assert_eq!(
        tsels.resource().id(),
        Some("warhol"),
        "transposed annotation must reference the target resource"
    );
    assert_eq!(
        tsels.len(),
        2,
        "transposed annotation must reference two text selections"
    );
    let mut iter = tsels.iter();
    let tsel1 = iter.next().unwrap();
    let tsel2 = iter.next().unwrap();
    assert_eq!(tsel1.begin(), 1, "transposed offset check (1)");
    assert_eq!(tsel1.end(), 3, "transposed offset check (1)");
    assert_eq!(tsel2.begin(), 5, "transposed offset check (2)");
    assert_eq!(tsel2.end(), 8, "transposed offset check (2)");
    let resegmentation = store.annotation("NewResegmentation").or_fail()?;
    assert_eq!(
        resegmentation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "resegmentation must have two target annotations (source annotation and resegmented annotation)"
    );
    Ok(())
}

#[test]
#[cfg(feature = "transpose")]
fn transpose_over_complex_transposition_invalid() -> Result<(), StamError> {
    let mut store = setup_example_8b()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "species", "homo sapiens")
            .with_target(SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(0, 9), //"all human"
            )),
    )?;
    let transposition = store.annotation("ComplexTransposition1").or_fail()?;
    let source = store.annotation("A3").or_fail()?;
    assert_eq!(
        source.text_simple(),
        Some("all human"),
        "sanity check for source annotation"
    );
    let config = TransposeConfig {
        transposition_id: Some("NewTransposition".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        ..Default::default()
    };
    assert!(source.transpose(&transposition, config).is_err());
    Ok(())
}

#[test]
fn segmentation() -> Result<(), StamError> {
    let store = setup_example_6()?;
    let segmentation: Vec<_> = store
        .resource("humanrights")
        .or_fail()?
        .segmentation()
        .collect();
    assert_eq!(segmentation[0].begin(), 0);
    assert_eq!(segmentation[0].end(), 17);
    assert_eq!(segmentation[0].text(), "All human beings ");
    assert_eq!(segmentation[1].begin(), 17);
    assert_eq!(segmentation[1].end(), 40);
    assert_eq!(segmentation[1].text(), "are born free and equal");
    assert_eq!(segmentation[2].begin(), 40);
    assert_eq!(segmentation[2].end(), 63);
    assert_eq!(segmentation[2].text(), " in dignity and rights.");
    Ok(())
}

#[test]
#[cfg(feature = "translate")]
fn translation_simple() -> Result<(), StamError> {
    let store = setup_example_11()?;
    let annotation = store.annotation("SimpleTranslation1").or_fail()?;
    assert_eq!(annotation.text().count(), 2);
    let mut iter = annotation.text();
    assert_eq!(iter.next(), Some("благодарю"));
    assert_eq!(iter.next(), Some("blagodaryu"));
    Ok(())
}

#[test]
#[cfg(feature = "translate")]
fn translation_complex() -> Result<(), StamError> {
    let store = setup_example_11b()?;
    let phrase1 = store.annotation("A1").or_fail()?;
    assert_eq!(phrase1.text_join(""), "Я благодарю вас", "phrase 1 text");
    let phrase2 = store.annotation("A2").or_fail()?;
    assert_eq!(phrase2.text_join(""), "Ya blagodaryu vas", "phrase 2 text");
    let annotation = store.annotation("ComplexTranslation1").or_fail()?;
    assert_eq!(
        annotation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2
    );
    Ok(())
}

#[test]
#[cfg(feature = "translate")]
fn translate_over_complex_translation_with_resegmentation() -> Result<(), StamError> {
    let mut store = setup_example_11b()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "selection", "phrase")
            .with_target(SelectorBuilder::textselector(
                "source",
                Offset::simple(2, 15), //благодарю вас
            )),
    )?;
    let translation = store.annotation("ComplexTranslation1").or_fail()?;
    let source = store.annotation("A3").or_fail()?;
    assert_eq!(
        source.text_join(""),
        "благодарю вас",
        "sanity check for source annotation"
    );
    let config = TranslateConfig {
        translation_id: Some("NewTranslation".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        resegmentation_id: Some("NewResegmentation".to_string()),
        debug: true,
        ..Default::default()
    };
    let targets = store.annotate_from_iter(source.translate(&translation, config)?.into_iter())?;
    assert_eq!(targets.len(), 4, "Expecting four annotations");
    let new_translation = store.annotation("NewTranslation").or_fail()?;
    assert_eq!(
        new_translation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "new transposition must have two target annotations (source annotation and transposed annotation)"
    );
    let translated = store.annotation("A3t").or_fail()?;
    assert_eq!(
        translated.text_join(""),
        "blagodaryu vas",
        "checking translated text"
    );
    assert_eq!(
        translated.text().count(),
        3,
        "number of text selectors for translated text"
    );
    let tsel = translated.textselections().next().unwrap();
    assert_eq!(
        tsel.resource().id(),
        Some("translit"),
        "translated annotation must reference the target resource"
    );
    let resegmentation = store.annotation("NewResegmentation").or_fail()?;
    assert_eq!(
        resegmentation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "resegmentation must have two target annotations (source annotation and resegmented annotation)"
    );
    Ok(())
}

#[test]
#[cfg(feature = "translate")]
fn translate_over_complex_translation_no_resegmentation() -> Result<(), StamError> {
    let mut store = setup_example_11b()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "selection", "phrase")
            .with_target(SelectorBuilder::textselector(
                "source",
                Offset::simple(2, 15), //благодарю вас
            )),
    )?;
    let translation = store
        .annotation("ComplexTranslation1")
        .expect("ComplexTranslation1");
    let source = store.annotation("A3").expect("A3");
    assert_eq!(
        source.text_join(""),
        "благодарю вас",
        "sanity check for source annotation"
    );
    let config = TranslateConfig {
        translation_id: Some("NewTranslation".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        no_resegmentation: true,
        debug: true,
        ..Default::default()
    };
    let targets = store.annotate_from_iter(source.translate(&translation, config)?.into_iter())?;
    assert_eq!(
        targets.len(),
        2,
        "Expecting two annotations (the translation and the target annotation)"
    );
    let new_translation = store
        .annotation("NewTranslation")
        .expect("NewTranslation Annotation");
    assert_eq!(
        new_translation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "new transposition must have two target annotations (source annotation and transposed annotation)"
    );
    let translated = store.annotation("A3t").expect("Target annotation A3t");
    assert_eq!(
        translated.text_join(""),
        "blagodaryu vas",
        "checking translated text"
    );
    assert_eq!(
        translated.text().count(),
        1,
        "number of text selectors for translated text"
    );
    let tsel = translated.textselections().next().unwrap();
    assert_eq!(
        tsel.resource().id(),
        Some("translit"),
        "translated annotation must reference the target resource"
    );
    Ok(())
}

#[test]
#[cfg(feature = "translate")]
fn translate_over_complex_translation_with_deletion() -> Result<(), StamError> {
    let mut store = setup_example_11c()?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("A3")
            .with_data("mydataset", "selection", "phrase")
            .with_target(SelectorBuilder::textselector(
                "physical",
                Offset::simple(7, 18), //inter-rupt
            )),
    )?;
    let translation = store
        .annotation("ComplexTranslation1")
        .expect("ComplexTranslation1");
    let source = store.annotation("A3").expect("A3");
    assert_eq!(
        source.text_join(""),
        "inter-\nrupt",
        "sanity check for source annotation"
    );
    let config = TranslateConfig {
        translation_id: Some("NewTranslation".to_string()),
        target_side_ids: vec!["A3t".to_string()],
        no_resegmentation: true,
        debug: true,
        ..Default::default()
    };
    let targets = store.annotate_from_iter(source.translate(&translation, config)?.into_iter())?;
    assert_eq!(
        targets.len(),
        2,
        "Expecting two annotations (the translation and the target annotation)"
    );
    let new_translation = store
        .annotation("NewTranslation")
        .expect("NewTranslation Annotation");
    assert_eq!(
        new_translation
            .annotations_in_targets(AnnotationDepth::One)
            .count(),
        2,
        "new transposition must have two target annotations (source annotation and transposed annotation)"
    );
    let translated = store.annotation("A3t").expect("Target annotation A3t");
    assert_eq!(
        translated.text_join(""),
        "interrupt",
        "checking translated text"
    );
    assert_eq!(
        translated.text().count(),
        1,
        "number of text selectors for translated text"
    );
    let tsel = translated.textselections().next().unwrap();
    assert_eq!(
        tsel.resource().id(),
        Some("logical"),
        "translated annotation must reference the target resource"
    );
    Ok(())
}

#[test]
fn remove_annotation() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.remove_annotation("A1")?;
    assert_eq!(store.annotation("A1"), None);
    Ok(())
}

#[test]
fn remove_resource() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.remove_resource("testres")?;
    assert_eq!(store.resource("testres"), None);
    assert_eq!(store.annotation("A1"), None); //annotation should be gone too
    Ok(())
}

#[test]
fn remove_dataset() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.remove_dataset("testdataset")?;
    assert_eq!(store.dataset("testdataset"), None);
    assert_eq!(store.annotation("A1"), None); //annotation should be gone too
    Ok(())
}

#[test]
fn remove_data_strict() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.remove_data("testdataset", "D1", true)?;
    let set = store.dataset("testdataset").or_fail()?;
    assert_eq!(set.annotationdata("D1"), None);
    assert_eq!(store.annotation("A1"), None); //annotation should be gone too
    Ok(())
}

#[test]
fn remove_data_nonstrict() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.remove_data("testdataset", "D1", true)?;
    let set = store.dataset("testdataset").or_fail()?;
    assert_eq!(set.annotationdata("D1"), None);
    assert_eq!(store.annotation("A1"), None); //annotation should be gone too, despite strict mode, since there was only one data item associated
    Ok(())
}

#[test]
fn remove_key_strict() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.remove_key("testdataset", "pos", true)?;
    let set = store.dataset("testdataset").or_fail()?;
    assert_eq!(set.key("pos"), None);
    assert_eq!(store.annotation("A1"), None); //annotation should be gone too
    Ok(())
}

#[test]
#[cfg(feature = "textvalidation")]
fn textvalidation_none() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let annotation = store.annotation("A1").or_fail()?;
    assert_eq!(annotation.validate_text(), None);
    Ok(())
}

#[test]
#[cfg(feature = "textvalidation")]
fn textvalidation_checksum() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.protect_text(TextValidationMode::Checksum)?;
    let annotation = store.annotation("A1").or_fail()?;
    assert_eq!(
        annotation.validation_checksum(),
        Some("7c211433f02071597741e6ff5a8ea34789abbf43")
    );
    assert_eq!(annotation.validate_text(), Some(true));
    assert!(store.validate_text(true).is_ok());
    Ok(())
}

#[test]
#[cfg(feature = "textvalidation")]
fn textvalidation_texts() -> Result<(), StamError> {
    let mut store = setup_example_1()?;
    store.protect_text(TextValidationMode::Text)?;
    let annotation = store.annotation("A1").or_fail()?;
    assert_eq!(annotation.validation_text(), Some("world"));
    assert_eq!(annotation.validate_text(), Some(true));
    assert!(store.validate_text(true).is_ok());
    Ok(())
}

#[cfg(not(target_os = "windows"))]
#[test]
fn add_substore_absolute() -> Result<(), StamError> {
    let mut store = AnnotationStore::new(Config::default().with_debug(true))
        .with_id("superstore")
        .with_filename("/tmp/superstore.stam.json");

    //add substore with an absolute path
    store.add_substore(&format!(
        "{}/tests/test.store.stam.json",
        CARGO_MANIFEST_DIR
    ))?;

    // Create a new dataset and insert it into the main store
    let dataset = AnnotationDataSet::new(Config::default())
        .with_id("metadataset")
        .with_filename("/tmp/metadata.dataset.stam.json");
    let dataset_handle = store.insert(dataset)?;

    store.annotate(
        AnnotationBuilder::new()
            .with_data(dataset_handle, "author", "proycon")
            .with_target(SelectorBuilder::ResourceSelector("hello.txt".into())),
    )?;

    store.save()?;

    Ok(())
}

#[test]
fn annotation_map() -> Result<(), StamError> {
    let store = setup_example_10()?;
    let annotation = store.annotation("einstein").unwrap();
    assert_eq!(annotation.text_simple(), Some("Albert Einstein"));
    for data in annotation.data() {
        if let DataValue::Map(v) = data.value() {
            assert_eq!(v.get("firstname").unwrap(), "Albert");
            assert_eq!(v.get("lastname").unwrap(), "Einstein");
        }
        assert_eq!(data.value().to_json_compact().unwrap(), "{\"@type\":\"Map\",\"value\":{\"firstname\":{\"@type\":\"String\",\"value\":\"Albert\"},\"lastname\":{\"@type\":\"String\",\"value\":\"Einstein\"}}}");
    }
    let data = annotation
        .data()
        .filter_value(DataOperator::GetKey(
            std::borrow::Cow::Borrowed("firstname"),
            Some(DataOperator::Equals("Albert".into()).into()),
        ))
        .next()
        .unwrap();
    if let DataValue::Map(v) = data.value() {
        assert_eq!(v.get("firstname").unwrap(), "Albert");
        assert_eq!(v.get("lastname").unwrap(), "Einstein");
    }
    //test without value test (just tests subkey presence)
    let data = annotation
        .data()
        .filter_value(DataOperator::GetKey(
            std::borrow::Cow::Borrowed("firstname"),
            None,
        ))
        .next()
        .unwrap();
    if let DataValue::Map(v) = data.value() {
        assert_eq!(v.get("firstname").unwrap(), "Albert");
        assert_eq!(v.get("lastname").unwrap(), "Einstein");
    }
    Ok(())
}
