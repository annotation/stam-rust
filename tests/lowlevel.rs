use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::ops::Deref;

mod common;
use crate::common::*;

use stam::*;

#[test]
fn store_instantiation_naive() -> Result<(), StamError> {
    let mut store = AnnotationStore::default().with_id("test");

    let _res_intid = store.insert(TextResource::from_string(
        "testres",
        "Hello world",
        Config::default(),
    ));

    let mut dataset = AnnotationDataSet::new(Config::default()).with_id("testdataset");
    dataset.insert(DataKey::new("pos"))?;
    store.insert(dataset)?;

    Ok(())
}

#[test]
fn store_sanity_check() -> Result<(), StamError> {
    // Instantiate the store
    let mut store = AnnotationStore::default().with_id("test");

    // Insert a text resource into the store
    let _res_handle = store.insert(TextResource::from_string(
        "testres",
        "Hello world",
        Config::default(),
    ));

    // Create a dataset with one key and insert it into the store
    let mut dataset = AnnotationDataSet::new(Config::default()).with_id("testdataset");
    dataset.insert(DataKey::new("pos"))?; //returns a DataKeyHandle, not further used in this test
    let set_handle = store.insert(dataset)?;

    //get by handle (internal id)
    let dataset: &AnnotationDataSet = store.get(set_handle)?;
    assert_eq!(dataset.id(), Some("testdataset"));

    //get by directly by id
    let resource: &TextResource = store.get("testres")?;
    assert_eq!(resource.id(), Some("testres"));
    assert_eq!(resource.textlen(), 11);
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
fn store_get() -> Result<(), StamError> {
    let store = setup_example_1()?;

    //test by public ID
    let _resource: &TextResource = store.get("testres")?;
    let dataset: &AnnotationDataSet = store.get("testdataset")?;
    let _datakey: &DataKey = dataset.get("pos")?;
    let _annotationdata: &AnnotationData = dataset.get("D1")?;
    let _annotation: &Annotation = store.get("A1")?;
    Ok(())
}

#[test]
fn store_get_2() -> Result<(), StamError> {
    let store = setup_example_2()?;

    //test by public ID
    let _resource: &TextResource = store.get("testres")?;
    let dataset: &AnnotationDataSet = store.get("testdataset")?;
    let _datakey: &DataKey = dataset.get("pos")?;
    let _annotationdata: &AnnotationData = dataset.get("D1")?;
    let _annotation: &Annotation = store.get("A1")?;
    Ok(())
}

#[test]
fn store_temp_id() -> Result<(), StamError> {
    let store = setup_example_1()?;

    let annotation: &Annotation = store.get("A1")?;
    assert_eq!(annotation.temp_id().unwrap(), "!A0");

    let dataset: &AnnotationDataSet = store.get("testdataset")?;
    assert_eq!(dataset.temp_id().unwrap(), "!S0");
    let key: &DataKey = dataset.get("pos")?;
    assert_eq!(key.temp_id().unwrap(), "!K0");

    let annotation2 = store
        .annotation("!A0")
        .expect("resolving via temporary ID should work too");
    assert_eq!(annotation2.id().unwrap(), "A1");

    let key2 = dataset
        .key("!K0")
        .expect("resolving via temporary ID should work too");
    assert_eq!(key2.id().unwrap(), "pos");

    Ok(())
}

#[test]
fn annotation_data() -> Result<(), StamError> {
    let store = setup_example_1()?;
    let annotation = store.annotation("A1").unwrap();

    let mut count = 0;
    for (sethandle, datahandle) in annotation.as_ref().data() {
        //there should be only one so we can safely test in the loop body
        let set: &AnnotationDataSet = store.get(*sethandle)?;
        let data: &AnnotationData = set.get(*datahandle)?;
        let key: &DataKey = set.get(data.key())?;
        count += 1;
        assert_eq!(key.id(), Some("pos"));
        assert_eq!(key.as_str(), "pos"); //shortcut for the same as above
        assert_eq!(data.value(), &DataValue::String("noun".to_string()));
        assert_eq!(data.value(), "noun"); //shortcut for the same as above (and more efficient without heap allocated string)
        assert_eq!(data.id(), Some("D1"));
        assert_eq!(set.id(), Some("testdataset"));
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
fn text_by_offset() -> Result<(), StamError> {
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
    assert_eq!(data.dataset(), &BuildItem::from("testdataset"));
    assert_eq!(data.dataset(), "testdataset");

    let mut store = setup_example_2().unwrap();
    let dataset: &mut AnnotationDataSet = store.get_mut("testdataset").unwrap();
    //we already had this annotation, check prior to insert
    let datahandle1: AnnotationDataHandle = dataset.annotationdata("D1").unwrap().handle().unwrap();
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
fn as_resultitem() -> Result<(), StamError> {
    let store = setup_example_2()?;

    let annotation: &Annotation = store.get("A1")?;
    let wrappedannotation = annotation.as_resultitem(&store, &store);

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
fn resource_textselection_existing() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let resource: &TextResource = store.get("testres")?;
    let handle = resource.known_textselection(&Offset::simple(6, 11))?;
    assert!(
        handle.is_some(),
        "testing whether TextSelection has a handle"
    );

    Ok(())
}

#[test]
fn textselections_by_resource_unsorted() -> Result<(), StamError> {
    let store = setup_example_4()?;
    let resource: &TextResource = store.get("testres")?;
    let v: Vec<_> = resource.textselections_unsorted().collect(); //lower-level method
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
fn data_by_value() -> Result<(), StamError> {
    let store = setup_example_2()?;
    let annotationset: &AnnotationDataSet = store.get("testdataset")?;
    let annotationdata: Option<&AnnotationData> =
        annotationset.data_by_value("pos", &"noun".into());
    assert!(annotationdata.is_some());
    assert_eq!(annotationdata.unwrap().id(), Some("D1"));
    Ok(())
}

#[test]
fn test_multiselector2_creation_and_sanity() -> Result<(), StamError> {
    let mut store = setup_example_multiselector_2()?;

    //annotate existing data (just to test sanity)
    store.annotate(
        AnnotationBuilder::new()
            .with_target(SelectorBuilder::textselector(
                "testres",
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

    let v3: Vec<_> = resource.textselections_unsorted().collect();
    assert_eq!(v3.len(), 2);

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
                    .textselections_unsorted()
                    .map(|textselection| textselection)
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
        .datasets()
        .map(|annotationset| {
            (
                annotationset.as_ref(),
                annotationset.as_ref().keys().map(|key| key).collect(),
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
