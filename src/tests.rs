#[cfg(test)]
use crate::*;

#[test]
fn parse_json_datakey() {
    let json = r#"{ 
        "@type": "DataKey",
        "@id": "pos"
    }"#;

    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    let key: DataKey = serde_json::from_value(v).unwrap();

    assert_eq!(key.id().unwrap(), "pos");
}

#[test]
fn parse_json_annotationdatabuilder() -> Result<(), std::io::Error> {
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

    assert_eq!(data.id, BuildItem::from("D2"));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, BuildItem::from("pos"));
    assert_eq!(data.key, "pos");
    assert_eq!(data.value, DataValue::from("verb"));
    assert_eq!(data.value, "verb"); //shorter version
    Ok(())
}

#[test]
fn parse_json_annotationdatabuilder_int() -> Result<(), std::io::Error> {
    let json = r#"{ 
        "@type": "AnnotationData",
        "@id": "D2",
        "key": "num",
        "value": {
            "@type": "Int",
            "value": 42
        }
    }"#;

    let data: AnnotationDataBuilder = serde_json::from_str(json)?;

    assert_eq!(data.id, BuildItem::from("D2"));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, BuildItem::from("num"));
    assert_eq!(data.key, "num");
    assert_eq!(data.value, DataValue::from(42));
    assert_eq!(data.value, 42); //shorter version
    Ok(())
}

#[test]
fn parse_json_annotationdatabuilder_float() -> Result<(), std::io::Error> {
    let json = r#"{ 
        "@type": "AnnotationData",
        "@id": "D2",
        "key": "num",
        "value": {
            "@type": "Float",
            "value": 4.2
        }
    }"#;

    let data: AnnotationDataBuilder = serde_json::from_str(json)?;

    assert_eq!(data.id, BuildItem::from("D2"));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, BuildItem::from("num"));
    assert_eq!(data.key, "num");
    assert_eq!(data.value, DataValue::from(4.2));
    assert_eq!(data.value, 4.2); //shorter version
    Ok(())
}

#[test]
fn parse_json_annotationdatabuilder_null() -> Result<(), std::io::Error> {
    let json = r#"{ 
        "@type": "AnnotationData",
        "@id": "D2",
        "key": "foo"
    }"#;

    let data: AnnotationDataBuilder = serde_json::from_str(json)?;

    assert_eq!(data.id, BuildItem::from("D2"));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, BuildItem::from("foo"));
    assert_eq!(data.key, "foo");
    assert_eq!(data.value, DataValue::Null);
    Ok(())
}

#[test]
fn parse_json_annotationdatabuilder_list() -> Result<(), std::io::Error> {
    let json = r#"{ 
        "@type": "AnnotationData",
        "@id": "D2",
        "key": "list",
        "value": {
            "@type": "List",
            "value": [
            {
                "@type": "String",
                "value": "foo"
            },
            {
                "@type": "String",
                "value": "bar"
            }
            ]
        }
    }"#;

    let data: AnnotationDataBuilder = serde_json::from_str(json)?;

    assert_eq!(data.id, BuildItem::from("D2"));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, BuildItem::from("list"));
    assert_eq!(data.key, "list");
    assert_eq!(
        data.value,
        DataValue::from(vec!(DataValue::from("foo"), DataValue::from("bar")))
    );
    Ok(())
}

#[test]
fn parse_json_cursor() -> Result<(), std::io::Error> {
    let data = r#"{
        "@type": "BeginAlignedCursor",
        "value": 0
    }"#;

    let cursor: Cursor = serde_json::from_str(data)?;
    assert_eq!(cursor, Cursor::BeginAligned(0));
    Ok(())
}

#[test]
fn parse_json_cursor_end() -> Result<(), std::io::Error> {
    let data = r#"{
        "@type": "EndAlignedCursor",
        "value": 0
    }"#;

    let cursor: Cursor = serde_json::from_str(data)?;
    assert_eq!(cursor, Cursor::EndAligned(0));
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

    let offset: Offset = serde_json::from_str(data)?;
    assert_eq!(offset, Offset::simple(0, 5));
    Ok(())
}

#[test]
fn offset_from_str() -> Result<(), StamError> {
    let offset: Offset = "0:5".try_into()?;
    assert_eq!(offset, Offset::simple(0, 5));
    Ok(())
}

#[test]
fn offset_from_str_2() -> Result<(), StamError> {
    let offset: Offset = "0:-5".try_into()?;
    assert_eq!(
        offset,
        Offset::new(Cursor::BeginAligned(0), Cursor::EndAligned(-5))
    );
    Ok(())
}

#[test]
fn offset_from_str_3() -> Result<(), StamError> {
    let offset: Offset = "-4:-5".try_into()?;
    assert_eq!(
        offset,
        Offset::new(Cursor::EndAligned(-4), Cursor::EndAligned(-5))
    );
    Ok(())
}

#[test]
fn offset_from_str_4() -> Result<(), StamError> {
    let offset: Offset = "0:".try_into()?;
    assert_eq!(offset, Offset::whole());
    Ok(())
}

#[test]
fn offset_from_str_5() -> Result<(), StamError> {
    let offset: Offset = ":0".try_into()?;
    assert_eq!(offset, Offset::whole());
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

    let _builder: SelectorBuilder = serde_json::from_str(data)?;
    Ok(())
}

#[test]
fn parse_json_complexselector() -> Result<(), std::io::Error> {
    let data = r#"{
    "@type": "MultiSelector",
    "selectors": [
        { 
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
        { 
            "@type": "TextSelector",
            "resource": "testres",
            "offset": {
                "begin": {
                    "@type": "BeginAlignedCursor",
                    "value": 9
                },
                "end": {
                    "@type": "BeginAlignedCursor",
                    "value": 12
                }
            }
        }
    ]}"#;

    let _builder: SelectorBuilder = serde_json::from_str(data)?;
    Ok(())
}

#[test]
fn textresource() {
    let resource = TextResource::from_string("testres", "Hello world", Config::default());
    assert_eq!(resource.id(), Some("testres"));
}

#[test]
fn serialize_datakey() {
    let datakey = DataKey::new("pos");
    serde_json::to_string(&datakey).expect("serialization");
}

#[test]
fn serialize_cursor() {
    let cursor = Cursor::BeginAligned(42);
    serde_json::to_string(&cursor).expect("serialization");
}

#[test]
fn serialize_cursor_end() {
    let cursor = Cursor::EndAligned(-2);
    serde_json::to_string(&cursor).expect("serialization");
}

#[test]
fn serialize_offset() {
    let offset = Offset::simple(0, 5);
    serde_json::to_string(&offset).expect("serialization");
}

#[test]
fn textselectionoperator_equals_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    assert!(a.test(
        &TextSelectionOperator::Equals {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(a.test(
        &TextSelectionOperator::Overlaps {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_equals_false_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 11,
        end: 25,
    };
    //not equal
    assert!(!a.test(
        &TextSelectionOperator::Equals {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_overlaps1_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 11,
        end: 25,
    };
    //overlaps
    assert!(a.test(
        &TextSelectionOperator::Overlaps {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::Overlaps {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_overlaps2_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 18,
        end: 32,
    };
    assert!(a.test(
        &TextSelectionOperator::Overlaps {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::Overlaps {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_overlaps_false_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 24,
        end: 48,
    };
    assert!(!a.test(
        &TextSelectionOperator::Overlaps {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Overlaps {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_embed_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 11,
        end: 25,
    };
    assert!(a.test(
        &TextSelectionOperator::Embedded {
            all: false,
            negate: false,
            limit: None
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Embedded {
            all: false,
            negate: false,
            limit: None
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::Embeds {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}
#[test]
fn textselectionoperator_embed_test_limit() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 16,
        end: 20,
    };
    assert!(b.test(
        &TextSelectionOperator::Embedded {
            all: false,
            negate: false,
            limit: Some(4),
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Embedded {
            all: false,
            negate: false,
            limit: Some(1)
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_embed_false_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 18,
        end: 32,
    };
    //overlap is not embedding
    assert!(!a.test(
        &TextSelectionOperator::Embedded {
            all: false,
            negate: false,
            limit: None
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Embedded {
            all: false,
            negate: false,
            limit: None
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_precedes_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 24,
        end: 48,
    };
    assert!(a.test(
        &TextSelectionOperator::Before {
            all: false,
            negate: false,
            limit: None
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Before {
            all: false,
            negate: false,
            limit: None
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::After {
            all: false,

            negate: false,
            limit: None
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!a.test(
        &TextSelectionOperator::After {
            all: false,
            negate: false,
            limit: None
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_precedes2_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 0,
        end: 12,
    };
    let b = TextSelection {
        intid: None,
        begin: 24,
        end: 48,
    };
    assert!(a.test(
        &TextSelectionOperator::Before {
            all: false,
            negate: false,
            limit: None
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Before {
            all: false,
            negate: false,
            limit: None
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::After {
            all: false,
            negate: false,
            limit: None
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!a.test(
        &TextSelectionOperator::After {
            all: false,
            negate: false,
            limit: None
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_adjacent_1vs1_nowhitespace() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 24,
        end: 48,
    };
    assert!(a.test(
        &TextSelectionOperator::Precedes {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Precedes {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::Succeeds {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!a.test(
        &TextSelectionOperator::Succeeds {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_adjacent_1vs1_whitespace() {
    let a = TextSelection {
        intid: None,
        begin: 0,
        end: 5,
    };
    let b = TextSelection {
        intid: None,
        begin: 6,
        end: 11,
    };
    let resource = TextResource::new("", Config::default()).with_string("hello world");
    assert!(a.test(
        &TextSelectionOperator::Precedes {
            all: false,
            negate: false,
            allow_whitespace: true,
        },
        &b,
        &resource //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Precedes {
            all: false,
            negate: false,
            allow_whitespace: true,
        },
        &a,
        &resource
    ));
    assert!(b.test(
        &TextSelectionOperator::Succeeds {
            all: false,
            negate: false,
            allow_whitespace: true,
        },
        &a,
        &resource
    ));
    assert!(!a.test(
        &TextSelectionOperator::Succeeds {
            all: false,
            negate: false,
            allow_whitespace: true,
        },
        &b,
        &resource
    ));
}

#[test]
fn textselectionoperator_adjacent_false_1vs1_nowhitespace() {
    let a = TextSelection {
        intid: None,
        begin: 0,
        end: 12,
    };
    let b = TextSelection {
        intid: None,
        begin: 24,
        end: 48,
    };
    //these are all not adjacent
    assert!(!a.test(
        &TextSelectionOperator::Precedes {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Precedes {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::Succeeds {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!a.test(
        &TextSelectionOperator::Succeeds {
            all: false,
            negate: false,
            allow_whitespace: false,
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_samebegin_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 12,
        end: 18,
    };
    assert!(a.test(
        &TextSelectionOperator::SameBegin {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::SameBegin {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!a.test(
        &TextSelectionOperator::SameEnd {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::SameEnd {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn textselectionoperator_sameend_1vs1() {
    let a = TextSelection {
        intid: None,
        begin: 12,
        end: 24,
    };
    let b = TextSelection {
        intid: None,
        begin: 17,
        end: 24,
    };
    assert!(a.test(
        &TextSelectionOperator::SameEnd {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(b.test(
        &TextSelectionOperator::SameEnd {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!a.test(
        &TextSelectionOperator::SameBegin {
            all: false,
            negate: false
        },
        &b,
        &TextResource::new("", Config::default()) //dummy
    ));
    assert!(!b.test(
        &TextSelectionOperator::SameBegin {
            all: false,
            negate: false
        },
        &a,
        &TextResource::new("", Config::default()) //dummy
    ));
}

#[test]
fn unicode2utf8() {
    let resource = TextResource::new("testres", Config::default()).with_string("Hallå världen");
    assert_eq!(resource.utf8byte(0).unwrap(), 0); //H
    assert_eq!(resource.utf8byte(1).unwrap(), 1); //a
    assert_eq!(resource.utf8byte(2).unwrap(), 2); //l
    assert_eq!(resource.utf8byte(3).unwrap(), 3); //l
    assert_eq!(resource.utf8byte(4).unwrap(), 4); //å
    assert_eq!(resource.utf8byte(5).unwrap(), 6); //
    assert_eq!(resource.utf8byte(6).unwrap(), 7); //v
    assert_eq!(resource.utf8byte(7).unwrap(), 8); //ä
    assert_eq!(resource.utf8byte(8).unwrap(), 10); //r
    assert_eq!(resource.utf8byte(9).unwrap(), 11); //l
    assert_eq!(resource.utf8byte(10).unwrap(), 12); //d
    assert_eq!(resource.utf8byte(11).unwrap(), 13); //e
    assert_eq!(resource.utf8byte(12).unwrap(), 14); //n
    assert_eq!(
        resource
            .utf8byte(13)
            .expect("non-inclusive end must exist too"),
        15
    );
}

#[test]
fn utf82unicode() {
    let resource = TextResource::new("testres", Config::default()).with_string("Hallå världen");
    assert_eq!(resource.utf8byte_to_charpos(0).unwrap(), 0); //H
    assert_eq!(resource.utf8byte_to_charpos(1).unwrap(), 1); //a
    assert_eq!(resource.utf8byte_to_charpos(2).unwrap(), 2); //l
    assert_eq!(resource.utf8byte_to_charpos(3).unwrap(), 3); //l
    assert_eq!(resource.utf8byte_to_charpos(4).unwrap(), 4); //å
    assert_eq!(resource.utf8byte_to_charpos(6).unwrap(), 5); //
    assert_eq!(resource.utf8byte_to_charpos(7).unwrap(), 6); //v
    assert_eq!(resource.utf8byte_to_charpos(8).unwrap(), 7); //ä
    assert_eq!(resource.utf8byte_to_charpos(10).unwrap(), 8); //r
    assert_eq!(resource.utf8byte_to_charpos(11).unwrap(), 9); //l
    assert_eq!(resource.utf8byte_to_charpos(12).unwrap(), 10); //d
    assert_eq!(resource.utf8byte_to_charpos(13).unwrap(), 11); //e
    assert_eq!(resource.utf8byte_to_charpos(14).unwrap(), 12); //n
    assert_eq!(
        resource
            .utf8byte_to_charpos(15)
            .expect("non-inclusive end must exist too"),
        13
    );
}

#[test]
fn find_text_single() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hallå världen"),
    )?;
    let resource = store.resource("testres").unwrap();
    let textselection = resource.find_text("världen").next().unwrap();
    assert_eq!(textselection.begin(), 6);
    assert_eq!(textselection.end(), 13);
    Ok(())
}

#[test]
fn find_text_single2() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hallå världen"),
    )?;
    let resource = store.resource("testres").unwrap();
    let textselection = resource.find_text("Hallå").next().unwrap();
    assert_eq!(textselection.begin(), 0);
    assert_eq!(textselection.end(), 5);
    Ok(())
}

#[test]
fn find_text_multi() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("To be or not to be, that's the question"),
    )?;
    let resource = store.resource("testres").unwrap();
    let textselections: Vec<_> = resource.find_text("be").collect();
    assert_eq!(textselections.len(), 2);
    assert_eq!(textselections[0].begin(), 3);
    assert_eq!(textselections[0].end(), 5);
    assert_eq!(textselections[1].begin(), 16);
    assert_eq!(textselections[1].end(), 18);
    Ok(())
}

#[test]
fn find_text_none() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hallå världen"),
    )?;
    let resource = store.resource("testres").unwrap();
    let v: Vec<_> = resource.find_text("blah").collect();
    assert!(v.is_empty());
    Ok(())
}

#[test]
fn split_text() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("To be or not to be"),
    )?;
    let resource = store.resource("testres").unwrap();
    let textselections: Vec<_> = resource.split_text(" ").collect();
    eprintln!("{:?}", textselections);
    assert_eq!(textselections.len(), 6);
    assert_eq!(textselections[0].begin(), 0);
    assert_eq!(textselections[0].end(), 2);
    assert_eq!(textselections[5].begin(), 16);
    assert_eq!(textselections[5].end(), 18);
    Ok(())
}

#[test]
fn split_text_whitespace() -> Result<(), StamError> {
    //with leading and trailing 'empty' texts
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("\nTo be or not to be\nthat is the question\n"),
    )?;
    let resource = store.resource("testres").unwrap();
    let textselections: Vec<_> = resource.split_text("\n").collect();
    eprintln!("{:?}", textselections);
    assert_eq!(textselections.len(), 4);
    Ok(())
}

#[test]
fn split_text_none() -> Result<(), StamError> {
    //with no occurrences at all
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("To be or not to be"),
    )?;
    let resource = store.resource("testres").unwrap();
    let textselections: Vec<_> = resource.split_text("?").collect();
    eprintln!("{:?}", textselections);
    assert_eq!(textselections.len(), 1);
    Ok(())
}

#[test]
fn trim_text() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("  To be or not to be   "),
    )?;
    let resource = store.resource("testres").unwrap();
    let textselection = resource.trim_text(&[' ']).unwrap();
    assert_eq!(textselection.begin(), 2);
    assert_eq!(textselection.end(), 20);
    assert_eq!(textselection.text(), "To be or not to be");
    Ok(())
}

#[test]
fn textselection_out_of_bounds() -> Result<(), StamError> {
    let mut store = AnnotationStore::default();
    let handle = store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text("Hello world"),
    )?;
    let resource = store.resource(handle).unwrap();
    let result = resource.textselection(&Offset::simple(0, 999));
    assert!(result.is_err());
    Ok(())
}
