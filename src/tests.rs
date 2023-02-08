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

    assert_eq!(data.id, AnyId::Id("D2".into()));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, AnyId::Id("pos".into()));
    assert_eq!(data.key, "pos");
    assert_eq!(data.value, DataValue::String("verb".into()));
    assert_eq!(data.value, "verb"); //shorter version
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
fn textresource() {
    let resource = TextResource::from_string("testres".into(), "Hello world".into());
    assert_eq!(resource.id(), Some("testres"));
}

#[test]
fn serialize_datakey() {
    let datakey = DataKey::new("pos".into());
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
    assert!(a.test(&TextSelectionOperator::Equals(&b.into())));
    assert!(a.test(&TextSelectionOperator::Overlaps(&b.into())));
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
    assert!(!a.test(&TextSelectionOperator::Equals(&b.into())));
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
    assert!(a.test(&TextSelectionOperator::Overlaps(&b.into())));
    assert!(b.test(&TextSelectionOperator::Overlaps(&a.into())));
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
    assert!(a.test(&TextSelectionOperator::Overlaps(&b.into())));
    assert!(b.test(&TextSelectionOperator::Overlaps(&a.into())));
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
    assert!(!a.test(&TextSelectionOperator::Overlaps(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Overlaps(&a.into())));
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
    assert!(a.test(&TextSelectionOperator::Embedded(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Embedded(&a.into())));
    assert!(b.test(&TextSelectionOperator::Embeds(&a.into())));
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
    assert!(!a.test(&TextSelectionOperator::Embedded(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Embedded(&a.into())));
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
    assert!(a.test(&TextSelectionOperator::Precedes(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Precedes(&a.into())));
    assert!(b.test(&TextSelectionOperator::Succeeds(&a.into())));
    assert!(!a.test(&TextSelectionOperator::Succeeds(&b.into())));
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
    assert!(a.test(&TextSelectionOperator::Precedes(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Precedes(&a.into())));
    assert!(b.test(&TextSelectionOperator::Succeeds(&a.into())));
    assert!(!a.test(&TextSelectionOperator::Succeeds(&b.into())));
}

#[test]
fn textselectionoperator_adjacent_1vs1() {
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
    assert!(a.test(&TextSelectionOperator::LeftAdjacent(&b.into())));
    assert!(!b.test(&TextSelectionOperator::LeftAdjacent(&a.into())));
    assert!(b.test(&TextSelectionOperator::RightAdjacent(&a.into())));
    assert!(!a.test(&TextSelectionOperator::RightAdjacent(&b.into())));
}

#[test]
fn textselectionoperator_adjacent_false_1vs1() {
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
    assert!(!a.test(&TextSelectionOperator::LeftAdjacent(&b.into())));
    assert!(!b.test(&TextSelectionOperator::LeftAdjacent(&a.into())));
    assert!(!b.test(&TextSelectionOperator::RightAdjacent(&a.into())));
    assert!(!a.test(&TextSelectionOperator::RightAdjacent(&b.into())));
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
    assert!(a.test(&TextSelectionOperator::SameBegin(&b.into())));
    assert!(b.test(&TextSelectionOperator::SameBegin(&a.into())));
    assert!(!a.test(&TextSelectionOperator::SameEnd(&b.into())));
    assert!(!b.test(&TextSelectionOperator::SameEnd(&a.into())));
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
    assert!(a.test(&TextSelectionOperator::SameEnd(&b.into())));
    assert!(b.test(&TextSelectionOperator::SameEnd(&a.into())));
    assert!(!a.test(&TextSelectionOperator::SameBegin(&b.into())));
    assert!(!b.test(&TextSelectionOperator::SameBegin(&a.into())));
}

#[test]
fn unicode2utf8() {
    let resource = TextResource::new("testres".into()).with_string("Hallå världen".into());
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
