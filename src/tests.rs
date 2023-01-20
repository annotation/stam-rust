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
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    assert!(a.test(&TextSelectionOperator::Equals(&b.into())));
    assert!(a.test(&TextSelectionOperator::Overlaps(&b.into())));
}

#[test]
fn textselectionoperator_equals_false_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 11,
        endbyte: 25,
    };
    //not equal
    assert!(!a.test(&TextSelectionOperator::Equals(&b.into())));
}

#[test]
fn textselectionoperator_overlaps1_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 11,
        endbyte: 25,
    };
    //overlaps
    assert!(a.test(&TextSelectionOperator::Overlaps(&b.into())));
    assert!(b.test(&TextSelectionOperator::Overlaps(&a.into())));
}

#[test]
fn textselectionoperator_overlaps2_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 18,
        endbyte: 32,
    };
    assert!(a.test(&TextSelectionOperator::Overlaps(&b.into())));
    assert!(b.test(&TextSelectionOperator::Overlaps(&a.into())));
}

#[test]
fn textselectionoperator_overlaps_false_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 24,
        endbyte: 48,
    };
    assert!(!a.test(&TextSelectionOperator::Overlaps(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Overlaps(&a.into())));
}

#[test]
fn textselectionoperator_embed_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 11,
        endbyte: 25,
    };
    assert!(a.test(&TextSelectionOperator::Embedded(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Embedded(&a.into())));
    assert!(b.test(&TextSelectionOperator::Embeds(&a.into())));
}

#[test]
fn textselectionoperator_embed_false_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 18,
        endbyte: 32,
    };
    //overlap is not embedding
    assert!(!a.test(&TextSelectionOperator::Embedded(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Embedded(&a.into())));
}

#[test]
fn textselectionoperator_precedes_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 24,
        endbyte: 48,
    };
    assert!(a.test(&TextSelectionOperator::Precedes(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Precedes(&a.into())));
    assert!(b.test(&TextSelectionOperator::Succeeds(&a.into())));
    assert!(!a.test(&TextSelectionOperator::Succeeds(&b.into())));
}

#[test]
fn textselectionoperator_precedes2_1vs1() {
    let a = TextSelection {
        beginbyte: 0,
        endbyte: 12,
    };
    let b = TextSelection {
        beginbyte: 24,
        endbyte: 48,
    };
    assert!(a.test(&TextSelectionOperator::Precedes(&b.into())));
    assert!(!b.test(&TextSelectionOperator::Precedes(&a.into())));
    assert!(b.test(&TextSelectionOperator::Succeeds(&a.into())));
    assert!(!a.test(&TextSelectionOperator::Succeeds(&b.into())));
}

#[test]
fn textselectionoperator_adjacent_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 24,
        endbyte: 48,
    };
    assert!(a.test(&TextSelectionOperator::LeftAdjacent(&b.into())));
    assert!(!b.test(&TextSelectionOperator::LeftAdjacent(&a.into())));
    assert!(b.test(&TextSelectionOperator::RightAdjacent(&a.into())));
    assert!(!a.test(&TextSelectionOperator::RightAdjacent(&b.into())));
}

#[test]
fn textselectionoperator_adjacent_false_1vs1() {
    let a = TextSelection {
        beginbyte: 0,
        endbyte: 12,
    };
    let b = TextSelection {
        beginbyte: 24,
        endbyte: 48,
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
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 12,
        endbyte: 18,
    };
    assert!(a.test(&TextSelectionOperator::SameBegin(&b.into())));
    assert!(b.test(&TextSelectionOperator::SameBegin(&a.into())));
    assert!(!a.test(&TextSelectionOperator::SameEnd(&b.into())));
    assert!(!b.test(&TextSelectionOperator::SameEnd(&a.into())));
}

#[test]
fn textselectionoperator_sameend_1vs1() {
    let a = TextSelection {
        beginbyte: 12,
        endbyte: 24,
    };
    let b = TextSelection {
        beginbyte: 17,
        endbyte: 24,
    };
    assert!(a.test(&TextSelectionOperator::SameEnd(&b.into())));
    assert!(b.test(&TextSelectionOperator::SameEnd(&a.into())));
    assert!(!a.test(&TextSelectionOperator::SameBegin(&b.into())));
    assert!(!b.test(&TextSelectionOperator::SameBegin(&a.into())));
}
