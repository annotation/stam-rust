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
    let offset = crate::selector::Offset::simple(0, 5);
    serde_json::to_string(&offset).expect("serialization");
}
