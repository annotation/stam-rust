#[cfg(tests)]

use crate::types::{StoreFor,Storable,Cursor};
use crate::error::StamError;
use crate::annotationdata::AnnotationDataBuilder;
use crate::annotationdataset::AnnotationDataSet;
use crate::datakey::DataKey;
use crate::datavalue::DataValue;
use crate::selector::{SelectorBuilder,Offset};
use crate::resources::TextResource;



#[test]
fn parse_json_datakey() {
    let json = r#"{ 
        "@type": "DataKey",
        "@id": "pos"
    }"#;

    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    let key: DataKey = serde_json::from_value(v).unwrap();

    //not sure why I have to annotate the type verbosely here, worked fine in integration tests,
    //perhaps because unit tests can see private fields and there's one named 'id'?
    //assert_eq!(key.id(&key).unwrap(), "pos");
    assert_eq!(<DataKey as crate::types::Storable>::id(&key).unwrap(), "pos");
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

    assert_eq!(data.id, crate::types::AnyId::Id("D2".into()));
    assert_eq!(data.id, "D2"); //can also be compared with &str etc
    assert_eq!(data.key, crate::types::AnyId::Id("pos".into()));
    assert_eq!(data.key, "pos");
    assert_eq!(data.value, DataValue::String("verb".into()));
    assert_eq!(data.value, "verb");  //shorter version
    Ok(())
}

#[test]
fn parse_json_cursor() -> Result<(), std::io::Error> {
    let data = r#"{
        "@type": "BeginAlignedCursor",
        "value": 0
    }"#;

    let cursor: crate::types::Cursor = serde_json::from_str(data)?;
    assert_eq!( cursor, crate::types::Cursor::BeginAligned(0) );
    Ok(())
}

#[test]
fn parse_json_cursor_end() -> Result<(), std::io::Error> {
    let data = r#"{
        "@type": "EndAlignedCursor",
        "value": 0
    }"#;

    let cursor: crate::types::Cursor = serde_json::from_str(data)?;
    assert_eq!( cursor, crate::types::Cursor::EndAligned(0) );
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

    let _builder: SelectorBuilder = serde_json::from_str(data)?;
    Ok(())
}

#[test]
fn textresource() {
    let resource = TextResource::from_string("testres".into(),"Hello world".into());
    assert_eq!(<TextResource as crate::types::Storable>::id(&resource), Some("testres"));
}


#[test]
fn serialize_datakey()  {
    let datakey = DataKey::new("pos".into());
    serde_json::to_string(&datakey).expect("serialization");
}

#[test]
fn serialize_cursor()  {
    let cursor = crate::types::Cursor::BeginAligned(42);
    serde_json::to_string(&cursor).expect("serialization");
}

#[test]
fn serialize_cursor_end()  {
    let cursor = crate::types::Cursor::EndAligned(-2);
    serde_json::to_string(&cursor).expect("serialization");
}

#[test]
fn serialize_offset()  {
    let offset = crate::selector::Offset::simple(0,5);
    serde_json::to_string(&offset).expect("serialization");
}
