#[cfg(tests)]

use crate::types::{StoreFor,Storable};
use crate::error::StamError;
use crate::annotationdata::AnnotationDataBuilder;
use crate::datakey::DataKey;
use crate::datavalue::DataValue;

#[test]
fn serialize_datakey()  {
    let datakey = DataKey::new("pos".into());
    serde_json::to_string(&datakey).expect("serialization");
}


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
