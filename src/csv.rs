use csv;
use sealed::sealed;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::types::*;
use crate::{AnnotationDataSet, AnnotationStore, DataValue, Selector, SelectorKind, StamError};

#[derive(Serialize)]
struct AnnotationStoreCsv<'a> {
    #[serde(rename = "Type")]
    tp: Type,
    #[serde(rename = "Id")]
    id: Option<Cow<'a, str>>,
    #[serde(rename = "Filename")]
    filename: Option<Cow<'a, str>>,
}

#[derive(Serialize)]
struct AnnotationDataCsv<'a> {
    #[serde(rename = "Id")]
    id: Option<Cow<'a, str>>,
    #[serde(rename = "Key")]
    key: Cow<'a, str>,

    //TODO: Add Type
    #[serde(rename = "Value")]
    value: Option<&'a DataValue>,
}

#[derive(Serialize, Deserialize)]
struct AnnotationCsv<'a> {
    #[serde(rename = "Id")]
    id: Option<Cow<'a, str>>,
    #[serde(rename = "AnnotationData")]
    data_ids: Cow<'a, str>,
    #[serde(rename = "AnnotationDataSet")]
    set_ids: Cow<'a, str>,
    #[serde(rename = "SelectorType")]
    selectortype: Cow<'a, str>,
}

impl<'a> AnnotationCsv<'a> {
    fn set_selectortype(selector: &Selector) -> Cow<'a, str> {
        Cow::Borrowed(selector.kind().as_str())
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Deserialize, Serialize)]
pub enum CsvTable {
    StoreManifest,
    Annotation,
    AnnotationDataSet,
}

impl Default for CsvTable {
    fn default() -> Self {
        CsvTable::StoreManifest
    }
}

#[sealed]
impl Writable for AnnotationStore {
    /// Writes CSV output, for the specified table, to the writer
    fn csv_writer<W>(&self, writer: W, table: CsvTable) -> Result<(), StamError>
    where
        W: std::io::Write,
    {
        let mut writer = csv::Writer::from_writer(writer);
        match table {
            CsvTable::StoreManifest => {
                if self.annotations_filename().is_none() {
                    return Err(StamError::SerializationError(format!(
                    "An annotations filename must be associated with the AnnotationStore for CSV serialization to work",
                    )));
                }
                writer
                    .serialize(AnnotationStoreCsv {
                        tp: Type::AnnotationStore,
                        id: self.id().map(|x| Cow::Borrowed(x)),
                        filename: self
                            .annotations_filename()
                            .map(|x| Cow::Borrowed(x.to_str().expect("valid utf-8 filename"))),
                    })
                    .map_err(|e| {
                        StamError::SerializationError(format!("Failure serializing CSV: {:?}", e))
                    })?;
                for dataset in self.annotationsets() {
                    if dataset.filename().is_none() {
                        return Err(StamError::SerializationError(format!(
                            "AnnotationDataSet must have a set filename for CSV serialization to work",
                        )));
                    } else if dataset.filename().unwrap().ends_with(".json") {
                        return Err(StamError::SerializationError(format!(
                            "AnnotationDataSet is still associated with a STAM JSON file, can't serialize to CSV yet",
                        )));
                    }
                    writer
                        .serialize(AnnotationStoreCsv {
                            tp: Type::AnnotationDataSet,
                            id: self.id().map(|x| Cow::Borrowed(x)),
                            filename: dataset.filename().map(|x| Cow::Borrowed(x)),
                        })
                        .map_err(|e| {
                            StamError::SerializationError(format!(
                                "Failure serializing CSV: {:?}",
                                e
                            ))
                        })?;
                }
                for resource in self.resources() {
                    if resource.filename().is_none() {
                        return Err(StamError::SerializationError(format!(
                            "TextResource must have a set filename (plain text!) for CSV serialization to work",
                        )));
                    } else if resource.filename().unwrap().ends_with(".json") {
                        return Err(StamError::SerializationError(format!(
                            "TextResource is still associated with a STAM JSON file, can't serialize yet",
                        )));
                    }
                    writer
                        .serialize(AnnotationStoreCsv {
                            tp: Type::TextResource,
                            id: resource.id().map(|x| Cow::Borrowed(x)),
                            filename: resource.filename().map(|x| Cow::Borrowed(x)),
                        })
                        .map_err(|e| {
                            StamError::SerializationError(format!(
                                "Failure serializing CSV: {:?}",
                                e
                            ))
                        })?;
                }
                Ok(())
            }
            CsvTable::Annotation => {
                for annotation in self.annotations() {
                    let out = if annotation.len() == 0 {
                        AnnotationCsv {
                            id: annotation.id().map(|x| Cow::Borrowed(x)),
                            data_ids: Cow::Borrowed(""),
                            set_ids: Cow::Borrowed(""),
                            selectortype: AnnotationCsv::set_selectortype(annotation.target()),
                        }
                    } else if annotation.len() == 1 {
                        //only one data item, we needn't make any copies
                        let (_key, data, set) = self.data_by_annotation(annotation).next().unwrap();
                        if data.id().is_none() {
                            return Err(StamError::SerializationError(format!(
                                "AnnotationData must have a public id for csv serialization",
                            )));
                        }
                        if set.id().is_none() {
                            return Err(StamError::SerializationError(format!(
                                "AnnotationDataSet must have a public id for csv serialization",
                            )));
                        }
                        AnnotationCsv {
                            id: annotation.id().map(|x| Cow::Borrowed(x)),
                            data_ids: Cow::Borrowed(data.id().unwrap()),
                            set_ids: Cow::Borrowed(set.id().unwrap()),
                            selectortype,
                        }
                    } else {
                        let mut data_ids = String::new();
                        let mut set_ids = String::new();
                        for (_key, data, set) in self.data_by_annotation(annotation) {
                            if data.id().is_none() {
                                return Err(StamError::SerializationError(format!(
                                    "AnnotationData must have a public id for csv serialization",
                                )));
                            }
                            if set.id().is_none() {
                                return Err(StamError::SerializationError(format!(
                                    "AnnotationDataSet must have a public id for csv serialization",
                                )));
                            }
                            if !data_ids.is_empty() {
                                data_ids.push(';');
                                set_ids.push(';');
                            }
                            data_ids += data.id().unwrap();
                            set_ids += set.id().unwrap();
                        }
                        AnnotationCsv {
                            id: annotation.id().map(|x| Cow::Borrowed(x)),
                            data_ids: Cow::Owned(data_ids),
                            set_ids: Cow::Owned(set_ids),
                            selectortype,
                        }
                    };
                    writer.serialize(out).map_err(|e| {
                        StamError::SerializationError(format!("Failure serializing CSV: {:?}", e))
                    })?;
                }
                Ok(())
            }
            _ => Err(StamError::SerializationError(format!(
                "Can only serialize CSV table StoreManifest or Annotation, not {:?}",
                table
            ))),
        }
    }
}

#[sealed]
impl Writable for AnnotationDataSet {
    /// Writes CSV output, for the specified table, to the writer
    fn csv_writer<W>(&self, writer: W, table: CsvTable) -> Result<(), StamError>
    where
        W: std::io::Write,
    {
        let mut writer = csv::Writer::from_writer(writer);
        for key in self.keys() {
            writer
                .serialize(AnnotationDataCsv {
                    id: None,
                    key: key
                        .id()
                        .map(|x| Cow::Borrowed(x))
                        .expect("key must have ID"),
                    value: None,
                })
                .map_err(|e| {
                    StamError::SerializationError(format!("Failure serializing CSV: {:?}", e))
                })?;
        }
        for data in self.data() {
            if data.id().is_none() {
                return Err(StamError::SerializationError(format!(
                    "All AnnotationData must have a public ID for CSV serialization to work",
                )));
            }
            let key = self.key(&AnyId::from(data.key())).expect("key must exist");
            writer
                .serialize(AnnotationDataCsv {
                    id: data.id().map(|x| Cow::Borrowed(x)),
                    key: key
                        .id()
                        .map(|x| Cow::Borrowed(x))
                        .expect("key must have a public ID"),
                    value: Some(data.value()),
                })
                .map_err(|e| {
                    StamError::SerializationError(format!("Failure serializing CSV: {:?}", e))
                })?;
        }
        Ok(())
    }
}
