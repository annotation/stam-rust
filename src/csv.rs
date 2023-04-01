use csv;
use sealed::sealed;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::BufWriter;

use crate::types::*;
use crate::{
    Annotation, AnnotationDataSet, AnnotationStore, Config, DataValue, Selector, SelectorKind,
    StamError, TextResource,
};

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
    #[serde(rename = "TargetResource")]
    targetresource: Cow<'a, str>,
    #[serde(rename = "TargetAnnotation")]
    targetannotation: Cow<'a, str>,
    #[serde(rename = "TargetDataSet")]
    targetdataset: Cow<'a, str>,
    #[serde(rename = "BeginOffset")]
    begin: String,
    #[serde(rename = "EndOffset")]
    end: String,
}

impl<'a> AnnotationCsv<'a> {
    fn set_selectortype(selector: &Selector) -> Cow<'a, str> {
        if selector.is_complex() {
            let mut selectortype: String = selector.kind().as_str().to_string();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    selectortype.push(';'); //delimiter
                    selectortype += subselector.kind().as_str();
                }
            }
            Cow::Owned(selectortype)
        } else {
            Cow::Borrowed(selector.kind().as_str())
        }
    }

    fn set_beginoffset(selector: &Selector) -> String {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    out += Self::set_beginoffset(subselector).as_str();
                }
            }
            out
        } else {
            match selector {
                Selector::TextSelector(_, offset)
                | Selector::AnnotationSelector(_, Some(offset)) => format!("{}", offset.begin),
                _ => String::new(),
            }
        }
    }

    fn set_endoffset(selector: &Selector) -> String {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    out += Self::set_endoffset(subselector).as_str();
                }
            }
            out
        } else {
            match selector {
                Selector::TextSelector(_, offset)
                | Selector::AnnotationSelector(_, Some(offset)) => format!("{}", offset.end),
                _ => String::new(),
            }
        }
    }

    fn set_targetresource(selector: &Selector, store: &'a AnnotationStore) -> Cow<'a, str> {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    match subselector {
                        Selector::ResourceSelector(res) | Selector::TextSelector(res, _) => {
                            let res: &TextResource = store.get(*res).expect("resource must exist");
                            out += res.id().expect("resource must have an id");
                        }
                        _ => {}
                    }
                }
            }
            Cow::Owned(out)
        } else {
            match selector {
                Selector::ResourceSelector(res) | Selector::TextSelector(res, _) => {
                    let res: &TextResource = store.get(*res).expect("resource must exist");
                    Cow::Borrowed(res.id().expect("resource must have an id"))
                }
                _ => Cow::Borrowed(""),
            }
        }
    }

    fn set_targetdataset(selector: &Selector, store: &'a AnnotationStore) -> Cow<'a, str> {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    match subselector {
                        Selector::DataSetSelector(dataset) => {
                            let dataset: &AnnotationDataSet =
                                store.get(*dataset).expect("dataset must exist");
                            out += dataset.id().expect("dataset must have an id");
                        }
                        _ => {}
                    }
                }
            }
            Cow::Owned(out)
        } else {
            match selector {
                Selector::DataSetSelector(dataset) => {
                    let dataset: &AnnotationDataSet =
                        store.get(*dataset).expect("dataset must exist");
                    Cow::Borrowed(dataset.id().expect("dataset must have an id"))
                }
                _ => Cow::Borrowed(""),
            }
        }
    }

    fn set_targetannotation(selector: &Selector, store: &'a AnnotationStore) -> Cow<'a, str> {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    match subselector {
                        Selector::AnnotationSelector(ann, _) => {
                            let ann: &Annotation = store.get(*ann).expect("annotation must exist");
                            out += ann.id().expect("annotation must have an id");
                        }
                        _ => {}
                    }
                }
            }
            Cow::Owned(out)
        } else {
            match selector {
                Selector::AnnotationSelector(ann, _) => {
                    let ann: &Annotation = store.get(*ann).expect("annotation must exist");
                    Cow::Borrowed(ann.id().expect("annotation must have an id"))
                }
                _ => Cow::Borrowed(""),
            }
        }
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

#[sealed(pub(crate))] //<-- this ensures nobody outside this crate can implement the trait
pub trait ToCsv
where
    Self: TypeInfo + serde::Serialize,
{
    fn to_csv_writer<W>(&self, writer: W, table: CsvTable) -> Result<(), StamError>
    where
        W: std::io::Write;

    /// Writes this structure to a file
    /// The actual dataformat can be set via `config`, the default is STAM JSON.
    fn to_csv_file(
        &self,
        filename: &str,
        config: &Config,
        table: Option<CsvTable>,
    ) -> Result<(), StamError> {
        debug(config, || {
            format!("{}.to_csv_file: filename={:?}", Self::typeinfo(), filename)
        });
        if table == Some(CsvTable::StoreManifest) && config.dataformat != DataFormat::Csv {
            return Err(StamError::SerializationError(format!(
                "Unable to serialize to CSV for {} when config dataformat is set to {}",
                Self::typeinfo(),
                config.dataformat
            )));
        }
        let writer = open_file_writer(filename, &config)?;
        let result = self.to_csv_writer(writer, table.unwrap_or_default());
        result
    }

    fn to_csv_string(&self, table: Option<CsvTable>) -> Result<String, StamError> {
        let mut writer = BufWriter::new(Vec::new());
        self.to_csv_writer(&mut writer, table.unwrap_or_default())?;
        let bytes = writer.into_inner().expect("unwrapping buffer");
        Ok(String::from_utf8(bytes).expect("valid utf-8"))
    }
}

#[sealed]
impl ToCsv for AnnotationStore {
    /// Writes CSV output, for the specified table, to the writer
    fn to_csv_writer<W>(&self, writer: W, table: CsvTable) -> Result<(), StamError>
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
                            targetdataset: AnnotationCsv::set_targetdataset(
                                annotation.target(),
                                self,
                            ),
                            targetresource: AnnotationCsv::set_targetresource(
                                annotation.target(),
                                self,
                            ),
                            targetannotation: AnnotationCsv::set_targetannotation(
                                annotation.target(),
                                self,
                            ),
                            begin: AnnotationCsv::set_beginoffset(annotation.target()),
                            end: AnnotationCsv::set_endoffset(annotation.target()),
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
                            selectortype: AnnotationCsv::set_selectortype(annotation.target()),
                            targetdataset: AnnotationCsv::set_targetdataset(
                                annotation.target(),
                                self,
                            ),
                            targetresource: AnnotationCsv::set_targetresource(
                                annotation.target(),
                                self,
                            ),
                            targetannotation: AnnotationCsv::set_targetannotation(
                                annotation.target(),
                                self,
                            ),
                            begin: AnnotationCsv::set_beginoffset(annotation.target()),
                            end: AnnotationCsv::set_endoffset(annotation.target()),
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
                            selectortype: AnnotationCsv::set_selectortype(annotation.target()),
                            targetdataset: AnnotationCsv::set_targetdataset(
                                annotation.target(),
                                self,
                            ),
                            targetresource: AnnotationCsv::set_targetresource(
                                annotation.target(),
                                self,
                            ),
                            targetannotation: AnnotationCsv::set_targetannotation(
                                annotation.target(),
                                self,
                            ),
                            begin: AnnotationCsv::set_beginoffset(annotation.target()),
                            end: AnnotationCsv::set_endoffset(annotation.target()),
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
impl ToCsv for AnnotationDataSet {
    /// Writes CSV output, for the specified table, to the writer
    fn to_csv_writer<W>(&self, writer: W, table: CsvTable) -> Result<(), StamError>
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
