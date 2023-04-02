use csv;
use sealed::sealed;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::io::{BufRead, BufReader, BufWriter};
use std::ops::Deref;
use std::path::PathBuf;

use crate::types::*;
use crate::{
    Annotation, AnnotationBuilder, AnnotationDataBuilder, AnnotationDataSet,
    AnnotationDataSetBuilder, AnnotationStore, AnnotationStoreBuilder, Config, DataKey, DataValue,
    Offset, Selector, SelectorBuilder, SelectorKind, SerializeMode, StamError, TextResource,
    TextResourceBuilder,
};

#[derive(Serialize, Deserialize, Debug)]
/// Annotation Store Manifest
struct StoreManifestCsv<'a> {
    #[serde(rename = "Type")]
    tp: Type,
    #[serde(rename = "Id")]
    id: Option<Cow<'a, str>>,
    #[serde(rename = "Filename")]
    filename: Cow<'a, str>,
}

#[derive(Serialize, Deserialize, Debug)]
struct AnnotationDataCsv<'a> {
    #[serde(rename = "Id")]
    id: Option<Cow<'a, str>>,
    #[serde(rename = "Key")]
    key: Cow<'a, str>,

    //TODO: Add Type
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Serialize, Deserialize, Debug)]
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
    /// Note: This only writes a single table! Just use [`AnnotationStore.save()`] to write everything.
    fn to_csv_file(
        &self,
        filename: &str,
        config: &Config,
        table: CsvTable,
    ) -> Result<(), StamError> {
        debug(config, || {
            format!("{}.to_csv_file: filename={:?}", Self::typeinfo(), filename)
        });
        if table == CsvTable::StoreManifest && config.dataformat != DataFormat::Csv {
            return Err(StamError::SerializationError(format!(
                "Unable to serialize to CSV for {} when config dataformat is set to {}",
                Self::typeinfo(),
                config.dataformat
            )));
        }
        let writer = open_file_writer(filename, &config)?;
        self.to_csv_writer(writer, table)
    }

    fn to_csv_string(&self, table: Option<CsvTable>) -> Result<String, StamError> {
        let mut writer = BufWriter::new(Vec::new());
        self.to_csv_writer(&mut writer, table.unwrap_or_default())?;
        let bytes = writer.into_inner().expect("unwrapping buffer");
        Ok(String::from_utf8(bytes).expect("valid utf-8"))
    }
}

impl AnnotationStore {
    /// Saves all files as CSV. It is better to use [`AnnotationStore::save()`] instead, setting
    /// [`AnnotationStore::set_dataformat(DataFormat::Csv)`] before invocation.
    pub fn to_csv_files(&self, filename: &str) -> Result<(), StamError> {
        debug(self.config(), || {
            format!("{}.to_csv_files: filename={:?}", Self::typeinfo(), filename)
        });
        let basename = strip_known_extension(filename);
        self.to_csv_file(filename, self.config(), CsvTable::StoreManifest)?;
        self.to_csv_file(
            self.annotations_filename()
                .map(|x| x.to_str().expect("valid utf-8").to_owned())
                .unwrap_or_else(|| format!("{}.annotation.csv", basename))
                .as_str(),
            self.config(),
            CsvTable::Annotation,
        )?;
        for annotationset in self.annotationsets() {
            if annotationset.changed() {
                debug(self.config(), || {
                    format!(
                        "{}.to_csv_files: writing AnnotationDataSet",
                        Self::typeinfo()
                    )
                });
                annotationset.to_csv_file(
                    annotationset
                        .filename()
                        .map(|x| x.to_owned())
                        .unwrap_or_else(|| {
                            // no filename is associated with the resource yet, try to infer one from the ID
                            let basename =
                                sanitize_id_to_filename(self.id().expect("resource must have id"));
                            format!("{}.annotationset.stam.csv", basename)
                        })
                        .as_str(),
                    annotationset.config(),
                    CsvTable::AnnotationDataSet,
                )?;
                if annotationset.filename().is_some() {
                    annotationset.mark_unchanged();
                }
            }
        }
        for resource in self.resources() {
            if resource.changed() {
                resource.to_txt_file(
                    resource
                        .filename()
                        .map(|x| x.to_owned())
                        .unwrap_or_else(|| {
                            // no filename is associated with the resource yet, try to infer one from the ID
                            let basename =
                                sanitize_id_to_filename(self.id().expect("resource must have id"));
                            format!("{}.txt", basename)
                        })
                        .as_str(),
                )?;
                if resource.filename().is_some() {
                    resource.mark_unchanged();
                }
            }
        }
        Ok(())
    }
}

#[sealed]
impl ToCsv for AnnotationStore
where
    Self: TypeInfo,
{
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
                    .serialize(StoreManifestCsv {
                        tp: Type::AnnotationStore,
                        id: self.id().map(|x| Cow::Borrowed(x)),
                        filename: self
                            .annotations_filename()
                            .map(|x| Cow::Borrowed(x.to_str().expect("valid utf-8 filename")))
                            .unwrap(),
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
                        .serialize(StoreManifestCsv {
                            tp: Type::AnnotationDataSet,
                            id: self.id().map(|x| Cow::Borrowed(x)),
                            filename: dataset.filename().map(|x| Cow::Borrowed(x)).unwrap(),
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
                        .serialize(StoreManifestCsv {
                            tp: Type::TextResource,
                            id: resource.id().map(|x| Cow::Borrowed(x)),
                            filename: resource.filename().map(|x| Cow::Borrowed(x)).unwrap(),
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
                    value: String::new(),
                })
                .map_err(|e| {
                    StamError::SerializationError(format!(
                        "Failure serializing CSV for key {:?}: {:?}",
                        key, e
                    ))
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
                    value: data.value().to_string(),
                })
                .map_err(|e| {
                    StamError::SerializationError(format!(
                        "Failure serializing CSV for data {:?}: {:?}",
                        data, e
                    ))
                })?;
        }
        Ok(())
    }
}

#[sealed] //<-- this ensures nobody outside this crate can implement the trait
pub trait FromCsv<'a>
where
    Self: TypeInfo + serde::Deserialize<'a>,
{
    fn from_csv_reader(
        reader: Box<dyn BufRead>,
        filename: Option<&str>,
        config: Config,
    ) -> Result<Self, StamError>;

    fn from_csv_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!("{}::from_csv_file: filename={}", Self::typeinfo(), filename)
        });
        let reader = open_file_reader(filename, &config)?;
        Self::from_csv_reader(reader, Some(filename), config)
    }
}

impl<'a> TryInto<AnnotationBuilder> for AnnotationCsv<'a> {
    type Error = StamError;
    fn try_into(self) -> Result<AnnotationBuilder, Self::Error> {
        let mut builder = AnnotationBuilder::new();
        if let Some(id) = self.id {
            builder = builder.with_id(id.to_string());
        }
        if !self.data_ids.is_empty() {
            let set_ids: SmallVec<[&str; 1]> = self.set_ids.split(";").collect();
            if set_ids.is_empty() {
                return Err(StamError::CsvError(
                    format!("AnnotationDataSet column may not be empty",),
                    "",
                ));
            }
            for (i, data_id) in self.data_ids.split(";").enumerate() {
                let set_id = set_ids.get(i).unwrap_or(set_ids.last().unwrap());
                builder = builder.with_data_by_id(AnyId::from(*set_id), AnyId::from(data_id));
            }
            let mut selectortypes: SmallVec<[SelectorKind; 1]> = SmallVec::new();
            let mut complex = false;
            for (i, selectortype) in self.selectortype.split(";").enumerate() {
                let selectortype: SelectorKind = selectortype.try_into()?;
                if i == 0 {
                    complex = selectortype.is_complex();
                }
                selectortypes.push(selectortype);
            }
            if complex && selectortypes.len() == 1 {
                return Err(StamError::CsvError(
                    format!("A complex selector can not be defined without any subselectors"),
                    "",
                ));
            } else if selectortypes.is_empty() {
                return Err(StamError::CsvError(
                    format!("The SelectorType column can not be empty"),
                    "",
                ));
            }
            let selectorbuilder = if !complex {
                //validation to ensure all fields have only a single value
                if self.targetresource.find(";").is_some() {
                    return Err(StamError::CsvError(
                        format!("Multiple target resources were specified, but without a complex selector"),
                        "",
                    ));
                }
                if self.targetdataset.find(";").is_some() {
                    return Err(StamError::CsvError(
                        format!("Multiple target annotation datasets were specified, but without a complex selector"),
                        "",
                    ));
                }
                if self.targetannotation.find(";").is_some() {
                    return Err(StamError::CsvError(
                        format!("Multiple target annotations were specified, but without a complex selector"),
                        "",
                    ));
                }
                if self.begin.find(";").is_some() {
                    return Err(StamError::CsvError(
                        format!(
                            "Multiple begin offsets were specified, but without a complex selector"
                        ),
                        "",
                    ));
                }
                if self.end.find(";").is_some() {
                    return Err(StamError::CsvError(
                        format!(
                            "Multiple end offsets were specified, but without a complex selector"
                        ),
                        "",
                    ));
                }
                match selectortypes[0] {
                    SelectorKind::TextSelector => {
                        let resource = self.targetresource;
                        let begin: Cursor = self.begin.as_str().try_into()?;
                        let end: Cursor = self.end.as_str().try_into()?;
                        SelectorBuilder::TextSelector(
                            AnyId::Id(resource.to_string()),
                            Offset::new(begin, end),
                        )
                    }
                    SelectorKind::AnnotationSelector => {
                        let annotation = self.targetannotation;
                        let offset =
                            if !self.begin.as_str().is_empty() && !self.end.as_str().is_empty() {
                                let begin: Cursor = self.begin.as_str().try_into()?;
                                let end: Cursor = self.end.as_str().try_into()?;
                                Some(Offset::new(begin, end))
                            } else {
                                None
                            };
                        SelectorBuilder::AnnotationSelector(
                            AnyId::Id(annotation.to_string()),
                            offset,
                        )
                    }
                    SelectorKind::ResourceSelector => {
                        let resource = self.targetresource;
                        SelectorBuilder::ResourceSelector(AnyId::Id(resource.to_string()))
                    }
                    SelectorKind::DataSetSelector => {
                        let dataset = self.targetdataset;
                        SelectorBuilder::DataSetSelector(AnyId::Id(dataset.to_string()))
                    }
                    _ => unreachable!(),
                }
            } else {
                let targetresources: SmallVec<[&str; 1]> = self.targetresource.split(";").collect();
                let targetdatasets: SmallVec<[&str; 1]> = self.targetdataset.split(";").collect();
                let targetannotations: SmallVec<[&str; 1]> =
                    self.targetannotation.split(";").collect();
                let beginoffsets: SmallVec<[&str; 1]> = self.begin.split(";").collect();
                let endoffsets: SmallVec<[&str; 1]> = self.end.split(";").collect();
                let mut maxlen = selectortypes.len();
                if targetresources.len() > maxlen {
                    maxlen = targetresources.len()
                };
                if targetdatasets.len() > maxlen {
                    maxlen = targetdatasets.len()
                };
                if targetannotations.len() > maxlen {
                    maxlen = targetannotations.len()
                };
                if beginoffsets.len() > maxlen {
                    maxlen = beginoffsets.len()
                };
                if endoffsets.len() > maxlen {
                    maxlen = endoffsets.len()
                };
                let mut subselectors = Vec::new();
                for i in 1..maxlen {
                    //first one can be skipped because it's the complex selector
                    subselectors.push(match selectortypes.get(i).unwrap_or(selectortypes.last().unwrap()) {
                        SelectorKind::TextSelector => {
                            let resource = targetresources.get(i).unwrap_or(targetresources.last().unwrap());
                            if resource.is_empty() {
                                return Err(StamError::CsvError(
                                format!(
                                    "No resource specified for subselector #{}", i
                                ),
                                "TextSelector",
                                ));
                            }
                            let begin: Cursor = beginoffsets.get(i).ok_or_else(|| StamError::CsvError(
                                format!(
                                    "No begin offset found for subselector #{}", i
                                ),
                                "TextSelector",
                            ))?.deref().try_into()?;
                            let end: Cursor = endoffsets.get(i).ok_or_else(|| StamError::CsvError(
                                format!(
                                    "No end offset found for subselector #{}", i
                                ),
                                "TextSelector",
                            ))?.deref().try_into()?;
                            SelectorBuilder::TextSelector(
                                AnyId::Id(resource.to_string()),
                                Offset::new(begin, end),
                            )
                        }
                        SelectorKind::AnnotationSelector => {
                            let annotation = targetannotations.get(i).unwrap_or(targetannotations.last().unwrap());
                            if annotation.is_empty() {
                                return Err(StamError::CsvError(
                                format!(
                                    "No annotation specified for subselector #{}", i
                                ),
                                "AnnotationSelector",
                                ));
                            }
                            let offset: Option<Offset> = if beginoffsets.get(i).is_some() && !beginoffsets.get(i).unwrap().is_empty() {
                                if endoffsets.get(i).is_none() && !endoffsets.get(i).unwrap().is_empty() {
                                    return Err(StamError::CsvError(
                                    format!(
                                        "No end offset specified for subselector #{}", i
                                    ),
                                    "AnnotationSelector",
                                    ));
                                }
                                let begin: Cursor = beginoffsets.get(i).unwrap().deref().try_into()?;
                                let end: Cursor = endoffsets.get(i).unwrap().deref().try_into()?;
                                Some(Offset::new(begin, end))
                            } else {
                                None
                            };
                            SelectorBuilder::AnnotationSelector(
                                AnyId::Id(annotation.to_string()),
                                offset,
                            )
                        }
                        SelectorKind::ResourceSelector => {
                            let resource = targetresources.get(i).unwrap_or(targetresources.last().unwrap());
                            if resource.is_empty() {
                                return Err(StamError::CsvError(
                                format!(
                                    "No resource specified for subselector #{}", i
                                ),
                                "ResourceSelector",
                                ));
                            }
                            SelectorBuilder::ResourceSelector(AnyId::Id(resource.to_string()))
                        }
                        SelectorKind::DataSetSelector => {
                            let dataset = targetdatasets.get(i).unwrap_or(targetdatasets.last().unwrap());
                            if dataset.is_empty() {
                                return Err(StamError::CsvError(
                                format!(
                                    "No dataset specified for subselector #{}", i
                                ),
                                "DataSetSelector",
                                ));
                            }
                            SelectorBuilder::DataSetSelector(AnyId::Id(dataset.to_string()))
                        }
                        x =>
                            return Err(StamError::CsvError(
                                format!(
                                    "SelectorType can't be a subselector under a complex selector: {:?}", x
                                ),
                                "",
                            ))
                    });
                }

                match selectortypes[0] {
                    SelectorKind::CompositeSelector => {
                        SelectorBuilder::CompositeSelector(subselectors)
                    }
                    SelectorKind::MultiSelector => SelectorBuilder::MultiSelector(subselectors),
                    SelectorKind::DirectionalSelector => {
                        SelectorBuilder::DirectionalSelector(subselectors)
                    }
                    _ => unreachable!(),
                }
            };
            builder = builder.with_target(selectorbuilder);
        }
        Ok(builder)
    }
}

impl AnnotationStoreBuilder {
    fn from_csv_annotations_reader(
        &mut self,
        reader: Box<dyn BufRead>,
        config: &Config,
    ) -> Result<(), StamError> {
        debug(&config, || {
            format!("AnnotationStoreBuilder::from_csv_annotations_reader")
        });
        let mut reader = csv::Reader::from_reader(reader);
        for result in reader.deserialize() {
            let record: AnnotationCsv = result
                .map_err(|e| StamError::CsvError(format!("{}", e), "while parsing Annotation"))?;
            self.annotations.push(record.try_into()?);
        }
        Ok(())
    }
}

#[sealed]
impl<'a> FromCsv<'a> for AnnotationStoreBuilder {
    fn from_csv_reader(
        reader: Box<dyn BufRead>,
        _filename: Option<&str>,
        config: Config,
    ) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationStoreBuilder::from_csv_reader: processing store manifest")
        });
        let mut reader = csv::Reader::from_reader(reader);
        let mut first = true;
        let mut builder = AnnotationStoreBuilder::default();
        for result in reader.deserialize() {
            let record: StoreManifestCsv = result.map_err(|e| {
                StamError::CsvError(format!("{}", e), "while parsing AnnotationStore manifest")
            })?;
            if first {
                if record.tp != Type::AnnotationStore {
                    return Err(StamError::CsvError(
                        format!("First row of CSV Store Manifest must have type AnnotationStore, got {} instead", record.tp),
                        "",
                    ));
                }
                builder.id = record.id.map(|x| x.to_string());
                builder.annotations_filename = Some(PathBuf::from(record.filename.to_string()));
            } else {
                match record.tp {
                    Type::AnnotationDataSet => {
                        debug(&config, || {
                            format!(
                                "AnnotationStoreBuilder::from_csv_reader: processing dataset {}",
                                record.filename
                            )
                        });
                        let setbuilder = AnnotationDataSetBuilder::from_csv_file(
                            &record.filename,
                            config.clone(),
                        )?;
                        builder.annotationsets.push(setbuilder);
                    }
                    Type::TextResource => {
                        debug(&config, || {
                            format!("AnnotationStoreBuilder::from_csv_reader: processing textresource {}", record.filename)
                        });
                        let mut resourcebuilder =
                            TextResourceBuilder::from_txt_file(&record.filename, config.clone())?;
                        if record.id.is_some() {
                            resourcebuilder =
                                resourcebuilder.with_id(record.id.map(|x| x.to_string()).unwrap());
                        }
                        builder.resources.push(resourcebuilder);
                    }
                    Type::AnnotationStore => {
                        return Err(StamError::CsvError(
                        format!("There may be only one record with type AnnotationStore (the first one)"),
                        "",
                    ));
                    }
                    _ => {
                        return Err(StamError::CsvError(
                            format!(
                                "CSV Store Manifest Record has unsupported type {}",
                                record.tp
                            ),
                            "",
                        ));
                    }
                }
            }
            first = false;
        }
        debug(&config, || {
            format!("AnnotationStoreBuilder::from_csv_reader: finished processing store manifest")
        });
        if builder.annotations_filename.is_some() {
            let filename = builder.annotations_filename.as_ref().unwrap();
            let filename = filename.to_str().expect("valid utf-8");
            debug(&config, || {
                format!(
                    "AnnotationStoreBuilder::from_csv_reader: processing annotations {}",
                    filename
                )
            });
            let reader = open_file_reader(filename, &config)?;
            builder.from_csv_annotations_reader(reader, &config)?;
        }
        debug(&config, || {
            format!("AnnotationStoreBuilder::from_csv_reader: finished processing annotations, entire builder ready, returning")
        });
        Ok(builder)
    }
}

#[sealed]
impl<'a> FromCsv<'a> for AnnotationDataSetBuilder {
    fn from_csv_reader(
        reader: Box<dyn BufRead>,
        filename: Option<&str>,
        config: Config,
    ) -> Result<Self, StamError> {
        let mut reader = csv::Reader::from_reader(reader);
        let mut keys: Vec<DataKey> = Vec::new();
        let mut databuilders: Vec<AnnotationDataBuilder> = Vec::new();
        for result in reader.deserialize() {
            let record: AnnotationDataCsv = result.map_err(|e| {
                StamError::CsvError(format!("{}", e), "while parsing AnnotationDataSet")
            })?;
            if (record.id.is_none() || record.id.as_ref().unwrap().is_empty())
                && !record.key.is_empty()
                && record.value.is_empty()
            {
                keys.push(DataKey::new(record.key.to_string()));
            } else {
                databuilders.push(AnnotationDataBuilder {
                    id: if record.id.is_none() || record.id.as_ref().unwrap().is_empty() {
                        AnyId::None
                    } else {
                        AnyId::Id(record.id.as_ref().unwrap().to_string())
                    },
                    key: if record.key.is_empty() {
                        AnyId::None //will produce an error later on
                    } else {
                        AnyId::Id(record.key.to_string())
                    },
                    annotationset: AnyId::None, //we're confined to a single set so don't need this
                    value: record.value.into(),
                });
            }
        }
        Ok(AnnotationDataSetBuilder {
            id: None, //determined by StoreManifest rather than the set itself
            keys: Some(keys),
            data: Some(databuilders),
            filename: filename.map(|x| x.to_string()),
            config,
            mode: SerializeMode::NoInclude,
        })
    }
}
