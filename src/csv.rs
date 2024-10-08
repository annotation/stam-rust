/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module implements serialisation and deserialisation to CSV, as defined in the
//! [STAM CSV Extension](https://github.com/annotation/stam/tree/master/extensions/stam-csv).
//!
//! Being a STAM extension, this module is implemented as an extra feature and may be enabled/disabled
//! at compile time.

use csv;
use sealed::sealed;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::io::{BufRead, BufWriter};
use std::ops::Deref;
use std::path::PathBuf;

use crate::file::*;
use crate::store::*;
use crate::types::*;
use crate::{
    Annotation, AnnotationBuilder, AnnotationDataBuilder, AnnotationDataSet, AnnotationStore,
    Config, Configurable, DataKey, Offset, Selector, SelectorBuilder, SelectorKind, StamError,
    TextResource, TextResourceBuilder, AnnotationData
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
    #[serde(rename = "TargetKey")]
    targetkey: Option<Cow<'a, str>>,
    #[serde(rename = "TargetData")]
    targetdata: Option<Cow<'a, str>>,
}

impl<'a> AnnotationCsv<'a> {
    fn set_selectortype(selector: &Selector, store: &AnnotationStore) -> Cow<'a, str> {
        if selector.is_complex() {
            let mut selectortype: String = selector.kind().as_str().to_string();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    selectortype.push(';'); //delimiter
                    if let Selector::RangedTextSelector { .. }
                    | Selector::RangedAnnotationSelector { .. } = subselector
                    {
                        for (i, subselector) in subselector.iter(store, false).enumerate() {
                            if i > 0 {
                                selectortype.push(';');
                            }
                            selectortype += subselector.as_ref().kind().as_str();
                        }
                    } else {
                        selectortype += subselector.kind().as_str();
                    }
                }
            }
            Cow::Owned(selectortype)
        } else {
            Cow::Borrowed(selector.kind().as_str())
        }
    }

    fn set_beginoffset(selector: &Selector, store: &AnnotationStore) -> String {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    if let Selector::RangedTextSelector { .. }
                    | Selector::RangedAnnotationSelector { .. } = subselector
                    {
                        for (i, subselector) in subselector.iter(store, false).enumerate() {
                            if i > 0 {
                                out.push(';');
                            }
                            out += Self::set_beginoffset(subselector.as_ref(), store).as_str();
                        }
                    } else {
                        out += Self::set_beginoffset(subselector, store).as_str();
                    }
                }
            }
            out
        } else {
            if let Some(offset) = selector.offset(store) {
                format!("{}", offset.begin)
            } else {
                String::new()
            }
        }
    }

    fn set_endoffset(selector: &Selector, store: &AnnotationStore) -> String {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    if let Selector::RangedTextSelector { .. }
                    | Selector::RangedAnnotationSelector { .. } = subselector
                    {
                        for (i, subselector) in subselector.iter(store, false).enumerate() {
                            if i > 0 {
                                out.push(';');
                            }
                            out += Self::set_endoffset(subselector.as_ref(), store).as_str();
                        }
                    } else {
                        out += Self::set_endoffset(subselector, store).as_str();
                    }
                }
            }
            out
        } else {
            if let Some(offset) = selector.offset(store) {
                format!("{}", offset.end)
            } else {
                String::new()
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
                        Selector::RangedTextSelector { .. } => {
                            for (i, subselector) in subselector.iter(store, false).enumerate() {
                                if i > 0 {
                                    out.push(';');
                                }
                                out += &Self::set_targetresource(subselector.as_ref(), store);
                            }
                        }
                        Selector::ResourceSelector(res) | Selector::TextSelector(res, _, _) => {
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
                Selector::ResourceSelector(res) | Selector::TextSelector(res, _, _) => {
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
                        Selector::DataSetSelector(dataset) | Selector::DataKeySelector(dataset, _) | Selector::AnnotationDataSelector(dataset, _) => {
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
                Selector::DataSetSelector(dataset) | Selector::DataKeySelector(dataset, _) | Selector::AnnotationDataSelector(dataset, _) => {
                    let dataset: &AnnotationDataSet =
                        store.get(*dataset).expect("dataset must exist");
                    if let Some(id) = dataset.id() {
                        Cow::Borrowed(id)
                    } else {
                        Cow::Owned(dataset.temp_id().expect("temp_id must succeed"))
                    }
                }
                _ => Cow::Borrowed(""),
            }
        }
    }

    fn set_targetkey(selector: &Selector, store: &'a AnnotationStore) -> Cow<'a, str> {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    match subselector {
                        Selector::DataKeySelector(dataset, key) => {
                            let dataset: &AnnotationDataSet =
                                store.get(*dataset).expect("dataset must exist");
                            let key: &DataKey =
                                dataset.get(*key).expect("key must exist");
                            out += key.id().expect("key must have an id");
                        }
                        _ => {}
                    }
                }
            }
            Cow::Owned(out)
        } else {
            match selector {
                Selector::DataKeySelector(dataset, key) => {
                    let dataset: &AnnotationDataSet =
                        store.get(*dataset).expect("dataset must exist");
                    let key: &DataKey =
                        dataset.get(*key).expect("key must exist");
                    if let Some(id) = key.id() {
                        Cow::Borrowed(id)
                    } else {
                        Cow::Owned(key.temp_id().expect("temp_id must succeed"))
                    }
                }
                _ => Cow::Borrowed(""),
            }
        }
    }

    fn set_targetdata(selector: &Selector, store: &'a AnnotationStore) -> Cow<'a, str> {
        if selector.is_complex() {
            let mut out: String = String::new();
            if let Some(subselectors) = selector.subselectors() {
                for subselector in subselectors {
                    out.push(';'); //delimiter
                    match subselector {
                        Selector::AnnotationDataSelector(dataset, data) => {
                            let dataset: &AnnotationDataSet =
                                store.get(*dataset).expect("dataset must exist");
                            let data: &AnnotationData =
                                dataset.get(*data).expect("key must exist");
                            if let Some(id) = data.id() {
                                out += id;
                            } else {
                                out += data.temp_id().expect("temp_id must succeed").as_str();
                            }
                        }
                        _ => {}
                    }
                }
            }
            Cow::Owned(out)
        } else {
            match selector {
                Selector::AnnotationDataSelector(dataset, data) => {
                    let dataset: &AnnotationDataSet =
                        store.get(*dataset).expect("dataset must exist");
                    let data: &AnnotationData =
                        dataset.get(*data).expect("key must exist");
                    if let Some(id) = data.id() {
                        Cow::Borrowed(id)
                    } else {
                        Cow::Owned(data.temp_id().expect("temp_id must succeed"))
                    }
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
                        Selector::RangedAnnotationSelector { .. } => {
                            for (i, subselector) in subselector.iter(store, false).enumerate() {
                                if i > 0 {
                                    out.push(';');
                                }
                                out += &Self::set_targetannotation(subselector.as_ref(), store);
                            }
                        }
                        Selector::AnnotationSelector(ann, _) => {
                            let ann: &Annotation = store.get(*ann).expect("annotation must exist");
                            if let Some(id) = ann.id() {
                                out += id;
                            } else {
                                out += &ann.temp_id().expect("temp_id must succeed");
                            }
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
                    if let Some(id) = ann.id() {
                        Cow::Borrowed(id)
                    } else {
                        Cow::Owned(ann.temp_id().expect("temp_id must succeed"))
                    }
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
        let basename = strip_known_extension(filename);
        debug(self.config(), || {
            format!("{}.to_csv_files: filename={:?}, basename={:?}", Self::typeinfo(), filename, basename)
        });
        self.to_csv_file(filename, self.config(), CsvTable::StoreManifest)?;
        self.to_csv_file(
            self.annotations_filename()
                .map(|x| x.to_str().expect("valid utf-8").to_owned())
                .unwrap_or_else(|| format!("{}.annotation.csv", basename))
                .as_str(),
            self.config(),
            CsvTable::Annotation,
        )?;
        for dataset in self.datasets().map(|x| x.as_ref()) {
            if dataset.changed() {
                debug(self.config(), || {
                    format!(
                        "{}.to_csv_files: writing AnnotationDataSet",
                        Self::typeinfo()
                    )
                });
                dataset.to_csv_file(
                    dataset
                        .filename()
                        .map(|x| x.to_owned())
                        .unwrap_or_else(|| {
                            // no filename is associated with the resource yet, try to infer one from the ID
                            let basename =
                                sanitize_id_to_filename(self.id().expect("resource must have id"));
                            format!("{}.annotationset.stam.csv", basename)
                        })
                        .as_str(),
                    dataset.config(),
                    CsvTable::AnnotationDataSet,
                )?;
                if dataset.filename().is_some() {
                    dataset.mark_unchanged();
                }
            }
        }
        for resource in self.resources().map(|x| x.as_ref()) {
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
    Self: TypeInfo + Configurable,
{
    /// Writes CSV output, for the specified table, to the writer
    fn to_csv_writer<W>(&self, writer: W, table: CsvTable) -> Result<(), StamError>
    where
        W: std::io::Write,
    {
        let mut writer = csv::Writer::from_writer(writer);

        //create a new config config for derived resources, so filenames are outputted correctly
        let new_config = self.new_config();

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
                        filename: Cow::Borrowed(filename_without_workdir(
                            &self
                                .annotations_filename()
                                .map(|x| Cow::Borrowed(x.to_str().expect("valid utf-8 filename")))
                                .unwrap(),
                            &new_config,
                        )),
                    })
                    .map_err(|e| {
                        StamError::SerializationError(format!("Failure serializing CSV: {:?}", e))
                    })?;
                for dataset in self.datasets().map(|x| x.as_ref()) {
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
                            id: dataset.id().map(|x| Cow::Borrowed(x)),
                            filename: dataset
                                .filename_without_workdir()
                                .map(|x| Cow::Borrowed(x))
                                .unwrap(),
                        })
                        .map_err(|e| {
                            StamError::SerializationError(format!(
                                "Failure serializing CSV: {:?}",
                                e
                            ))
                        })?;
                }
                for resource in self.resources().map(|x| x.as_ref()) {
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
                            filename: resource
                                .filename_without_workdir()
                                .map(|x| Cow::Borrowed(x))
                                .unwrap(),
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
                    let out = if annotation.as_ref().len() == 0 {
                        AnnotationCsv {
                            id: if let Some(id) = annotation.id() {
                                Some(Cow::Borrowed(id))
                            } else {
                                Some(Cow::Owned(
                                    annotation.as_ref().temp_id().expect("temp id must succeed"),
                                ))
                            },
                            data_ids: Cow::Borrowed(""),
                            set_ids: Cow::Borrowed(""),
                            selectortype: AnnotationCsv::set_selectortype(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetdataset: AnnotationCsv::set_targetdataset(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetresource: AnnotationCsv::set_targetresource(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetannotation: AnnotationCsv::set_targetannotation(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetkey: Some(AnnotationCsv::set_targetkey(
                                annotation.as_ref().target(),
                                self,
                            )),
                            targetdata: Some(AnnotationCsv::set_targetdata(
                                annotation.as_ref().target(),
                                self,
                            )),
                            begin: AnnotationCsv::set_beginoffset(
                                annotation.as_ref().target(),
                                self,
                            ),
                            end: AnnotationCsv::set_endoffset(annotation.as_ref().target(), self),
                        }
                    } else if annotation.as_ref().len() == 1 {
                        //only one data item, we needn't make any copies
                        let data = annotation.data().next().unwrap();
                        let set = data.store();
                        AnnotationCsv {
                            id: annotation.id().map(|x| Cow::Borrowed(x)),
                            data_ids: if let Some(id) = data.id() {
                                Cow::Borrowed(id)
                            } else {
                                Cow::Owned(data.as_ref().temp_id().expect("temp id must succeed"))
                            },
                            set_ids: if let Some(id) = set.id() {
                                Cow::Borrowed(id)
                            } else {
                                Cow::Owned(set.temp_id().expect("temp id must succeed"))
                            },
                            selectortype: AnnotationCsv::set_selectortype(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetdataset: AnnotationCsv::set_targetdataset(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetresource: AnnotationCsv::set_targetresource(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetannotation: AnnotationCsv::set_targetannotation(
                                annotation.as_ref().target(),
                                self,
                            ),
                            begin: AnnotationCsv::set_beginoffset(
                                annotation.as_ref().target(),
                                self,
                            ),
                            end: AnnotationCsv::set_endoffset(annotation.as_ref().target(), self),
                            targetkey: Some(AnnotationCsv::set_targetkey(
                                annotation.as_ref().target(),
                                self,
                            )),
                            targetdata: Some(AnnotationCsv::set_targetdata(
                                annotation.as_ref().target(),
                                self,
                            )),
                        }
                    } else {
                        let mut data_ids = String::new();
                        let mut set_ids = String::new();
                        //get data via high-level method
                        for data in annotation.data() {
                            let set = data.store();
                            if !data_ids.is_empty() {
                                data_ids.push(';');
                                set_ids.push(';');
                            }
                            if let Some(id) = data.id() {
                                data_ids += id;
                            } else {
                                data_ids += &data.as_ref().temp_id().expect("temp id must succeed");
                            };
                            if let Some(id) = set.id() {
                                set_ids += id;
                            } else {
                                set_ids += &set.temp_id().expect("temp id must succeed");
                            };
                        }
                        AnnotationCsv {
                            id: annotation.id().map(|x| Cow::Borrowed(x)),
                            data_ids: Cow::Owned(data_ids),
                            set_ids: Cow::Owned(set_ids),
                            selectortype: AnnotationCsv::set_selectortype(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetdataset: AnnotationCsv::set_targetdataset(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetresource: AnnotationCsv::set_targetresource(
                                annotation.as_ref().target(),
                                self,
                            ),
                            targetannotation: AnnotationCsv::set_targetannotation(
                                annotation.as_ref().target(),
                                self,
                            ),
                            begin: AnnotationCsv::set_beginoffset(
                                annotation.as_ref().target(),
                                self,
                            ),
                            end: AnnotationCsv::set_endoffset(annotation.as_ref().target(), self),
                            targetkey: Some(AnnotationCsv::set_targetkey(
                                annotation.as_ref().target(),
                                self,
                            )),
                            targetdata: Some(AnnotationCsv::set_targetdata(
                                annotation.as_ref().target(),
                                self,
                            )),
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
    fn to_csv_writer<W>(&self, writer: W, _table: CsvTable) -> Result<(), StamError>
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
            let key = self.get(data.key())?;
            writer
                .serialize(AnnotationDataCsv {
                    id: if let Some(id) = data.id() {
                        Some(Cow::Borrowed(id))
                    } else {
                        Some(Cow::Owned(data.temp_id().expect("temp id must succeed")))
                    },
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
pub trait FromCsv
where
    Self: TypeInfo + Sized,
{
    fn from_csv_reader(
        reader: Box<dyn BufRead>,
        filename: Option<&str>,
        config: Config,
    ) -> Result<Self, StamError>;

    fn from_csv_file(filename: &str, config: Config) -> Result<Self, StamError> {
        debug(&config, || {
            format!(
                "{}::from_csv_file: filename={}, config={:?}",
                Self::typeinfo(),
                filename,
                config
            )
        });
        let reader = open_file_reader(filename, &config)?;
        Self::from_csv_reader(reader, Some(filename), config)
    }
}

impl<'a, 'b> TryInto<AnnotationBuilder<'a>> for AnnotationCsv<'a> {
    type Error = StamError;
    fn try_into(self) -> Result<AnnotationBuilder<'a>, Self::Error> {
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
                //MAYBE TODO: fix the warnings that arise more elegantly
                #[allow(suspicious_double_ref_op)]
                let set_id = set_ids.get(i).unwrap_or(set_ids.last().unwrap()).deref();
                builder = builder.with_existing_data(
                    BuildItem::from(set_id.to_owned()), //had to make it owned because of borrow checker, would rather have kept reference
                    BuildItem::from(data_id.to_owned()),
                );
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
                if self.targetkey.unwrap_or(Cow::Borrowed("")).find(";").is_some() {
                    return Err(StamError::CsvError(
                        format!("Multiple target keys were specified, but without a complex selector"),
                        "",
                    ));
                }
                if self.targetdata.unwrap_or(Cow::Borrowed("")).find(";").is_some() {
                    return Err(StamError::CsvError(
                        format!("Multiple target data were specified, but without a complex selector"),
                        "",
                    ));
                }
                match selectortypes[0] {
                    SelectorKind::TextSelector => {
                        let resource = self.targetresource;
                        let begin: Cursor = self.begin.as_str().try_into()?;
                        let end: Cursor = self.end.as_str().try_into()?;
                        SelectorBuilder::TextSelector(
                            BuildItem::Id(resource.to_string()),
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
                            BuildItem::Id(annotation.to_string()),
                            offset,
                        )
                    }
                    SelectorKind::ResourceSelector => {
                        let resource = self.targetresource;
                        SelectorBuilder::ResourceSelector(BuildItem::Id(resource.to_string()))
                    }
                    SelectorKind::DataSetSelector => {
                        let dataset = self.targetdataset;
                        SelectorBuilder::DataSetSelector(BuildItem::Id(dataset.to_string()))
                    }
                    _ => unreachable!(),
                }
            } else {
                let targetresources: SmallVec<[&str; 1]> = self.targetresource.split(";").collect();
                let targetdatasets: SmallVec<[&str; 1]> = self.targetdataset.split(";").collect();
                let targetannotations: SmallVec<[&str; 1]> =
                    self.targetannotation.split(";").collect();
                let targetkeys: SmallVec<[&str; 1]> = if let Some(targetkey) = self.targetkey.as_ref() {
                    targetkey.split(";").collect()
                } else {
                    SmallVec::new()
                };
                let targetdata: SmallVec<[&str; 1]> = if let Some(targetdata) = self.targetdata.as_ref() {
                    targetdata.split(";").collect()
                } else {
                    SmallVec::new()
                };
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
                if targetkeys.len() > maxlen {
                    maxlen = targetkeys.len()
                };
                if targetdata.len() > maxlen {
                    maxlen = targetdata.len()
                };
                let mut subselectors = Vec::new();
                //MAYBE TODO: fix the warnings that arise more elegantly
                #[allow(suspicious_double_ref_op)]
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
                                BuildItem::Id(resource.to_string()),
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
                                BuildItem::Id(annotation.to_string()),
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
                            SelectorBuilder::ResourceSelector(BuildItem::Id(resource.to_string()))
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
                            SelectorBuilder::DataSetSelector(BuildItem::Id(dataset.to_string()))
                        }
                        SelectorKind::DataKeySelector  => {
                            let dataset = targetdatasets.get(i).unwrap_or(targetdatasets.last().unwrap());
                            let datakey = targetkeys.get(i).unwrap_or(targetkeys.last().unwrap());
                            if dataset.is_empty() {
                                return Err(StamError::CsvError(
                                format!(
                                    "No dataset specified for subselector #{}", i
                                ),
                                "DataKeySelector",
                                ));
                            }
                            SelectorBuilder::DataKeySelector(BuildItem::Id(dataset.to_string()), BuildItem::Id(datakey.to_string()))
                        }
                        SelectorKind::AnnotationDataSelector  => {
                            let dataset = targetdatasets.get(i).unwrap_or(targetdatasets.last().unwrap());
                            let data = targetdata.get(i).unwrap_or(targetdata.last().unwrap());
                            if dataset.is_empty() {
                                return Err(StamError::CsvError(
                                format!(
                                    "No dataset specified for subselector #{}", i
                                ),
                                "AnnotationDataSelector",
                                ));
                            }
                            SelectorBuilder::AnnotationDataSelector(BuildItem::Id(dataset.to_string()), BuildItem::Id(data.to_string()))
                        }
                        SelectorKind::MultiSelector | SelectorKind::CompositeSelector | SelectorKind::DirectionalSelector =>
                            return Err(StamError::CsvError(
                                format!(
                                    "Complex selectors can't be a subselector under a complex selector"
                                ),
                                "",
                            )),
                        SelectorKind::InternalRangedSelector => 
                            return Err(StamError::CsvError(
                                format!(
                                    "Internal ranged selectors should have been resolved at this stage"
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

impl AnnotationStore {
    fn from_csv_annotations_reader(&mut self, reader: Box<dyn BufRead>) -> Result<(), StamError> {
        debug(self.config(), || {
            format!("AnnotationStore::from_csv_annotations_reader")
        });
        let mut reader = csv::Reader::from_reader(reader);
        for result in reader.deserialize() {
            let record: AnnotationCsv = result
                .map_err(|e| StamError::CsvError(format!("{}", e), "while parsing Annotation"))?;
            self.annotate(record.try_into()?)?;
        }
        Ok(())
    }
}

#[sealed]
impl FromCsv for AnnotationStore {
    fn from_csv_reader(
        reader: Box<dyn BufRead>,
        filename: Option<&str>,
        config: Config,
    ) -> Result<Self, StamError> {
        debug(&config, || {
            format!("AnnotationStore::from_csv_reader: processing store manifest")
        });
        let mut reader = csv::Reader::from_reader(reader);
        let mut first = true;
        let mut store = if let Some(filename) = filename {
            AnnotationStore::new(config).with_filename(filename)
        } else {
            AnnotationStore::new(config)
        };
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
                store.id = record.id.map(|x| x.to_string());
                store.annotations_filename = Some(PathBuf::from(record.filename.to_string()));
            } else {
                match record.tp {
                    Type::AnnotationDataSet => {
                        debug(store.config(), || {
                            format!(
                                "AnnotationStore::from_csv_reader: processing dataset {}",
                                record.filename
                            )
                        });
                        let mut dataset = AnnotationDataSet::from_csv_file(
                            &record.filename,
                            store.new_config(),
                        )?;
                        if record.id.is_some() {
                            dataset = dataset.with_id(record.id.map(|x| x.to_string()).unwrap());
                        }
                        store.insert(dataset)?;
                    }
                    Type::TextResource => {
                        debug(store.config(), || {
                            format!(
                                "AnnotationStore::from_csv_reader: processing textresource {}",
                                record.filename
                            )
                        });
                        let mut resourcebuilder = TextResourceBuilder::new().with_filename(record.filename);
                        if record.id.is_some() {
                            resourcebuilder =
                                resourcebuilder.with_id(record.id.map(|x| x.to_string()).unwrap());
                        }
                        store.add_resource(resourcebuilder)?;
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
        debug(store.config(), || {
            format!("AnnotationStore::from_csv_reader: finished processing store manifest, workdir={:?}", store.config().workdir())
        });
        if store.annotations_filename.is_some() {
            let filename = store.annotations_filename.as_ref().unwrap();
            let filename = filename.to_str().expect("valid utf-8");
            debug(store.config(), || {
                format!(
                    "AnnotationStore::from_csv_reader: processing annotations {}",
                    filename
                )
            });
            let new_config = store.new_config();
            let reader = open_file_reader(filename, &new_config)?;
            store.from_csv_annotations_reader(reader)?;
        }
        debug(store.config(), || {
            format!("AnnotationStore::from_csv_reader: finished processing annotations, entire builder ready, returning, ")
        });
        if let Some(filename) = filename {
            store.filename = Some(PathBuf::from(filename));
        }
        if store.config().shrink_to_fit {
            store.shrink_to_fit(true);
        }
        Ok(store)
    }
}

#[sealed]
impl FromCsv for AnnotationDataSet {
    fn from_csv_reader(
        reader: Box<dyn BufRead>,
        filename: Option<&str>,
        config: Config,
    ) -> Result<Self, StamError> {
        let mut reader = csv::Reader::from_reader(reader);
        let mut dataset = if let Some(filename) = filename {
            AnnotationDataSet::new(config).with_filename(filename)
        } else {
            AnnotationDataSet::new(config)
        };
        for result in reader.deserialize() {
            let record: AnnotationDataCsv = result.map_err(|e| {
                StamError::CsvError(format!("{}", e), "while parsing AnnotationDataSet")
            })?;
            if (record.id.is_none() || record.id.as_ref().unwrap().is_empty())
                && !record.key.is_empty()
                && record.value.is_empty()
            {
                dataset.insert(DataKey::new(record.key))?;
            } else {
                let builder = AnnotationDataBuilder {
                    id: if record.id.is_none() || record.id.as_ref().unwrap().is_empty() {
                        BuildItem::None
                    } else {
                        BuildItem::Id(record.id.as_ref().unwrap().to_string())
                    },
                    key: if record.key.is_empty() {
                        BuildItem::None //will produce an error later on
                    } else {
                        BuildItem::Id(record.key.to_string())
                    },
                    dataset: BuildItem::None, //we're confined to a single set so don't need this
                    value: record.value.into(), //TODO: does this mean we only deserialize String types??
                };
                dataset.build_insert_data(builder, false)?; //safety is off for faster parsing (data should not have duplicates)
            }
        }
        //Note: ID is determined by StoreManifest rather than the set itself and will be associated later
        Ok(dataset)
    }
}
