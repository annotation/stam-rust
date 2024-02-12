use crate::api::*;
use crate::AnnotationDataSet;
use crate::DataValue;
use crate::Selector;
use crate::TextResource;
use chrono::{DateTime, Local};

use nanoid::nanoid;
use std::borrow::Cow;

const CONTEXT_ANNO: &str = "http://www.w3.org/ns/anno.jsonld";
const NS_ANNO: &str = "http://www.w3.org/ns/anno/";

pub trait IRI<'store> {
    /// Return the identifier as an IRI, suitable to identify RDF resources
    /// This will apply some transformations if there are invalid characters in the ID
    /// A default prefix will be prepended if the identifier was not an IRI yet.
    fn iri(&self, default_prefix: &str) -> Option<Cow<'store, str>>;
}

impl<'store> IRI<'store> for ResultItem<'store, DataKey> {
    fn iri(&self, default_prefix: &str) -> Option<Cow<'store, str>> {
        Some(into_iri(
            self.id().expect("key must have an ID"),
            &self.set().iri(default_prefix).expect("set must have an ID"),
        ))
    }
}

impl<'store> IRI<'store> for ResultItem<'store, Annotation> {
    fn iri(&self, default_prefix: &str) -> Option<Cow<'store, str>> {
        self.id().map(|x| into_iri(x, default_prefix))
    }
}
impl<'store> IRI<'store> for ResultItem<'store, TextResource> {
    fn iri(&self, default_prefix: &str) -> Option<Cow<'store, str>> {
        self.id().map(|x| into_iri(x, default_prefix))
    }
}
impl<'store> IRI<'store> for ResultItem<'store, AnnotationDataSet> {
    fn iri(&self, default_prefix: &str) -> Option<Cow<'store, str>> {
        self.id().map(|x| into_iri(x, default_prefix))
    }
}

/// Tests if a character is valid or not in an IRI
fn invalid_in_iri(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\n' || c == '"'
}

/// Tests whether a string is a valid IRI
pub fn is_iri(s: &str) -> bool {
    if let Some(pos) = s.find(":") {
        if s.find(invalid_in_iri).is_some() {
            return false;
        }
        let scheme = &s[..pos];
        match scheme {
            "http" | "https" | "urn" | "file" | "_" => true,
            _ => false,
        }
    } else {
        false
    }
}

/// Transforms a string into an IRI, by prepending the prefix if necessary
fn into_iri<'a>(s: &'a str, mut prefix: &str) -> Cow<'a, str> {
    if is_iri(s) {
        Cow::Borrowed(s)
    } else {
        if prefix.is_empty() {
            prefix = "_:";
        }
        let separator = prefix.chars().last();
        if separator == Some('/') || separator == Some('#') || separator == Some(':') {
            Cow::Owned(format!(
                "{}{}",
                prefix,
                s.replace(invalid_in_iri, "-").as_str()
            ))
        } else {
            Cow::Owned(format!(
                "{}/{}",
                prefix,
                s.replace(invalid_in_iri, "-").as_str()
            ))
        }
    }
}

fn value_to_json(value: &DataValue) -> String {
    match value {
        DataValue::String(s) => format!("\"{}\"", s.replace("\n", "\\n").replace("\"", "\\\"")),
        x => x.to_string(),
    }
}

pub struct WebAnnoConfig {
    /// IRI prefix for Annotation Identifiers. Will be prepended if the annotations public ID is not an IRI yet.
    pub default_annotation_iri: String,

    /// Generate a random annotation IRI if it does not exist yet? (non-deterministic!)
    pub generate_annotation_iri: bool,

    /// IRI prefix for Annotation Data Sets. Will be prepended if the annotation data set public ID is not an IRI yet.
    pub default_set_iri: String,

    /// IRI prefix for Text Resources. Will be prepended if the resource public ID is not an IRI yet.
    pub default_resource_iri: String,

    /// Extra JSON-LD context to export, these must be URLs to JSONLD files.
    pub extra_context: Vec<String>,

    /// Automatically add a 'generated' triple for each annotation, with the timestamp of serialisation
    pub auto_generated: bool,

    /// Automatically add a 'generator' triple for each annotation, with the software details
    pub auto_generator: bool,
}

impl Default for WebAnnoConfig {
    fn default() -> Self {
        Self {
            default_annotation_iri: "_:".to_string(),
            generate_annotation_iri: false,
            default_set_iri: "_:".to_string(),
            default_resource_iri: "_:".to_string(),
            extra_context: Vec::new(),
            auto_generated: true,
            auto_generator: true,
        }
    }
}

impl<'store> ResultItem<'store, Annotation> {
    pub fn to_webannotation(&self, config: &WebAnnoConfig) -> String {
        if let Selector::AnnotationDataSelector(..) | Selector::DataKeySelector(..) =
            self.as_ref().target()
        {
            //these can not be serialized
            return String::new();
        }
        let mut ann_out = String::with_capacity(1024);
        ann_out += "{\n";
        ann_out += "  \"@context\": ";
        if !config.extra_context.is_empty() {
            ann_out += &format!(
                "[ \"{}\", {} ]",
                CONTEXT_ANNO,
                config.extra_context.join(", ")
            );
        } else {
            ann_out += &format!("\"{}\"", CONTEXT_ANNO);
        }
        ann_out += ",\n";
        if let Some(iri) = self.iri(&config.default_annotation_iri) {
            ann_out += &format!("  \"id\": \"{}\",\n", iri);
        } else if config.generate_annotation_iri {
            let id = nanoid!();
            ann_out += &format!(
                "  \"id\": \"{}\",\n",
                into_iri(&id, &config.default_annotation_iri)
            )
        }
        ann_out += "  \"type\": \"Annotation\",\n";

        let mut body_out = String::with_capacity(512);
        let mut suppress_default_body_type = false;
        let mut suppress_auto_generated = false;
        let mut suppress_auto_generator = false;
        //gather annotation properties (outside of body)
        for data in self.data() {
            let key = data.key();
            let key_id = key.id().expect("keys must have an ID");
            match data.set().id() {
                Some(CONTEXT_ANNO) | Some(NS_ANNO) => match key_id {
                    "generated" => {
                        suppress_auto_generated = true;
                        ann_out += &output_predicate_datavalue(key_id, data.value(), "  ");
                    }
                    "generator" => {
                        suppress_auto_generator = true;
                        ann_out += &output_predicate_datavalue(key_id, data.value(), "  ");
                    }
                    "motivation" | "created" | "creator" => {
                        ann_out += &output_predicate_datavalue(key_id, data.value(), "  ");
                    }
                    key_id => {
                        //go to body
                        if key_id == "type" {
                            suppress_default_body_type = true; //no need for the default because we provided one explicitly
                        }
                        ann_out += &output_predicate_datavalue(key_id, data.value(), "    ");
                    }
                },
                Some(_set_id) => {
                    let predicate = key
                        .iri(
                            &data
                                .set()
                                .iri(&config.default_set_iri)
                                .expect("set must have ID"),
                        )
                        .expect("key must have ID");
                    body_out += &output_predicate_datavalue(&predicate, data.value(), "    ");
                }
                None => unreachable!("all sets should have a public identifier"),
            }
        }

        if config.auto_generated && !suppress_auto_generated {
            ann_out += &format!("  \"generated\": \"{}\",\n", Local::now().to_rfc3339());
        }
        if config.auto_generator && !suppress_auto_generator {
            ann_out += "  \"generator\": {{ \"id\": \"https://github.com/annotation/stam-rust\", \"type\": \"Software\", \"name\": \"STAM Library\"  }},\n";
        }

        if !body_out.is_empty() {
            ann_out += "  \"body\": {\n";
            if !suppress_default_body_type {
                ann_out += "      \"type\": \"Dataset\",\n";
            }
            ann_out += &body_out;
            ann_out += "\n  },\n";
        }

        ann_out += "  \"target\": {\n";
        ann_out += &output_selector(self.as_ref().target(), self.store(), config, false);
        ann_out += "\n  }\n";

        ann_out += "}";
        ann_out
    }
}

fn output_predicate_datavalue(predicate: &str, datavalue: &DataValue, indentation: &str) -> String {
    let value_is_iri = if let DataValue::String(s) = datavalue {
        is_iri(s)
    } else {
        false
    };
    if value_is_iri {
        // Any String value that is a valid IRI *SHOULD* be interpreted as such
        // in conversion from/to RDF.
        format!(
            "{}\"{}\": {{ \"id\": \"{}\" }},\n",
            indentation, predicate, datavalue
        )
    } else {
        format!(
            "{}\"{}\": {},\n",
            indentation,
            predicate,
            &value_to_json(datavalue)
        )
    }
}

fn output_selector(
    selector: &Selector,
    store: &AnnotationStore,
    config: &WebAnnoConfig,
    nested: bool,
) -> String {
    let mut ann_out = String::new();
    match selector {
        Selector::TextSelector(res_handle, tsel_handle, _)
        | Selector::AnnotationSelector(_, Some((res_handle, tsel_handle, _))) => {
            let resource = store.resource(*res_handle).expect("resource must exist");
            let textselection = resource
                .as_ref()
                .get(*tsel_handle)
                .expect("text selection must exist");
            ann_out += &format!(
                "      \"source\": \"{}\",\n    \"selector\": {{\n      \"type\": \"TextPositionSelector\",\n      \"start\": {},\n      \"end\": {}\n    }}",
                into_iri(
                    resource.id().expect("resource must have ID"),
                    &config.default_resource_iri
                ),
                textselection.begin(),
                textselection.end(),
            );
        }
        Selector::AnnotationSelector(a_handle, None) => {
            let annotation = store.annotation(*a_handle).expect("annotation must exist");
            if let Some(iri) = annotation.iri(&config.default_annotation_iri) {
                ann_out += &format!(
                    "      \"id\": \"{}\",\n    \"type\": \"Annotation\"\n    }}",
                    iri
                );
            } else {
                ann_out += "    \"id\": null }";
                eprintln!("WARNING: Annotation points to an annotation that has no public ID! Unable to serialize to Web Annotatations");
            }
        }
        Selector::ResourceSelector(res_handle) => {
            let resource = store.resource(*res_handle).expect("resource must exist");
            ann_out += &format!(
                "      \"id\": \"{}\",\n    \"type\": \"Text\"\n    }}",
                into_iri(
                    resource.id().expect("resource must have ID"),
                    &config.default_resource_iri
                ),
            );
        }
        Selector::DataSetSelector(set_handle) => {
            let dataset = store.dataset(*set_handle).expect("resource must exist");
            ann_out += &format!(
                "      \"id\": \"{}\",\n    \"type\": \"Dataset\"\n    }}",
                into_iri(
                    dataset.id().expect("dataset must have ID"),
                    &config.default_resource_iri
                ),
            );
        }
        Selector::CompositeSelector(selectors) => {
            ann_out += "      \"type\": \"http://www.w3.org/ns/oa#Composite\",\n    \"items\": [";
            for (i, selector) in selectors.iter().enumerate() {
                if i != selectors.len() - 1 {
                    ann_out += &output_selector(selector, store, config, true);
                    ann_out += ",\n";
                }
            }
            ann_out += " ] }";
        }
        Selector::MultiSelector(selectors) => {
            ann_out +=
                "      \"type\": \"http://www.w3.org/ns/oa#Independents\",\n    \"items\": [";
            for (i, selector) in selectors.iter().enumerate() {
                ann_out += &output_selector(selector, store, config, true);
                if i != selectors.len() - 1 {
                    ann_out += ",\n";
                }
            }
            ann_out += " ] }";
        }
        Selector::DirectionalSelector(selectors) => {
            ann_out += "      \"type\": \"http://www.w3.org/ns/oa#List\",\n    \"items\": [";
            for (i, selector) in selectors.iter().enumerate() {
                ann_out += &output_selector(selector, store, config, true);
                if i != selectors.len() - 1 {
                    ann_out += ",\n";
                }
            }
            ann_out += " ] }";
        }
        Selector::DataKeySelector(..) | Selector::AnnotationDataSelector(..) => {
            if nested {
                eprintln!("WARNING: DataKeySelector and AnnotationDataSelectors can not be serialized to Web Annotation, skipping!!");
            } else {
                unreachable!("DataKeySelector and AnnotationDataSelectors can not be serialized to Web Annotation (was tested earlier)");
            }
        }
        Selector::RangedTextSelector { .. } | Selector::RangedAnnotationSelector { .. } => {
            if nested {
                todo!("Serialisation of ranged selectors not implemented yet");
            } else {
                unreachable!(
                "Internal Ranged selectors can not be serialized directly, they can be serialized only when under a complex selector",
            );
            }
        }
    }
    ann_out
}
