use crate::api::*;
use crate::AnnotationDataSet;
use crate::DataValue;
use crate::Selector;
use crate::TextResource;
use chrono::Local;
use smallvec::{smallvec, SmallVec};

use nanoid::nanoid;
use std::borrow::Cow;

const CONTEXT_ANNO: &str = "http://www.w3.org/ns/anno.jsonld";
const NS_ANNO: &str = "http://www.w3.org/ns/anno/";
const NS_RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

pub trait IRI<'store> {
    /// Return the identifier as an IRI, suitable to identify RDF resources
    /// This will apply some transformations if there are invalid characters in the ID
    /// A default prefix will be prepended if the identifier was not an IRI yet.
    fn iri(&self, default_prefix: &str) -> Option<Cow<'store, str>>;
}

impl<'store> IRI<'store> for ResultItem<'store, DataKey> {
    fn iri(&self, default_set_prefix: &str) -> Option<Cow<'store, str>> {
        Some(into_iri(
            self.id().expect("key must have an ID"),
            &self
                .set()
                .iri(default_set_prefix)
                .expect("set must have an ID"),
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
        DataValue::List(l) => {
            let mut json_out = "[".to_string();
            for (i, value) in l.iter().enumerate() {
                if i > 0 {
                    json_out.push(',');
                }
                json_out.push_str(&value_to_json(value));
            }
            json_out.push(']');
            json_out
        }
        DataValue::Map(m) => {
            let mut json_out = "{".to_string();
            for (i, (key, value)) in m.iter().enumerate() {
                if i > 0 {
                    json_out.push(',');
                }
                json_out.push_str(&format!("\"{}\": {}", key, value_to_json(value)));
            }
            json_out.push('}');
            json_out
        }
        x => x.to_string(),
    }
}

#[derive(Clone, Debug)]
pub struct WebAnnoConfig {
    /// IRI prefix for Annotation Identifiers. Will be prepended if the annotations public ID is not an IRI yet.
    pub default_annotation_iri: String,

    /// Generate a random annotation IRI if it does not exist yet? (non-deterministic!)
    pub generate_annotation_iri: bool,

    /// IRI prefix for Annotation Data Sets. Will be prepended if the annotation data set public ID is not an IRI yet.
    pub default_set_iri: String,

    /// IRI prefix for Text Resources. Will be prepended if the resource public ID is not an IRI yet.
    pub default_resource_iri: String,

    /// Extra JSON-LD context to export, these must be URLs to JSONLD files. The contexts you
    /// provide also double as possible STAM dataset IDs. Keys in these sets that are not full IRIs
    /// will then be copied as-is to the output (as alias rather than joined with the set ID to
    /// form a full IRI ), leaving interpretation it up to the JSON-LD context.
    pub extra_context: Vec<String>,

    /// Automatically add a 'generated' triple for each annotation, with the timestamp of serialisation
    pub auto_generated: bool,

    /// Automatically add a 'generator' triple for each annotation, with the software details
    pub auto_generator: bool,

    /// Automatically generate a JSON-LD context alias for all URIs in keys, maps URI prefixes to namespace prefixes
    pub context_namespaces: Vec<(String, String)>,

    /// Adds an extra targets alongside the usual target with TextPositionSelector. This can
    /// be used for provide a direct URL to fetch the exact textselection (if the backend system supports it).
    /// In the template, you should use the variables {resource_iri} (which is the resource IRI) or {resource} (which is the ID), {begin}, and {end} , they will be substituted accordingly.
    /// A common value is {resource_iri}/{begin}/{end} or https://example.com/{resource}/{begin}/{end}.
    pub extra_target_templates: Vec<String>,

    /// Do not output @context (useful if already done at an earlier stage)
    pub skip_context: bool,
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
            skip_context: false,
            context_namespaces: Vec::new(),
            extra_target_templates: Vec::new(),
        }
    }
}

impl WebAnnoConfig {
    pub fn with_namespace(mut self, prefix: String, uri: String) -> Self {
        self.context_namespaces.push((uri, prefix));
        self
    }

    /// converts a full URI to a compact form with a namespace prefix (if possible)
    pub fn uri_to_namespace<'a>(&self, s: Cow<'a, str>) -> Cow<'a, str> {
        for (uri_prefix, ns_prefix) in self.context_namespaces.iter() {
            if s.starts_with(uri_prefix) {
                return Cow::Owned(format!("{}:{}", ns_prefix, &s[uri_prefix.len()..]));
            }
        }
        s
    }

    /// Automatically add any datasets with IDs ending in `.jsonld` or `.jsonld` to the extra context list.
    pub fn auto_extra_context(mut self, store: &AnnotationStore) -> Self {
        for dataset in store.datasets() {
            if let Some(dataset_id) = dataset.id() {
                if (dataset_id.ends_with(".jsonld") || dataset_id.ends_with(".json"))
                    && is_iri(dataset_id)
                {
                    if self.extra_context.iter().all(|x| x != dataset_id) {
                        self.extra_context.push(dataset_id.to_string());
                    }
                }
            }
        }
        self
    }

    /// Generates a JSON-LD string to use for @context
    pub fn serialize_context(&self) -> String {
        let mut out = String::new();
        if !self.extra_context.is_empty() || !self.context_namespaces.is_empty() {
            out += "[ \"";
        } else {
            out += "\"";
        }
        out += CONTEXT_ANNO;
        out += "\"";
        for context in self.extra_context.iter() {
            if context != CONTEXT_ANNO {
                out += ", \"";
                out += context;
                out += "\"";
            }
        }
        if !self.context_namespaces.is_empty() {
            out += ", {";
            for (i, (uri, namespace)) in self.context_namespaces.iter().enumerate() {
                if i > 0 {
                    out += ", ";
                }
                out += "\"";
                out += namespace;
                out += "\": \"";
                out += uri;
                out += "\"";
            }
            out += "}";
        }
        if !self.extra_context.is_empty() || !self.context_namespaces.is_empty() {
            out += " ]";
        }
        out
    }
}

impl<'store> ResultItem<'store, Annotation> {
    /// Outputs the annotation as a W3C Web Annotation, the output will be JSON-LD on a single line without pretty formatting.
    pub fn to_webannotation(&self, config: &WebAnnoConfig) -> String {
        if let Selector::AnnotationDataSelector(..) | Selector::DataKeySelector(..) =
            self.as_ref().target()
        {
            //these can not be serialized
            return String::new();
        }
        let mut ann_out = String::with_capacity(1024);
        if config.skip_context {
            ann_out += "{ "
        } else {
            ann_out += "{ \"@context\": ";
            ann_out += &config.serialize_context();
            ann_out += ",";
            if let Some(iri) = self.iri(&config.default_annotation_iri) {
                ann_out += &format!("  \"id\": \"{}\",", iri);
            } else if config.generate_annotation_iri {
                let id = nanoid!();
                ann_out += &format!(
                    " \"id\": \"{}\",",
                    into_iri(&id, &config.default_annotation_iri)
                )
            }
        }
        ann_out += " \"type\": \"Annotation\",";

        let mut suppress_default_body_type = false;
        let mut suppress_body_id = false;
        let mut suppress_auto_generated = false;
        let mut suppress_auto_generator = false;
        let mut target_extra_out = String::new();

        let mut body_out = OutputMap::new();

        let mut outputted_to_main = false;
        //gather annotation properties (outside of body)
        for data in self.data() {
            let key = data.key();
            let key_id = key.id().expect("keys must have an ID");
            match data.set().id() {
                Some(CONTEXT_ANNO) | Some(NS_ANNO) => match key_id {
                    "generated" => {
                        if outputted_to_main {
                            ann_out.push(',');
                        }
                        suppress_auto_generated = true;
                        outputted_to_main = true;
                        ann_out += &output_predicate_datavalue(key_id, data.value(), config);
                    }
                    "generator" => {
                        if outputted_to_main {
                            ann_out.push(',');
                        }
                        suppress_auto_generator = true;
                        outputted_to_main = true;
                        ann_out += &output_predicate_datavalue(key_id, data.value(), config);
                    }
                    "motivation" | "created" | "creator" => {
                        if outputted_to_main {
                            ann_out.push(',');
                        }
                        outputted_to_main = true;
                        ann_out += &output_predicate_datavalue(key_id, data.value(), config);
                    }
                    "target" => {
                        if !target_extra_out.is_empty() {
                            target_extra_out.push(',');
                        }
                        target_extra_out += &value_to_json(data.value());
                    }
                    key_id => {
                        //other predicates -> go into body
                        if key_id == "type"
                            || key_id == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                        {
                            suppress_default_body_type = true; //no need for the default because we provided one explicitly
                        } else if key_id == "id" {
                            suppress_body_id = true;
                        }
                        body_out.add(
                            Cow::Borrowed(key_id),
                            output_datavalue(key_id, data.value()),
                        );
                    }
                },
                Some(NS_RDF) if key_id == "type" => {
                    suppress_default_body_type = true; //no need for the default because we provided one explicitly
                    body_out.add(
                        Cow::Borrowed(key_id),
                        output_datavalue(key_id, data.value()),
                    );
                }
                Some(set_id) => {
                    //different set, go into body
                    body_out.add(
                        config.uri_to_namespace(
                            if config.extra_context.iter().any(|s| s == set_id) {
                                //the set doubles as JSON-LD context: return the key as is (either a full IRI already or an alias)
                                key.id().expect("key must have ID").into()
                            } else {
                                //turn it into an IRI per standard identifier rules
                                key.iri(&config.default_set_iri).expect("set must have ID")
                            },
                        ),
                        output_datavalue(key_id, data.value()),
                    );
                }
                None => unreachable!("all sets should have a public identifier"),
            }
        }

        if config.auto_generated && !suppress_auto_generated {
            ann_out += &format!(" \"generated\": \"{}\",", Local::now().to_rfc3339());
        }
        if config.auto_generator && !suppress_auto_generator {
            ann_out += "  \"generator\": { \"id\": \"https://github.com/annotation/stam-rust\", \"type\": \"Software\", \"name\": \"STAM Library\"  },";
        }

        if !body_out.is_empty() {
            ann_out += " \"body\": {";
            if !suppress_default_body_type {
                ann_out += " \"type\": \"Dataset\",";
            }
            if !suppress_body_id {
                if let Some(iri) = self.iri(&config.default_annotation_iri) {
                    ann_out += &format!(" \"id\": \"{}/body\",", iri);
                } else if config.generate_annotation_iri {
                    let id = nanoid!();
                    ann_out += &format!(
                        " \"id\": \"{}\",",
                        into_iri(&id, &config.default_annotation_iri)
                    )
                }
            }
            let l = body_out.len();
            for (i, (key, value)) in body_out.iter().enumerate() {
                //value is already fully JSON encoded and key is already an IRI
                ann_out += &format!("\"{}\": {}", key, value);
                if i < l - 1 {
                    ann_out.push(',');
                }
            }
            ann_out += "},";
        }

        // a second pass may be needed if we have an extra_target_template AND nested targets
        let mut need_second_pass = false;
        let output_selector_out = &output_selector(
            self.as_ref().target(),
            self.store(),
            config,
            false,
            &mut need_second_pass,
            false, //first pass
        );
        if need_second_pass {
            let second_pass_out = &output_selector(
                self.as_ref().target(),
                self.store(),
                config,
                false,
                &mut need_second_pass,
                true, //second pass
            );
            if !target_extra_out.is_empty() {
                //with extra target from second pass and extra target(s) from annotation data
                ann_out += &format!(
                    " \"target\": [ {}, {}, {} ]",
                    output_selector_out, &second_pass_out, &target_extra_out
                );
            } else {
                //with extra target from second pass
                ann_out += &format!(
                    " \"target\": [ {}, {} ]",
                    output_selector_out, &second_pass_out
                );
            }
        } else if !target_extra_out.is_empty() {
            //with extra target(s) from annotation data
            ann_out += &format!(
                " \"target\": [ {}, {} ]",
                &output_selector_out, &target_extra_out
            );
        } else {
            //normal situation, no extra targets
            ann_out += &format!(" \"target\": {}", &output_selector_out);
        }
        ann_out += "}";
        ann_out
    }
}

fn output_predicate_datavalue(
    predicate: &str,
    datavalue: &DataValue,
    config: &WebAnnoConfig,
) -> String {
    let value_is_iri = if let DataValue::String(s) = datavalue {
        is_iri(s)
    } else {
        false
    };
    if is_iri(predicate) && value_is_iri {
        // If the predicate is an IRI and the value *(looks like* an IRI, then the latter will be interpreted as an IRI rather than a string literal
        // (This is not formally defined in the spec! the predicate check is needed because we don't want this behaviour if the predicate is an alias defined in the JSON-LD context)
        format!(
            "\"{}\": {{ \"id\": \"{}\" }}",
            config.uri_to_namespace(predicate.into()),
            datavalue
        )
    } else {
        format!(
            "\"{}\": {}",
            config.uri_to_namespace(predicate.into()),
            &value_to_json(datavalue)
        )
    }
}

fn output_datavalue(predicate: &str, datavalue: &DataValue) -> String {
    let value_is_iri = if let DataValue::String(s) = datavalue {
        is_iri(s)
    } else {
        false
    };
    if is_iri(predicate) && value_is_iri {
        // If the predicate is an IRI and the value *(looks like* an IRI, then the latter will be interpreted as an IRI rather than a string literal
        // (This is not formally defined in the spec! the predicate check is needed because we don't want this behaviour if the predicate is an alias defined in the JSON-LD context)
        format!("{{ \"id\": \"{}\" }}", datavalue)
    } else {
        value_to_json(datavalue)
    }
}

fn output_selector(
    selector: &Selector,
    store: &AnnotationStore,
    config: &WebAnnoConfig,
    nested: bool,
    need_second_pass: &mut bool,
    second_pass: bool,
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
            if !second_pass {
                if !config.extra_target_templates.is_empty() && !nested {
                    ann_out += "[";
                }
                ann_out += &format!(
                    "{{ \"source\": \"{}\", \"selector\": {{ \"type\": \"TextPositionSelector\", \"start\": {}, \"end\": {} }} }}",
                    into_iri(
                        resource.id().expect("resource must have ID"),
                        &config.default_resource_iri
                    ),
                    textselection.begin(),
                    textselection.end(),
                );
            }
            if (!nested && !second_pass) || (nested && second_pass) {
                for extra_target_template in config.extra_target_templates.iter() {
                    let mut template = extra_target_template.clone();
                    template = template.replace(
                        "{resource_iri}",
                        &into_iri(
                            resource.id().expect("resource must have ID"),
                            &config.default_resource_iri,
                        ),
                    );
                    template = template
                        .replace("{resource}", resource.id().expect("resource must have ID"));
                    template = template.replace("{begin}", &format!("{}", textselection.begin()));
                    template = template.replace("{end}", &format!("{}", textselection.end()));
                    if !ann_out.is_empty() {
                        ann_out.push(',');
                    }
                    ann_out += &format!("\"{}\"", &template);
                }
                if !nested && !second_pass && !config.extra_target_templates.is_empty() {
                    ann_out += " ]";
                }
            } else if !config.extra_target_templates.is_empty() && !second_pass {
                //we need a second pass to serialize the items using extra_target_template
                *need_second_pass = true;
            }
        }
        Selector::AnnotationSelector(a_handle, None) => {
            let annotation = store.annotation(*a_handle).expect("annotation must exist");
            if let Some(iri) = annotation.iri(&config.default_annotation_iri) {
                ann_out += &format!("{{ \"id\": \"{}\", \"type\": \"Annotation\" }}", iri);
            } else {
                ann_out += "{ \"id\": null }";
                eprintln!("WARNING: Annotation points to an annotation that has no public ID! Unable to serialize to Web Annotatations");
            }
        }
        Selector::ResourceSelector(res_handle) => {
            let resource = store.resource(*res_handle).expect("resource must exist");
            ann_out += &format!(
                "{{ \"id\": \"{}\", \"type\": \"Text\" }}",
                into_iri(
                    resource.id().expect("resource must have ID"),
                    &config.default_resource_iri
                ),
            );
        }
        Selector::DataSetSelector(set_handle) => {
            let dataset = store.dataset(*set_handle).expect("resource must exist");
            ann_out += &format!(
                "{{ \"id\": \"{}\", \"type\": \"Dataset\" }}",
                into_iri(
                    dataset.id().expect("dataset must have ID"),
                    &config.default_resource_iri
                ),
            );
        }
        Selector::CompositeSelector(selectors) => {
            ann_out += "{ \"type\": \"http://www.w3.org/ns/oa#Composite\", \"items\": [";
            for (i, selector) in selectors.iter().enumerate() {
                ann_out += &format!(
                    "{}",
                    &output_selector(selector, store, config, true, need_second_pass, second_pass)
                );
                if i != selectors.len() - 1 {
                    ann_out += ",";
                }
            }
            ann_out += " ]}";
        }
        Selector::MultiSelector(selectors) => {
            ann_out += "{ \"type\": \"http://www.w3.org/ns/oa#Independents\", \"items\": [";
            for (i, selector) in selectors.iter().enumerate() {
                ann_out += &format!(
                    "{}",
                    &output_selector(selector, store, config, true, need_second_pass, second_pass)
                );
                if i != selectors.len() - 1 {
                    ann_out += ",";
                }
            }
            ann_out += " ]}";
        }
        Selector::DirectionalSelector(selectors) => {
            ann_out += "{ \"type\": \"http://www.w3.org/ns/oa#List\", \"items\": [";
            for (i, selector) in selectors.iter().enumerate() {
                ann_out += &format!(
                    "{}",
                    &output_selector(selector, store, config, true, need_second_pass, second_pass)
                );
                if i != selectors.len() - 1 {
                    ann_out += ",";
                }
            }
            ann_out += " ]}";
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
                let subselectors: Vec<_> = selector.iter(store, false).collect();
                for (i, subselector) in subselectors.iter().enumerate() {
                    ann_out += &format!(
                        "{}",
                        &output_selector(
                            &subselector,
                            store,
                            config,
                            true,
                            need_second_pass,
                            second_pass
                        )
                    );
                    if i != subselectors.len() - 1 {
                        ann_out += ",";
                    }
                }
            } else {
                unreachable!(
                "Internal Ranged selectors can not be serialized directly, they can be serialized only when under a complex selector",
            );
            }
        }
    }
    ann_out
}

// helper structure to allow singular or multiple values per property
#[derive(Default)]
struct OutputMap<'a>(Vec<(Cow<'a, str>, SmallVec<[String; 1]>)>);

impl<'a> OutputMap<'a> {
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, key: Cow<'a, str>, value: String) {
        let mut value = Some(value);
        for item in self.0.iter_mut() {
            if item.0 == key {
                item.1.push(value.take().unwrap());
                break;
            }
        }
        if let Some(value) = value {
            self.0.push((key, smallvec!(value)));
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = (&str, String)> {
        self.0.iter().map(|(key, value)| {
            (
                key.as_ref(),
                if value.len() == 1 {
                    let s: String = value.join(", ");
                    s
                } else {
                    format!("[ {} ]", value.join(", ")) //MAYBE TODO: if value is already a list, join lists?
                },
            )
        })
    }
}
