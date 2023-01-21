#![allow(unused_imports)]
#![allow(dead_code)]
use crate::annotation::AnnotationHandle;
use crate::annotationdata::{AnnotationDataHandle, DataOperator};
use crate::textselection::{TextRelationOperator, TextSelectionOperator};
use crate::types::*;
use crate::AnnotationDataSetHandle;
use crate::TextResourceHandle;

pub struct AnnotationSelectionSetHandle(u16);

pub struct SelectQuery<'a> {
    selections: Vec<AnnotationSelectionSet>,
    selections_done: Vec<AnnotationSelectionSet>,

    /// a filter is the input form for a constraint
    filters: Vec<(AnnotationSelectionSetHandle, AnnotationFilter<'a>)>,

    /// a constraint is computed from a filter and can be used to query
    constraints: Vec<(AnnotationSelectionSetHandle, AnnotationConstraint<'a>)>,
}

pub struct AnnotationSelectionSet {
    id: Option<String>,
    annotations: Vec<AnnotationHandle>,
}

/// Determines a conditional test to filter annotations
pub enum AnnotationFilter<'a> {
    Id(&'a str),
    AnnotationSet(&'a str),
    Data(&'a DataOperator<'a>),
    Key(&'a str),
    InSelectionSet(&'a AnnotationSelectionSet),
    Resource(&'a str),
    Text(&'a str),
    TextRelation(&'a str, TextRelationOperator),
    TargetResource(TextResourceHandle),
    References(&'a AnnotationSelectionSet),
    ReferencedBy(&'a AnnotationSelectionSet),
    And(Vec<AnnotationFilter<'a>>),
    Or(Vec<AnnotationFilter<'a>>),
    Not(Box<AnnotationFilter<'a>>),
}

/// This is the realized/processed form of an [`AnnotationFilter`]
pub enum AnnotationConstraint<'a> {
    Id(AnnotationHandle),
    AnnotationSet(AnnotationDataSetHandle),
    Data(AnnotationDataSetHandle, AnnotationDataHandle),
    Resource(TextResourceHandle),
    Text(&'a str),
    TextRelation(TextResourceHandle, TextSelectionOperator<'a>),
    InSelectionSet(&'a AnnotationSelectionSet),
}

/*
impl AnnotationStore {
    pub fn select_by_query<'a>(&self, SelectQuery<'a>) {

    }
}
*/
