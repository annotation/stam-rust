#![allow(unused_imports)]
#![allow(dead_code)]
use crate::annotation::AnnotationHandle;
use crate::annotationdata::{AnnotationDataHandle, DataOperator};
use crate::annotationstore::AnnotationStore;
use crate::datakey::DataKeyHandle;
use crate::textselection::{TextRelationOperator, TextSelectionOperator};
use crate::types::*;
use crate::AnnotationDataSetHandle;
use crate::TextResourceHandle;

use std::borrow::Cow;

pub struct AnnotationSelectionSetHandle(u16);

pub struct SelectQuery<'a, 'store> {
    selections: Vec<AnnotationSelectionSet<'a>>,
    selections_done: Vec<AnnotationSelectionSet<'a>>,

    /// a constraint is computed from a filter and can be used to query
    constraints: Vec<(
        AnnotationSelectionSetHandle,
        AnnotationConstraint<'a, 'store>,
    )>,
}

pub struct AnnotationSelectionSet<'store> {
    id: Option<String>,
    annotations: Cow<'store, Vec<AnnotationHandle>>,
}

impl<'store> AnnotationSelectionSet<'store> {
    pub fn len(&self) -> usize {
        self.annotations.len()
    }
}

pub enum AnnotationConstraintBuilder<'a> {
    Id(&'a str),

    //
    AnnotationSet(&'a str),

    ///Annotation must have this associated data
    Data {
        dataset: &'a str,
        key: &'a str,
        operator: &'a str,
        data: &'a str,
    },

    ///Annotation must have this key (regardless of value)
    Key {
        dataset: &'a str,
        key: &'a str
    },
    InSelectionSet(&'a str),
    Resource(&'a str),
    Text(&'a str),
    TextRelation(&'a str),
    TargetResource(TextResourceHandle),
    /*
    References(&'a AnnotationSelectionSet),
    ReferencedBy(&'a AnnotationSelectionSet),
    And(Vec<AnnotationConstraintBuilder<'a>>),
    Or(Vec<AnnotationConstraintBuilder<'a>>),
    Not(Box<AnnotationConstraintBuilder<'a>>),
    */
}

/// This is the realized/processed form of an [`AnnotationFilter`]
pub enum AnnotationConstraint<'a, 'store> {
    Id(AnnotationHandle),
    AnnotationSet(AnnotationDataSetHandle),
    Key(AnnotationDataSetHandle, DataKeyHandle),
    Data(AnnotationDataSetHandle, AnnotationDataHandle),
    Resource(TextResourceHandle),
    Text(&'a str),
    TextRelation(TextResourceHandle, TextSelectionOperator<'a>),
    InSelectionSet(&'a AnnotationSelectionSet<'store>),
}

impl<'a, 'store> AnnotationConstraint<'a, 'store> {
    /// Returns the number of annotations this constraint will return, None if unknown/unbounded or too costly to compute
    pub fn result_size(&'a self, store: &'store AnnotationStore) -> Option<usize> {
        match self {
            Self::Id(_) => Some(1),
            Self::InSelectionSet(set) => Some(set.len()),da            _ => None,
        }
    }

    pub fn cost(&'a self) -> usize {
        match self {
            Self::Id(_) => 1,
            Self::InSelectionSet(_) => 1,
            Self::Data(_, _) => 1,
            _ => panic!("not implemented yet!"), // TODO
        }
    }
}

impl<'a, 'store> SelectQuery<'a, 'store> {
    pub fn run(&mut self, store: &AnnotationStore) {}
}

/*
impl AnnotationStore {
    pub fn select_by_query<'a>(&self, SelectQuery<'a>) {

    }
}
*/
