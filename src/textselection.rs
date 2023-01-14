use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::types::*;
use crate::annotation::AnnotationHandle;
use crate::resources::TextResourceHandle;

#[derive(PartialEq,Eq,Debug)]
/// Corresponds to a slice of the text. The result of applying a [`crate::selector:Selector::TextSelector`].
pub struct TextSelection {
    pub(crate) beginbyte: usize,
    pub(crate) endbyte: usize
}

impl Ord for TextSelection {
    // this  determines the canonical ordering for text selections (applied offsets)
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.beginbyte.cmp(&other.beginbyte);
        if ord != Ordering::Equal {
            ord
        } else {
            self.endbyte.cmp(&other.endbyte)
        } 
    }
}

impl PartialOrd for TextSelection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }

}

impl TextSelection {
    pub fn beginbyte(&self) -> usize {
        self.beginbyte
    }

    pub fn endbyte(&self) -> usize {
        self.endbyte
    }
}

pub struct TextSelectionSet(pub Vec<TextSelection>);


/// Maps TextResourceHandle => TextSelection => AnnotationHandle
/// The text selection map is ordered
pub struct TextRelationMap {
    //primary indices correspond to TextResourceHandle
    data: Vec<BTreeMap<TextSelection,AnnotationHandle>>
}

impl Default for TextRelationMap {
    fn default() -> Self {
        Self {
            data: Vec::new()
        }
    }
}

impl TextRelationMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, x: TextResourceHandle, y: TextSelection, z: AnnotationHandle) {
        if x.unwrap() >= self.data.len() {
            //expand the map
            self.data.resize_with(x.unwrap() + 1, Default::default);
        }
        self.data[x.unwrap()].insert(y,z);
    }


    //TODO: implement remove()
}

