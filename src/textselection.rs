use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::annotation::AnnotationHandle;
use crate::error::StamError;
use crate::resources::TextResourceHandle;
use crate::types::*;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
/// Corresponds to a slice of the text. The result of applying a [`crate::selector:Selector::TextSelector`].
pub struct TextSelection {
    pub(crate) beginbyte: usize,
    pub(crate) endbyte: usize,
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

    /// Resolves a cursor *relative to the text selection* to a utf8 byte position, the text of the TextSelection has to be explicitly passed
    pub fn resolve_cursor(&self, slice_text: &str, cursor: &Cursor) -> Result<usize, StamError> {
        //TODO: implementation is not efficient on large text slices
        match *cursor {
            Cursor::BeginAligned(cursor) => {
                let mut prevcharindex = 0;
                for (charindex, (byteindex, _)) in slice_text.char_indices().enumerate() {
                    if cursor == charindex {
                        return Ok(byteindex);
                    } else if cursor < charindex {
                        break;
                    }
                    prevcharindex = charindex;
                }
                //is the cursor at the very end? (non-inclusive)
                if cursor == prevcharindex + 1 {
                    return Ok(slice_text.len());
                }
            }
            Cursor::EndAligned(0) => return Ok(slice_text.len()),
            Cursor::EndAligned(cursor) => {
                let mut iter = slice_text.char_indices();
                let mut endcharindex: isize = 0;
                while let Some((byteindex, _)) = iter.next_back() {
                    endcharindex -= 1;
                    if cursor == endcharindex {
                        return Ok(byteindex);
                    } else if cursor > endcharindex {
                        break;
                    }
                }
            }
        };
        Err(StamError::CursorOutOfBounds(
            *cursor,
            "TextSelection::resolve_cursor()",
        ))
    }
}

/// Holds one or more TextSelection items. This structure is optimized to be quick when there is only one item (which is often).
#[derive(Clone, Debug)]
pub struct TextSelectionSet {
    head: TextSelection,
    tail: Option<Box<TextSelectionSet>>,
}

impl TextSelectionSet {
    pub fn new(textselection: TextSelection) -> Self {
        Self {
            head: textselection,
            tail: None,
        }
    }

    pub fn push(self, textselection: TextSelection) -> Self {
        Self {
            head: textselection,
            tail: Some(Box::new(self)),
        }
    }

    pub fn pop(self) -> (TextSelection, Option<Self>) {
        if let Some(tail) = self.tail {
            (
                self.head,
                Some(Self {
                    head: tail.head,
                    tail: tail.tail,
                }),
            )
        } else {
            (self.head, None)
        }
    }

    pub fn head(&self) -> &TextSelection {
        &self.head
    }

    pub fn tail(&self) -> Option<&Box<TextSelectionSet>> {
        self.tail.as_ref()
    }

    pub fn iter<'a>(&'a self) -> TextSelectionSetIter<'a> {
        TextSelectionSetIter {
            set: Some(self),
            next: None,
        }
    }

    pub fn len(&self) -> usize {
        let mut count = 0;
        let mut tail = self.tail();
        loop {
            if tail.is_some() {
                count += 1;
                tail = tail.unwrap().tail();
            } else {
                break;
            }
        }
        count
    }
}

pub struct TextSelectionSetIter<'a> {
    set: Option<&'a TextSelectionSet>,
    next: Option<Box<TextSelectionSetIter<'a>>>,
}

impl<'a> Iterator for TextSelectionSetIter<'a> {
    type Item = &'a TextSelection;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next.is_some() {
            self.next.as_mut().unwrap().next()
        } else if let Some(set) = self.set {
            if let Some(tail) = set.tail() {
                self.next = Some(Box::new(TextSelectionSetIter {
                    set: Some(tail.as_ref()),
                    next: None,
                }));
            }
            Some(set.head())
        } else {
            None
        }
    }
}

/// Maps TextResourceHandle => TextSelection => AnnotationHandle
/// The text selection map is ordered
pub struct TextRelationMap {
    //primary indices correspond to TextResourceHandle
    data: Vec<BTreeMap<TextSelection, AnnotationHandle>>,
}

impl Default for TextRelationMap {
    fn default() -> Self {
        Self { data: Vec::new() }
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
        self.data[x.unwrap()].insert(y, z);
    }

    //TODO: implement remove()
}

impl Extend<(TextResourceHandle, TextSelection, AnnotationHandle)> for TextRelationMap {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (TextResourceHandle, TextSelection, AnnotationHandle)>,
    {
        for (x, y, z) in iter {
            self.insert(x, y, z);
        }
    }
}

#[derive(Debug, Clone)]
pub enum TextSelectionOperator<'a> {
    // Both sets occupy cover the exact same TextSelections, and all are covered (cf. textfabric's `==`), commutative, transitive
    Equals(&'a TextSelectionSet),

    // There are TextSelections in A that are also in B (cf. textfabric's `&&`), commutative
    Overlaps(&'a TextSelectionSet),

    Not(Box<TextSelectionOperator<'a>>),
}

impl TextSelectionSet {
    pub fn test(&self, operator: &TextSelectionOperator) -> bool {
        match operator {
            TextSelectionOperator::Equals(otherset) => {
                if self.len() != otherset.len() {
                    //each item must have a counterpart so the sets must be equal length
                    return false;
                }
                //all of the items in this set must match with an item in the otherset
                for item in self.iter() {
                    if !item.test(&operator) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Overlaps(_) => {
                //all of the items in this set must match with an item in the otherset
                for item in self.iter() {
                    if !item.test(&operator) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Not(suboperator) => !self.test(suboperator),
        }
    }
}

impl TextSelection {
    pub fn test(&self, operator: &TextSelectionOperator) -> bool {
        match operator {
            TextSelectionOperator::Equals(otherset) => {
                //item must be equal to ANY of the items in the other set
                for other in otherset.iter() {
                    if self == other {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::Overlaps(otherset) => {
                for other in otherset.iter() {
                    return (other.beginbyte >= self.beginbyte && other.beginbyte < self.endbyte)
                        || (other.endbyte > self.beginbyte && other.endbyte <= self.endbyte);
                }
                false
            }
            TextSelectionOperator::Not(suboperator) => !self.test(suboperator),
        }
    }
}
