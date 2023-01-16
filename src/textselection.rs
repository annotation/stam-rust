use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::types::*;
use crate::annotation::AnnotationHandle;
use crate::resources::TextResourceHandle;
use crate::error::StamError;

#[derive(PartialEq,Eq,Debug,Clone,Copy)]
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

    /// Resolves a cursor *relative to the text selection* to a utf8 byte position, the text of the TextSelection has to be explicitly passed
    pub fn resolve_cursor(&self, slice_text: &str, cursor: &Cursor) -> Result<usize,StamError> {
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
            },
            Cursor::EndAligned(0) => {
                return Ok(slice_text.len())
            },
            Cursor::EndAligned(cursor) => {
                let mut iter = slice_text.char_indices();
                let mut endcharindex: isize = 0;
                while let Some((byteindex, _)) = iter.next_back() {
                    endcharindex -= 1;
                    if cursor == endcharindex {
                        return Ok(byteindex)
                    } else if cursor > endcharindex {
                        break;
                    }
                }
            }
        };
        Err(StamError::CursorOutOfBounds(*cursor,"TextSelection::resolve_cursor()"))
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

impl Extend<(TextResourceHandle,TextSelection,AnnotationHandle)> for TextRelationMap  {
    fn extend<T>(&mut self, iter: T)  where T: IntoIterator<Item=(TextResourceHandle,TextSelection,AnnotationHandle)> {
        for (x,y,z) in iter {
            self.insert(x,y,z);
        }
    }
}
