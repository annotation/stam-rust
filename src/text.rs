/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module defines and partially implements the [`Text`] trait.

use crate::error::StamError;
use crate::selector::Offset;
use crate::types::*;

/// This trait provides methods that operate on structures that hold or represent text content.
/// They are fairly low-level methods but are exposed in the public API. The [`FindText`](crate::FindText)
/// trait subsequently builds upon this one with high-level search methods.
pub trait Text<'store, 'slf>
where
    'store: 'slf,
{
    /// Returns a reference to the text
    fn text(&'slf self) -> &'store str;

    /// Returns the length of the text in unicode points
    /// For bytes, use `Self::text().len()` instead.
    fn textlen(&'slf self) -> usize;

    /// Returns a string reference to a slice of text as specified by the offset
    fn text_by_offset(&'slf self, offset: &Offset) -> Result<&'store str, StamError>;

    /// Finds the utf-8 byte position where the specified text subslice begins
    /// The returned offset is relative to the TextSelection
    fn subslice_utf8_offset(&'slf self, subslice: &str) -> Option<usize> {
        let self_begin = self.text().as_ptr() as usize;
        let sub_begin = subslice.as_ptr() as usize;
        if sub_begin < self_begin || sub_begin > self_begin.wrapping_add(self.text().len()) {
            None
        } else {
            Some(sub_begin.wrapping_sub(self_begin))
        }
    }

    fn is_empty(&'slf self) -> bool {
        self.text().is_empty()
    }

    /// Converts a unicode character position to a UTF-8 byte position
    fn utf8byte(&'slf self, abscursor: usize) -> Result<usize, StamError>;

    /// Converts a UTF-8 byte position into a unicode position
    fn utf8byte_to_charpos(&'slf self, bytecursor: usize) -> Result<usize, StamError>;

    /// Resolves a begin-aligned cursor to an absolute cursor (i.e. relative to the TextResource).
    fn absolute_cursor(&'slf self, cursor: usize) -> usize;

    /// Resolves a relative offset (relative to another TextSelection) to an absolute one (in terms of to the underlying TextResource)
    fn absolute_offset(&'slf self, offset: &Offset) -> Result<Offset, StamError> {
        Ok(Offset::simple(
            self.absolute_cursor(self.beginaligned_cursor(&offset.begin)?),
            self.absolute_cursor(self.beginaligned_cursor(&offset.end)?),
        ))
    }

    /// Resolves a cursor to a begin aligned cursor, resolving all relative end-aligned positions
    fn beginaligned_cursor(&'slf self, cursor: &Cursor) -> Result<usize, StamError> {
        match *cursor {
            Cursor::BeginAligned(cursor) => Ok(cursor),
            Cursor::EndAligned(cursor) => {
                if cursor.abs() as usize > self.textlen() {
                    Err(StamError::CursorOutOfBounds(
                        Cursor::EndAligned(cursor),
                        "TextResource::beginaligned_cursor(): end aligned cursor ends up before the beginning",
                    ))
                } else {
                    Ok(self.textlen() - cursor.abs() as usize)
                }
            }
        }
    }
}
