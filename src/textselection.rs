/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contains the low-level API for [`TextSelection`]. It defines and implements the
//! struct, the handle, and things like serialisation, deserialisation to STAM JSON.
//!
//! It also implements [`TextSelectionSet`], a collection of textselections that can be tested
//! against as a group. and it implements the various tests (defined by the [`TestTextSelection`]
//! trait) that can be done on text selections, mediated by the [`TextSelectionOperator`] which is
//! also directly exposed for the high-level API.

use sealed::sealed;
use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::slice::Iter;

use datasize::DataSize;
use minicbor::{Decode, Encode};
use smallvec::SmallVec;

use crate::annotationstore::AnnotationStore;
use crate::cbor::*;
use crate::error::*;
use crate::resources::{TextResource, TextResourceHandle, TextSelectionIter};
use crate::selector::{Offset, OffsetMode};
use crate::store::*;
use crate::text::*;
use crate::types::*;

#[derive(PartialEq, Eq, Debug, Clone, Copy, DataSize, Encode, Decode)]
/// Corresponds to a slice of the text. This only contains minimal
/// information; i.e. the begin offset, end offset and optionally a handle.
/// if the textselection is already known in the model.
////
/// This is similar to [`Offset`], but that one uses cursors which may
/// be relative. TextSelection specifies an offset in more absolute terms.
///
/// The actual reference to the [`crate::TextResource`] is not stored in this structure but should
/// accompany it explicitly when needed, such as in the higher-level wrapper [`ResultTextSelection`].
pub struct TextSelection {
    #[n(0)] //for cbor (de)serialisation
    pub(crate) intid: Option<TextSelectionHandle>,
    #[n(1)]
    pub(crate) begin: usize,
    #[n(2)]
    pub(crate) end: usize,
}

/// [Handle] to an instance of [`TextSelection`] in the store ([`TextResource`]).
#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash, PartialOrd, Ord, DataSize, Encode, Decode)]
#[cbor(transparent)]
pub struct TextSelectionHandle(#[n(0)] pub(crate) u32); //if this u32 ever changes, make sure to also adapt the CborLen implementation in cbor.rs otherwise things will go horribly wrong

// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> Request<TextSelection> for TextSelectionHandle {
    fn to_handle<'store, S>(&self, _store: &'store S) -> Option<TextSelectionHandle>
    where
        S: StoreFor<TextSelection>,
    {
        Some(*self)
    }
}

impl<'a> From<&TextSelectionHandle> for BuildItem<'a, TextSelection> {
    fn from(handle: &TextSelectionHandle) -> Self {
        BuildItem::Handle(*handle)
    }
}
impl<'a> From<Option<&TextSelectionHandle>> for BuildItem<'a, TextSelection> {
    fn from(handle: Option<&TextSelectionHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(*handle)
        } else {
            BuildItem::None
        }
    }
}
impl<'a> From<TextSelectionHandle> for BuildItem<'a, TextSelection> {
    fn from(handle: TextSelectionHandle) -> Self {
        BuildItem::Handle(handle)
    }
}
impl<'a> From<Option<TextSelectionHandle>> for BuildItem<'a, TextSelection> {
    fn from(handle: Option<TextSelectionHandle>) -> Self {
        if let Some(handle) = handle {
            BuildItem::Handle(handle)
        } else {
            BuildItem::None
        }
    }
}

impl From<TextSelection> for Offset {
    fn from(textselection: TextSelection) -> Offset {
        Offset {
            begin: Cursor::BeginAligned(textselection.begin),
            end: Cursor::BeginAligned(textselection.end),
        }
    }
}

impl From<&TextSelection> for Offset {
    fn from(textselection: &TextSelection) -> Offset {
        Offset {
            begin: Cursor::BeginAligned(textselection.begin),
            end: Cursor::BeginAligned(textselection.end),
        }
    }
}

impl From<&ResultTextSelection<'_>> for Offset {
    fn from(textselection: &ResultTextSelection<'_>) -> Offset {
        Offset {
            begin: Cursor::BeginAligned(textselection.begin()),
            end: Cursor::BeginAligned(textselection.end()),
        }
    }
}

impl From<&ResultItem<'_, TextSelection>> for Offset {
    fn from(textselection: &ResultItem<'_, TextSelection>) -> Offset {
        Offset {
            begin: Cursor::BeginAligned(textselection.as_ref().begin()),
            end: Cursor::BeginAligned(textselection.as_ref().end()),
        }
    }
}

#[sealed]
impl Handle for TextSelectionHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

#[sealed]
impl TypeInfo for TextSelection {
    fn typeinfo() -> Type {
        Type::TextSelection
    }
}

#[sealed]
impl Storable for TextSelection {
    type HandleType = TextSelectionHandle;
    type StoreHandleType = TextResourceHandle;
    type FullHandleType = (TextResourceHandle, TextSelectionHandle);
    type StoreType = TextResource;

    fn id(&self) -> Option<&str> {
        None
    }
    fn handle(&self) -> Option<TextSelectionHandle> {
        self.intid
    }
    fn with_handle(mut self, handle: TextSelectionHandle) -> Self {
        self.intid = Some(handle);
        self
    }

    fn carries_id() -> bool {
        false
    }

    fn fullhandle(
        storehandle: Self::StoreHandleType,
        handle: Self::HandleType,
    ) -> Self::FullHandleType {
        (storehandle, handle)
    }
}

impl Hash for TextSelection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let h = (self.begin, self.end);
        h.hash(state);
    }
}

impl Ord for TextSelection {
    // this  determines the canonical ordering for text selections (applied offsets)
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.begin.cmp(&other.begin);
        if ord != Ordering::Equal {
            ord
        } else {
            self.end.cmp(&other.end)
        }
    }
}

impl PartialOrd for TextSelection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TextSelection {
    /// Return the begin position (unicode points)
    pub fn begin(&self) -> usize {
        self.begin
    }

    /// Return the end position (non-inclusive) in unicode points
    pub fn end(&self) -> usize {
        self.end
    }

    /// Returns the begin cursor of this text selection in another. Returns None if they are not embedded.
    /// **Note:** this does *NOT* check whether the textselections pertain to the same resource, that is up to the caller.
    pub fn relative_begin(&self, container: &TextSelection) -> Option<usize> {
        if self.begin() >= container.begin() {
            Some(self.begin() - container.begin())
        } else {
            None
        }
    }

    /// Returns the end cursor (begin-aligned) of this text selection in another. Returns None if they are not embedded.
    /// **Note:** this does *NOT* check whether the textselections pertain to the same resource, that is up to the caller.
    pub fn relative_end(&self, container: &TextSelection) -> Option<usize> {
        if self.end() <= container.end() {
            Some(self.end() - container.begin())
        } else {
            None
        }
    }

    /// Returns the begin cursor of this text selection in another, as an end aligned cursor. Returns None if they are not embedded.
    /// **Note:** this does *NOT* check whether the textselections pertain to the same resource, that is up to the caller.
    fn relative_begin_endaligned(&self, container: &TextSelection) -> Option<isize> {
        if self.begin() >= container.begin() {
            let beginaligned = self.begin() - container.begin();
            let containerlen = container.end() as isize - container.begin() as isize;
            Some(containerlen - beginaligned as isize)
        } else {
            None
        }
    }

    /// Returns the begin cursor of this text selection in another, as an end aligned cursor. Returns None if they are not embedded.
    /// **Note:** this does *NOT* check whether the textselections pertain to the same resource, that is up to the caller.
    fn relative_end_endaligned(&self, container: &TextSelection) -> Option<isize> {
        if self.end() <= container.end() {
            let beginaligned = self.end() - container.begin();
            let containerlen = container.end() as isize - container.begin() as isize;
            Some(containerlen - beginaligned as isize)
        } else {
            None
        }
    }

    /// Returns the offset of this text selection in another. Returns None if they are not embedded.
    /// **Note:** this does *NOT* check whether the textselections pertain to the same resource, that is up to the caller.
    pub fn relative_offset(
        &self,
        container: &TextSelection,
        offsetmode: OffsetMode,
    ) -> Option<Offset> {
        match offsetmode {
            OffsetMode::BeginBegin => {
                if let (Some(begin), Some(end)) =
                    (self.relative_begin(container), self.relative_end(container))
                {
                    Some(Offset::simple(begin, end))
                } else {
                    None
                }
            }
            OffsetMode::BeginEnd => {
                if let (Some(begin), Some(end)) = (
                    self.relative_begin(container),
                    self.relative_end_endaligned(container),
                ) {
                    Some(Offset::new(
                        Cursor::BeginAligned(begin),
                        Cursor::EndAligned(end),
                    ))
                } else {
                    None
                }
            }
            OffsetMode::EndEnd => {
                if let (Some(begin), Some(end)) = (
                    self.relative_begin_endaligned(container),
                    self.relative_end_endaligned(container),
                ) {
                    Some(Offset::new(
                        Cursor::EndAligned(begin),
                        Cursor::EndAligned(end),
                    ))
                } else {
                    None
                }
            }
            OffsetMode::EndBegin => {
                if let (Some(begin), Some(end)) = (
                    self.relative_begin_endaligned(container),
                    self.relative_end(container),
                ) {
                    Some(Offset::new(
                        Cursor::EndAligned(begin),
                        Cursor::BeginAligned(end),
                    ))
                } else {
                    None
                }
            }
        }
    }

    /// Resolves a relative cursor to a relative begin aligned cursor, resolving all end-aligned positions
    fn beginaligned_cursor(&self, cursor: &Cursor) -> Result<usize, StamError> {
        let textlen = self.end() - self.begin();
        match *cursor {
            Cursor::BeginAligned(cursor) => Ok(cursor),
            Cursor::EndAligned(cursor) => {
                if cursor.abs() as usize > textlen {
                    Err(StamError::CursorOutOfBounds(
                        Cursor::EndAligned(cursor),
                        "TextResource::beginaligned_cursor(): end aligned cursor ends up before the beginning",
                    ))
                } else {
                    Ok(textlen - cursor.abs() as usize)
                }
            }
        }
    }

    /// Low-level method to get a textselection inside the current one
    /// Note: this is a low level method and will always return an unbound textselection!
    pub fn textselection_by_offset(&self, offset: &Offset) -> Result<TextSelection, StamError> {
        let (begin, end) = (
            self.begin + self.beginaligned_cursor(&offset.begin)?,
            self.begin + self.beginaligned_cursor(&offset.end)?,
        );
        Ok(TextSelection {
            intid: None,
            begin,
            end,
        })
    }
}

#[derive(Debug, Clone, DataSize, Decode, Encode)]
#[cbor(transparent)]
pub(crate) struct PositionIndex(#[n(0)] pub(crate) BTreeMap<usize, PositionIndexItem>);

impl Default for PositionIndex {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl PositionIndex {
    //Returns an iterator over all positions in in index, in sorted order
    pub fn keys(&self) -> btree_map::Keys<usize, PositionIndexItem> {
        self.0.keys()
    }
    pub fn iter(&self) -> btree_map::Iter<usize, PositionIndexItem> {
        self.0.iter()
    }
}

#[derive(Debug, Clone, DataSize, Decode, Encode)]
pub struct PositionIndexItem {
    /// Position in bytes (UTF-8 encoded)
    #[n(0)]
    pub(crate) bytepos: usize,

    /// Lists all text selections that start here
    #[cbor(
        n(1),
        decode_with = "cbor_decode_positionitem_smallvec",
        encode_with = "cbor_encode_positionitem_smallvec",
        cbor_len = "cbor_len_positionitem_smallvec"
    )]
    pub(crate) end2begin: SmallVec<[(usize, TextSelectionHandle); 1]>, //heap allocation only needed when there are more than one

    /// Lists all text selections that end here (non-inclusive)
    #[cbor(
        n(2),
        decode_with = "cbor_decode_positionitem_smallvec",
        encode_with = "cbor_encode_positionitem_smallvec",
        cbor_len = "cbor_len_positionitem_smallvec"
    )]
    pub(crate) begin2end: SmallVec<[(usize, TextSelectionHandle); 1]>, //heap allocation only needed when there are more than one
}

impl PositionIndexItem {
    pub fn bytepos(&self) -> usize {
        self.bytepos
    }
    pub fn iter_end2begin<'a>(&'a self) -> Iter<'a, (usize, TextSelectionHandle)> {
        self.end2begin.iter()
    }

    pub fn iter_begin2end<'a>(&'a self) -> Iter<'a, (usize, TextSelectionHandle)> {
        self.begin2end.iter()
    }

    pub fn len_begin2end<'a>(&'a self) -> usize {
        self.begin2end.len()
    }

    pub fn len_end2begin<'a>(&'a self) -> usize {
        self.end2begin.len()
    }
}

impl Hash for PositionIndexItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytepos.hash(state);
    }
}

impl PartialEq<PositionIndexItem> for PositionIndexItem {
    fn eq(&self, other: &Self) -> bool {
        self.bytepos == other.bytepos
    }
}

impl Eq for PositionIndexItem {}

impl PartialOrd for PositionIndexItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.bytepos.cmp(&other.bytepos))
    }
}

impl Ord for PositionIndexItem {
    // this  determines the canonical ordering for text selections (applied offsets)
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytepos.cmp(&other.bytepos)
    }
}

/// A TextSelectionSet holds one or more [`TextSelection`] items and a reference to the TextResource from which they're drawn.
/// All textselections in a set must reference the same resource, which implies they are comparable.
#[derive(Clone, Debug, PartialEq)]
pub struct TextSelectionSet {
    data: SmallVec<[TextSelection; 1]>,
    resource: TextResourceHandle,
    sorted: bool,
}

/// A TextSelectionSet holds one or more [`TextSelection`] items and a reference to the TextResource from which they're drawn.
/// This structure encapsulates such a [`TextSelectionSet`] and contains a reference to the underlying [`AnnotationStore`].
pub struct ResultTextSelectionSet<'store> {
    pub(crate) tset: TextSelectionSet,
    pub(crate) rootstore: &'store AnnotationStore,
}

impl<'store> ResultTextSelectionSet<'store> {
    pub fn as_ref(&self) -> &TextSelectionSet {
        &self.tset
    }
}

pub struct TextSelectionSetIntoIter {
    tset: TextSelectionSet,
    cursor: Option<usize>,
}

impl IntoIterator for TextSelectionSet {
    type Item = TextSelection;
    type IntoIter = TextSelectionSetIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        TextSelectionSetIntoIter {
            tset: self,
            cursor: Some(0),
        }
    }
}

impl Iterator for TextSelectionSetIntoIter {
    type Item = TextSelection;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cursor) = self.cursor.as_mut() {
            let result: Option<&TextSelection> = self.tset.data.get(*cursor);
            if result.is_some() {
                *cursor += 1;
                return result.copied();
            }
        }
        self.cursor = None;
        None
    }
}

impl<'store> From<ResultItem<'store, TextSelection>> for TextSelectionSet {
    fn from(textselection: ResultItem<'store, TextSelection>) -> Self {
        let mut tset = Self::new(
            textselection
                .store()
                .handle()
                .expect("Resource must have a handle"),
        );
        tset.add(textselection.as_ref().clone());
        tset
    }
}

impl<'store> From<ResultTextSelection<'store>> for TextSelectionSet {
    fn from(textselection: ResultTextSelection<'store>) -> Self {
        let mut tset = Self::new(
            textselection
                .store()
                .handle()
                .expect("Resource must have a handle"),
        );
        tset.add(match textselection {
            ResultTextSelection::Unbound(_, _, item) => item,
            ResultTextSelection::Bound(item) => item.as_ref().clone(),
        });
        tset
    }
}

impl<'store> FromIterator<ResultItem<'store, TextSelection>> for TextSelectionSet {
    fn from_iter<T: IntoIterator<Item = ResultItem<'store, TextSelection>>>(iter: T) -> Self {
        let mut tset = Self {
            data: SmallVec::new(),
            resource: TextResourceHandle::new(0), //dummy! to be set later
            sorted: false,
        };
        let mut first = true;
        for item in iter {
            if first {
                tset.resource = item.store().handle().expect("resource must have handle");
                first = false;
            }
            tset.add(item.as_ref().clone());
        }
        tset
    }
}

impl<'store> FromIterator<ResultTextSelection<'store>> for TextSelectionSet {
    fn from_iter<T: IntoIterator<Item = ResultTextSelection<'store>>>(iter: T) -> Self {
        let mut tset = Self {
            data: SmallVec::new(),
            resource: TextResourceHandle::new(0), //dummy! to be set later
            sorted: false,
        };
        let mut first = true;
        for item in iter {
            if first {
                tset.resource = item.store().handle().expect("resource must have handle");
                first = false;
            }
            tset.add(match item {
                ResultTextSelection::Unbound(_, _, item) => item,
                ResultTextSelection::Bound(item) => item.as_ref().clone(),
            });
        }
        tset
    }
}

impl<'store> From<ResultTextSelectionSet<'store>> for TextSelectionSet {
    fn from(other: ResultTextSelectionSet<'store>) -> Self {
        other.tset
    }
}

impl PartialOrd for TextSelectionSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if let (Some(begin), Some(otherbegin)) = (self.begin(), other.begin()) {
            let ord = begin.cmp(&otherbegin);
            if ord != Ordering::Equal {
                Some(ord)
            } else {
                let end = self.end().unwrap();
                let otherend = other.end().unwrap();
                Some(end.cmp(&otherend))
            }
        } else {
            None
        }
    }
}

#[sealed]
impl TypeInfo for TextSelectionSet {
    fn typeinfo() -> Type {
        Type::TextSelectionSet
    }
}

pub struct TextSelectionSetIter<'a> {
    iter: Iter<'a, TextSelection>,
    count: usize,
    len: usize,
}

impl<'a> Iterator for TextSelectionSetIter<'a> {
    type Item = &'a TextSelection;

    fn next(&mut self) -> Option<Self::Item> {
        self.count += 1;
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let l = self.len - self.count;
        //the lower-bound may be an overestimate (if there are deleted items)
        (l, Some(l))
    }
}

/*
    /// Returns a text selection => annotation map that is referenced from the specified resource
    pub fn get_by_resource(
        &self,
        resource_handle: TextResourceHandle,
    ) -> Option<&BTreeMap<TextSelection, Vec<AnnotationHandle>>> {
        self.data.get(resource_handle.unwrap())
    }

    /// Returns annotations that are referenced from the specified resource and text selection
    pub fn get_by_textselection(
        &self,
        resource_handle: TextResourceHandle,
        textselection: &TextSelection,
    ) -> Option<&Vec<AnnotationHandle>> {
        if let Some(map) = self.get_by_resource(resource_handle) {
            map.get(textselection)
        } else {
            None
        }
    }
*/

/// The TextSelectionOperator, simply put, allows comparison of two [`TextSelection`] instances. It
/// allows testing for all kinds of spatial relations (as embodied by this enum) in which two
/// [`TextSelection`] instances can be, such as overlap, embedding, adjacency, etc...
///
/// Rather than operator on single [`TextSelection`] instances, the implementation goes a bit
/// further and can act also on the basis of [`TextSelectionSet`] rather than [`TextSelection`],
/// allowing you to compare two sets, each containing possibly multiple TextSelections, at once.
///
/// Use the various methods on this type to quickly instantiate a variant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TextSelectionOperator {
    /// Both sets cover the exact same TextSelections, and all are covered (cf. textfabric's `==`), commutative, transitive
    Equals { all: bool, negate: bool },

    /// All items in both sets must cover the exact same TextSelection. This would be fairly useless, it just means both sets contain only one TextSelection and it's the same one
    // EqualsAll(&'a TextSelectionSet),

    /// Each TextSelection in A overlaps with a TextSelection in B (cf. textfabric's `&&`), commutative
    /// If modifier `all` is set: Each TextSelection in A overlaps with all TextSelection in B (cf. textfabric's `&&`), commutative
    Overlaps { all: bool, negate: bool },

    /// All TextSelections in B are embedded by a TextSelection in A (cf. textfabric's `[[`)
    /// If modifier `all` is set: All TextSelections in B are embedded by all TextSelection in A (cf. textfabric's `[[`)
    Embeds { all: bool, negate: bool },

    /// All TextSelections in A are embedded by a TextSelection in B (cf. textfabric's `]]`)
    /// If modifier `all` is set: All TextSelections in A are embedded by all TextSelection in B (cf. textfabric's `]]`)
    /// The `limit`, if set, constrains the lookup range (in unicode points), which can positively affect performance
    Embedded {
        all: bool,
        negate: bool,
        limit: Option<usize>,
    },

    /// Each TextSelections in A comes before a textselection in B
    /// If modifier `all` is set: All TextSelections in A precede (come before) all textselections in B. There is no overlap (cf. textfabric's `<<`)
    /// The `limit`, if set, constrains the lookup range (in unicode points), which can positively affect performance
    Before {
        all: bool,
        negate: bool,
        limit: Option<usize>,
    },

    /// Each TextSeleciton In A succeeds (comes after) a textselection in B
    /// If modifier `all` is set: All TextSelections in A succeed (come after) all textselections in B. There is no overlap (cf. textfabric's `>>`)
    /// The `limit`, if set, constrains the lookup range (in unicode points), which can positively affect performance
    After {
        all: bool,
        negate: bool,
        limit: Option<usize>,
    },

    /// Each TextSelection in A ends where at least one TextSelection in B begins.
    /// If modifier `all` is set: The rightmost TextSelections in A end where the leftmost TextSelection in B begins  (cf. textfabric's `<:`)
    Precedes { all: bool, negate: bool },

    /// Each TextSelection in A begis where at least one TextSelection in A ends.
    /// If modifier `all` is set: The leftmost TextSelection in A starts where the rightmost TextSelection in B ends  (cf. textfabric's `:>`)
    Succeeds { all: bool, negate: bool },

    /// Each TextSelection in A starts where a TextSelection in B starts
    /// If modifier `all` is set: The leftmost TextSelection in A starts where the leftmost TextSelection in B start  (cf. textfabric's `=:`)
    SameBegin { all: bool, negate: bool },

    /// Each TextSelection in A ends where a TextSelection in B ends
    /// If modifier `all` is set: The rightmost TextSelection in A ends where the rights TextSelection in B ends  (cf. textfabric's `:=`)
    SameEnd { all: bool, negate: bool },

    /// Each TextSelection in A is in B as well, this is similar to Equals but allows
    /// for set B having unmatched items
    InSet { all: bool, negate: bool },

    /// The leftmost TextSelection in A starts where the leftmost TextSelection in A starts  and
    /// the rightmost TextSelection in A ends where the rights TextSelection in B ends  (cf. textfabric's `::`)
    SameRange { all: bool, negate: bool },
}

impl TextSelectionOperator {
    // Is this operator an All variant?
    pub fn all(&self) -> bool {
        match self {
            Self::Equals { all, .. }
            | Self::Overlaps { all, .. }
            | Self::Embeds { all, .. }
            | Self::Embedded { all, .. }
            | Self::Before { all, .. }
            | Self::After { all, .. }
            | Self::Precedes { all, .. }
            | Self::Succeeds { all, .. }
            | Self::SameBegin { all, .. }
            | Self::SameEnd { all, .. }
            | Self::InSet { all, .. }
            | Self::SameRange { all, .. } => *all,
        }
    }

    pub fn equals() -> Self {
        Self::Equals {
            all: false,
            negate: false,
        }
    }

    pub fn overlaps() -> Self {
        Self::Overlaps {
            all: false,
            negate: false,
        }
    }

    pub fn embeds() -> Self {
        Self::Embeds {
            all: false,
            negate: false,
        }
    }

    pub fn embedded() -> Self {
        Self::Embedded {
            all: false,
            negate: false,
            limit: None,
        }
    }

    pub fn before() -> Self {
        Self::Before {
            all: false,
            negate: false,
            limit: None,
        }
    }

    pub fn after() -> Self {
        Self::After {
            all: false,
            negate: false,
            limit: None,
        }
    }

    pub fn precedes() -> Self {
        Self::Precedes {
            all: false,
            negate: false,
        }
    }

    pub fn succeeds() -> Self {
        Self::Succeeds {
            all: false,
            negate: false,
        }
    }

    pub fn samebegin() -> Self {
        Self::SameBegin {
            all: false,
            negate: false,
        }
    }

    pub fn sameend() -> Self {
        Self::SameEnd {
            all: false,
            negate: false,
        }
    }

    pub fn samerange() -> Self {
        Self::SameRange {
            all: false,
            negate: false,
        }
    }

    pub fn inset() -> Self {
        Self::InSet {
            all: false,
            negate: false,
        }
    }

    /// Constrains the operator to a limit range (in unicode points)
    pub fn with_limit(self, limit: usize) -> Self {
        match self {
            Self::Embedded { all, negate, .. } => Self::Embedded {
                all,
                negate,
                limit: Some(limit),
            },
            Self::Before { all, negate, .. } => Self::Before {
                all,
                negate,
                limit: Some(limit),
            },
            Self::After { all, negate, .. } => Self::After {
                all,
                negate,
                limit: Some(limit),
            },
            _ => self,
        }
    }

    pub fn toggle_negate(&self) -> Self {
        match self {
            Self::Equals { all, negate } => Self::Equals {
                all: *all,
                negate: !negate,
            },
            Self::Overlaps { all, negate } => Self::Overlaps {
                all: *all,
                negate: !negate,
            },
            Self::Embeds { all, negate } => Self::Embeds {
                all: *all,
                negate: !negate,
            },
            Self::Embedded { all, negate, limit } => Self::Embedded {
                all: *all,
                negate: !negate,
                limit: *limit,
            },
            Self::Before { all, negate, limit } => Self::Before {
                all: *all,
                negate: !negate,
                limit: *limit,
            },
            Self::After { all, negate, limit } => Self::After {
                all: *all,
                negate: !negate,
                limit: *limit,
            },
            Self::Precedes { all, negate } => Self::Precedes {
                all: *all,
                negate: !negate,
            },
            Self::Succeeds { all, negate } => Self::Succeeds {
                all: *all,
                negate: !negate,
            },
            Self::SameBegin { all, negate } => Self::SameBegin {
                all: *all,
                negate: !negate,
            },
            Self::SameEnd { all, negate } => Self::SameEnd {
                all: *all,
                negate: !negate,
            },
            Self::InSet { all, negate } => Self::InSet {
                all: *all,
                negate: !negate,
            },
            Self::SameRange { all, negate } => Self::SameRange {
                all: *all,
                negate: !negate,
            },
        }
    }

    pub fn toggle_all(&self) -> Self {
        match self {
            Self::Equals { all, negate } => Self::Equals {
                all: !all,
                negate: *negate,
            },
            Self::Overlaps { all, negate } => Self::Overlaps {
                all: !all,
                negate: *negate,
            },
            Self::Embeds { all, negate } => Self::Embeds {
                all: !all,
                negate: *negate,
            },
            Self::Embedded { all, negate, limit } => Self::Embedded {
                all: !all,
                negate: *negate,
                limit: *limit,
            },
            Self::Before { all, negate, limit } => Self::Before {
                all: !all,
                negate: *negate,
                limit: *limit,
            },
            Self::After { all, negate, limit } => Self::After {
                all: !all,
                negate: *negate,
                limit: *limit,
            },
            Self::Precedes { all, negate } => Self::Precedes {
                all: !all,
                negate: *negate,
            },
            Self::Succeeds { all, negate } => Self::Succeeds {
                all: !all,
                negate: *negate,
            },
            Self::SameBegin { all, negate } => Self::SameBegin {
                all: !all,
                negate: *negate,
            },
            Self::SameEnd { all, negate } => Self::SameEnd {
                all: !all,
                negate: *negate,
            },
            Self::InSet { all, negate } => Self::InSet {
                all: !all,
                negate: *negate,
            },
            Self::SameRange { all, negate } => Self::SameRange {
                all: !all,
                negate: *negate,
            },
        }
    }
}

/// This trait defines the `test()` methods for testing relations between two text selections (or sets thereof).
pub trait TestTextSelection {
    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two TextSelections.
    /// A boolean is returned with the test result.
    fn test(&self, operator: &TextSelectionOperator, reftextsel: &TextSelection) -> bool;

    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two TextSelections .
    /// A boolean is returned with the test result.
    fn test_set(&self, operator: &TextSelectionOperator, refset: &TextSelectionSet) -> bool;
}

impl TextSelectionSet {
    pub fn new(resource: TextResourceHandle) -> Self {
        Self {
            data: SmallVec::new(),
            resource,
            sorted: false,
        }
    }

    pub fn as_resultset(self, store: &AnnotationStore) -> ResultTextSelectionSet {
        ResultTextSelectionSet {
            tset: self,
            rootstore: store,
        }
    }

    pub fn add(&mut self, textselection: TextSelection) -> &mut Self {
        if self.sorted {
            //once sorted, we respect the order
            match self.data.binary_search(&textselection) {
                Ok(_) => {} //element already exists
                Err(pos) => self.data.insert(pos, textselection),
            };
        } else {
            self.data.push(textselection);
        }
        self
    }

    /// Iterate over the text selections
    pub fn iter<'a>(&'a self) -> TextSelectionSetIter<'a> {
        TextSelectionSetIter {
            iter: self.data.iter(),
            count: 0,
            len: self.data.len(),
        }
    }

    pub fn has_handle(&self, handle: TextSelectionHandle) -> bool {
        self.data.iter().any(|t| t.handle() == Some(handle))
    }

    pub fn get(&self, index: usize) -> Option<&TextSelection> {
        self.data.get(index)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn resource(&self) -> TextResourceHandle {
        self.resource
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /*
    /// Intersect this set (A) with another (B). Modifies this set so only the elements
    /// present in both are left after the operation.
    pub fn intersect_mut(&mut self, other: &TextSelectionSet) {
        self.test_intersect_mut(&TextSelectionOperator::InSet(other))
    }

    /// Intersect this set (A) with another (B) using a specific test operator (which also includes set B). Modifies this set so only the elements
    /// present in both are left after the operation.
    pub fn test_intersect_mut(&mut self, operator: &TextSelectionOperator) {
        self.data.retain(|(item, _, _)| item.test_set(operator, refset));
    }

    /// Intersect this set (A) with another (B) and returns the new intersection set.
    pub fn intersect(&mut self, other: &TextSelectionSet) -> Self {
        self.test_intersect(&TextSelectionOperator::InSet, other)
    }

    /// Intersect this set (A) with another (B) using a specific operator (which also includes set B) and returns the new intersection set.
    pub fn test_intersect(&mut self, operator: &TextSelectionOperator) -> Self {
        let mut intersection = Self {
            data: smallvec![],
            sorted: self.sorted,
        };
        for (item, resource, annotation) in self.iter() {
            if item.test(operator) {
                intersection.insert(*item, *resource, *annotation);
            }
        }
        intersection
    }
    */

    pub fn begin(&self) -> Option<usize> {
        self.leftmost().map(|x| x.begin())
    }
    pub fn end(&self) -> Option<usize> {
        self.rightmost().map(|x| x.end())
    }

    /// Returns the left-most TextSelection (the one with the lowest start offset) in the set.
    pub fn leftmost(&self) -> Option<&TextSelection> {
        if self.is_empty() {
            None
        } else {
            if self.sorted {
                self.data.get(0)
            } else {
                let mut leftmost: Option<&TextSelection> = None;
                for item in self.iter() {
                    if leftmost.is_none() || item.begin < leftmost.unwrap().begin {
                        leftmost = Some(item);
                    }
                }
                leftmost
            }
        }
    }

    /// Returns the right-most TextSelection (the one with the highest end offset) in the set.
    pub fn rightmost(&self) -> Option<&TextSelection> {
        if self.is_empty() {
            None
        } else {
            if self.sorted {
                self.data.get(self.data.len() - 1)
            } else {
                let mut rightmost: Option<&TextSelection> = None;
                for item in self.iter() {
                    if rightmost.is_none() || item.end > rightmost.unwrap().end {
                        rightmost = Some(item);
                    }
                }
                rightmost
            }
        }
    }

    /// Sorts the TextSelections in this set in  canonical text order. This needs to be done only once.
    /// Once the set is sorted, future inserts will retain the order (and therefore be slower)
    pub fn sort(&mut self) {
        if !self.sorted {
            self.data.sort_unstable();
            self.sorted = true;
        }
    }
}

impl TestTextSelection for TextSelectionSet {
    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two [`TextSelectionSet`]s.
    /// A boolean is returned with the test result.
    fn test(&self, operator: &TextSelectionOperator, reftextsel: &TextSelection) -> bool {
        if self.is_empty() {
            return false;
        }
        match operator {
            TextSelectionOperator::Equals {
                all: false,
                negate: false,
            } => {
                //ALL of the items in this set must match with ANY item in the otherset
                for item in self.iter() {
                    if !item.test(operator, reftextsel) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Overlaps {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Embeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Embedded {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::Before {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::After {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::Precedes {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::SameBegin {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::SameEnd {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::InSet {
                all: false,
                negate: false,
            } => {
                // ALL of the items in this set must match with ANY item in the otherset
                // This is a weaker form of Equals (could have also been called SameRange)
                for item in self.iter() {
                    if !item.test(operator, reftextsel) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Overlaps {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Embeds {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Embedded {
                all: true,
                negate: false,
                ..
            } => {
                //all of the items in this set must match with all item in the otherset (this code isn't different from the previous one, the different code happens in the delegated test() method
                for item in self.iter() {
                    if !item.test(operator, reftextsel) {
                        return false;
                    }
                }
                true
            }
            //we can unrwap leftmost/rightmost safely because we tested at the start whether the set was empty or not
            TextSelectionOperator::Precedes {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Before {
                all: true,
                negate: false,
                ..
            }
            | TextSelectionOperator::SameEnd {
                all: true,
                negate: false,
            } => self.rightmost().unwrap().test(operator, reftextsel),
            TextSelectionOperator::Succeeds {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::After {
                all: true,
                negate: false,
                ..
            }
            | TextSelectionOperator::SameBegin {
                all: true,
                negate: false,
            } => self.leftmost().unwrap().test(operator, reftextsel),
            TextSelectionOperator::SameRange {
                all: true,
                negate: false,
            } => {
                self.leftmost().unwrap().test(operator, reftextsel)
                    && self.rightmost().unwrap().test(operator, reftextsel)
            }

            //negations
            TextSelectionOperator::Equals { negate: true, .. }
            | TextSelectionOperator::Overlaps { negate: true, .. }
            | TextSelectionOperator::Embeds { negate: true, .. }
            | TextSelectionOperator::Embedded { negate: true, .. }
            | TextSelectionOperator::Before { negate: true, .. }
            | TextSelectionOperator::After { negate: true, .. }
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::SameBegin { negate: true, .. }
            | TextSelectionOperator::SameEnd { negate: true, .. }
            | TextSelectionOperator::InSet { negate: true, .. } => {
                !self.test(&operator.toggle_negate(), reftextsel)
            }
            _ => unreachable!("unknown operator+modifier combination"),
        }
    }

    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two [`TextSelectionSet`]s.
    /// A boolean is returned with the test result.
    fn test_set(&self, operator: &TextSelectionOperator, refset: &TextSelectionSet) -> bool {
        if self.is_empty() {
            return false;
        }
        match operator {
            TextSelectionOperator::Equals {
                all: false,
                negate: false,
            } => {
                if self.len() != refset.len() {
                    //each item must have a counterpart so the sets must be equal length
                    return false;
                }
                //ALL of the items in this set must match with ANY item in the otherset
                for item in self.iter() {
                    if !item.test_set(operator, refset) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Overlaps {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Embeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Embedded {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::Before {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::After {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::Precedes {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::SameBegin {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::SameEnd {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::InSet {
                all: false,
                negate: false,
            } => {
                // ALL of the items in this set must match with ANY item in the otherset
                // This is a weaker form of Equals (could have also been called SameRange)
                for item in self.iter() {
                    if !item.test_set(operator, refset) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Overlaps {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Embeds {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Embedded {
                all: true,
                negate: false,
                ..
            } => {
                //all of the items in this set must match with all item in the otherset (this code isn't different from the previous one, the different code happens in the delegated test() method
                for item in self.iter() {
                    if !item.test_set(operator, refset) {
                        return false;
                    }
                }
                true
            }
            //we can unrwap leftmost/rightmost safely because we tested at the start whether the set was empty or not
            TextSelectionOperator::Precedes {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Before {
                all: true,
                negate: false,
                ..
            }
            | TextSelectionOperator::SameEnd {
                all: true,
                negate: false,
            } => self.rightmost().unwrap().test_set(operator, refset),
            TextSelectionOperator::Succeeds {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::After {
                all: true,
                negate: false,
                ..
            }
            | TextSelectionOperator::SameBegin {
                all: true,
                negate: false,
            } => self.leftmost().unwrap().test_set(operator, refset),
            TextSelectionOperator::SameRange {
                all: true,
                negate: false,
            } => {
                self.leftmost().unwrap().test_set(operator, refset)
                    && self.rightmost().unwrap().test_set(operator, refset)
            }

            //negations
            TextSelectionOperator::Equals { negate: true, .. }
            | TextSelectionOperator::Overlaps { negate: true, .. }
            | TextSelectionOperator::Embeds { negate: true, .. }
            | TextSelectionOperator::Embedded { negate: true, .. }
            | TextSelectionOperator::Before { negate: true, .. }
            | TextSelectionOperator::After { negate: true, .. }
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::SameBegin { negate: true, .. }
            | TextSelectionOperator::SameEnd { negate: true, .. }
            | TextSelectionOperator::InSet { negate: true, .. } => {
                !self.test_set(&operator.toggle_negate(), refset)
            }
            _ => unreachable!("unknown operator+modifier combination"),
        }
    }
}

impl TestTextSelection for TextSelection {
    /// This method is called to test whether a specific spatial relation (as expressed by the
    /// passed operator) holds between a [`TextSelection`] and another.
    /// A boolean is returned with the test result.
    fn test(&self, operator: &TextSelectionOperator, reftextsel: &TextSelection) -> bool {
        //note: at this level we deal with two singletons and there is no different between the *All variants
        match operator {
            TextSelectionOperator::Equals { negate: false, .. }
            | TextSelectionOperator::InSet { negate: false, .. } => self == reftextsel,
            TextSelectionOperator::Overlaps { negate: false, .. } => {
                //item must be equal overlap with any of the items in the other set
                (reftextsel.begin >= self.begin && reftextsel.begin < self.end)
                    || (reftextsel.end > self.begin && reftextsel.end <= self.end)
                    || (reftextsel.begin <= self.begin && reftextsel.end >= self.end)
                    || (self.begin <= reftextsel.begin && self.end >= reftextsel.end)
            }
            TextSelectionOperator::Embeds { negate: false, .. } => {
                // TextSelection embeds reftextsel
                reftextsel.begin >= self.begin && reftextsel.end <= self.end
            }
            TextSelectionOperator::Embedded {
                negate: false,
                limit: Some(limit),
                ..
            } => {
                // TextSelection is embedded reftextsel
                self.begin >= reftextsel.begin
                    && self.end <= reftextsel.end
                    && self.begin - reftextsel.begin <= *limit
                    && reftextsel.end - self.end <= *limit
            }
            TextSelectionOperator::Embedded { negate: false, .. } => {
                // TextSelection is embedded reftextsel
                self.begin >= reftextsel.begin && self.end <= reftextsel.end
            }
            TextSelectionOperator::Before {
                negate: false,
                limit: Some(limit),
                ..
            } => self.end <= reftextsel.begin && reftextsel.begin - self.end <= *limit,
            TextSelectionOperator::Before { negate: false, .. } => self.end <= reftextsel.begin,
            TextSelectionOperator::After {
                negate: false,
                limit: Some(limit),
                ..
            } => self.begin >= reftextsel.end && self.begin - reftextsel.end <= *limit,
            TextSelectionOperator::After { negate: false, .. } => self.begin >= reftextsel.end,
            TextSelectionOperator::Precedes { negate: false, .. } => self.end == reftextsel.begin,
            TextSelectionOperator::Succeeds { negate: false, .. } => reftextsel.end == self.begin,
            TextSelectionOperator::SameBegin { negate: false, .. } => {
                self.begin == reftextsel.begin
            }
            TextSelectionOperator::SameEnd { negate: false, .. } => self.end == reftextsel.end,
            TextSelectionOperator::SameRange { negate: false, .. } => {
                self.begin == reftextsel.begin && self.end == reftextsel.end
            }

            //negations
            TextSelectionOperator::Equals { negate: true, .. }
            | TextSelectionOperator::Overlaps { negate: true, .. }
            | TextSelectionOperator::Embeds { negate: true, .. }
            | TextSelectionOperator::Embedded { negate: true, .. }
            | TextSelectionOperator::Before { negate: true, .. }
            | TextSelectionOperator::After { negate: true, .. }
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::SameBegin { negate: true, .. }
            | TextSelectionOperator::SameEnd { negate: true, .. }
            | TextSelectionOperator::InSet { negate: true, .. } => {
                !self.test(&operator.toggle_negate(), reftextsel)
            }
            _ => unreachable!("unknown operator+modifier combination"),
        }
    }
    /// This method is called to test whether a specific spatial relation (as expressed by the
    /// passed operator) holds between a [`TextSelection`] and another (or multiple)
    /// ([`TextSelectionSet`]). The operator contains the other part of the equation that is tested
    /// against. A boolean is returned with the test result.
    fn test_set(&self, operator: &TextSelectionOperator, refset: &TextSelectionSet) -> bool {
        match operator {
            TextSelectionOperator::Equals {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Overlaps {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Embeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Embedded {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::Before {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::After {
                all: false,
                negate: false,
                ..
            }
            | TextSelectionOperator::Precedes {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::SameBegin {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::SameEnd {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::InSet {
                all: false,
                negate: false,
            } => {
                for reftextsel in refset.iter() {
                    if self.test(operator, reftextsel) {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::Overlaps {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Embeds {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Embedded {
                all: true,
                negate: false,
                ..
            }
            | TextSelectionOperator::Before {
                all: true,
                negate: false,
                ..
            }
            | TextSelectionOperator::After {
                all: true,
                negate: false,
                ..
            } => {
                if refset.is_empty() {
                    return false;
                }
                for reftextsel in refset.iter() {
                    if !self.test(operator, reftextsel) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Precedes {
                all: true,
                negate: false,
            } => {
                if refset.is_empty() {
                    return false;
                }
                let mut leftmost = None;
                for other in refset.iter() {
                    if leftmost.is_none() || other.begin < leftmost.unwrap() {
                        leftmost = Some(other.begin);
                    }
                }
                Some(self.end) == leftmost
            }
            TextSelectionOperator::Succeeds {
                all: true,
                negate: false,
            } => {
                if refset.is_empty() {
                    return false;
                }
                let mut rightmost = None;
                for other in refset.iter() {
                    if rightmost.is_none() || other.end > rightmost.unwrap() {
                        rightmost = Some(other.end);
                    }
                }
                Some(self.begin) == rightmost
            }
            TextSelectionOperator::SameBegin {
                all: true,
                negate: false,
            } => {
                if refset.is_empty() {
                    return false;
                }
                self.begin == refset.leftmost().unwrap().begin()
            }
            TextSelectionOperator::SameEnd {
                all: true,
                negate: false,
            } => {
                if refset.is_empty() {
                    return false;
                }
                self.end == refset.rightmost().unwrap().end()
            }
            TextSelectionOperator::SameRange {
                all: true,
                negate: false,
            } => {
                if refset.is_empty() {
                    return false;
                }
                self.begin == refset.leftmost().unwrap().begin()
                    && self.end == refset.rightmost().unwrap().end()
            }

            //negations
            TextSelectionOperator::Equals { negate: true, .. }
            | TextSelectionOperator::Overlaps { negate: true, .. }
            | TextSelectionOperator::Embeds { negate: true, .. }
            | TextSelectionOperator::Embedded { negate: true, .. }
            | TextSelectionOperator::Before { negate: true, .. }
            | TextSelectionOperator::After { negate: true, .. }
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::SameBegin { negate: true, .. }
            | TextSelectionOperator::SameEnd { negate: true, .. }
            | TextSelectionOperator::InSet { negate: true, .. } => {
                !self.test_set(&operator.toggle_negate(), refset)
            }
            _ => unreachable!("unknown operator+modifier combination"),
        }
    }

    //there are no to_json() methods etc here, but you can convert a TextSelection to an Offset, which does have them
}

impl Extend<TextSelection> for TextSelectionSet {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = TextSelection>,
    {
        for x in iter {
            self.add(x);
        }
    }
}

impl TextResource {
    /// Apply a [`TextSelectionOperator`] to find text selections. Iterates over them in textual order.
    /// This is a low-level method. Use [`ResultItem<TextResource>::find_textselections()`] instead.
    pub(crate) fn textselections_by_operator<'store>(
        &'store self,
        operator: TextSelectionOperator,
        refset: TextSelectionSet,
    ) -> FindTextSelectionsIter<'store> {
        FindTextSelectionsIter {
            resource: self,
            operator,
            refset,
            textseliter_index: 0,
            textseliters: Vec::new(),
            buffer: VecDeque::new(),
            drain_buffer: false,
        }
    }
}

/// Iterator that finds text selections. This iterator owns the [`TextSelectionSet`] that is being compared against.
pub struct FindTextSelectionsIter<'store> {
    resource: &'store TextResource,
    operator: TextSelectionOperator,
    refset: TextSelectionSet,

    /// Iterator over TextSelections in self (second-level)
    /// The boolean represents direction of the iterator (forward=true,backwards=false)
    textseliters: Vec<(TextSelectionIter<'store>, bool)>,
    textseliter_index: usize,

    buffer: VecDeque<TextSelectionHandle>,

    // once bufferiter is set, we simply drain the buffer
    drain_buffer: bool,
}

impl<'store> Iterator for FindTextSelectionsIter<'store> {
    type Item = TextSelectionHandle;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.drain_buffer {
                return self.buffer.pop_front();
            } else if let Some(result) = self.next_textselection() {
                //this will eventually set self.drain_buffer = true and trigger the stop condition once the buffer is empty
                return Some(result);
            }
        }
    }
}

/// Private trait implementing the actual behaviour for [`FindTextSelectionsIter`]
impl<'store> FindTextSelectionsIter<'store> {
    /// Returns the resource associated with this iterator
    pub fn resource(&self) -> &'store TextResource {
        self.resource
    }

    /// This method returns an iterator over TextSelections in the resource
    /// It attempts to return the smallest sliced iterator possible, depending
    /// on the operator
    /// The reference text selection is always in the subject position for the associated [`TextSelectionOperator`] (`operator()`)
    /// The boolean returns the direction of iteration (true = forward, false = backwards)
    fn init_textseliters(&mut self) {
        match self.operator {
            TextSelectionOperator::Embeds { .. } => {
                for reftextselection in self.refset.iter() {
                    self.textseliters.push((
                        self.resource
                            .range(reftextselection.begin(), reftextselection.end()),
                        true,
                    ));
                }
            }
            TextSelectionOperator::SameBegin { .. } => {
                self.textseliters.push((
                    self.resource.range(
                        self.refset.begin().unwrap(),
                        self.refset.begin().unwrap() + 1,
                    ),
                    true,
                ));
            }
            TextSelectionOperator::SameEnd { .. } => {
                self.textseliters.push((
                    self.resource
                        .range(self.refset.end().unwrap(), self.refset.end().unwrap() + 1),
                    false, //search backwards! end must be in range above
                ));
            }
            TextSelectionOperator::After { limit, .. } => {
                //self comes after found items, so find items before self:
                let begin = if let Some(limit) = limit {
                    if limit >= self.refset.begin().unwrap() {
                        0
                    } else {
                        self.refset.begin().unwrap() - limit
                    }
                } else {
                    0
                };
                self.textseliters.push((
                    self.resource.range(begin, self.refset.begin().unwrap()),
                    true,
                ));
            }
            TextSelectionOperator::Succeeds { .. } => {
                self.textseliters.push((
                    self.resource.range(
                        self.refset.begin().unwrap(),
                        self.refset.begin().unwrap() + 1,
                    ),
                    false, //search backwards!! end must be in range above
                ));
            }
            TextSelectionOperator::Before { limit, .. } => {
                //self comes before found items, so find items after self:
                let end = if let Some(limit) = limit {
                    self.refset.end().unwrap() + limit
                } else {
                    self.resource.textlen()
                };
                self.textseliters
                    .push((self.resource.range(self.refset.end().unwrap(), end), true));
            }
            TextSelectionOperator::Precedes { .. } => {
                self.textseliters.push((
                    self.resource
                        .range(self.refset.end().unwrap(), self.refset.end().unwrap() + 1),
                    true,
                ));
            }
            TextSelectionOperator::Embedded {
                limit: Some(limit), ..
            } => {
                let halfway = self.resource.textlen() / 2;
                for reftextselection in self.refset.iter() {
                    if reftextselection.begin() <= halfway {
                        let begin = if reftextselection.begin() > limit {
                            reftextselection.begin() - limit
                        } else {
                            0
                        };
                        self.textseliters
                            .push((self.resource.range(begin, reftextselection.end()), true));
                    } else {
                        let mut end = reftextselection.end() + limit;
                        if end > self.resource.textlen() {
                            end = self.resource.textlen();
                        }
                        self.textseliters.push((
                            self.resource.range(reftextselection.end(), end),
                            false, //search backwards!!
                        ));
                    }
                }
            }
            TextSelectionOperator::Overlaps { .. } | TextSelectionOperator::Embedded { .. } => {
                let halfway = self.resource.textlen() / 2;
                for reftextselection in self.refset.iter() {
                    if reftextselection.begin() <= halfway {
                        self.textseliters
                            .push((self.resource.range(0, reftextselection.end()), true));
                    } else {
                        self.textseliters.push((
                            self.resource
                                .range(reftextselection.end(), self.resource.textlen()),
                            false, //search backwards!!
                        ));
                    }
                }
            }
            _ => {
                self.textseliters.push((self.resource.iter(), true)); //return the maximum slice
            }
        }
    }

    /// Main function invoked from calling iterator's next() method
    /// Returns the next textselection (if any) and a boolean indicating whether to recurse further
    /// If this function returns None, the caller function will loop/recurse
    /// Internally this may iterate backwards over a double ended iterator (but results will be reversed and ordered again)
    fn next_textselection(&mut self) -> Option<TextSelectionHandle> {
        if let TextSelectionOperator::Equals {
            negate: false,
            all: false,
        } = self.operator
        {
            // this operator is handled separately, we don't need a secondary iterator (textseliter) for it at all
            // we just find the exact selections by offset
            for reftextselection in self.refset.iter() {
                if let Ok(Some(handle)) = self
                    .resource
                    .known_textselection(&Offset::from(reftextselection))
                {
                    if self.refset.len() == 1 {
                        //shortcut without buffer
                        self.drain_buffer = true;
                        return Some(handle);
                    } else {
                        self.buffer.push_back(handle);
                    }
                } else {
                    //all in refset must be found, or none are returned at all
                    self.drain_buffer = true;
                    return None;
                }
            }
            self.drain_buffer = true;
            None //triggers normal looping behaviour
        } else {
            //------ normal behaviour ----------
            // The relationship A <operator> B (A=store, B=ref) must hold for each item in A with ANY item in B, all matches are immediately returned
            // note: negation is considered by the test() and self.new_textseliter() methods
            // note: the 'all' property has no impact on item collection as done by FindTextSelectionsIter, items in B are considered one by one
            if self.textseliters.is_empty() {
                //sets the iterator for TextSelections in the store to iterate over,
                //which we prefer to keep as small as possible (based on the operator)
                self.init_textseliters();
            }
            let forward = self.textseliters.get_mut(self.textseliter_index).unwrap().1;
            if forward {
                if let Some(textselection) = self
                    .textseliters
                    .get_mut(self.textseliter_index)
                    .unwrap()
                    .0
                    .next()
                {
                    if self.refset.test(&self.operator, textselection)
                        && !self.refset.has_handle(textselection.handle().unwrap())
                    //       ^------ do not include the item itself
                    {
                        if !self.buffer.is_empty() {
                            //we've already used the buffer, so we'll have to keep doing it otherwise results are not in proper order
                            self.buffer.push_back(textselection.handle().unwrap());
                        } else {
                            return Some(textselection.handle().unwrap());
                        }
                    }
                } else {
                    self.next_iterator();
                }
            } else {
                //------ reverse iteration ----------
                while let Some(textselection) = self
                    .textseliters
                    .get_mut(self.textseliter_index)
                    .as_mut()
                    .unwrap()
                    .0
                    .next_back()
                {
                    if self.refset.test(&self.operator, textselection)
                        && !self.refset.has_handle(textselection.handle().unwrap())
                    //       ^------ do not include the item itself
                    {
                        self.buffer.push_front(textselection.handle().unwrap())
                    }
                }
                self.next_iterator();
            }
            None //triggers normal looping behaviour
        }
    }

    fn next_iterator(&mut self) {
        self.textseliter_index += 1;
        if self.textseliter_index >= self.textseliters.len() {
            //no more iterators, trigger buffer draining
            self.drain_buffer = true;
        }
    }
}

#[derive(Debug, Clone)]
/// This structure holds a [`TextSelection`], along with references to its [`TextResource`] and the
/// [`AnnotationStore`] and provides a high-level API on it.
///
/// The text selection may either be bound, in which case it corresponds to a known/existing text selection
/// and carries a handle, or unbound, in which case it is an arbitrary textselection.
pub enum ResultTextSelection<'store> {
    Bound(ResultItem<'store, TextSelection>),
    Unbound(&'store AnnotationStore, &'store TextResource, TextSelection),
}

impl<'store> PartialEq for ResultTextSelection<'store> {
    fn eq(&self, other: &Self) -> bool {
        if std::ptr::eq(self.rootstore(), other.rootstore()) {
            if std::ptr::eq(self.store(), other.store()) {
                self.inner().eq(other.inner())
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl<'store> PartialOrd for ResultTextSelection<'store> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner().partial_cmp(other.inner())
    }
}

impl<'store> From<ResultItem<'store, TextSelection>> for ResultTextSelection<'store> {
    fn from(textselection: ResultItem<'store, TextSelection>) -> Self {
        Self::Bound(textselection)
    }
}

impl<'store> PartialEq for ResultTextSelectionSet<'store> {
    fn eq(&self, other: &Self) -> bool {
        if self.resource() == other.resource() {
            self.tset == other.tset
        } else {
            false
        }
    }
}

impl<'store> PartialOrd for ResultTextSelectionSet<'store> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.tset.partial_cmp(&other.tset)
    }
}
