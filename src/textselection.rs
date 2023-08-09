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
/// information; i.e. the begin offset and end offset.
////
/// This is similar to `Offset`, but that one uses cursors which may
/// be relative. TextSelection specified an offset in more absolute terms.
///
/// The actual reference to the [`crate::TextResource`] is not stored in this structure but should
/// accompany it explicitly when needed
///
/// On the lowest-level, this struct is obtain by a call to [`crate::annotationstore::AnnotationStore::textselection()`], which
/// resolves a [`crate::Selector::TextSelector`]  to a [`TextSelection`]. Such calls are often abstracted away by higher level methods such as [`crate::annotationstore::AnnotationStore::textselections_by_annotation()`].
pub struct TextSelection {
    #[n(0)] //for cbor (de)serialisation
    pub(crate) intid: Option<TextSelectionHandle>,
    #[n(1)]
    pub(crate) begin: usize,
    #[n(2)]
    pub(crate) end: usize,
}

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

/// The TextSelectionOperator, simply put, allows comparison of two [`TextSelection'] instances. It
/// allows testing for all kinds of spatial relations (as embodied by this enum) in which two
/// [`TextSelection`] instances can be.
///
/// Rather than operator on single [`TextSelection`] instances, te implementation goes a bit
/// further and can act also on the basis of [`TextSelectionSet`] rather than [`TextSelection`],
/// allowing you to compare two sets, each containing possibly multiple TextSelections, at once.
#[derive(Debug, Clone, Copy)]
pub enum TextSelectionOperator {
    /// Both sets occupy cover the exact same TextSelections, and all are covered (cf. textfabric's `==`), commutative, transitive
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
    Embedded { all: bool, negate: bool },

    /// Each TextSelections in A comes before a textselection in B
    /// If modifier `all` is set: All TextSelections in A precede (come before) all textselections in B. There is no overlap (cf. textfabric's `<<`)
    Before { all: bool, negate: bool },

    /// Each TextSeleciton In A succeeds (comes after) a textselection in B
    /// If modifier `all` is set: All TextSelections in A succeed (come after) all textselections in B. There is no overlap (cf. textfabric's `>>`)
    After { all: bool, negate: bool },

    /// Each TextSelection in A is ends where at least one TextSelection in B begins.
    /// If modifier `all` is set: The rightmost TextSelections in A end where the leftmost TextSelection in B begins  (cf. textfabric's `<:`)
    Precedes { all: bool, negate: bool },

    /// Each TextSelection in A is begis where at least one TextSelection in A ends.
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
        }
    }

    pub fn before() -> Self {
        Self::Before {
            all: false,
            negate: false,
        }
    }

    pub fn after() -> Self {
        Self::After {
            all: false,
            negate: false,
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
            Self::Embedded { all, negate } => Self::Embedded {
                all: *all,
                negate: !negate,
            },
            Self::Before { all, negate } => Self::Before {
                all: *all,
                negate: !negate,
            },
            Self::After { all, negate } => Self::After {
                all: *all,
                negate: !negate,
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
            Self::Embedded { all, negate } => Self::Embedded {
                all: !all,
                negate: *negate,
            },
            Self::Before { all, negate } => Self::Before {
                all: !all,
                negate: *negate,
            },
            Self::After { all, negate } => Self::After {
                all: !all,
                negate: *negate,
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
            }
            | TextSelectionOperator::Before {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::After {
                all: false,
                negate: false,
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
            }
            | TextSelectionOperator::Before {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::After {
                all: false,
                negate: false,
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
            TextSelectionOperator::Embedded { negate: false, .. } => {
                // TextSelection is embedded reftextsel
                self.begin >= reftextsel.begin && self.end <= reftextsel.end
            }
            TextSelectionOperator::Before { negate: false, .. } => self.end <= reftextsel.begin,
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
            }
            | TextSelectionOperator::Before {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::After {
                all: false,
                negate: false,
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
            }
            | TextSelectionOperator::Before {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::After {
                all: true,
                negate: false,
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
    /// Apply a [`TextSelectionOperator`] to find text selections
    /// This is a low-level method. Use [`ResultItem<TextResource>::find_textselections()`] instead.
    pub(crate) fn textselections_by_operator_ref<'store, 'q>(
        &'store self,
        operator: TextSelectionOperator,
        refset: &'q TextSelectionSet,
    ) -> FindTextSelectionsIter<'store, 'q> {
        FindTextSelectionsIter {
            resource: self,
            operator,
            refset,
            index: 0,
            textseliter: None,
            buffer: VecDeque::new(),
            drain_buffer: false,
        }
    }

    /// Apply a [`TextSelectionOperator`] to find text selections
    /// This is a low-level method. Use [`ResultItem<TextResource>::find_textselections()`] instead.
    pub(crate) fn textselections_by_operator<'store>(
        &'store self,
        operator: TextSelectionOperator,
        refset: TextSelectionSet,
    ) -> FindTextSelectionsOwnedIter<'store> {
        FindTextSelectionsOwnedIter {
            resource: self,
            operator,
            refset,
            index: 0,
            textseliter: None,
            buffer: VecDeque::new(),
            drain_buffer: false,
        }
    }
}

/// Iterator that finds text selections. This iterator borrows the [`TextSelectionSet'] that is being compared against, use [`FindTextSelectionsOwnedIter'] for an owned variant.
pub struct FindTextSelectionsIter<'store, 'q> {
    resource: &'store TextResource,
    operator: TextSelectionOperator,
    refset: &'q TextSelectionSet,

    //Iterator over the reference text selections in the operator (first-level)
    index: usize,

    /// Iterator over TextSelections in self (second-level)
    textseliter: Option<TextSelectionIter<'store>>,

    buffer: VecDeque<TextSelectionHandle>,

    // once bufferiter is set, we simply drain the buffer
    drain_buffer: bool,
}

/// Iterator that finds text selections. This iterator owns the [`TextSelectionSet'] that is being compared against. Use [`FindTextSelectionsIter'] for an borrowed variant.
pub struct FindTextSelectionsOwnedIter<'store> {
    resource: &'store TextResource,
    operator: TextSelectionOperator,
    refset: TextSelectionSet,
    index: usize,

    /// Iterator over TextSelections in self (second-level)
    textseliter: Option<TextSelectionIter<'store>>,

    buffer: VecDeque<TextSelectionHandle>,

    // once bufferiter is set, we simply drain the buffer
    drain_buffer: bool,
}

impl<'store, 'q> Iterator for FindTextSelectionsIter<'store, 'q> {
    type Item = TextSelectionHandle;

    fn next(&mut self) -> Option<Self::Item> {
        if self.drain_buffer {
            self.buffer.pop_front()
        } else {
            loop {
                match self.next_textselection() {
                    (None, false) => {
                        self.update();
                        return None;
                    }
                    (result, false) => return result,
                    (_, true) => continue, //loop
                }
            }
        }
    }
}

impl<'store> Iterator for FindTextSelectionsOwnedIter<'store> {
    type Item = TextSelectionHandle;

    fn next(&mut self) -> Option<Self::Item> {
        if self.drain_buffer {
            self.buffer.pop_front()
        } else {
            loop {
                match self.next_textselection() {
                    (None, false) => {
                        self.update();
                        return None;
                    }
                    (result, false) => return result,
                    (_, true) => continue, //loop
                }
            }
        }
    }
}

/// Private trait implementing the actual behaviour for [`FindTextSelectionsIter`]
trait FindTextSelections<'store> {
    //getters:

    fn buffer(&mut self) -> &mut VecDeque<TextSelectionHandle>;
    fn set_drain_buffer(&mut self);
    fn resource(&self) -> &'store TextResource;
    fn operator(&self) -> &TextSelectionOperator;
    fn textseliter(&mut self) -> Option<&mut TextSelectionIter<'store>>;
    fn reftextselection(&self) -> Option<TextSelection>;
    fn set_textseliter(&mut self, iter: TextSelectionIter<'store>);
    fn update(&mut self);

    /// This method returns an iterator over TextSelections in the resource
    /// It attempts to return the smallest sliced iterator possible, depending
    /// on the operator
    /// The reference text selection is always in the subject position for the associated [`TextSelectionOperator`] (`operator()`)
    fn new_textseliter(&self, reftextselection: &TextSelection) -> TextSelectionIter<'store> {
        match self.operator() {
            TextSelectionOperator::Embeds { .. } => self
                .resource()
                .range(reftextselection.begin(), reftextselection.end()),
            TextSelectionOperator::After { .. } | TextSelectionOperator::Succeeds { .. } => {
                self.resource().range(0, reftextselection.begin())
            }
            TextSelectionOperator::Before { .. } | TextSelectionOperator::Precedes { .. } => self
                .resource()
                .range(reftextselection.end(), self.resource().textlen()),
            TextSelectionOperator::Overlaps { .. } | TextSelectionOperator::Embedded { .. } => {
                //this is more efficient for reference text selections at the beginning of a text than at the end
                //(we could reverse the iterator to more efficiently find fragments more at the end, but then we have bookkeeping to do to store and finally revert
                //the results, as they need to be returned in textual order)
                self.resource().range(0, reftextselection.end())
            }
            _ => self.resource().iter(), //return the maximum slice
        }
    }

    /// aux function for buffering
    fn update_buffer(&mut self, buffer2: Vec<TextSelectionHandle>) {
        if self.buffer().is_empty() {
            //initial population of buffer
            *self.buffer() = buffer2.into_iter().collect();
        } else {
            //remove items from buffer that are not in buffer2  (I want to use Vec::drain_filter() here but that's nightly-only still)
            *self.buffer() = self
                .buffer()
                .iter()
                .filter(|handle| buffer2.contains(handle))
                .map(|x| *x)
                .collect();
        }
        //if the buffer is still empty, it will never be filled and we move on to the draining stage (will which immediately return None becasue there is nothing)
        if self.buffer().is_empty() {
            self.set_drain_buffer()
        }
    }

    /// Main function invoked from calling iterator's next() method
    /// Returns the next textselection (if any) and a boolean indicating whether to recurse further
    fn next_textselection(&mut self) -> (Option<TextSelectionHandle>, bool) {
        //                              ^--- indicates whether caller should recurse immediately or not
        if let Some(reftextselection) = self.reftextselection() {
            if let TextSelectionOperator::Equals {
                negate: false,
                all: false,
            } = self.operator()
            {
                //this operator is handled separately, we don't need a secondary iterator (texteliter) for it at all
                if let Ok(Some(handle)) = self
                    .resource()
                    .known_textselection(&Offset::from(&reftextselection))
                {
                    return (Some(handle), false);
                } else {
                    return (None, false);
                }
            };

            if self.operator().all() {
                // --------  the 'all' modifier is on ---------
                // The relationship A <operator> B (A=store, B=ref) must hold for each item in A with ALL items in B, otherwise NONE will be returned
                // note: negation is considered by the test() and self.new_textseliter() methods
                let mut buffer2: Vec<TextSelectionHandle> = Vec::new();
                for textselection in self.new_textseliter(&reftextselection) {
                    if textselection.handle() != reftextselection.handle()
                        && reftextselection.test(self.operator(), textselection)
                    {
                        //do not include the item itself
                        buffer2.push(textselection.handle().unwrap())
                    }
                }
                self.update_buffer(buffer2);
                (None, true) //signal recurse
            } else {
                //------ normal behaviour ----------
                // The relationship A <operator> B (A=store, B=ref) must hold for each item in A with ANY item in B, all matches are immediately returned
                // note: negation is considered by the test() and self.new_textseliter() methods
                if self.textseliter().is_none() {
                    //sets the iterastor for TextSelections in the store to iterator over,
                    //which we prefer to keep as small as possible (based on the operator)
                    self.set_textseliter(self.new_textseliter(&reftextselection));
                }
                if let Some(textselection) = self.textseliter().as_mut().unwrap().next() {
                    if textselection.handle() != reftextselection.handle() //do not include the item itself
                        && reftextselection.test(self.operator(), textselection)
                    {
                        return (Some(textselection.handle().unwrap()), false);
                    }
                } else {
                    return (None, false);
                }
                //else:
                (None, true) //signal recurse
            }
        } else {
            if self.operator().all() {
                //This is for the *All variants  that use a buffer:
                //iteration done, start draining buffer stage
                self.set_drain_buffer();
                (None, true) //signal recurse
            } else {
                (None, false) //all done
            }
        }
    }
}

impl<'store, 'q> FindTextSelections<'store> for FindTextSelectionsIter<'store, 'q> {
    fn buffer(&mut self) -> &mut VecDeque<TextSelectionHandle> {
        &mut self.buffer
    }
    fn set_drain_buffer(&mut self) {
        self.drain_buffer = true;
    }
    fn resource(&self) -> &'store TextResource {
        self.resource
    }
    fn operator(&self) -> &TextSelectionOperator {
        &self.operator
    }
    fn textseliter(&mut self) -> Option<&mut TextSelectionIter<'store>> {
        self.textseliter.as_mut()
    }
    fn update(&mut self) {
        self.textseliter = None;
        self.index += 1;
    }

    fn reftextselection(&self) -> Option<TextSelection> {
        let result = self.refset.get(self.index).map(|x| x.clone());
        result
    }
    fn set_textseliter(&mut self, iter: TextSelectionIter<'store>) {
        self.textseliter = Some(iter);
    }
}

impl<'store> FindTextSelections<'store> for FindTextSelectionsOwnedIter<'store> {
    fn buffer(&mut self) -> &mut VecDeque<TextSelectionHandle> {
        &mut self.buffer
    }
    fn set_drain_buffer(&mut self) {
        self.drain_buffer = true;
    }
    fn resource(&self) -> &'store TextResource {
        self.resource
    }
    fn operator(&self) -> &TextSelectionOperator {
        &self.operator
    }
    fn textseliter(&mut self) -> Option<&mut TextSelectionIter<'store>> {
        self.textseliter.as_mut()
    }
    fn update(&mut self) {
        self.textseliter = None;
        self.index += 1;
    }
    fn reftextselection(&self) -> Option<TextSelection> {
        let result = self.refset.get(self.index).map(|x| x.clone());
        result
    }
    fn set_textseliter(&mut self, iter: TextSelectionIter<'store>) {
        self.textseliter = Some(iter);
    }
}

#[derive(Debug, Clone)]
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
