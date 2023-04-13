use regex::{Regex, RegexSet};
use sealed::sealed;
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::slice::Iter;

use smallvec::{smallvec, SmallVec};

use crate::annotation::{Annotation, AnnotationHandle, TargetIter, TargetIterItem};
use crate::annotationstore::AnnotationStore;
use crate::config::Configurable;
use crate::error::StamError;
use crate::resources::{TextResource, TextResourceHandle, TextSelectionIter};
use crate::selector::{Offset, Selector};
use crate::store::*;
use crate::text::*;
use crate::types::*;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
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
    pub(crate) intid: Option<TextSelectionHandle>,
    pub(crate) begin: usize,
    pub(crate) end: usize,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash, PartialOrd, Ord)]
pub struct TextSelectionHandle(u32);

// I tried making this generic but failed, so let's spell it out for the handle
impl<'a> From<&TextSelectionHandle> for Item<'a, TextSelection> {
    fn from(handle: &TextSelectionHandle) -> Self {
        Item::Handle(*handle)
    }
}
impl<'a> From<Option<&TextSelectionHandle>> for Item<'a, TextSelection> {
    fn from(handle: Option<&TextSelectionHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(*handle)
        } else {
            Item::None
        }
    }
}
impl<'a> From<TextSelectionHandle> for Item<'a, TextSelection> {
    fn from(handle: TextSelectionHandle) -> Self {
        Item::Handle(handle)
    }
}
impl<'a> From<Option<TextSelectionHandle>> for Item<'a, TextSelection> {
    fn from(handle: Option<TextSelectionHandle>) -> Self {
        if let Some(handle) = handle {
            Item::Handle(handle)
        } else {
            Item::None
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

impl From<&WrappedItem<'_, TextSelection>> for Offset {
    fn from(textselection: &WrappedItem<'_, TextSelection>) -> Offset {
        Offset {
            begin: Cursor::BeginAligned(textselection.begin()),
            end: Cursor::BeginAligned(textselection.end()),
        }
    }
}

#[sealed]
impl Handle for TextSelectionHandle {
    fn new(intid: usize) -> Self {
        Self(intid as u32)
    }
    fn unwrap(&self) -> usize {
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
    fn set_handle(&mut self, handle: TextSelectionHandle) {
        self.intid = Some(handle);
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
    pub fn relative_begin_in(&self, container: &TextSelection) -> Option<usize> {
        if self.begin() >= container.begin() {
            Some(self.begin() - container.begin())
        } else {
            None
        }
    }

    /// Returns the end cursor (begin-aligned) of this text selection in another. Returns None if they are not embedded.
    /// **Note:** this does *NOT* check whether the textselections pertain to the same resource, that is up to the caller.
    pub fn relative_end_in(&self, container: &TextSelection) -> Option<usize> {
        if self.end() >= container.end() {
            Some(self.end() - container.end())
        } else {
            None
        }
    }

    /// Returns the offset of this text selection in another. Returns None if they are not embedded.
    /// **Note:** this does *NOT* check whether the textselections pertain to the same resource, that is up to the caller.
    pub fn relative_offset_in(&self, container: &TextSelection) -> Option<Offset> {
        if let (Some(begin), Some(end)) = (
            self.relative_begin_in(container),
            self.relative_end_in(container),
        ) {
            Some(Offset::simple(begin, end))
        } else {
            None
        }
    }
}

impl<'store, 'slf> Text<'store, 'slf> for WrappedItem<'store, TextSelection>
where
    'store: 'slf,
{
    fn text(&'slf self) -> &'store str {
        let resource = self.store(); //courtesy of WrappedItem
        let beginbyte = resource
            .utf8byte(self.begin())
            .expect("utf8byte conversion should succeed");
        let endbyte = resource
            .utf8byte(self.end())
            .expect("utf8byte conversion should succeed");
        &resource.text()[beginbyte..endbyte]
    }

    fn textlen(&self) -> usize {
        self.end - self.begin
    }

    /// Returns a string reference to a slice of text as specified by the offset
    fn text_by_offset(&'slf self, offset: &Offset) -> Result<&'store str, StamError> {
        let beginbyte =
            self.utf8byte(self.absolute_cursor(self.beginaligned_cursor(&offset.begin)?))?;
        let endbyte =
            self.utf8byte(self.absolute_cursor(self.beginaligned_cursor(&offset.end)?))?;
        if endbyte < beginbyte {
            Err(StamError::InvalidOffset(
                Cursor::BeginAligned(beginbyte),
                Cursor::BeginAligned(endbyte),
                "End must be greater than begin. (Cursor should be interpreted as UTF-8 bytes in this error context only)",
            ))
        } else {
            Ok(&self.text()[beginbyte..endbyte])
        }
    }

    /// Finds the utf-8 byte position where the specified text subslice begins
    /// The returned offset is relative to the TextSelection
    fn subslice_utf8_offset(&self, subslice: &str) -> Option<usize> {
        let self_begin = self.text().as_ptr() as usize;
        let sub_begin = subslice.as_ptr() as usize;
        if sub_begin < self_begin || sub_begin > self_begin.wrapping_add(self.text().len()) {
            None
        } else {
            Some(sub_begin.wrapping_sub(self_begin))
        }
    }

    /// This converts a unicode point to utf-8 byte, all in *relative* offsets to this textselection
    fn utf8byte(&self, abscursor: usize) -> Result<usize, StamError> {
        //Convert from and to absolute coordinates so we don't have to reimplemented all the logic
        //and can just call this same method on TextResource, which has the proper indices for this
        let beginbyte = self
            .store()
            .subslice_utf8_offset(self.text())
            .expect("subslice should succeed");
        Ok(self.store().utf8byte(self.absolute_cursor(abscursor))? - beginbyte)
    }

    /// This converts utf-8 byte to charpos, all in *relative* offsets to this textselection
    fn utf8byte_to_charpos(&self, bytecursor: usize) -> Result<usize, StamError> {
        //Convert from and to absolute coordinates so we don't have to reimplemented all the logic
        //and can just call this same method on TextResource, which has the proper indices for this
        let beginbyte = self
            .store()
            .subslice_utf8_offset(self.text())
            .expect("subslice should succeed");
        Ok(self
            .store()
            .utf8byte_to_charpos(self.absolute_cursor(beginbyte + bytecursor))?
            - self.begin())
    }

    /// Searches the text using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// is held by the [`FindRegexMatch'] struct.
    ///
    /// Passing multiple regular expressions at once is more efficient than calling this function anew for each one.
    /// If capture groups are used in the regular expression, only those parts will be returned (the rest is context). If none are used,
    /// the entire expression is returned.
    ///
    /// An `offset` can be specified to work on a sub-part rather than the entire text (like an existing TextSelection).
    ///
    /// The `allow_overlap` parameter determines if the matching expressions are allowed to
    /// overlap. It you are doing some form of tokenisation, you also likely want this set to
    /// false. All of this only matters if you supply multiple regular expressions.
    ///
    /// Results are returned in the exact order they are found in the text
    fn find_text_regex<'regex>(
        &'slf self,
        expressions: &'regex [Regex],
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<FindRegexIter<'store, 'regex>, StamError> {
        debug(self.store().config(), || {
            format!(
                "TextSelection::find_text_regex: expressions={:?}",
                expressions
            )
        });
        let text = self.text();
        let selectexpressions =
            find_text_regex_select_expressions(text, expressions, precompiledset)?;
        //Returns an iterator that does the remainder of the actual searching
        Ok(FindRegexIter {
            resource: self.store(),
            expressions,
            selectexpressions,
            matchiters: Vec::new(),
            nextmatches: Vec::new(),
            text: self.text(),
            begincharpos: self.begin(),
            beginbytepos: self
                .store()
                .subslice_utf8_offset(text)
                .expect("Subslice must be found"),
            allow_overlap,
        })
    }

    /// Searches for the specified text fragment. Returns an iterator to iterate over all matches in the text.
    /// The iterator returns [`TextSelection`] items.
    ///
    /// For more complex and powerful searching use [`Self.find_text_regex()`] instead
    ///
    /// If you want to search only a subpart of the text, extract a ['TextSelection`] first and then run `find_text()` on that instead.
    fn find_text<'b, 'c>(&'b self, fragment: &'c str) -> FindTextIter<'b, 'c> {
        FindTextIter {
            resource: self.store(),
            fragment,
            offset: Offset::from(self.deref()),
        }
    }

    fn split_text<'b>(&'slf self, delimiter: &'b str) -> SplitTextIter<'store, 'b> {
        SplitTextIter {
            resource: self.store(),
            iter: self.store().text().split(delimiter),
            byteoffset: self
                .subslice_utf8_offset(self.text())
                .expect("subslice must succeed for split_text"),
        }
    }

    /// Returns a [`TextSelection'] that corresponds to the offset **WITHIN** the textselection.
    /// This returns a [`TextSelection`] with absolute coordinates in the resource.
    ///
    /// If the textselection is known (i.e. it has associated annotations), it will be returned as such with a handle (borrowed).
    /// If it doesn't exist yet, a new one will be returned, and it won't have a handle, nor will it be added to the store automatically.
    ///
    /// The [`TextSelection`] is returned as in a far pointer (`WrappedItem`) that also contains reference to the underlying store (the [`TextResource`]).
    ///
    /// Use [`Self::has_textselection()`] instead if you want to limit to existing text selections (i.e. those pertaining to annotations) only.
    fn textselection(
        &'slf self,
        offset: &Offset,
    ) -> Result<WrappedItem<'store, TextSelection>, StamError> {
        let resource = self.store(); //courtesy of WrappedItem
        let offset = self.absolute_offset(&offset)?; //turns the relative offset into an absolute one (i.e. offsets in TextResource)
        resource.textselection(&offset)
    }

    fn absolute_cursor(&self, cursor: usize) -> usize {
        self.begin + cursor
    }
}

impl<'store, 'slf> WrappedItem<'store, TextSelection> {
    pub fn resource(&'slf self) -> &'store TextResource {
        self.store()
    }
    /// Iterates over all annotations that are reference by this TextSelection, if any.
    /// Note that you need to explicitly specify the `AnnotationStore` for this method.
    pub fn annotations(
        &'slf self,
        annotationstore: &'store AnnotationStore,
    ) -> Option<impl Iterator<Item = WrappedItem<'store, Annotation>> + 'slf> {
        match self {
            Self::Borrowed {
                item: textselection,
                store,
            } => {
                if let Some(vec) = annotationstore
                    .annotations_by_textselection(store.handle().unwrap(), textselection)
                {
                    Some(vec.iter().map(|a_handle| {
                        annotationstore
                            .annotation(&Item::Handle(*a_handle))
                            .unwrap()
                    }))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn find_textselections<'q>(
        &'slf self,
        operator: TextSelectionOperator,
    ) -> impl Iterator<Item = WrappedItem<'store, TextSelection>> + 'q
    where
        'store: 'q, //store lives at least as long as 'q
    {
        let tset: TextSelectionSet = self.clone().into();
        self.resource().find_textselections(operator, tset)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PositionIndex(pub(crate) BTreeMap<usize, PositionIndexItem>);

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

#[derive(Debug, Clone)]
pub struct PositionIndexItem {
    /// Position in bytes (UTF-8 encoded)
    pub(crate) bytepos: usize,
    /// Lists all text selections that start here
    pub(crate) end2begin: SmallVec<[(usize, TextSelectionHandle); 1]>, //heap allocation only needed when there are more than one
    /// Lists all text selections that end here (non-inclusive)
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
#[derive(Clone, Debug)]
pub struct TextSelectionSet {
    data: SmallVec<[TextSelection; 8]>,
    resource: TextResourceHandle,
    sorted: bool,
}

impl<'store> From<WrappedItem<'store, TextSelection>> for TextSelectionSet {
    fn from(textselection: WrappedItem<'store, TextSelection>) -> Self {
        let mut tset = Self::new(
            textselection
                .store()
                .handle()
                .expect("Resource must have a handle"),
        );
        if textselection.is_ref() {
            tset.add(textselection.unwrap().clone());
        } else {
            tset.add(textselection.unwrap_owned());
        }
        tset
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
///
/// This enum encapsulates both the operator as well the the object of the operation (a
/// `TextSelectionSet`). As a whole, it can then be applied to another [`TextSelectionSet`] or
/// [`TextSelection`] via its [`TextSelectionSet::test()`] method.
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

    /// Each TextSelections in A precedes (comes before) a textselection in B
    /// If modifier `all` is set: All TextSelections in A precede (come before) all textselections in B. There is no overlap (cf. textfabric's `<<`)
    Precedes { all: bool, negate: bool },

    /// Each TextSeleciton In A succeeds (comes after) a textselection in B
    /// If modifier `all` is set: All TextSelections in A succeed (come after) all textselections in B. There is no overlap (cf. textfabric's `>>`)
    Succeeds { all: bool, negate: bool },

    /// Each TextSelection in A is ends where at least one TextSelection in B begins.
    /// If modifier `all` is set: The rightmost TextSelections in A end where the leftmost TextSelection in B begins  (cf. textfabric's `<:`)
    LeftAdjacent { all: bool, negate: bool },

    /// The rightmost TextSelecti
    /// Each TextSelection in A is begis where at least one TextSelection in A ends.
    /// If modifier `all` is set: The leftmost TextSelection in A starts where the rightmost TextSelection in B ends  (cf. textfabric's `:>`)
    RightAdjacent { all: bool, negate: bool },

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
            | Self::Precedes { all, .. }
            | Self::Succeeds { all, .. }
            | Self::LeftAdjacent { all, .. }
            | Self::RightAdjacent { all, .. }
            | Self::SameBegin { all, .. }
            | Self::SameEnd { all, .. }
            | Self::InSet { all, .. }
            | Self::SameRange { all, .. } => *all,
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
            Self::Precedes { all, negate } => Self::Precedes {
                all: *all,
                negate: !negate,
            },
            Self::Succeeds { all, negate } => Self::Succeeds {
                all: *all,
                negate: !negate,
            },
            Self::LeftAdjacent { all, negate } => Self::LeftAdjacent {
                all: *all,
                negate: !negate,
            },
            Self::RightAdjacent { all, negate } => Self::RightAdjacent {
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
            Self::Precedes { all, negate } => Self::Precedes {
                all: !all,
                negate: *negate,
            },
            Self::Succeeds { all, negate } => Self::Succeeds {
                all: !all,
                negate: *negate,
            },
            Self::LeftAdjacent { all, negate } => Self::LeftAdjacent {
                all: !all,
                negate: *negate,
            },
            Self::RightAdjacent { all, negate } => Self::RightAdjacent {
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

impl TextSelectionSet {
    pub fn new(resource: TextResourceHandle) -> Self {
        Self {
            data: SmallVec::new(),
            resource,
            sorted: false,
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

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two [`TextSelectionSet`]s.
    /// The operator contains the other part of the equation that is tested against. A boolean is returned with the test result.
    pub fn test(&self, operator: &TextSelectionOperator, reftextsel: &TextSelection) -> bool {
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
            | TextSelectionOperator::Precedes {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::LeftAdjacent {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::RightAdjacent {
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
            TextSelectionOperator::LeftAdjacent {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Precedes {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::SameEnd {
                all: true,
                negate: false,
            } => self.rightmost().unwrap().test(operator, reftextsel),
            TextSelectionOperator::RightAdjacent {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
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
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::LeftAdjacent { negate: true, .. }
            | TextSelectionOperator::RightAdjacent { negate: true, .. }
            | TextSelectionOperator::SameBegin { negate: true, .. }
            | TextSelectionOperator::SameEnd { negate: true, .. }
            | TextSelectionOperator::InSet { negate: true, .. } => {
                !self.test(&operator.toggle_negate(), reftextsel)
            }
            _ => unreachable!("unknown operator+modifier combination"),
        }
    }

    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two [`TextSelectionSet`]s.
    /// The operator contains the other part of the equation that is tested against. A boolean is returned with the test result.
    pub fn test_set(&self, operator: &TextSelectionOperator, refset: &TextSelectionSet) -> bool {
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
            | TextSelectionOperator::Precedes {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::LeftAdjacent {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::RightAdjacent {
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
            TextSelectionOperator::LeftAdjacent {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Precedes {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::SameEnd {
                all: true,
                negate: false,
            } => self.rightmost().unwrap().test_set(operator, refset),
            TextSelectionOperator::RightAdjacent {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
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
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::LeftAdjacent { negate: true, .. }
            | TextSelectionOperator::RightAdjacent { negate: true, .. }
            | TextSelectionOperator::SameBegin { negate: true, .. }
            | TextSelectionOperator::SameEnd { negate: true, .. }
            | TextSelectionOperator::InSet { negate: true, .. } => {
                !self.test_set(&operator.toggle_negate(), refset)
            }
            _ => unreachable!("unknown operator+modifier combination"),
        }
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

impl TextSelection {
    /// This method is called to test whether a specific spatial relation (as expressed by the
    /// passed operator) holds between a [`TextSelection`] and another.
    /// A boolean is returned with the test result.
    pub fn test(&self, operator: &TextSelectionOperator, reftextsel: &TextSelection) -> bool {
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
            TextSelectionOperator::Precedes { negate: false, .. } => self.end <= reftextsel.begin,
            TextSelectionOperator::Succeeds { negate: false, .. } => self.begin >= reftextsel.end,
            TextSelectionOperator::LeftAdjacent { negate: false, .. } => {
                self.end == reftextsel.begin
            }
            TextSelectionOperator::RightAdjacent { negate: false, .. } => {
                reftextsel.end == self.begin
            }
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
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::LeftAdjacent { negate: true, .. }
            | TextSelectionOperator::RightAdjacent { negate: true, .. }
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
    pub fn test_set(&self, operator: &TextSelectionOperator, refset: &TextSelectionSet) -> bool {
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
            | TextSelectionOperator::Precedes {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::LeftAdjacent {
                all: false,
                negate: false,
            }
            | TextSelectionOperator::RightAdjacent {
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
            | TextSelectionOperator::Precedes {
                all: true,
                negate: false,
            }
            | TextSelectionOperator::Succeeds {
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
            TextSelectionOperator::LeftAdjacent {
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
            TextSelectionOperator::RightAdjacent {
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
            | TextSelectionOperator::Precedes { negate: true, .. }
            | TextSelectionOperator::Succeeds { negate: true, .. }
            | TextSelectionOperator::LeftAdjacent { negate: true, .. }
            | TextSelectionOperator::RightAdjacent { negate: true, .. }
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
    /// This is a low-level method. Use [`Self::find_textselections()`] instead.
    pub fn textselections_by_operator_ref<'store, 'q>(
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

    pub fn textselections_by_operator<'store>(
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

    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections (in an [`TextSelectionSet']). Returns an iterator over all matching
    /// text selections in the resource, as [`WrappedItem<TextSelection>`].
    pub fn find_textselections_ref<'store, 'q>(
        &'store self,
        operator: TextSelectionOperator,
        refset: &'q TextSelectionSet,
    ) -> impl Iterator<Item = WrappedItem<'store, TextSelection>> + 'q
    where
        'store: 'q, //store lives at least as long as 'q
    {
        self.textselections_by_operator_ref(operator, refset)
            .map(|ts_handle| {
                let textselection: &'store TextSelection = self
                    .get(&Item::Handle(ts_handle))
                    .expect("textselection handle must be valid");
                textselection.wrap_in(self).expect("wrap must succeed")
            })
    }

    /// Find textselections by applying a text selection operator ([`TextSelectionOperator`]) to a
    /// one or more querying textselections (in an [`TextSelectionSet']). Returns an iterator over all matching
    /// text selections in the resource, as [`WrappedItem<TextSelection>`].
    pub fn find_textselections<'store>(
        &'store self,
        operator: TextSelectionOperator,
        refset: TextSelectionSet,
    ) -> impl Iterator<Item = WrappedItem<'store, TextSelection>> {
        self.textselections_by_operator(operator, refset)
            .map(|ts_handle| {
                let textselection: &'store TextSelection = self
                    .get(&Item::Handle(ts_handle))
                    .expect("textselection handle must be valid");
                textselection.wrap_in(self).expect("wrap must succeed")
            })
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
            match self.next_textselection() {
                (None, false) => {
                    self.update();
                    None
                }
                (result, false) => result,
                (_, true) => self.next(), //recurse
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
            match self.next_textselection() {
                (None, false) => {
                    self.update();
                    None
                }
                (result, false) => result,
                (_, true) => self.next(), //recurse
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
    fn new_textseliter(&self, reftextselection: &TextSelection) -> TextSelectionIter<'store> {
        match self.operator() {
            TextSelectionOperator::Embeds { .. } => self
                .resource()
                .range(reftextselection.begin(), reftextselection.end()),
            TextSelectionOperator::Succeeds { .. }
            | TextSelectionOperator::RightAdjacent { .. } => self
                .resource()
                .range(reftextselection.end(), self.resource().textlen()),
            TextSelectionOperator::Precedes { .. } | TextSelectionOperator::LeftAdjacent { .. } => {
                self.resource().range(0, reftextselection.begin())
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
                let mut buffer2: Vec<TextSelectionHandle> = Vec::new();
                for textselection in self.new_textseliter(&reftextselection) {
                    if textselection.intid != reftextselection.intid
                        && reftextselection.test(self.operator(), &textselection)
                    {
                        //do not include the item itself
                        buffer2.push(textselection.handle().expect("handle must exist"))
                    }
                }
                self.update_buffer(buffer2);
                (None, true) //signal recurse
            } else {
                //------ normal behaviour ----------
                if self.textseliter().is_none() {
                    self.set_textseliter(
                        //we can restrict to a small subrange (= more efficient)
                        self.new_textseliter(&reftextselection),
                    );
                }
                if let Some(textselection) = self.textseliter().as_mut().unwrap().next() {
                    if textselection.handle() != reftextselection.handle() //do not include the item itself
                        && reftextselection.test(self.operator(), &textselection)
                    {
                        return (
                            Some(textselection.handle().expect("handle must exist")),
                            false,
                        );
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

impl<'a> Iterator for TargetIter<'a, TextSelection> {
    type Item = TargetIterItem<'a, TextSelection>;

    fn next(&mut self) -> Option<Self::Item> {
        let selectoritem = self.iter.next();
        if let Some(selectoritem) = selectoritem {
            match selectoritem.selector().as_ref() {
                Selector::InternalTextSelector {
                    resource,
                    textselection,
                } => {
                    let resource: &TextResource = self
                        .iter
                        .store
                        .get(&resource.into())
                        .expect("Resource must exist");
                    let textselection: &TextSelection = resource
                        .get(&textselection.into())
                        .expect("TextSelection must exist");
                    Some(TargetIterItem {
                        item: textselection.wrap_in(resource).expect("wrap must succeed"),
                        selectoriteritem: selectoritem,
                    })
                }
                _ => self.next(),
            }
        } else {
            None
        }
    }
}
