use sealed::sealed;
use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::slice::Iter;

use smallvec::{smallvec, SmallVec};

use crate::annotation::AnnotationHandle;
use crate::annotationstore::{TargetIter, TargetIterItem};
use crate::error::StamError;
use crate::resources::{TextResource, TextResourceHandle};
use crate::selector::{Offset, Selector};
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
/// On the lowest-level, this struct is obtain by a call to [`crate::annotationstore::AnnotationStore::text_selection()`], which
/// resolves a [`crate::Selector::TextSelector`]  to a [`TextSelection`]. Such calls are often abstracted away by higher level methods such as [`crate::annotationstore::AnnotationStore::textselections_by_annotation()`].
pub struct TextSelection {
    pub(crate) intid: Option<TextSelectionHandle>,
    pub(crate) begin: usize,
    pub(crate) end: usize,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash, PartialOrd, Ord)]
pub struct TextSelectionHandle(u32);

impl From<&TextSelection> for Offset {
    fn from(textselection: &TextSelection) -> Offset {
        Offset {
            begin: Cursor::BeginAligned(textselection.begin),
            end: Cursor::BeginAligned(textselection.end),
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
impl Storable for TextSelection {
    type HandleType = TextSelectionHandle;

    fn id(&self) -> Option<&str> {
        None
    }
    fn handle(&self) -> Option<TextSelectionHandle> {
        self.intid
    }
    fn set_handle(&mut self, handle: TextSelectionHandle) {
        self.intid = Some(handle);
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

    /// Resolves a cursor that is formulated **relative to this text selection** to an absolute position (by definition begin aligned)
    pub fn absolute_cursor(&self, cursor: &Cursor) -> Result<usize, StamError> {
        let length = self.end() - self.begin();
        match *cursor {
            Cursor::BeginAligned(cursor) => Ok(self.begin + cursor),
            Cursor::EndAligned(cursor) => {
                if cursor.abs() as usize > length {
                    Err(StamError::CursorOutOfBounds(
                        Cursor::EndAligned(cursor),
                        "TextResource::absolute_cursor(): end aligned cursor ends up before the beginning",
                    ))
                } else {
                    Ok(self.begin + (length - cursor.abs() as usize))
                }
            }
        }
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

/// A TextSelectionSet holds one or more [`TextSelection`] items
/// It may also optionally carry the TextResourceHandle and AnnotationHandle associated to reference the source of the TextSelection
#[derive(Clone, Debug)]
pub struct TextSelectionSet {
    data: SmallVec<
        [(
            TextSelection,
            Option<TextResourceHandle>,
            Option<AnnotationHandle>,
        ); 8],
    >,
    sorted: bool, //TODO implement
}

impl From<TextSelection> for TextSelectionSet {
    fn from(other: TextSelection) -> Self {
        Self::new(other, None, None)
    }
}

pub struct TextSelectionSetIter<'a> {
    iter: Iter<
        'a,
        (
            TextSelection,
            Option<TextResourceHandle>,
            Option<AnnotationHandle>,
        ),
    >,
    count: usize,
    len: usize,
}

impl<'a> Iterator for TextSelectionSetIter<'a> {
    type Item = &'a (
        TextSelection,
        Option<TextResourceHandle>,
        Option<AnnotationHandle>,
    );

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
/// The implementation goes a bit further and acts on the basis of [`TextSelectionSet`] rather than
/// `TextSelection`, allowing you to compare two sets, each containing possibly multiple
/// TextSelections, at once.
///
/// This enum encapsulates both the operator as well the the object of the operation (a
/// `TextSelectionSet`). As a whole, it can then be applied to another [`TextSelectionSet`] or
/// [`TextSelection`] via its [`TextSelectionSet::test()`] method.
#[derive(Debug, Clone)]
pub enum TextSelectionOperator<'a> {
    /// Both sets occupy cover the exact same TextSelections, and all are covered (cf. textfabric's `==`), commutative, transitive
    Equals(&'a TextSelectionSet),

    /// All items in both sets must cover the exact same TextSelection
    EqualsAll(&'a TextSelectionSet),

    /// Each TextSelections in A overlaps with a TextSelection in B (cf. textfabric's `&&`), commutative
    Overlaps(&'a TextSelectionSet),

    /// Each TextSelections in A overlaps with all TextSelection in B (cf. textfabric's `&&`), commutative
    OverlapsAll(&'a TextSelectionSet),

    /// All TextSelections in B are embedded by a TextSelection in A (cf. textfabric's `[[`)
    Embeds(&'a TextSelectionSet),

    /// All TextSelections in B are embedded by all TextSelection in A (cf. textfabric's `[[`)
    EmbedsAll(&'a TextSelectionSet),

    /// All TextSelections in A are embedded by a TextSelection in B (cf. textfabric's `]]`)
    Embedded(&'a TextSelectionSet),

    /// All TextSelections in A are embedded by all TextSelection in B (cf. textfabric's `]]`)
    EmbeddedAll(&'a TextSelectionSet),

    /// Each TextSelections in A precedes (comes before) a textselection in B
    Precedes(&'a TextSelectionSet),

    /// All TextSelections in A precede (come before) all textselections in B. There is no overlap (cf. textfabric's `<<`)
    PrecedesAll(&'a TextSelectionSet),

    /// Each TextSeleciton In A succeeds (comes after) a textselection in B
    Succeeds(&'a TextSelectionSet),

    /// All TextSelections in A succeed (come after) all textselections in B. There is no overlap (cf. textfabric's `>>`)
    SucceedsAll(&'a TextSelectionSet),

    /// Each TextSelection in A is ends where at least one TextSelection in B begins.
    LeftAdjacent(&'a TextSelectionSet),

    /// The rightmost TextSelections in A end where the leftmost TextSelection in B begins  (cf. textfabric's `<:`)
    //TODO: add mindistance,maxdistance arguments
    LeftAdjacentAll(&'a TextSelectionSet),

    /// Each TextSelection in A is begis where at least one TextSelection in A ends.
    RightAdjacent(&'a TextSelectionSet),

    /// The leftmost TextSelection in A starts where the rightmost TextSelection in B ends  (cf. textfabric's `:>`)
    //TODO: add mindistance,maxdistance argument
    RightAdjacentAll(&'a TextSelectionSet),

    /// Each TextSelection in A starts where a TextSelection in B starts
    SameBegin(&'a TextSelectionSet),

    /// The leftmost TextSelection in A starts where the leftmost TextSelection in B start  (cf. textfabric's `=:`)
    SameBeginAll(&'a TextSelectionSet),

    /// Each TextSelection in A ends where a TextSelection in B ends
    SameEnd(&'a TextSelectionSet),

    /// The rightmost TextSelection in A ends where the rights TextSelection in B ends  (cf. textfabric's `:=`)
    SameEndAll(&'a TextSelectionSet),

    /// Each TextSelection in A is in B as well, this is similar to Equals but allows
    /// for set B having unmatched items
    InSet(&'a TextSelectionSet),

    /// The leftmost TextSelection in A starts where the leftmost TextSelection in A starts  and
    /// the rightmost TextSelection in A ends where the rights TextSelection in B ends  (cf. textfabric's `::`)
    SameRangeAll(&'a TextSelectionSet),

    Not(Box<TextSelectionOperator<'a>>),
}

impl TextSelectionSet {
    pub fn new(
        textselection: TextSelection,
        resource: Option<TextResourceHandle>,
        annotation: Option<AnnotationHandle>,
    ) -> Self {
        Self {
            data: smallvec![(textselection, resource, annotation)],
            sorted: false,
        }
    }

    pub fn insert(
        &mut self,
        textselection: TextSelection,
        resource: Option<TextResourceHandle>,
        annotation: Option<AnnotationHandle>,
    ) {
        let elem = (textselection, resource, annotation);
        if self.sorted {
            //once sorted, we respect the order
            match self.data.binary_search(&elem) {
                Ok(_) => {} //element already exists
                Err(pos) => self.data.insert(pos, elem),
            };
        } else {
            self.data.push(elem);
        }
    }

    /// Iterate over the store
    pub fn iter<'a>(&'a self) -> TextSelectionSetIter<'a> {
        TextSelectionSetIter {
            iter: self.data.iter(),
            count: 0,
            len: self.data.len(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two [`TextSelectionSet`]s.
    /// The operator contains the other part of the equation that is tested against. A boolean is returned with the test result.
    pub fn test(&self, operator: &TextSelectionOperator) -> bool {
        if self.is_empty() {
            return false;
        }
        match operator {
            TextSelectionOperator::Equals(otherset) => {
                if self.len() != otherset.len() {
                    //each item must have a counterpart so the sets must be equal length
                    return false;
                }
                //ALL of the items in this set must match with ANY item in the otherset
                for (item, _, _) in self.iter() {
                    if !item.test(operator) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Overlaps(_)
            | TextSelectionOperator::Embeds(_)
            | TextSelectionOperator::Embedded(_)
            | TextSelectionOperator::Precedes(_)
            | TextSelectionOperator::Succeeds(_)
            | TextSelectionOperator::LeftAdjacent(_)
            | TextSelectionOperator::RightAdjacent(_)
            | TextSelectionOperator::SameBegin(_)
            | TextSelectionOperator::SameEnd(_)
            | TextSelectionOperator::InSet(_) => {
                // ALL of the items in this set must match with ANY item in the otherset
                // This is a weaker form of Equals (could have also been called SameRange)
                for (item, _, _) in self.iter() {
                    if !item.test(operator) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::OverlapsAll(_)
            | TextSelectionOperator::EmbedsAll(_)
            | TextSelectionOperator::EmbeddedAll(_)
            | TextSelectionOperator::EqualsAll(_) => {
                //all of the items in this set must match with all item in the otherset (this code isn't different from the previous one, the different code happens in the delegated test() method
                for (item, _, _) in self.iter() {
                    if !item.test(operator) {
                        return false;
                    }
                }
                true
            }
            //we can unrwap leftmost/rightmost safely because we tested at the start whether the set was empty or not
            TextSelectionOperator::LeftAdjacentAll(_) | TextSelectionOperator::PrecedesAll(_) => {
                self.rightmost().unwrap().test(operator)
            }
            TextSelectionOperator::RightAdjacentAll(_) | TextSelectionOperator::SucceedsAll(_) => {
                self.leftmost().unwrap().test(operator)
            }
            TextSelectionOperator::SameBeginAll(_) => self.leftmost().unwrap().test(operator),
            TextSelectionOperator::SameEndAll(_) => self.rightmost().unwrap().test(operator),
            TextSelectionOperator::SameRangeAll(_) => {
                self.leftmost().unwrap().test(operator) && self.rightmost().unwrap().test(operator)
            }
            TextSelectionOperator::Not(suboperator) => !self.test(suboperator),
        }
    }

    /// Intersect this set (A) with another (B). Modifies this set so only the elements
    /// present in both are left after the operation.
    pub fn intersect_mut(&mut self, other: &TextSelectionSet) {
        self.test_intersect_mut(&TextSelectionOperator::InSet(other))
    }

    /// Intersect this set (A) with another (B) using a specific test operator (which also includes set B). Modifies this set so only the elements
    /// present in both are left after the operation.
    pub fn test_intersect_mut(&mut self, operator: &TextSelectionOperator) {
        self.data.retain(|(item, _, _)| item.test(operator));
    }

    /// Intersect this set (A) with another (B) and returns the new intersection set.
    pub fn intersect(&mut self, other: &TextSelectionSet) -> Self {
        self.test_intersect(&TextSelectionOperator::InSet(other))
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

    /// Returns the left-most TextSelection (the one with the lowest start offset) in the set.
    pub fn leftmost(&self) -> Option<&TextSelection> {
        if self.is_empty() {
            None
        } else {
            if self.sorted {
                self.data.get(0).map(|(item, _, _)| item)
            } else {
                let mut leftmost: Option<&TextSelection> = None;
                for (item, _, _) in self.iter() {
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
                self.data.get(self.data.len() - 1).map(|(item, _, _)| item)
            } else {
                let mut rightmost: Option<&TextSelection> = None;
                for (item, _, _) in self.iter() {
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
    /// This methods is called to test whether a specific spatial relation (as expressed by the passed operator) holds between a [`TextSelection`] and another (or multiple) ([`TextSelectionSet`]).
    /// The operator contains the other part of the equation that is tested against. A boolean is returned with the test result.
    pub fn test(&self, operator: &TextSelectionOperator) -> bool {
        match operator {
            TextSelectionOperator::Equals(otherset) | TextSelectionOperator::InSet(otherset) => {
                //item must be equal to ANY of the items in the other set
                for (other, _, _) in otherset.iter() {
                    if self == other {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::EqualsAll(otherset) => {
                //item must be equal to ALL of the items in the other set
                if otherset.is_empty() {
                    return false;
                }
                for (other, _, _) in otherset.iter() {
                    if self == other {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Overlaps(otherset) => {
                //item must be equal overlap with any of the items in the other set
                for (other, _, _) in otherset.iter() {
                    if (other.begin >= self.begin && other.begin < self.end)
                        || (other.end > self.begin && other.end <= self.end)
                        || (other.begin <= self.begin && other.end >= self.end)
                        || (self.begin <= other.begin && self.end >= other.end)
                    {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::OverlapsAll(otherset) => {
                //item must be equal overlap with all of the items in the other set
                if otherset.is_empty() {
                    return false;
                }
                for (other, _, _) in otherset.iter() {
                    if !((other.begin >= self.begin && other.begin < self.end)
                        || (other.end > self.begin && other.end <= self.end)
                        || (other.begin <= self.begin && other.end >= self.end)
                        || (self.begin <= other.begin && self.end >= other.end))
                    {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Embeds(otherset) => {
                // TextSelection embeds an item in other set
                for (other, _, _) in otherset.iter() {
                    if other.begin >= self.begin && other.end <= self.end {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::EmbedsAll(otherset) => {
                // TextSelection embeds all items in other
                if otherset.is_empty() {
                    return false;
                }
                for (other, _, _) in otherset.iter() {
                    if !(other.begin >= self.begin && other.end <= self.end) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Embedded(otherset) => {
                // TextSelection is embedded by an item in B
                for (other, _, _) in otherset.iter() {
                    if self.begin >= other.begin && self.end <= other.end {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::EmbeddedAll(otherset) => {
                // each item in A is embedded by all items in B
                if otherset.is_empty() {
                    return false;
                }
                for (other, _, _) in otherset.iter() {
                    if !(self.begin >= other.begin && self.end <= other.end) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Precedes(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.end <= other.begin {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::PrecedesAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                for (other, _, _) in otherset.iter() {
                    if self.end > other.begin {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Succeeds(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.begin >= other.end {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::SucceedsAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                for (other, _, _) in otherset.iter() {
                    if self.begin < other.end {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::LeftAdjacent(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.end == other.begin {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::LeftAdjacentAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                let mut leftmost = None;
                for (other, _, _) in otherset.iter() {
                    if leftmost.is_none() || other.begin < leftmost.unwrap() {
                        leftmost = Some(other.begin);
                    }
                }
                Some(self.end) == leftmost
            }
            TextSelectionOperator::RightAdjacent(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if other.end == self.begin {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::RightAdjacentAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                let mut rightmost = None;
                for (other, _, _) in otherset.iter() {
                    if rightmost.is_none() || other.end > rightmost.unwrap() {
                        rightmost = Some(other.end);
                    }
                }
                Some(self.begin) == rightmost
            }
            TextSelectionOperator::SameBegin(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.begin == other.begin {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::SameBeginAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                self.begin == otherset.leftmost().unwrap().begin()
            }
            TextSelectionOperator::SameEnd(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.end == other.end {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::SameEndAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                self.end == otherset.rightmost().unwrap().end()
            }
            TextSelectionOperator::SameRangeAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                self.begin == otherset.leftmost().unwrap().begin()
                    && self.end == otherset.rightmost().unwrap().end()
            }
            TextSelectionOperator::Not(suboperator) => !self.test(suboperator),
        }
    }
}

impl Extend<TextSelection> for TextSelectionSet {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = TextSelection>,
    {
        for x in iter {
            self.insert(x, None, None);
        }
    }
}

impl
    Extend<(
        TextSelection,
        Option<TextResourceHandle>,
        Option<AnnotationHandle>,
    )> for TextSelectionSet
{
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<
            Item = (
                TextSelection,
                Option<TextResourceHandle>,
                Option<AnnotationHandle>,
            ),
        >,
    {
        for (textselection, resource, annotation) in iter {
            self.insert(textselection, resource, annotation);
        }
    }
}

/// The TextRelationOperator, simply put, allows comparison of two text regions, specified by their offset. It
/// allows testing for all kinds of spatial relations (as embodied by this enum) in which two
/// [`TextSelection`] instances (the realisation once an [`Offset`] is resolved) can be.
///
/// This enum encapsulates both the operator as well the the object of the operation (another
/// `Offset`).
///
/// To apply it in e.g. a query, it is first converted to a [`TextSelectionOperator`].
#[derive(Debug, Clone)]
pub enum TextRelationOperator {
    /// Offsets are equal
    Equals(Offset),

    /// Offset A that overlaps with offsets B (cf. textfabric's `&&`), commutative
    Overlaps(Offset),

    /// Offset B is embedded in a offset in A (cf. textfabric's `[[`)
    Embeds(Offset),

    /// Offset A is embedded in a offset in B (cf. textfabric's `[[`)
    Embedded(Offset),

    /// Offset A precedes offset B (come before) all offsets in B. There is no overlap (cf. textfabric's `<<`)
    Precedes(Offset),

    /// Offset A succeeds offset B (cf. textfabric's `>>`)
    Succeeds(Offset),

    // Offset A is immediately to the left of offset B
    LeftAdjacent(Offset),

    // Offset A is immediately to the right of offset B
    RightAdjacent(Offset),

    /// Offsets A and B have the same begin
    SameBegin(Offset),

    /// Offsets A and B have the same end
    SameEnd(Offset),
    //note: SameRange would be the same as Equals for this operator:
}

impl TextRelationOperator {
    pub fn offset(&self) -> &Offset {
        match self {
            Self::Equals(offset)
            | Self::Overlaps(offset)
            | Self::Embeds(offset)
            | Self::Embedded(offset)
            | Self::Precedes(offset)
            | Self::Succeeds(offset)
            | Self::LeftAdjacent(offset)
            | Self::RightAdjacent(offset)
            | Self::SameBegin(offset)
            | Self::SameEnd(offset) => offset,
        }
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
                    let resource: &TextResource =
                        self.iter.store.get(*resource).expect("Resource must exist");
                    let textselection: &TextSelection = resource
                        .get(*textselection)
                        .expect("TextSelection must exist");
                    Some(TargetIterItem {
                        item: textselection,
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
