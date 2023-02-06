use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::slice::Iter;

use smallvec::{smallvec, SmallVec};

use crate::annotation::AnnotationHandle;
use crate::error::StamError;
use crate::resources::TextResourceHandle;
use crate::selector::Offset;
use crate::types::*;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
/// Corresponds to a slice of the text. This only contains minimal
/// information; i.e. the begin byte end end byte in a UTF-8 encoded text.
/// This is similar to `Offset`, which uses unicode points. [`Offset`] points at a resource but still requires
/// some computation to actually retrieve the pointed bit. [`TextSelection`] is the Result
/// after this computation is made.
///
/// The actual reference to the [`crate::TextResource`] is not stored in this structured but should
/// accompany it explicitly when needed
///
/// On the lowest-level, this struct is obtain by a call to [`crate::annotationstore::AnnotationStore::text_selection()`], which
/// resolves a [`crate::Selector::TextSelector`]  to a [`TextSelection`]. Such calls are often abstracted away by higher level methods such as [`crate::annotationstore::AnnotationStore::textselections_by_annotation()`].
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
    /// Return the begin byte in a UTF-8 encoded piece of text
    pub fn beginbyte(&self) -> usize {
        self.beginbyte
    }

    /// Return the end byte (non-inclusive) in a UTF-8 encoded piece of text
    pub fn endbyte(&self) -> usize {
        self.endbyte
    }

    /// Resolves a [`Cursor`] *relative to the text selection* to a utf8 byte position, the text of the TextSelection has to be explicitly passed
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

/// This structure holds the primary reverse index for texts, pointing per text and per selected region in the text,
/// to the annotations that reference it.
////
/// More formally, it maps [`TextResourceHandle`] => [`TextSelection`] => [`AnnotationHandle`]
///
/// The text selection map is ordered.
pub struct TextRelationMap {
    //primary indices correspond to TextResourceHandle
    data: Vec<BTreeMap<TextSelection, Vec<AnnotationHandle>>>,
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
        self.data[x.unwrap()].entry(y).or_default().push(z);
    }

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
                    if leftmost.is_none() || item.beginbyte < leftmost.unwrap().beginbyte {
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
                    if rightmost.is_none() || item.endbyte > rightmost.unwrap().endbyte {
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
                    if (other.beginbyte >= self.beginbyte && other.beginbyte < self.endbyte)
                        || (other.endbyte > self.beginbyte && other.endbyte <= self.endbyte)
                        || (other.beginbyte <= self.beginbyte && other.endbyte >= self.endbyte)
                        || (self.beginbyte <= other.beginbyte && self.endbyte >= other.endbyte)
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
                    if !((other.beginbyte >= self.beginbyte && other.beginbyte < self.endbyte)
                        || (other.endbyte > self.beginbyte && other.endbyte <= self.endbyte)
                        || (other.beginbyte <= self.beginbyte && other.endbyte >= self.endbyte)
                        || (self.beginbyte <= other.beginbyte && self.endbyte >= other.endbyte))
                    {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Embeds(otherset) => {
                // TextSelection embeds an item in other set
                for (other, _, _) in otherset.iter() {
                    if other.beginbyte >= self.beginbyte && other.endbyte <= self.endbyte {
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
                    if !(other.beginbyte >= self.beginbyte && other.endbyte <= self.endbyte) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Embedded(otherset) => {
                // TextSelection is embedded by an item in B
                for (other, _, _) in otherset.iter() {
                    if self.beginbyte >= other.beginbyte && self.endbyte <= other.endbyte {
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
                    if !(self.beginbyte >= other.beginbyte && self.endbyte <= other.endbyte) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Precedes(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.endbyte <= other.beginbyte {
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
                    if self.endbyte > other.beginbyte {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Succeeds(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.beginbyte >= other.endbyte {
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
                    if self.beginbyte < other.endbyte {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::LeftAdjacent(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.endbyte == other.beginbyte {
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
                    if leftmost.is_none() || other.beginbyte < leftmost.unwrap() {
                        leftmost = Some(other.beginbyte);
                    }
                }
                Some(self.endbyte) == leftmost
            }
            TextSelectionOperator::RightAdjacent(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if other.endbyte == self.beginbyte {
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
                    if rightmost.is_none() || other.endbyte > rightmost.unwrap() {
                        rightmost = Some(other.endbyte);
                    }
                }
                Some(self.beginbyte) == rightmost
            }
            TextSelectionOperator::SameBegin(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.beginbyte == other.beginbyte {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::SameBeginAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                self.beginbyte == otherset.leftmost().unwrap().beginbyte()
            }
            TextSelectionOperator::SameEnd(otherset) => {
                for (other, _, _) in otherset.iter() {
                    if self.endbyte == other.endbyte {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::SameEndAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                self.endbyte == otherset.rightmost().unwrap().endbyte()
            }
            TextSelectionOperator::SameRangeAll(otherset) => {
                if otherset.is_empty() {
                    return false;
                }
                self.beginbyte == otherset.leftmost().unwrap().beginbyte()
                    && self.endbyte == otherset.rightmost().unwrap().endbyte()
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
