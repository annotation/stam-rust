use std::cmp::Ordering;
use std::collections::BTreeMap;

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
/// The actual reference to the [`TextResource`] is not stored in this structured but should
/// accompany it explicitly when needed
///
/// On the lowest-level, this struct is obtain by a call to [`crate::annotationstore::AnnotationStore::text_selection()`], which
/// resolves a [`TextSelector`]  to a [`TextSelection`]. Such calls are often abstracted away by higher level methods such as [`crate::annotationstore::AnnotationStore::textselections_by_annotation()`].
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

/// A TextSelectionSet holds one or more [`TextSelection`] items, in FIFO order. This structure is optimized to be
/// quickest (no heap allocation) when there is only one TextSelection (which is often) in it.
#[derive(Clone, Debug)]
pub struct TextSelectionSet {
    head: TextSelection,
    tail: Option<Box<TextSelectionSet>>,
}

impl From<TextSelection> for TextSelectionSet {
    fn from(other: TextSelection) -> Self {
        Self {
            head: other,
            tail: None,
        }
    }
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
            let head = set.head();
            self.set = None;
            Some(head)
        } else {
            None
        }
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

    /// A TextSelections in A that overlaps with a TextSelection in B (cf. textfabric's `&&`), commutative
    Overlaps(&'a TextSelectionSet),

    /// All TextSelections in B are embedded by a TextSelection in A (cf. textfabric's `[[`)
    Embeds(&'a TextSelectionSet),

    /// All TextSelections in A are embedded by a TextSelection in B (cf. textfabric's `]]`)
    Embedded(&'a TextSelectionSet),

    /// All TextSelections in A precede (come before) all textselections in B. There is no overlap (cf. textfabric's `<<`)
    Precedes(&'a TextSelectionSet),

    /// All TextSelections in A succeed (come after) all textselections in B. There is no overlap (cf. textfabric's `>>`)
    Succeeds(&'a TextSelectionSet),

    /// The rightmost TextSelections in A end where the leftmost TextSelection in B begins  (cf. textfabric's `<:`)
    //TODO: add mindistance,maxdistance arguments
    LeftAdjacent(&'a TextSelectionSet),

    /// The leftmost TextSelection in A starts where the rightmost TextSelection in B ends  (cf. textfabric's `:>`)
    //TODO: add mindistance,maxdistance argument
    RightAdjacent(&'a TextSelectionSet),

    /// The leftmost TextSelection in A starts where the leftmost TextSelection in B start  (cf. textfabric's `=:`)
    SameBegin(&'a TextSelectionSet),

    /// The rightmost TextSelection in A ends where the rights TextSelection in B ends  (cf. textfabric's `:=`)
    SameEnd(&'a TextSelectionSet),

    /// The leftmost TextSelection in A starts where the leftmost TextSelection in A starts  and
    /// the rightmost TextSelection in A ends where the rights TextSelection in B ends  (cf. textfabric's `::`)
    SameRange(&'a TextSelectionSet),

    Not(Box<TextSelectionOperator<'a>>),
}

impl TextSelectionSet {
    /// This method is called to test whether a specific spatial relation (as expressed by the passed operator) holds between two [`TextSelectionSet`]s.
    /// The operator contains the other part of the equation that is tested against. A boolean is returned with the test result.
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
                //any of the items in this set must match with any item in the otherset
                for item in self.iter() {
                    if item.test(&operator) {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::Embeds(otherset) => {
                otherset.test(&TextSelectionOperator::Embedded(self))
            }
            TextSelectionOperator::Embedded(_)
            | TextSelectionOperator::Precedes(_)
            | TextSelectionOperator::Succeeds(_) => {
                //all of the items in this set must be embedded by/precede/succeed any item in the other
                for item in self.iter() {
                    if !item.test(&operator) {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::LeftAdjacent(_) => self.rightmost().test(&operator),
            TextSelectionOperator::RightAdjacent(_) => self.leftmost().test(&operator),
            TextSelectionOperator::SameBegin(_) => self.leftmost().test(&operator),
            TextSelectionOperator::SameEnd(_) => self.rightmost().test(&operator),
            TextSelectionOperator::SameRange(_) => {
                self.leftmost().test(&operator) && self.rightmost().test(&operator)
            }
            TextSelectionOperator::Not(suboperator) => !self.test(suboperator),
        }
    }

    /// Returns the left-most TextSelection (the one with the lowest start offset) in the set.
    pub fn leftmost(&self) -> &TextSelection {
        let mut leftmost: Option<&TextSelection> = None;
        for item in self.iter() {
            if leftmost.is_none() || item.beginbyte < leftmost.unwrap().beginbyte {
                leftmost = Some(item);
            }
        }
        if let Some(leftmost) = leftmost {
            leftmost
        } else {
            panic!("There must always be a leftmost item");
        }
    }

    /// Returns the right-most TextSelection (the one with the highest end offset) in the set.
    pub fn rightmost(&self) -> &TextSelection {
        let mut rightmost: Option<&TextSelection> = None;
        for item in self.iter() {
            if rightmost.is_none() || item.endbyte > rightmost.unwrap().endbyte {
                rightmost = Some(item);
            }
        }
        if let Some(rightmost) = rightmost {
            rightmost
        } else {
            panic!("There must always be a rightmost item");
        }
    }
}

impl TextSelection {
    /// This methods is called to test whether a specific spatial relation (as expressed by the passed operator) holds between a [`TextSelection`] and another (or multiple) ([`TextSelectionSet`]).
    /// The operator contains the other part of the equation that is tested against. A boolean is returned with the test result.
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
            TextSelectionOperator::Embeds(otherset) => otherset.test(
                &TextSelectionOperator::Embedded(&TextSelectionSet::new(*self)),
            ),
            TextSelectionOperator::Embedded(otherset) => {
                // all in A is embedded in B
                for other in otherset.iter() {
                    if self.beginbyte >= other.beginbyte && self.endbyte <= other.endbyte {
                        return true;
                    }
                }
                false
            }
            TextSelectionOperator::Precedes(otherset) => {
                for other in otherset.iter() {
                    if self.endbyte > other.beginbyte {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::Succeeds(otherset) => {
                for other in otherset.iter() {
                    if self.beginbyte < other.endbyte {
                        return false;
                    }
                }
                true
            }
            TextSelectionOperator::LeftAdjacent(otherset) => {
                let mut leftmost = None;
                for other in otherset.iter() {
                    if leftmost.is_none() || other.beginbyte < leftmost.unwrap() {
                        leftmost = Some(other.beginbyte);
                    }
                }
                Some(self.endbyte) == leftmost
            }
            TextSelectionOperator::RightAdjacent(otherset) => {
                let mut rightmost = None;
                for other in otherset.iter() {
                    if rightmost.is_none() || other.endbyte > rightmost.unwrap() {
                        rightmost = Some(other.endbyte);
                    }
                }
                Some(self.beginbyte) == rightmost
            }
            TextSelectionOperator::SameBegin(otherset) => {
                self.beginbyte == otherset.leftmost().beginbyte()
            }
            TextSelectionOperator::SameEnd(otherset) => {
                self.endbyte == otherset.rightmost().endbyte()
            }
            TextSelectionOperator::SameRange(otherset) => {
                self.beginbyte == otherset.leftmost().beginbyte()
                    && self.endbyte == otherset.rightmost().endbyte()
            }
            TextSelectionOperator::Not(suboperator) => !self.test(suboperator),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OffsetOperator {
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

impl OffsetOperator {
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
