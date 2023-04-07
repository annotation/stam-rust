use regex::{Regex, RegexSet};
use smallvec::{smallvec, SmallVec};

use crate::annotationstore::AnnotationStore;
use crate::error::StamError;
use crate::resources::TextResource;
use crate::selector::Offset;
use crate::textselection::TextSelection;

use crate::config::Configurable;
use crate::store::*;
use crate::types::*;

/// This trait provides methods that operate on structures that hold or represent text content.
pub trait Textual<'store, 'slf>
where
    'store: 'slf,
{
    /// Returns a reference to the text
    fn text(&'slf self) -> &'store str;

    /// Returns the length of the text in unicode points
    /// For bytes, use `Self.text().len()` instead.
    fn textlen(&self) -> usize;

    fn text_by_offset(&'slf self, offset: &Offset) -> Result<&'store str, StamError>;

    fn subslice_utf8_offset(&self, subslice: &str) -> Option<usize>;

    fn utf8byte(&self, abscursor: usize) -> Result<usize, StamError>;
    fn utf8byte_to_charpos(&self, bytecursor: usize) -> Result<usize, StamError>;

    fn find_text_regex<'regex>(
        &'slf self,
        expressions: &'regex [Regex],
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<FindRegexIter<'store, 'regex>, StamError>;

    fn find_text<'a, 'b>(&'a self, fragment: &'b str) -> FindTextIter<'a, 'b>;

    /// Returns an iterator of ['TextSelection`] instances that represent partitions
    /// of the text given the specified delimiter.
    ///
    /// The iterator returns wrapped [`TextSelection`] items.
    fn split_text<'b>(&'slf self, delimiter: &'b str) -> SplitTextIter<'store, 'b>;

    /// Returns a [`TextSelection'] that corresponds to the offset. If the TextSelection
    /// exists, the existing one will be returned (as a copy, but it will have a `TextSelection.handle()`).
    /// If it doesn't exist yet, a new one will be returned, and it won't have a handle, nor will it be added to the store automatically.

    /// The [`TextSelection`] is returned as in a far pointer (`WrappedItem`) that also contains reference to the underlying store.
    ///
    /// Use [`Resource::has_textselection()`] instead if you want to limit to existing text selections on resources.
    fn textselection(
        &'slf self,
        offset: &Offset,
    ) -> Result<WrappedItem<'store, TextSelection>, StamError>;

    /// Resolves a cursor to a begin aligned cursor, resolving all relative end-aligned positions
    fn beginaligned_cursor(&self, cursor: &Cursor) -> Result<usize, StamError> {
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

    /// Resolves a begin-aligned cursor to an absolute cursor (i.e. relative to the TextResource).
    fn absolute_cursor(&self, cursor: usize) -> usize;

    fn absolute_offset(&self, offset: &Offset) -> Result<Offset, StamError> {
        Ok(Offset::simple(
            self.absolute_cursor(self.beginaligned_cursor(&offset.begin)?),
            self.absolute_cursor(self.beginaligned_cursor(&offset.end)?),
        ))
    }
}

/// Auxiliary function used by find_text_regex(). This method does, if needed, a single initial pass
/// over the regular expression set, identifying which regular expressions match and are to be searched
/// for in subsequent passes to find WHERE they match.
pub(crate) fn find_text_regex_select_expressions<'a, 'b>(
    text: &'a str,
    expressions: &'b [Regex],
    precompiledset: Option<&RegexSet>,
) -> Result<Vec<usize>, StamError> {
    Ok(if expressions.len() > 2 {
        //we have multiple expressions, first we do a pass to see WHICH of the regular expression matche (taking them all into account in a single pass!).
        //then afterwards we find for each of the matching expressions WHERE they are found
        let foundexpressions: Vec<_> = if let Some(regexset) = precompiledset {
            regexset.matches(text).into_iter().collect()
        } else {
            RegexSet::new(expressions.iter().map(|x| x.as_str()))
                .map_err(|e| {
                    StamError::RegexError(e, "Parsing regular expressions in search_text()")
                })?
                .matches(text)
                .into_iter()
                .collect()
        };
        foundexpressions
    } else {
        match expressions.len() {
            1 => vec![0],
            2 => vec![0, 1],
            _ => unreachable!("Expected 1 or 2 expressions"),
        }
    })
}

impl AnnotationStore {
    /*
    /// Searches for text in all resources using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// See [`TextResource.find_text_regex()`].
    /// Note that this method, unlike its counterpart [`TextResource.find_text_regex()`], silently ignores any deeper errors that might occur.
    pub fn find_text_regex<'store, 'r>(
        &'store self,
        expressions: &'r [Regex],
        precompiledset: &'r Option<RegexSet>,
        allow_overlap: bool,
    ) -> impl Iterator<Item = FindRegexMatch<'store, 'r>> {
        self.resources()
            .filter_map(move |resource: WrappedItem<'store, TextResource>| {
                //      ^-- the move is only needed to move the bool in, otherwise we had to make it &'r bool and that'd be weird
                resource
                    .find_text_regex(expressions, precompiledset.as_ref(), allow_overlap)
                    .ok() //ignore errors!
            })
            .flatten()
    }
    */
}

/// Wrapper over iterator regex Matches or CaptureMatches
pub(crate) enum Matches<'r, 'store> {
    NoCapture(regex::Matches<'r, 'store>),
    WithCapture(regex::CaptureMatches<'r, 'store>),
}

/// Wrapper over regex Match or Captures (as returned by the iterator)
pub(crate) enum Match<'store> {
    NoCapture(regex::Match<'store>),
    WithCapture(regex::Captures<'store>),
}

impl<'t> Match<'t> {
    /// Return the begin offset of the match (in utf-8 bytes)
    fn begin(&self) -> usize {
        match self {
            Self::NoCapture(m) => m.start(),
            Self::WithCapture(m) => {
                let mut begin = None;
                for group in m.iter() {
                    if let Some(group) = group {
                        if begin.is_none() || begin.unwrap() < group.start() {
                            begin = Some(group.start());
                        }
                    }
                }
                begin.expect("there must be at least one capture group that was found")
            }
        }
    }

    /// Return the end offset of the match (in utf-8 bytes)
    fn end(&self) -> usize {
        match self {
            Self::NoCapture(m) => m.end(),
            Self::WithCapture(m) => {
                let mut end = None;
                for group in m.iter() {
                    if let Some(group) = group {
                        if end.is_none() || end.unwrap() < group.start() {
                            end = Some(group.start());
                        }
                    }
                }
                end.expect("there must be at least one capture group that was found")
            }
        }
    }
}

impl<'r, 't> Iterator for Matches<'r, 't> {
    type Item = Match<'t>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::NoCapture(iter) => {
                if let Some(m) = iter.next() {
                    Some(Match::NoCapture(m))
                } else {
                    None
                }
            }
            Self::WithCapture(iter) => {
                if let Some(m) = iter.next() {
                    Some(Match::WithCapture(m))
                } else {
                    None
                }
            }
        }
    }
}

/// This match structure is returned by the [`FindRegexIter`] iterator, which is in turn produced by [`TextResource.find_text_regex()`] and searches a text based on regular expressions.
/// This structure represents a single regular-expression match of the iterator on the text.
pub struct FindRegexMatch<'t, 'r> {
    expression: &'r Regex,
    expression_index: usize,
    textselections: SmallVec<[WrappedItem<'t, TextSelection>; 2]>,
    //Records the numbers of the capture that match (1-indexed)
    capturegroups: SmallVec<[usize; 2]>,
    resource: &'t TextResource,
}

impl<'t, 'r> FindRegexMatch<'t, 'r> {
    /// Does this match return multiple text selections?
    /// Multiple text selections are returned only when the expression contains multiple capture groups.
    pub fn multi(&self) -> bool {
        self.textselections.len() > 1
    }

    /// Returns the regular expression that matched
    pub fn expression(&self) -> &'r Regex {
        self.expression
    }

    /// Returns the index of regular expression that matched
    pub fn expression_index(&self) -> usize {
        self.expression_index
    }

    pub fn textselections(&self) -> &[WrappedItem<'t, TextSelection>] {
        &self.textselections
    }

    pub fn resource(&self) -> &'t TextResource {
        self.resource
    }

    /// Records the number of the capture groups (1-indexed!) that match.
    /// This array has the same length as textselections and identifies precisely
    /// which textselection corresponds with which capture group.
    pub fn capturegroups(&self) -> &[usize] {
        &self.capturegroups
    }

    /// Return the text of the match, this only works
    /// if the regular expression targets a single
    /// consecutive text, i.e. by not using multiple capture groups.
    pub fn as_str(&self) -> Option<&'t str> {
        if self.multi() {
            None
        } else {
            self.textselections
                .first()
                .map(|textselection| textselection.text())
        }
    }

    /// This returns a vector of texts and is mainly useful in case multiple
    /// patterns were captured.
    /// Use [`Self::as_str()`] instead if you expect only a single text item.
    pub fn text(&self) -> Vec<&str> {
        self.textselections
            .iter()
            .map(|textselection| textselection.text())
            .collect()
    }
}

/// This iterator is produced by [`TextResource.find_text_regex()`] and searches a text based on regular expressions.
pub struct FindRegexIter<'store, 'regex> {
    pub(crate) resource: &'store TextResource,
    pub(crate) expressions: &'regex [Regex], // allows keeping all of the regular expressions external and borrow it, even if only a subset is found (subset is detected in prior pass by search_by_text())
    pub(crate) selectexpressions: Vec<usize>, //points at an expression, not used directly but via selectionexpression() method
    pub(crate) matchiters: Vec<Matches<'regex, 'store>>, //each expression (from selectexpressions) has its own interator  (same length as above vec)
    pub(crate) nextmatches: Vec<Option<Match<'store>>>, //this buffers the next match for each expression (from selectexpressions, same length as above vec)
    pub(crate) text: &'store str,
    pub(crate) begincharpos: usize,
    pub(crate) beginbytepos: usize,
    pub(crate) allow_overlap: bool,
}

impl<'store, 'regex> Iterator for FindRegexIter<'store, 'regex> {
    type Item = FindRegexMatch<'store, 'regex>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.matchiters.is_empty() {
            //instantiate the iterators for the expressions and retrieve the first item for each
            //this is only called once when the iterator first starts
            for i in self.selectexpressions.iter() {
                let re = &self.expressions[*i];
                let mut iter = if re.captures_len() > 1 {
                    Matches::WithCapture(re.captures_iter(self.text))
                } else {
                    Matches::NoCapture(re.find_iter(self.text))
                };
                self.nextmatches.push(iter.next());
                self.matchiters.push(iter);
            }
        }

        //find the best next match (the single one next in line amongst all the iterators)
        let mut bestnextmatch: Option<&Match<'store>> = None;
        let mut bestmatchindex = None;
        for (i, m) in self.nextmatches.iter().enumerate() {
            if let Some(m) = m {
                if bestnextmatch.is_none() || m.begin() < bestnextmatch.unwrap().begin() {
                    bestnextmatch = Some(m);
                    bestmatchindex = Some(i);
                }
            }
        }

        if let Some(i) = bestmatchindex {
            // this match will be the result, convert it to the proper structure
            let m = self.nextmatches[i].take().unwrap();

            // iterate any buffers than overlap with this result, discarding those matces in the process
            if !self.allow_overlap {
                for (j, m2) in self.nextmatches.iter_mut().enumerate() {
                    if j != i && m2.is_some() {
                        if m2.as_ref().unwrap().begin() >= m.begin()
                            && m2.as_ref().unwrap().begin() <= m.end()
                        {
                            //(note: no need to check whether m2.end in range m.begin-m.end)
                            *m2 = self.matchiters[j].next();
                        }
                    }
                }
            }

            let result = self.match_to_result(m, i);

            // iterate the iterator for this one and buffer the next match for next round
            self.nextmatches[i] = self.matchiters[i].next();

            Some(result)
        } else {
            //nothing found, we are all done
            None
        }
    }
}

impl<'store, 'regex> FindRegexIter<'store, 'regex> {
    /// Build the final match structure we return
    fn match_to_result(
        &self,
        m: Match<'store>,
        selectexpression_index: usize,
    ) -> FindRegexMatch<'store, 'regex> {
        let expression_index = self.selectexpressions[selectexpression_index];
        match m {
            Match::NoCapture(m) => {
                let textselection = self
                    .resource
                    .textselection(&Offset::simple(
                        self.begincharpos
                            + self
                                .resource
                                .utf8byte_to_charpos(self.beginbytepos + m.start())
                                .expect("byte to pos conversion must succeed"),
                        self.begincharpos
                            + self
                                .resource
                                .utf8byte_to_charpos(self.beginbytepos + m.end())
                                .expect("byte to pos conversion must succeed"),
                    ))
                    .expect("textselection from offset must succeed");
                FindRegexMatch {
                    expression: &self.expressions[expression_index],
                    expression_index,
                    resource: self.resource,
                    textselections: smallvec!(textselection),
                    capturegroups: smallvec!(),
                }
            }
            Match::WithCapture(m) => {
                let mut groupiter = m.iter();
                groupiter.next(); //The first match always corresponds to the overall match of the regex, we can ignore it
                let mut textselections: SmallVec<_> = SmallVec::new();
                let mut capturegroups: SmallVec<[usize; 2]> = SmallVec::new();
                for (i, group) in groupiter.enumerate() {
                    if let Some(group) = group {
                        capturegroups.push(i + 1); //1-indexed
                        textselections.push(
                            self.resource
                                .textselection(&Offset::simple(
                                    self.begincharpos
                                        + self
                                            .resource
                                            .utf8byte_to_charpos(self.beginbytepos + group.start())
                                            .expect("byte to pos conversion must succeed"),
                                    self.begincharpos
                                        + self
                                            .resource
                                            .utf8byte_to_charpos(self.beginbytepos + group.end())
                                            .expect("byte to pos conversion must succeed"),
                                ))
                                .expect("textselection from offset must succeed"),
                        )
                    }
                }
                FindRegexMatch {
                    expression: &self.expressions[expression_index],
                    expression_index,
                    resource: self.resource,
                    textselections,
                    capturegroups,
                }
            }
        }
    }
}

/// This iterator is produced by [`TextResource.find_text()`] and searches a text for a single fragment
pub struct FindTextIter<'a, 'b> {
    pub(crate) resource: &'a TextResource,
    pub(crate) fragment: &'b str,
    pub(crate) offset: Offset,
}

impl<'a, 'b> Iterator for FindTextIter<'a, 'b> {
    type Item = WrappedItem<'a, TextSelection>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(text) = self.resource.text_by_offset(&self.offset).ok() {
            let begincharpos = self
                .resource
                .beginaligned_cursor(&self.offset.begin)
                .expect("charpos must be valid");
            let beginbytepos = self
                .resource
                .subslice_utf8_offset(text)
                .expect("bytepos must be valid");
            text.find(self.fragment).map(|foundbytepos| {
                let endbytepos = foundbytepos + self.fragment.len();
                let newbegin = begincharpos
                    + self
                        .resource
                        .utf8byte_to_charpos(beginbytepos + foundbytepos)
                        .expect("utf-8 byte must resolve to valid charpos");
                let newend = begincharpos
                    + self
                        .resource
                        .utf8byte_to_charpos(beginbytepos + endbytepos)
                        .expect("utf-8 byte must resolve to valid charpos");
                //set offset for next run
                self.offset = Offset {
                    begin: Cursor::BeginAligned(newend),
                    end: self.offset.end,
                };
                self.resource
                    .textselection(&Offset::simple(newbegin, newend))
                    .expect("textselection must be returned")
            })
        } else {
            None
        }
    }
}

/// This iterator is produced by [`TextResource.split_text()`] and searches a text based on regular expressions.
pub struct SplitTextIter<'store, 'b> {
    pub(crate) resource: &'store TextResource,
    pub(crate) iter: std::str::Split<'store, &'b str>,
    pub(crate) byteoffset: usize,
}

impl<'store, 'b> Iterator for SplitTextIter<'store, 'b> {
    type Item = WrappedItem<'store, TextSelection>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(matchstr) = self.iter.next() {
            let beginbyte = self
                .resource
                .subslice_utf8_offset(matchstr)
                .expect("match must be found")
                - self.byteoffset;
            let endbyte = (beginbyte + matchstr.len()) - self.byteoffset;
            Some(
                self.resource
                    .textselection(&Offset::simple(
                        self.resource
                            .utf8byte_to_charpos(beginbyte)
                            .expect("utf-8 byte must resolve to char pos"),
                        self.resource
                            .utf8byte_to_charpos(endbyte)
                            .expect("utf-8 byte must resolve to char pos"),
                    ))
                    .expect("text selection must succeed"),
            )
        } else {
            None
        }
    }
}
