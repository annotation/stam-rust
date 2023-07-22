use regex::{Regex, RegexSet};
use smallvec::{smallvec, SmallVec};

use crate::annotationstore::AnnotationStore;
use crate::error::StamError;
use crate::resources::TextResource;
use crate::selector::Offset;
use crate::textselection::{ResultTextSelection, TextSelection};

use crate::store::*;
use crate::types::*;

/// This trait provides methods that operate on structures that hold or represent text content.
pub trait Text<'store, 'slf>
where
    'store: 'slf,
{
    /// Returns a reference to the text
    fn text(&'slf self) -> &'store str;

    /// Returns the length of the text in unicode points
    /// For bytes, use `Self.text().len()` instead.
    fn textlen(&self) -> usize;

    /// Returns a string reference to a slice of text as specified by the offset
    fn text_by_offset(&'slf self, offset: &Offset) -> Result<&'store str, StamError>;

    /// Finds the utf-8 byte position where the specified text subslice begins
    fn subslice_utf8_offset(&self, subslice: &str) -> Option<usize>;

    /// Converts a unicode character position to a UTF-8 byte position
    fn utf8byte(&self, abscursor: usize) -> Result<usize, StamError>;
    /// Converts a UTF-8 byte position into a unicode position
    fn utf8byte_to_charpos(&self, bytecursor: usize) -> Result<usize, StamError>;

    fn find_text_regex<'regex>(
        &'slf self,
        expressions: &'regex [Regex],
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<FindRegexIter<'store, 'regex>, StamError>;

    /// Searches for the specified text fragment. Returns an iterator to iterate over all matches in the text.
    /// The iterator returns [`TextSelection`] items.
    ///
    /// For more complex and powerful searching use [`Self.find_text_regex()`] instead
    ///
    /// If you want to search only a subpart of the text, extract a ['TextSelection`] first with
    /// [`Self.textselection()`] and then run `find_text()` on that instead.
    fn find_text<'fragment>(
        &'slf self,
        fragment: &'fragment str,
    ) -> FindTextIter<'store, 'fragment>;

    /// Searches for the specified text fragment. Returns an iterator to iterate over all matches in the text.
    /// The iterator returns [`TextSelection`] items.
    ///
    /// For more complex and powerful searching use [`Self.find_text_regex()`] instead
    ///
    /// If you want to search only a subpart of the text, extract a ['TextSelection`] first with
    /// [`Self.textselection()`] and then run `find_text()` on that instead.
    fn find_text_nocase(&'slf self, fragment: &str) -> FindNoCaseTextIter<'store>;

    /// Searches for the multiple text fragment in sequence. Returns a vector with (wrapped) [`TextSelection`] instances.
    ///
    /// Matches must appear in the exact order specified, but *may* have other intermittent text,
    /// determined by the `allow_skip_char` closure. A recommended closure for natural language
    /// text is: `|c| !c.is_alphabetic()`
    ///
    /// The `case_sensitive` parameter determines if the search is case sensitive or not, case insensitive searches have a performance penalty.
    fn find_text_sequence<'fragment, F>(
        &'slf self,
        fragments: &'fragment [&'fragment str],
        allow_skip_char: F,
        case_sensitive: bool,
    ) -> Option<Vec<ResultTextSelection<'store>>>
    where
        F: Fn(char) -> bool,
    {
        let mut results: Vec<ResultTextSelection<'store>> = Vec::with_capacity(fragments.len());
        let mut begin: usize = 0;
        let mut textselectionresult = self.textselection(&Offset::whole());
        for fragment in fragments {
            if let Ok(searchtext) = textselectionresult {
                if let Some(m) = if case_sensitive {
                    searchtext.find_text(fragment).next()
                } else {
                    searchtext.find_text_nocase(fragment).next()
                } {
                    if m.begin() > begin {
                        //we skipped some text since last match, check the characters in between matches
                        let skipped_text = self
                            .textselection(&Offset::simple(begin, m.begin()))
                            .expect("textselection must succeed")
                            .text();
                        for c in skipped_text.chars() {
                            if !allow_skip_char(c) {
                                return None;
                            }
                        }
                    }
                    begin = m.end();
                    results.push(m);
                } else {
                    return None;
                }
                //slice (shorten) new text for next test
                textselectionresult = searchtext.textselection(&Offset::new(
                    Cursor::BeginAligned(begin - searchtext.begin()), //offset must be relative
                    Cursor::EndAligned(0),
                ));
            } else {
                return None;
            }
        }

        Some(results)
    }

    /// Returns an iterator of ['TextSelection`] instances that represent partitions
    /// of the text given the specified delimiter. No text is modified.
    ///
    /// The iterator returns wrapped [`TextSelection`] items.
    fn split_text<'b>(&'slf self, delimiter: &'b str) -> SplitTextIter<'store, 'b>;

    /// Trims all occurrences of any character in `chars` from both the beginning and end of the text,
    /// returning a smaller TextSelection. No text is modified.
    fn trim_text(&'slf self, chars: &[char]) -> Result<ResultTextSelection<'store>, StamError> {
        let mut trimbegin = 0;
        let mut trimend = 0;
        for c in self.text().chars() {
            if chars.contains(&c) {
                trimbegin += 1;
            } else {
                break;
            }
        }
        for c in self.text().chars().rev() {
            if chars.contains(&c) {
                trimend -= 1;
            } else {
                break;
            }
        }
        self.textselection(&Offset::new(
            Cursor::BeginAligned(trimbegin),
            Cursor::EndAligned(trimend),
        ))
    }

    /// Returns a [`TextSelection'] that corresponds to the offset. If the TextSelection
    /// exists, the existing one will be returned (as a copy, but it will have a `TextSelection.handle()`).
    /// If it doesn't exist yet, a new one will be returned, and it won't have a handle, nor will it be added to the store automatically.

    /// The [`TextSelection`] is returned as in a far pointer (`WrappedItem`) that also contains reference to the underlying store.
    ///
    /// Use [`Resource::has_textselection()`] instead if you want to limit to existing text selections on resources.
    fn textselection(&'slf self, offset: &Offset)
        -> Result<ResultTextSelection<'store>, StamError>;

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

    /// Resolves a relative offset (relative to another TextSelection) to an absolute one (in terms of to the underlying TextResource)
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
    textselections: SmallVec<[ResultTextSelection<'t>; 2]>,
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

    pub fn textselections(&self) -> &[ResultTextSelection<'t>] {
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
                            && m2.as_ref().unwrap().begin() < m.end()
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
    type Item = ResultTextSelection<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(text) = self.resource.text_by_offset(&self.offset).ok() {
            let beginbytepos = self
                .resource
                .subslice_utf8_offset(text)
                .expect("bytepos must be valid");
            if let Some(foundbytepos) = text.find(self.fragment) {
                let endbytepos = foundbytepos + self.fragment.len();
                let newbegin = self
                    .resource
                    .utf8byte_to_charpos(beginbytepos + foundbytepos)
                    .expect("utf-8 byte must resolve to valid charpos");
                let newend = self
                    .resource
                    .utf8byte_to_charpos(beginbytepos + endbytepos)
                    .expect("utf-8 byte must resolve to valid charpos");
                //set offset for next run
                self.offset = Offset {
                    begin: Cursor::BeginAligned(newend),
                    end: self.offset.end,
                };
                match self
                    .resource
                    .textselection(&Offset::simple(newbegin, newend))
                {
                    Ok(textselection) => Some(textselection),
                    Err(e) => {
                        eprintln!("WARNING: FindTextIter ended prematurely: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}
/// This iterator is produced by [`TextResource.find_text_nocase()`] and searches a text for a single fragment, without regard for casing.
/// It has more overhead than the exact (case sensitive) variant [`FindTextIter`].
pub struct FindNoCaseTextIter<'a> {
    pub(crate) resource: &'a TextResource,
    pub(crate) fragment: String,
    pub(crate) offset: Offset,
}

impl<'a> Iterator for FindNoCaseTextIter<'a> {
    type Item = ResultTextSelection<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(text) = self.resource.text_by_offset(&self.offset).ok() {
            let text = text.to_lowercase();
            let begincharpos = self
                .resource
                .beginaligned_cursor(&self.offset.begin)
                .expect("charpos must be valid");
            let beginbytepos = self
                .resource
                .utf8byte(begincharpos)
                .expect("bytepos must be retrievable");
            if let Some(foundbytepos) = text.as_str().find(self.fragment.as_str()) {
                let endbytepos = foundbytepos + self.fragment.len();
                let newbegin = self
                    .resource
                    .utf8byte_to_charpos(beginbytepos + foundbytepos)
                    .expect("utf-8 byte must resolve to valid charpos");
                let newend = self
                    .resource
                    .utf8byte_to_charpos(beginbytepos + endbytepos)
                    .expect("utf-8 byte must resolve to valid charpos");
                //set offset for next run
                self.offset = Offset {
                    begin: Cursor::BeginAligned(newend),
                    end: self.offset.end,
                };
                match self
                    .resource
                    .textselection(&Offset::simple(newbegin, newend))
                {
                    Ok(textselection) => Some(textselection),
                    Err(e) => {
                        eprintln!("WARNING: FindNoCaseTextIter ended prematurely: {}", e);
                        None
                    }
                }
            } else {
                None
            }
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
    type Item = ResultTextSelection<'store>;
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
