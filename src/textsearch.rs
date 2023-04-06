use regex::{Regex, RegexSet};
use smallvec::{smallvec, SmallVec};

use crate::annotationstore::AnnotationStore;
use crate::error::StamError;
use crate::resources::TextResource;
use crate::selector::Offset;
use crate::textselection::TextSelection;

use crate::config::Configurable;
use crate::types::*;

pub trait HasText {
    fn find_text_regex<'a, 'b>(
        &'a self,
        expressions: &'b [Regex],
        offset: Option<&Offset>,
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<FindRegexIter<'a, 'b>, StamError>;

    fn find_text<'a, 'b>(
        &'a self,
        fragment: &'b str,
        offset: Option<Offset>,
    ) -> FindTextIter<'a, 'b>;

    fn split_text<'a>(&'a self, delimiter: &'a str)
        -> Box<dyn Iterator<Item = TextSelection> + 'a>;

    /// Returns a reference to the text
    fn text(&self) -> &str;

    /// Returns the length of the text in unicode points
    /// For bytes, use `Self.text().len()` instead.
    fn textlen(&self) -> usize;

    /// Returns a [`TextSelection'] that corresponds to the offset. If the TextSelection
    /// exists, the existing one will be returned (as a copy, but it will have a `TextSelection.handle()`).
    /// If it doesn't exist yet, a new one will be returned, and it won't have a handle, nor will it be added to the store automatically.

    ///
    /// Use [`Resource::has_textselection()`] instead if you want to limit to existing text selections on resources.
    fn textselection(&self, offset: &Offset) -> Result<TextSelection, StamError>;

    /// Returns a text selection by offset, does not check if it exists or not
    fn textselection_by_offset(&self, offset: &Offset) -> Result<TextSelection, StamError> {
        let begin = self.beginaligned_cursor(&offset.begin)?; //this can't fail because it would have already in find_selection()
        let end = self.beginaligned_cursor(&offset.end)?;
        if end > begin {
            Ok(TextSelection {
                intid: None,
                begin,
                end,
            })
        } else {
            Err(StamError::InvalidOffset(
                offset.begin,
                offset.end,
                "End must be greater than begin",
            ))
        }
    }

    /// Resolves a cursor to a being aligned cursor, resolving all relative end-aligned positions
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

    /// Directly extract a part of the text by offset.
    ///
    /// If you want to do operations on the text, you're better of using [`Self.textselection()`] instead.
    fn text_by_offset(&self, offset: Option<&Offset>) -> Result<(&str, usize, usize), StamError> {
        if let Some(offset) = offset {
            let selection = self.textselection(&offset)?;
            let text = self.text_by_textselection(&selection)?;
            Ok((text, selection.begin(), self.utf8byte(selection.begin())?))
        } else {
            Ok((self.text(), 0, 0))
        }
    }

    fn extract_text_by_offset(
        &self,
        offset: Option<&Offset>,
    ) -> Result<(&str, usize, usize), StamError>;

    fn subslice_utf8_offset(&self, subslice: &str) -> Option<usize>;
}

impl AnnotationStore {
    /// Searches for text in all resources using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// See [`TextResource.search_text()`].
    /// Note that this method, unlike its counterpart [`TextResource.find_text_regex()`], silently ignores any deeper errors that might occur.
    pub fn find_text_regex<'t, 'r>(
        &'t self,
        expressions: &'r [Regex],
        offset: &'r Option<Offset>,
        precompiledset: &'r Option<RegexSet>,
        allow_overlap: bool,
    ) -> Box<impl Iterator<Item = FindRegexMatch<'t, 'r>>> {
        Box::new(
            self.resources()
                .filter_map(move |resource| {
                    //      ^-- the move is only needed to move the bool in, otherwise we had to make it &'r bool and that'd be weird
                    resource
                        .find_text_regex(
                            expressions,
                            offset.as_ref(),
                            precompiledset.as_ref(),
                            allow_overlap,
                        )
                        .ok() //ignore errors!
                })
                .flatten(),
        )
    }
}

/// Wrapper over iterator regex Matches or CaptureMatches
enum Matches<'r, 't> {
    NoCapture(regex::Matches<'r, 't>),
    WithCapture(regex::CaptureMatches<'r, 't>),
}

/// Wrapper over regex Match or Captures (as returned by the iterator)
enum Match<'t> {
    NoCapture(regex::Match<'t>),
    WithCapture(regex::Captures<'t>),
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
    textselections: SmallVec<[TextSelection; 2]>,
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

    pub fn textselections(&self) -> &[TextSelection] {
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
    /// if there the regular expression targets a single
    /// consecutive text, i.e. by not using multiple capture groups.
    pub fn as_str(&self) -> Option<&'t str> {
        if self.multi() {
            None
        } else {
            Some(
                self.resource
                    .text_by_textselection(
                        self.textselections
                            .first()
                            .expect("there must be a textselection"),
                    )
                    .expect("textselection should exist"),
            )
        }
    }

    /// This returns a vector of texts and is mainly useful in case multiple
    /// patterns were captured.
    /// Use [`Self::as_str()`] instead if you expect only a single text item.
    pub fn text(&self) -> Vec<&str> {
        self.textselections
            .iter()
            .map(|textselection| {
                self.resource
                    .text_by_textselection(textselection)
                    .expect("textselection should exist")
            })
            .collect()
    }
}

/// This iterator is produced by [`TextResource.find_text_regex()`] and searches a text based on regular expressions.
pub struct FindRegexIter<'t, 'r> {
    resource: &'t TextResource,
    expressions: &'r [Regex], // allows keeping all of the regular expressions external and borrow it, even if only a subset is found (subset is detected in prior pass by search_by_text())
    selectexpressions: Vec<usize>, //points at an expression, not used directly but via selectionexpression() method
    matchiters: Vec<Matches<'r, 't>>, //each expression (from selectexpressions) has its own interator  (same length as above vec)
    nextmatches: Vec<Option<Match<'t>>>, //this buffers the next match for each expression (from selectexpressions, same length as above vec)
    text: &'t str,
    begincharpos: usize,
    beginbytepos: usize,
    allow_overlap: bool,
}

impl<'t, 'r> Iterator for FindRegexIter<'t, 'r> {
    type Item = FindRegexMatch<'t, 'r>;
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
        let mut bestnextmatch: Option<&Match<'t>> = None;
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

impl<'t, 'r> FindRegexIter<'t, 'r> {
    /// Build the final match structure we return
    fn match_to_result(
        &self,
        m: Match<'t>,
        selectexpression_index: usize,
    ) -> FindRegexMatch<'t, 'r> {
        let expression_index = self.selectexpressions[selectexpression_index];
        match m {
            Match::NoCapture(m) => {
                let textselection = TextSelection {
                    intid: None,
                    begin: self.begincharpos
                        + self
                            .resource
                            .utf8byte_to_charpos(self.beginbytepos + m.start())
                            .expect("byte to pos conversion must succeed"),
                    end: self.begincharpos
                        + self
                            .resource
                            .utf8byte_to_charpos(self.beginbytepos + m.end())
                            .expect("byte to pos conversion must succeed"),
                };
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
                let mut textselections: SmallVec<[TextSelection; 2]> = SmallVec::new();
                let mut capturegroups: SmallVec<[usize; 2]> = SmallVec::new();
                for (i, group) in groupiter.enumerate() {
                    if let Some(group) = group {
                        capturegroups.push(i + 1); //1-indexed
                        textselections.push(TextSelection {
                            intid: None,
                            begin: self.begincharpos
                                + self
                                    .resource
                                    .utf8byte_to_charpos(self.beginbytepos + group.start())
                                    .expect("byte to pos conversion must succeed"),
                            end: self.begincharpos
                                + self
                                    .resource
                                    .utf8byte_to_charpos(self.beginbytepos + group.end())
                                    .expect("byte to pos conversion must succeed"),
                        });
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

/// This iterator is produced by [`TextResource.find_text_regex()`] and searches a text based on regular expressions.
pub struct FindTextIter<'a, 'b> {
    resource: &'a TextResource,
    fragment: &'b str,
    offset: Offset,
}

impl<'a, 'b> Iterator for FindTextIter<'a, 'b> {
    type Item = TextSelection;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((text, begincharpos, beginbytepos)) = self
            .resource
            .extract_text_by_offset(Some(&self.offset))
            .ok()
        {
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

                TextSelection {
                    intid: None,
                    begin: newbegin,
                    end: newend,
                }
            })
        } else {
            None
        }
    }
}
