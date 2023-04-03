use regex::{Regex, RegexSet};
use smallvec::{smallvec, SmallVec};

use crate::annotationstore::AnnotationStore;
use crate::error::StamError;
use crate::resources::TextResource;
use crate::selector::Offset;
use crate::textselection::TextSelection;

use crate::config::Configurable;
use crate::types::*;

impl TextResource {
    /// Searches the text using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// is held by the [`SearchTextMatch'] struct.
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
    pub fn search_text<'a, 'b>(
        &'a self,
        expressions: &'b [Regex],
        offset: Option<&Offset>,
        precompiledset: Option<&RegexSet>,
        allow_overlap: bool,
    ) -> Result<SearchTextIter<'a, 'b>, StamError> {
        debug(self.config(), || {
            format!("search_text: expressions={:?}", expressions)
        });
        let (text, begincharpos, beginbytepos) = if let Some(offset) = offset {
            let selection = self.textselection(&offset)?;
            (
                self.text_by_textselection(&selection)?,
                selection.begin(),
                self.utf8byte(selection.begin())?,
            )
        } else {
            (self.text(), 0, 0)
        };
        let selectexpressions = if expressions.len() > 2 {
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
        };
        //Returns an iterator that does the remainder of the actual searching
        Ok(SearchTextIter {
            resource: self,
            expressions,
            selectexpressions,
            matchiters: Vec::new(),
            nextmatches: Vec::new(),
            text,
            begincharpos,
            beginbytepos,
            allow_overlap,
        })
    }
}

impl AnnotationStore {
    /// Searches for text in all resources using one or more regular expressions, returns an iterator over TextSelections along with the matching expression, this
    /// See [`TextResource.search_text()`].
    /// Note that this method, unlike its counterpart on TextResource, silently ignores any deeper errors that might occur
    pub fn search_text<'t, 'r>(
        &'t self,
        expressions: &'r [Regex],
        offset: &'r Option<Offset>,
        precompiledset: &'r Option<RegexSet>,
        allow_overlap: bool,
    ) -> Box<impl Iterator<Item = SearchTextMatch<'t, 'r>>> {
        Box::new(
            self.resources()
                .filter_map(move |resource| {
                    //      ^-- the move is only needed to move the bool in, otherwise we had to make it &'r bool and that'd be weird
                    resource
                        .search_text(
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

pub struct SearchTextMatch<'t, 'r> {
    expression: &'r Regex,
    expression_index: usize,
    textselections: SmallVec<[TextSelection; 2]>,
    //Records the numbers of the capture that match (1-indexed)
    capturegroups: SmallVec<[usize; 2]>,
    resource: &'t TextResource,
}

impl<'t, 'r> SearchTextMatch<'t, 'r> {
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

pub struct SearchTextIter<'t, 'r> {
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

impl<'t, 'r> Iterator for SearchTextIter<'t, 'r> {
    type Item = SearchTextMatch<'t, 'r>;
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

impl<'t, 'r> SearchTextIter<'t, 'r> {
    /// Build the final match structure we return
    fn match_to_result(
        &self,
        m: Match<'t>,
        selectexpression_index: usize,
    ) -> SearchTextMatch<'t, 'r> {
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
                SearchTextMatch {
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
                SearchTextMatch {
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
