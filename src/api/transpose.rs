use crate::api::*;
use crate::textselection::{ResultTextSelection, ResultTextSelectionSet};
use crate::AnnotationBuilder;
use crate::StamError;

pub enum TranspositionSide {
    /// Automatically determine the transposition side, only works for transpositions with two sides (not more)
    Auto,
    /// The n'th item in a directional selector (0-indexed!)
    ByIndex(usize),
    /// ById, selects the side where the annotation has a specific ID (complex transpositions only)
    ById(String),
}

impl TranspositionSide {
    fn is_index(&self, index: usize) -> bool {
        match self {
            Self::ByIndex(i) => index == *i,
            _ => false,
        }
    }

    fn is_id(&self, id: Option<&str>) -> bool {
        match self {
            Self::ById(s) => Some(s.as_str()) == id,
            _ => false,
        }
    }

    fn has_index(&self) -> bool {
        match self {
            Self::ByIndex(..) => true,
            _ => false,
        }
    }
}

impl Default for TranspositionSide {
    fn default() -> Self {
        TranspositionSide::Auto
    }
}

pub trait Transposable<'store> {
    fn transpose(
        &self,
        via: &ResultItem<'store, Annotation>,
        source: TranspositionSide,
        target: TranspositionSide,
    ) -> Result<AnnotationBuilder<'static>, StamError>;
}

impl<'store> Transposable<'store> for ResultItem<'store, Annotation> {
    fn transpose(
        &self,
        via: &ResultItem<'store, Annotation>,
        source: TranspositionSide,
        target: TranspositionSide,
    ) -> Result<AnnotationBuilder<'static>, StamError> {
        if let Some(tset) = self.textselectionset() {
            tset.transpose(via, source, target)
        } else {
            Err(StamError::TransposeError(
                "Can not transpose an annotation that references no text or text in multiple resources".to_string(),
                "(transpose annotation)",
            ))
        }
    }
}
impl<'store> Transposable<'store> for ResultTextSelectionSet<'store> {
    fn transpose(
        &self,
        via: &ResultItem<'store, Annotation>,
        source: TranspositionSide,
        target: TranspositionSide,
    ) -> Result<AnnotationBuilder<'static>, StamError> {
        via.valid_transposition()?;
        let mut found_source: Option<ResultItem<'store, Annotation>> = None;
        let mut found_target: Option<ResultItem<'store, Annotation>> = None;
        let mut simple_transposition = true; //falsify
        let mut sides = 0;

        // find the sides in a complex transposition (or ascertain that we are dealing with a simple transposition instead)
        // the source side can never be the same as the target side
        for (i, annotation) in via.annotations_in_targets(AnnotationDepth::One).enumerate() {
            simple_transposition = false;
            sides += 1;
            if source.is_index(i) || source.is_id(annotation.id()) {
                if annotation.test_textselectionset(&TextSelectionOperator::embeds(), self) {
                    found_source = Some(annotation);
                } else {
                    return Err(StamError::TransposeError(format!("Transposition Side with requested index {} or ID {} does not contain the source fragment", i, annotation.id().unwrap_or("(no id)")), "(transpose via complex transposition)"));
                }
            } else if target.is_index(i) {
                found_target = Some(annotation);
            } else if !target.is_id(annotation.id())
                && !source.has_index()
                && annotation.test_textselectionset(&TextSelectionOperator::embeds(), self)
            {
                found_source = Some(annotation);
            }
        }

        if let Some(found_source) = found_source {
            if found_target.is_none() {
                /*
                let found_target: ResultItem<'store,Annotation> = match &target {
                    TranspositionSide::ById(id) => self.rootstore().annotation(id.as_str()).or_fail()?,
                    TranspositionSide::ByIndex(i) => return Err(StamError::TransposeError(format!("Transposition side with index {} does not exist for transposition", i), "")),
                    TranspositionSide::Auto => if sides == 2 {
                        if
                    }
                }
                */
            }
        } else if simple_transposition {
            //we determined we're not a complex transposition but a simple one:
            //so find the source side in a simple transposition
            let mut found_source: Option<ResultTextSelection<'store>> = None;
            let mut found_target: Option<ResultTextSelection<'store>> = None;
            for (i, text) in via.textselections().enumerate() {
                sides += 1;
                if source.is_index(i) {
                    if text.test_set(&TextSelectionOperator::embeds(), self) {
                        found_source = Some(text);
                    } else {
                        return Err(StamError::TransposeError(format!("Transposition Side with requested index {} does not contain the source fragment", i), "(transpose via simple transposition)"));
                    }
                } else if target.is_index(i) {
                    found_target = Some(text);
                } else if !target.is_index(i)
                    && text.test_set(&TextSelectionOperator::embeds(), self)
                {
                    found_source = Some(text);
                }
            }
            if let Some(found_source) = found_source {
                if found_target.is_none() {
                    match &target {
                        TranspositionSide::ById(id) => {
                            return Err(StamError::TransposeError(format!("You can not specify a transposition target side by ID for simple transpositions"), ""));
                        }
                        TranspositionSide::ByIndex(i) => {
                            return Err(StamError::TransposeError(
                                format!(
                                "Transposition side with index {} does not exist for transposition",
                                i
                            ),
                                "",
                            ))
                        }
                        TranspositionSide::Auto => {
                            if sides == 2 {
                                for text in via.textselections() {
                                    if found_source != text {
                                        found_target = Some(text);
                                    }
                                }
                            } else {
                                return Err(StamError::TransposeError(format!("This transposition has more than two sides, a target needs to be explicitly specified"), ""));
                            }
                        }
                    };
                };
                let found_target = found_target.expect("target must have been be found now");
                return Ok(());
            } else {
                return Err(StamError::TransposeError(
                    format!(
                        "Source fragment not covered by the (simple) transposition {}",
                        via.id().unwrap_or("(no-id)")
                    ),
                    "",
                ));
            }
        } else {
            return Err(StamError::TransposeError(
                format!(
                    "Source fragment not covered by the (complex) transposition {}",
                    via.id().unwrap_or("(no-id)")
                ),
                "",
            ));
        }
    }
}

impl<'store> ResultItem<'store, Annotation> {
    /// Tests if this annotaton is valid transposition
    /// This is not an extensive test
    fn valid_transposition(&self) -> Result<(), StamError> {
        if !self.as_ref().target().is_complex() {
            Err(StamError::TransposeError(
                format!(
                    "Annotation {} is not a valid transposition. It must have a complex selector.",
                    self.id().unwrap_or("(no id)")
                ),
                "",
            ))
        } else {
            Ok(())
        }
    }
}
