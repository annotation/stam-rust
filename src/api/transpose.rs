use crate::api::*;
use crate::datavalue::DataValue;
use crate::selector::{Offset, OffsetMode, SelectorBuilder};
use crate::textselection::{ResultTextSelection, ResultTextSelectionSet};
use crate::AnnotationBuilder;
use crate::StamError;

use smallvec::SmallVec;

#[derive(Clone, Default)]
pub struct TransposeConfig {
    source_side: TranspositionSide,

    /// Allow a simple transposition as output, by default this is set to `false` as we usually want to have an transposed annotation
    allow_simple: bool,

    /// Do not produce a transposition annotation, only output the transposed annotation (allow_simple must be set to false)
    /// This effectively throws away the provenance information.
    no_transposition: bool,

    /// Identifier to assign to the newly outputted transposition (if not set, a random one will be generated)
    transposition_id: Option<String>,

    /// Identifiers to assign to the annotation (if not set, random ones will be generated)
    /// The indices correspond to the various sides of the transposition that is being transposed over (in the same order)
    side_ids: Vec<String>,
}

#[derive(Clone)]
pub enum TranspositionSide {
    /// Automatically determine the transposition side
    Auto,
    /// The n'th item in a directional selector (0-indexed!)
    ByIndex(usize),
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
        config: TransposeConfig,
    ) -> Result<Vec<AnnotationBuilder<'static>>, StamError>;
}

impl<'store> Transposable<'store> for ResultItem<'store, Annotation> {
    fn transpose(
        &self,
        via: &ResultItem<'store, Annotation>,
        config: TransposeConfig,
    ) -> Result<Vec<AnnotationBuilder<'static>>, StamError> {
        if let Some(tset) = self.textselectionset() {
            tset.transpose(via, config)
        } else {
            Err(StamError::TransposeError(
                "Can not transpose an annotation that references no text or text in multiple resources".to_string(),
                "(transpose annotation)",
            ))
        }
    }
}

/// Finds matches of one textselection in another
fn transpose_find_sources(
    tsel: &TextSelection,
    reftsel: &TextSelection,
    sources: &mut Vec<TextSelection>,
) -> bool {
    if let Some((intersection, tsel_remainder, reftsel_remainder)) = tsel.intersection(reftsel) {
        sources.push(intersection);
        if let (Some(tsel_remainder), Some(reftsel_remainder)) = (tsel_remainder, reftsel_remainder)
        {
            // there is still a remainder left to process:
            transpose_find_sources(&tsel_remainder, &reftsel_remainder, sources)
        } else {
            // we are done if there is no remainder anymore for the current text selection
            tsel_remainder.is_none()
        }
    } else {
        sources.clear(); //clear the buffer, results lead nowhere
        false
    }
}

impl<'store> Transposable<'store> for ResultTextSelectionSet<'store> {
    fn transpose(
        &self,
        via: &ResultItem<'store, Annotation>,
        config: TransposeConfig,
    ) -> Result<Vec<AnnotationBuilder<'static>>, StamError> {
        via.valid_transposition()?;

        let mut builders: Vec<AnnotationBuilder<'static>> = Vec::with_capacity(3);
        // Keeps track of which side of the transposition the source is found
        let mut source_side: Option<usize> =
            if let TranspositionSide::ByIndex(i) = config.source_side {
                Some(i)
            } else {
                None
            };
        // Found (source) or mapped (target) text selections per side, the first index corresponds to a side
        let mut relative_offsets = Vec::new();
        let mut selectors_per_side: SmallVec<[Vec<SelectorBuilder<'static>>; 2]> = SmallVec::new();

        let mut simple_transposition = true; //falsify

        // match the textselectionset against the sides in a complex transposition (or ascertain that we are dealing with a simple transposition instead)
        // the source side that matches can never be the same as the target side that is mappped to
        for annotation in via.annotations_in_targets(AnnotationDepth::One) {
            simple_transposition = false;
            if let Some(refset) = annotation.textselectionset_in(self.resource()) {
                //TODO
            }
        }

        if simple_transposition {
            let resource = self.resource();
            let mut source_found = false;

            // We may have multiple text selections to transpose (all must be found)
            for tsel in self.inner().iter() {
                // each text selection in a simple transposition corresponds to a side
                for (side_i, reftsel) in via.textselections().enumerate() {
                    if selectors_per_side.len() <= side_i {
                        selectors_per_side.push(Vec::new());
                    }
                    if reftsel.resource() == resource
                        && (source_side.is_none() || source_side == Some(side_i))
                    {
                        if let Some((intersection, None, _)) = tsel.intersection(reftsel.inner()) {
                            source_side = Some(side_i);
                            source_found = true; //source_side might have been pre-set so we need this extra flag
                            let relative_offset = intersection
                                .relative_offset(reftsel.inner(), OffsetMode::default())
                                .expect("intersection offset must be valid");
                            relative_offsets.push(relative_offset);
                            selectors_per_side[side_i].push(SelectorBuilder::TextSelector(
                                resource.handle().into(),
                                intersection.into(),
                            ));
                            break;
                        }
                    }
                }
            }

            // check if we found all text selections
            if !source_found
                || source_side.is_none()
                || selectors_per_side.get(source_side.unwrap()).unwrap().len() != self.inner().len()
            {
                return Err(StamError::TransposeError(
                    format!(
                        "{} out of {} source fragments were covered by the simple transposition {}, not enough to transpose",
                        if let Some(source_side) = source_side {
                            selectors_per_side.get(source_side).unwrap().len()
                        } else {
                            0
                        },
                        self.inner().len(),
                        via.id().unwrap_or("(no-id)"),
                    ),
                    "",
                ));
            }

            // now map the targets (there may be multiple target sides)
            for (side_i, reftsel) in via.textselections().enumerate() {
                let resource = reftsel.resource().handle();
                if selectors_per_side.len() <= side_i {
                    selectors_per_side.push(Vec::new());
                }
                if source_side != Some(side_i) {
                    for offset in relative_offsets.iter() {
                        let mapped_tsel = reftsel.textselection(&offset)?;
                        let mapped_selector: SelectorBuilder<'static> =
                            SelectorBuilder::TextSelector(
                                resource.into(),
                                mapped_tsel.inner().into(),
                            );
                        selectors_per_side[side_i].push(mapped_selector);
                    }
                }
            }
        }

        match selectors_per_side[source_side.expect("source side must exist at this point")].len() {
            0 => unreachable!("sources must have been found at this point"),
            1 if config.allow_simple && !config.no_transposition => {
                //output is a simple transposition
                builders.push(
                    AnnotationBuilder::new()
                        .with_id(
                            config
                                .transposition_id
                                .unwrap_or_else(|| generate_id("transposition-", "")),
                        )
                        .with_data(
                            "https://w3id.org/stam/extensions/stam-transpose/",
                            "Transposition",
                            DataValue::Null,
                        )
                        .with_target(SelectorBuilder::DirectionalSelector(
                            selectors_per_side.into_iter().flatten().collect(),
                        )),
                );
                Ok(builders)
            }
            _ => {
                //output is a complex transposition
                let transposition_id = config
                    .transposition_id
                    .clone()
                    .unwrap_or_else(|| generate_id("transposition-", ""));
                let mut subselectors: Vec<SelectorBuilder<'static>> = Vec::new();
                for (side_i, selectors) in selectors_per_side.into_iter().enumerate() {
                    let side_id = config
                        .side_ids
                        .get(side_i)
                        .map(|x| x.clone())
                        .unwrap_or_else(|| generate_id("", "-transposed"));
                    builders.push(
                        AnnotationBuilder::new()
                            .with_id(side_id.clone())
                            .with_target(SelectorBuilder::DirectionalSelector(selectors)),
                    );
                    if !config.no_transposition {
                        subselectors
                            .push(SelectorBuilder::AnnotationSelector(side_id.into(), None));
                    }
                }
                if !config.no_transposition {
                    builders.push(
                        AnnotationBuilder::new()
                            .with_id(transposition_id)
                            .with_data(
                                "https://w3id.org/stam/extensions/stam-transpose/",
                                "Transposition",
                                DataValue::Null,
                            )
                            .with_target(SelectorBuilder::DirectionalSelector(subselectors)),
                    );
                }
                Ok(builders)
            }
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
