use std::collections::VecDeque;

use crate::api::*;
use crate::datavalue::DataValue;
use crate::selector::{Offset, OffsetMode, SelectorBuilder};
use crate::textselection::ResultTextSelectionSet;
use crate::AnnotationBuilder;
use crate::StamError;

use smallvec::SmallVec;

#[derive(Clone, Default)]
pub struct TransposeConfig {
    pub source_side: TranspositionSide,

    /// Allow a simple transposition as output, by default this is set to `false` as we usually want to have an transposed annotation
    pub allow_simple: bool,

    /// Do not produce a transposition annotation, only output the transposed annotation (allow_simple must be set to false)
    /// This effectively throws away the provenance information.
    pub no_transposition: bool,

    /// Identifier to assign to the newly outputted transposition (if not set, a random one will be generated)
    pub transposition_id: Option<String>,

    /// Identifier to assign to the newly outputted resegmentation (if any is created and this is not set, a random ID will be generated)
    pub resegmentation_id: Option<String>,

    /// Identifier to assign to the source annotation, if this is an existing one set existing_source_side.
    pub source_side_id: Option<String>,

    /// Indicates that the source part of the transposition is an existing annotation. This adds some extra
    /// constraints as not all existing annotations may be transposable without a resegmentation!
    pub existing_source_side: bool,

    /// Do not produce a resegmentation annotation. If needed for a complex transposition, a resegmented annotation is still created, but
    /// the resegmented version (used as source in the transposition) is not linked to the original source annotation. This effectively throws away provenance information.
    /// This only comes into play if `no_transposition == false` , `existing_source_side == true` and `source_side_id.is_some()`.
    pub no_resegmentation: bool,

    /// Identifiers to assign to the annotation (if not set, random ones will be generated)
    /// The indices correspond to the various sides of the transposition that is being transposed over (in the same order, minus the source)
    pub target_side_ids: Vec<String>,
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
        mut config: TransposeConfig,
    ) -> Result<Vec<AnnotationBuilder<'static>>, StamError> {
        if let Some(tset) = self.textselectionset() {
            if config.source_side_id.is_none() {
                config.source_side_id = Some(
                    self.id()
                        .map(|x| x.to_string())
                        .unwrap_or_else(|| generate_id("", "")),
                );
                config.existing_source_side = true;
            }
            Ok(tset
                .transpose(via, config)?
                .into_iter()
                .map(|mut builder| {
                    //target annotations will have empty data, the transposition itself already has data (1), resegmentations already have data too (1):
                    if builder.data().is_empty() {
                        //copy the data from the transposed annotation to the empty target annotations
                        for data in self.data() {
                            builder =
                                builder.with_existing_data(data.set().handle(), data.handle());
                        }
                        builder
                    } else {
                        builder
                    }
                })
                .collect())
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
        let mut relative_offsets: Vec<Offset> = Vec::new();
        // Found (source) or mapped (target) text selections per side, the first index corresponds to a side
        let mut selectors_per_side: SmallVec<[Vec<SelectorBuilder<'static>>; 2]> = SmallVec::new();

        let resource = self.resource();
        let mut source_found = false;
        let mut simple_transposition = true; //falsify
        let mut resegment = false;

        let mut tselbuffer: VecDeque<(TextSelection, usize)> =
            self.inner().iter().map(|x| (x.clone(), 0)).collect(); //MAYBE TODO: slightly waste of time/space if the transposition turns out to be a simple transposition rather than a complex one

        // match the textselectionset against the sides in a complex transposition (or ascertain that we are dealing with a simple transposition instead)
        // the source side that matches can never be the same as the target side that is mappped to
        while let Some((tsel, remainder_begin)) = tselbuffer.pop_front() {
            for (side_i, annotation) in via.annotations_in_targets(AnnotationDepth::One).enumerate()
            {
                simple_transposition = false;
                if selectors_per_side.len() <= side_i {
                    selectors_per_side.push(Vec::new());
                }
                if let Some(refset) = annotation.textselectionset_in(self.resource()) {
                    // We may have multiple text selections to transpose (all must be found)
                    for reftsel in refset.iter() {
                        if reftsel.resource() == resource
                            && (source_side.is_none() || source_side == Some(side_i))
                        {
                            if let Some((intersection, remainder, _)) =
                                tsel.intersection(reftsel.inner())
                            {
                                source_side = Some(side_i);
                                source_found = true; //source_side might have been pre-set so we need this extra flag
                                let relative_offset = intersection
                                    .relative_offset(reftsel.inner(), OffsetMode::default())
                                    .expect("intersection offset must be valid"); //the relative offset will be used to select target fragments later
                                let mut source_offset: Offset = intersection.into();
                                if remainder_begin > 0 {
                                    //we may need to correct the source offset if we cut a source part in two
                                    source_offset = source_offset
                                        .transpose(remainder_begin as isize)
                                        .expect("transposition of offset must succeed");
                                }
                                if let Some(remainder) = remainder {
                                    if remainder.begin() < intersection.begin() {
                                        //not a valid intersection, skip to the next
                                        continue;
                                    }
                                    resegment = true;
                                    //the text selection was not matched/consumed entirely
                                    //add the remainder of the text selection back to the buffer
                                    tselbuffer
                                        .push_front((remainder, remainder.begin() - tsel.begin()));
                                }
                                relative_offsets.push(relative_offset);
                                selectors_per_side[side_i].push(SelectorBuilder::TextSelector(
                                    resource.handle().into(),
                                    source_offset,
                                ));
                                break;
                            }
                        }
                    }
                }
            }
            if simple_transposition {
                break;
            }
        }

        if simple_transposition {
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
        } else {
            //complex transposition
            if !tselbuffer.is_empty() {
                return Err(StamError::TransposeError(
                    format!(
                        "Not all source fragments were found in the complex transposition {}, not enough to transpose",
                        via.id().unwrap_or("(no-id)"),
                    ),
                    "",
                ));
            }

            // now map the targets (there may be multiple target sides)
            for (side_i, annotation) in via.annotations_in_targets(AnnotationDepth::One).enumerate()
            {
                if selectors_per_side.len() <= side_i {
                    selectors_per_side.push(Vec::new());
                }
                if source_side != Some(side_i) {
                    for (reftsel, offset) in
                        annotation.textselections().zip(relative_offsets.iter())
                    {
                        let resource = reftsel.resource().handle();
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
            0 => 
                Err(StamError::TransposeError(
                    format!(
                        "No source fragments were found in the complex transposition {}, unable to transpose",
                        via.id().unwrap_or("(no-id)"),
                    ),
                    "",
                )),
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
                let mut target_id_iter = config.target_side_ids.into_iter();
                let source_id = if resegment {
                    generate_id("", "-transpositionsource")
                } else {
                    config
                        .source_side_id
                        .clone()
                        .unwrap_or_else(|| generate_id("", "-transpositionsource"))
                };
                for (side_i, selectors) in selectors_per_side.into_iter().enumerate() {
                    if source_side != Some(side_i) || !config.existing_source_side {
                        let side_id = if source_side == Some(side_i) {
                            source_id.clone()
                        } else {
                            target_id_iter
                                .next()
                                .unwrap_or_else(|| generate_id("", "-transposed"))
                        };
                        builders.push(
                            AnnotationBuilder::new()
                                .with_id(side_id.clone())
                                .with_target(SelectorBuilder::DirectionalSelector(selectors)),
                        );
                        if !config.no_transposition {
                            subselectors
                                .push(SelectorBuilder::AnnotationSelector(side_id.into(), None));
                        }
                    } else {
                        if resegment {
                            let resegmentation_id = config
                                .resegmentation_id
                                .clone()
                                .unwrap_or_else(|| generate_id("resegmentation-", ""));

                            //add the resegmented annotation (will be the source side of the transposition):
                            builders.push(
                                AnnotationBuilder::new()
                                    .with_id(source_id.clone())
                                    .with_target(SelectorBuilder::DirectionalSelector(selectors)),
                            );

                            if !config.no_resegmentation
                                && config.existing_source_side
                                && config.source_side_id.is_some()
                            {
                                // create an extra resegmentation annotation
                                // linking the resegmented annotation to the real source
                                let real_source_id = config.source_side_id.clone().unwrap();
                                builders.push(
                                    AnnotationBuilder::new()
                                        .with_id(resegmentation_id)
                                        .with_data(
                                            "https://w3id.org/stam/extensions/stam-transpose/",
                                            "Resegmentation",
                                            DataValue::Null,
                                        )
                                        .with_target(SelectorBuilder::DirectionalSelector(vec![
                                            SelectorBuilder::AnnotationSelector(
                                                real_source_id.into(),
                                                None,
                                            ),
                                            SelectorBuilder::AnnotationSelector(
                                                source_id.clone().into(),
                                                None,
                                            ),
                                        ])),
                                );
                            }
                        }
                        subselectors.push(SelectorBuilder::AnnotationSelector(
                            source_id.clone().into(),
                            None,
                        ));
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
