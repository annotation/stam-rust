use std::collections::VecDeque;

use crate::{api::*, ResultTextSelection};
use crate::datavalue::DataValue;
use crate::selector::{Offset, OffsetMode, SelectorBuilder};
use crate::text::Text;
use crate::textselection::ResultTextSelectionSet;
use crate::AnnotationBuilder;
use crate::StamError;

use smallvec::SmallVec;

#[derive(Clone, Default)]
pub struct TransposeConfig {
    pub source_side: TranspositionSide,

    /// Allow a simple transposition as output, by default this is set to `false` as we usually want to have an transposed annotation
    /// This also implies `no_segmentation` 
    pub allow_simple: bool,

    /// Do not produce a transposition annotation, only output the transposed annotation (allow_simple must be set to false)
    /// This effectively throws away the provenance information.
    pub no_transposition: bool,

    /// Identifier to assign to the newly outputted transposition (if not set, a random one will be generated)
    pub transposition_id: Option<String>,

    /// Identifier to assign to the newly outputted resegmentation (if any is created and this is not set, a random ID will be generated)
    pub resegmentation_id: Option<String>,

    /// Identifier to assign to the source annotation, if this is an existing one set existing_source_side as well .
    pub source_side_id: Option<String>,

    /// Indicates that the source part of the transposition is an existing annotation. This adds some extra
    /// constraints as not all existing annotations may be transposable without a resegmentation!
    /// This is usually set automatically after setting `source_side_id` to an existing ID.
    pub existing_source_side: bool,

    /// Do not produce a resegmentation annotation. If needed for a complex transposition, a resegmented annotation is still created, but
    /// the resegmented version (used as source in the transposition) is not linked to the original source annotation. This effectively throws away provenance information.
    /// This only comes into play if `no_transposition == false` , `existing_source_side == true` and `source_side_id.is_some()`.
    pub no_resegmentation: bool,

    /// Identifiers to assign to the annotation (if not set, random ones will be generated)
    /// The indices correspond to the various sides of the transposition that is being transposed over (in the same order, minus the source)
    pub target_side_ids: Vec<String>,

    /// Enable debug mode (to stderr)
    pub debug: bool,
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
    /// The transpose function maps an annotation, textselection, or textselection set from
    /// one coordinate system to another. These mappings are defined in annotations called
    /// **transpositions** and are documented here: https://github.com/annotation/stam/blob/master/extensions/stam-transpose/README.md
    /// Transpositions link identical textual parts across resources, any annotations within
    /// the bounds of such a mapping can then be *transposed* using this function to the other coordinate system.
    ///
    /// The `via` parameter expresses the transposition that is being used.
    /// The result of a transpose operation is itself again a transposition.
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
            if config.source_side_id.is_none() && self.id().is_some() {
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
        let mut relative_offsets: Vec<(usize,Offset)> = Vec::new(); //first integer is the sequence number of the text selection (in a particular side)
        let mut source_textselections = Vec::new(); //Only used for verification when !config.no_check
        // Found (source) or mapped (target) text selections per side, the first index corresponds to a side
        let mut selectors_per_side: SmallVec<[Vec<SelectorBuilder<'static>>; 2]> = SmallVec::new();

        let resource = self.resource();
        let mut source_found = false;
        let mut simple_transposition = true; //falsify
        let mut resegment = false;

        let mut tselbuffer: VecDeque<TextSelection> =
            self.inner().iter().map(|x| x.clone()).collect(); //MAYBE TODO: slightly waste of time/space if the transposition turns out to be a simple transposition rather than a complex one

        if config.debug {
            eprintln!("[stam transpose] ----------------------------");
        }


        // match the current textselectionset against all the sides in a complex transposition (or ascertain
        // that we are dealing with a simple transposition instead) the source side that matches
        // can never be the same as the target side that is mappped to
        while let Some(tsel) = tselbuffer.pop_front() {

            // iterate over all the sides
            for (side_i, annotation) in via.annotations_in_targets(AnnotationDepth::One).enumerate()
            {
                simple_transposition = false;
                if selectors_per_side.len() <= side_i {
                    selectors_per_side.push(Vec::new());
                }

                if config.debug {
                    let tsel = ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,tsel.clone());
                    eprintln!("[stam transpose] Looking for source fragment \"{}\" in side {}", tsel.text().replace("\n","\\n"), side_i);
                }

                // We may have multiple text selections to transpose (all must be found)
                for (refseqnr, reftsel) in annotation.textselections().enumerate() {
                    if reftsel.resource() == resource && source_side.is_none() || source_side == Some(side_i) //source side check
                    {
                        // get the intersection of our text selection (tsel) and the one from the reference set (reftsel)
                        // this may be a partial match where we end up with a remainder containing
                        // the rest of our initial text selection (tsel)
                        if let Some((intersection, remainder, _)) =
                            tsel.intersection(reftsel.inner())
                        {
                            let relative_offset = intersection
                                .relative_offset(reftsel.inner(), OffsetMode::default())
                                .expect("intersection offset must be valid"); //the relative offset will be used to select target fragments later
                            let source_offset: Offset = intersection.into();
                            if let Some(remainder) = remainder {
                                if remainder.begin() < intersection.begin() {
                                    //not a valid intersection, skip to the next
                                    relative_offsets.clear();
                                    selectors_per_side[side_i].clear();
                                    source_textselections.clear();
                                    source_side = None;
                                    source_found = false;
                                    if config.debug {
                                        eprintln!("[stam transpose] remainder preceeds intersection, bailing out...");
                                    }
                                    continue;
                                }
                                resegment = true;
                                //the text selection was not matched/consumed entirely
                                //add the remainder of the text selection back to the buffer
                                tselbuffer
                                    .push_front(remainder);
                                if config.debug {
                                    let tmp = ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,tsel.clone());
                                    let remainder  = ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,remainder.clone());
                                    eprintln!("[stam transpose] Found source fragment #{}: {:?} \"{}\" in \"{}\" with remainder \"{}\"", 
                                        source_textselections.len(), 
                                        &relative_offset, 
                                        &tmp.text().replace("\n", "\\n"), 
                                        &reftsel.text().replace("\n", "\\n"),
                                        remainder.text().replace("\n","\\n")
                                    );
                                    source_textselections.push(ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,intersection.clone()));
                                }
                            } else {
                                source_side = Some(side_i);
                                source_found = true; //source_side might have been pre-set so we need this extra flag
                                if config.debug {
                                    let tmp = ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,tsel.clone());
                                    eprintln!("[stam transpose] Found source fragment #{}: {:?} \"{}\" in \"{}\"", 
                                        source_textselections.len(), 
                                        &relative_offset, 
                                        &tmp.text().replace("\n", "\\n"), 
                                        &reftsel.text().replace("\n", "\\n"),
                                    );
                                    source_textselections.push(ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,intersection.clone()));
                                }
                            }
                            relative_offsets.push((refseqnr, relative_offset));
                            selectors_per_side[side_i].push(SelectorBuilder::TextSelector(
                                resource.handle().into(),
                                source_offset,
                            ));
                            break;
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
                            relative_offsets.push((0,relative_offset));
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
                    for (_, offset) in relative_offsets.iter() {
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
                    for (j, (refseqnr, relative_offset)) in relative_offsets.iter().enumerate() {
                        //select the text selection we seek
                        let reftsel = annotation.textselections().nth(*refseqnr).expect("element must exist"); //MAYBE TODO: improve performance
                        //select the proper subselection thereof
                        let mapped_tsel = reftsel.textselection(&relative_offset)?;
                        if config.debug {
                            // extra sanity check
                            let source_tsel = &source_textselections[j];
                            if mapped_tsel.text() != source_tsel.text() {
                                return Err(StamError::TransposeError(
                                    format!(
                                        "Transposition failed due to internal error! \"{}\" -> \"{}\" is invalid, relative offset {:?}, reference target text selection: \"{}\", transposed via {}",
                                        source_tsel.text().replace("\n","\\n"),
                                        mapped_tsel.text().replace("\n","\\n"),
                                        &relative_offset,
                                        reftsel.text().replace("\n","\\n"),
                                        via.id().unwrap_or("(no-id)"),
                                    ),
                                    "(debug mode verification stage in transpose())",
                                ));
                            }
                        }
                        let mapped_selector: SelectorBuilder<'static> =
                            SelectorBuilder::TextSelector(
                                reftsel.resource().handle().into(),
                                mapped_tsel.inner().into(),
                            );
                        selectors_per_side[side_i].push(mapped_selector);
                    }
                }
            }
        }

        if source_side.is_none() {
            return Err(StamError::TransposeError(
                    format!(
                        "No source fragments were found in the complex transposition {}, source side could not be identified, unable to transpose",
                        via.id().unwrap_or("(no-id)"),
                    ),
                    "",
            ));
        }

        match selectors_per_side[source_side.expect("source side must exist at this point")].len() {
            0 => 
                Err(StamError::TransposeError(
                    format!(
                        "No source fragments were found in the complex transposition {}, source side has 0 fragments, unable to transpose",
                        via.id().unwrap_or("(no-id)"),
                    ),
                    "",
                )),
            1 if config.allow_simple && !config.no_transposition => {
                //output is a simple transposition
                let transposition_id = config
                    .transposition_id
                    .clone()
                    .unwrap_or_else(|| generate_id("transposition-", ""));
                if config.debug {
                    eprintln!("[stam transpose] outputting simple transposition {}", &transposition_id);
                }
                builders.push(
                    AnnotationBuilder::new()
                        .with_id(transposition_id)
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
                if config.debug {
                    eprintln!("[stam transpose] outputting complex transposition {}", &transposition_id);
                }
                let mut transposition_selectors: Vec<SelectorBuilder<'static>> = Vec::new();
                let mut target_id_iter = config.target_side_ids.into_iter();

                // ID for the source annotation in the transposition
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
                        if selectors.len() == 1 {
                            builders.push(
                                AnnotationBuilder::new()
                                    .with_id(side_id.clone())
                                    .with_target(selectors.into_iter().next().expect("a selector must exist")),
                            );
                        } else {
                            builders.push(
                                AnnotationBuilder::new()
                                    .with_id(side_id.clone())
                                    .with_target(SelectorBuilder::DirectionalSelector(selectors)),
                            );
                        }
                        if !config.no_transposition {
                            transposition_selectors
                                .push(SelectorBuilder::AnnotationSelector(side_id.into(), None));
                        }
                    } else {
                        if resegment {
                            let resegmentation_id = config
                                .resegmentation_id
                                .clone()
                                .unwrap_or_else(|| generate_id("resegmentation-", ""));
                            if config.debug {
                                eprintln!("[stam transpose] outputting resegmentated annotation {}", &source_id);
                            }

                            //add the resegmented annotation (will be the source side of the transposition):
                            if selectors.len() == 1 {
                                builders.push(
                                    AnnotationBuilder::new()
                                        .with_id(source_id.clone())
                                        .with_target(selectors.into_iter().next().expect("a selector must exist")),
                                );
                            } else {
                                builders.push(
                                    AnnotationBuilder::new()
                                        .with_id(source_id.clone())
                                        .with_target(SelectorBuilder::DirectionalSelector(selectors)),
                                );
                            }

                            if !config.no_resegmentation
                                && config.existing_source_side
                                && config.source_side_id.is_some()
                            {
                                // create an extra resegmentation annotation
                                // linking the resegmented annotation to the real source
                                let real_source_id = config.source_side_id.clone().unwrap();
                                if config.debug {
                                    eprintln!("[stam transpose] outputting resegmentation {} ({} -> {})", &resegmentation_id, &real_source_id, &source_id);
                                }
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
                        } else {
                            if !self.rootstore().annotation(source_id.as_str()).is_some() {
                                if !config.existing_source_side {
                                    //add a copied source annotation (will be the source side of the transposition):
                                    //this occurs if the original one has no ID to link to
                                    if config.debug {
                                        eprintln!("[stam transpose] adding a copy of the source annotation (because original has no ID to link to)");
                                    }
                                    if selectors.len() == 1 {
                                        builders.push(
                                            AnnotationBuilder::new()
                                                .with_id(source_id.clone())
                                                .with_target(selectors.into_iter().next().expect("a selector must exist")),
                                        );
                                    } else {
                                        builders.push(
                                            AnnotationBuilder::new()
                                                .with_id(source_id.clone())
                                                .with_target(SelectorBuilder::DirectionalSelector(selectors)),
                                        );
                                    }
                                } else {
                                    return Err(StamError::TransposeError(
                                        format!(
                                            "Expected existing source annotation with ID {}",
                                            source_id.as_str(),
                                        ),
                                        "",
                                    ));
                                }
                            }
                        }
                        transposition_selectors.push(SelectorBuilder::AnnotationSelector(
                            source_id.clone().into(),
                            None,
                        ));
                    }
                }
                if !config.no_transposition {
                    if transposition_selectors.len() < 2 {
                        return Err(StamError::TransposeError(
                            format!(
                                "Expected two sides for the transposition, got only {}",
                                transposition_selectors.len(),
                            ),
                            "",
                        ));
                    }
                    builders.push(
                        AnnotationBuilder::new()
                            .with_id(transposition_id)
                            .with_data(
                                "https://w3id.org/stam/extensions/stam-transpose/",
                                "Transposition",
                                DataValue::Null,
                            )
                            .with_target(SelectorBuilder::DirectionalSelector(transposition_selectors)),
                    );
                }
                if config.debug {
                    eprintln!("[stam transpose] returning {} annotations", builders.len());
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
