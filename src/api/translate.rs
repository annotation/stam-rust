use std::collections::VecDeque;

use crate::{api::*, Cursor, ResultTextSelection};
use crate::datavalue::DataValue;
use crate::selector::{Offset, OffsetMode, SelectorBuilder};
use crate::text::Text;
use crate::textselection::{ResultTextSelectionSet, TestTextSelection};
use crate::AnnotationBuilder;
use crate::StamError;

use smallvec::SmallVec;

#[derive(Clone, Default)]
pub struct TranslateConfig {
    pub source_side: TranslationSide,

    /// Allow a simple translation as output, by default this is set to `false` as we usually want to have an transposed annotation
    pub allow_simple: bool,


    /// Do not produce a translation annotation, only output the translated annotation (allow_simple must be set to false)
    /// This effectively throws away the provenance information.
    pub no_translation: bool,

    /// Identifier to assign to the newly outputted translation (if not set, a random one will be generated)
    pub translation_id: Option<String>,

    /// Identifier to assign to the newly outputted resegmentation (if any is created and this is not set, a random ID will be generated)
    pub resegmentation_id: Option<String>,

    /// Identifier to assign to the source annotation, if this is an existing one set existing_source_side as well.
    pub source_side_id: Option<String>,

    /// Indicates that the source part of the transposition is an existing annotation. This is usually set automatically after setting `source_side_id` to an existing ID.
    pub existing_source_side: bool,

    /// Do not produce a resegmentation annotation. 
    /// This maps a translation directly and allows losing segmentation information. 
    /// In doing so, it reduces complexity of the output annotations.
    /// If this is set, no resegmentations will be produced, but the resulting translations
    /// may lose some of its fine-grained information, which limits the ability to reuse them as a translation pivot
    /// for further translations.
    /// This only comes into play if `no_translation == false` , `existing_source_side == true` and `source_side_id.is_some()`.
    pub no_resegmentation: bool,

    /// Identifiers to assign to the annotation (if not set, random ones will be generated)
    /// The indices correspond to the various sides of the translation that is being translated over (in the same order, minus the source)
    pub target_side_ids: Vec<String>,

    /// Enable debug mode (to stderr)
    pub debug: bool,
}

#[derive(Clone)]
pub enum TranslationSide {
    /// Automatically determine the translation side
    Auto,
    /// The n'th item in a directional selector (0-indexed!)
    ByIndex(usize),
}

impl Default for TranslationSide {
    fn default() -> Self {
        TranslationSide::Auto
    }
}

pub trait Translatable<'store> {
    /// The translate function maps an annotation, textselection, or textselection set from
    /// one coordinate system to another. These mappings are defined in annotations called
    /// **translations** and are documented here: https://github.com/annotation/stam/blob/master/extensions/stam-translate/README.md
    /// translations link identical textual parts across resources, any annotations within
    /// the bounds of such a mapping can then be *translated* using this function to the other coordinate system.
    ///
    /// The `via` parameter expresses the translation that is being used.
    /// The result of a translate operation is itself again a translation.
    fn translate(
        &self,
        via: &ResultItem<'store, Annotation>,
        config: TranslateConfig,
    ) -> Result<Vec<AnnotationBuilder<'static>>, StamError>;
}

impl<'store> Translatable<'store> for ResultItem<'store, Annotation> {
    fn translate(
        &self,
        via: &ResultItem<'store, Annotation>,
        mut config: TranslateConfig,
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
                .translate(via, config)?
                .into_iter()
                .map(|mut builder| {
                    //target annotations will have empty data, the translation itself already has data (1), resegmentations already have data too (1):
                    if builder.data().is_empty() {
                        //copy the data from the transalted annotation to the empty target annotations
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
            Err(StamError::TranslateError(
                "Can not translate an annotation that references no text or text in multiple resources".to_string(),
                "(translate annotation)",
            ))
        }
    }
}

impl<'store> Translatable<'store> for ResultTextSelectionSet<'store> {
    fn translate(
        &self,
        via: &ResultItem<'store, Annotation>,
        config: TranslateConfig,
    ) -> Result<Vec<AnnotationBuilder<'static>>, StamError> {
        via.valid_translation()?;

        let mut builders: Vec<AnnotationBuilder<'static>> = Vec::with_capacity(3);
        // Keeps track of which side of the translation the source is found
        let mut source_side: Option<usize> =
            if let TranslationSide::ByIndex(i) = config.source_side {
                Some(i)
            } else {
                None
            };
        let mut refseqnrs: Vec<usize> = Vec::new(); //the the sequence number of the covered text selections (in a particular side)
        // Found (source) or mapped (target) text selections per side, the first index corresponds to a side
        let mut selectors_per_side: SmallVec<[Vec<SelectorBuilder<'static>>; 2]> = SmallVec::new();

        let resource = self.resource();
        let mut simple_translation = true; //falsify,  simple translation are not suitable as pivot (no-op)
        let mut resegment = false; //resegmentations are produced when the translated annotation covers multiple source text selections, and when users do not want to lose this segmentation (!no_resegmentation)


        if config.debug {
            eprintln!("[stam translate] ----------------------------");
        }


        let mut sourcecoverage = 0;
        // match the current textselectionset against all the sides in a complex translation (or ascertain
        // that we are dealing with a simple translation instead) the source side that matches
        // can never be the same as the target side that is mapped to
        for tsel in self.inner().iter() {

            // iterate over all the sides
            for (side_i, annotation) in via.annotations_in_targets(AnnotationDepth::One).enumerate()
            {
                simple_translation = false;
                if selectors_per_side.len() <= side_i {
                    selectors_per_side.push(Vec::new());
                }

                if config.debug {
                    let tsel = ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,tsel.clone());
                    eprintln!("[stam translate] Looking for source fragment \"{}\" in side {}", tsel.text().replace("\n","\\n"), side_i);
                }

                // We may have multiple text selections (tsel) to translate (all must be found)
                let mut remainder = Some(tsel.clone());

                for (refseqnr, reftsel) in annotation.textselections().enumerate() {
                    if reftsel.resource() == resource && (source_side.is_none() || source_side == Some(side_i)) //source side check
                    {
                        // get the all reference text selections that are embedded in our text selection (tsel)
                        // we must have full coverage for a translation to be valid
                        if tsel.test(&TextSelectionOperator::embeds(), reftsel.inner(), resource.as_ref()) {
                            refseqnrs.push(refseqnr);
                            selectors_per_side[side_i].push(SelectorBuilder::TextSelector(
                                resource.handle().into(),
                                reftsel.inner().into()
                            ));
                            if let Some((_, new_remainder,_)) = remainder.unwrap().intersection(reftsel.inner()) {
                                remainder = new_remainder;
                                if config.debug {
                                    let tmp = ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,tsel.clone());
                                    if let Some(remainder) = remainder {
                                        let remainder  = ResultTextSelection::Unbound(self.rootstore(), resource.as_ref() ,remainder.clone());
                                        eprintln!("[stam translate] Found source fragment: \"{}\" for \"{}\" with remainder \"{}\"", 
                                            &reftsel.text().replace("\n", "\\n"),
                                            &tmp.text().replace("\n", "\\n"), 
                                            remainder.text().replace("\n","\\n")
                                        );
                                    } else {
                                        eprintln!("[stam translate] Found source fragment: \"{}\" for \"{}\"  (no remainder)", 
                                            &reftsel.text().replace("\n", "\\n"),
                                            &tmp.text().replace("\n", "\\n"), 
                                        );
                                    }
                                }
                                if remainder.is_none() {
                                    //everything form the source is covered by the references
                                    source_side = Some(side_i);
                                    break;
                                }
                            } else {
                                unreachable!("[stam translate] Unexpected error: if text selections are embedded, there must be an intersection");
                            }
                        }
                    }
                }
                if remainder.is_none() {
                    sourcecoverage += 1;
                    if refseqnrs.len() > 1 {
                        //one textselection from the annotation needs to be resegmented into multiple text selections from the source side
                        resegment = true;
                    }
                    break;
                } else {
                    //no full match
                    refseqnrs.clear();
                    selectors_per_side[side_i].clear();
                }
            }
            if simple_translation {
                break;
            }
        }


        if simple_translation {
            //translating over a simple translation is a no-op, as it can only
            //produce the pivot as output
            // We may have multiple text selections to translate (all must be found)
            return Err(StamError::TranslateError(
                format!(
                    "Can not translate over a simple translation, pivot has to be complex"
                ),
                "",
            ));
        } else {
            //complex translation
            if sourcecoverage != self.inner().len() {
                return Err(StamError::TranslateError(
                    format!(
                        "Not all source fragments were found in the complex translation {}, not enough to translate",
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
                    for refseqnr in refseqnrs.iter() {
                        //select the text selection we seek
                        let reftsel = annotation.textselections().nth(*refseqnr).expect("element must exist"); //MAYBE TODO: improve performance
                        let mapped_selector: SelectorBuilder<'static> =
                            SelectorBuilder::TextSelector(
                                reftsel.resource().handle().into(),
                                reftsel.inner().into(),
                            );
                        selectors_per_side[side_i].push(mapped_selector);
                    }
                }
            }
        }

        if source_side.is_none() {
            return Err(StamError::TranslateError(
                    format!(
                        "No source fragments were found in the complex translation {}, source side could not be identified, unable to translate",
                        via.id().unwrap_or("(no-id)"),
                    ),
                    "",
            ));
        }

        if (config.allow_simple || config.no_resegmentation) && resegment {
            //try to simplify the translation by joining adjacent selectors
            selectors_per_side = merge_selectors(selectors_per_side, source_side.unwrap(), config.debug);
            resegment = false;
        }

        match selectors_per_side[source_side.expect("source side must exist at this point")].len() {
            0 => 
                Err(StamError::TranslateError(
                    format!(
                        "No source fragments were found in the complex translation {}, source side has 0 fragments, unable to translate",
                        via.id().unwrap_or("(no-id)"),
                    ),
                    "",
                )),
            1 if config.allow_simple && !config.no_translation => {
                //output is a simple translation
                let translation_id = config
                    .translation_id
                    .clone()
                    .unwrap_or_else(|| generate_id("translation-", ""));
                if config.debug {
                    eprintln!("[stam translate] outputting simple translation {}", &translation_id);
                }
                builders.push(
                    AnnotationBuilder::new()
                        .with_id(translation_id)
                        .with_data(
                            "https://w3id.org/stam/extensions/stam-translate/",
                            "Translation",
                            DataValue::Null,
                        )
                        .with_target(SelectorBuilder::DirectionalSelector(
                            selectors_per_side.into_iter().flatten().collect(),
                        )),
                );
                Ok(builders)
            }
            _ => {
                //output is a complex translation
                let translation_id = config
                    .translation_id
                    .clone()
                    .unwrap_or_else(|| generate_id("translation-", ""));
                if config.debug {
                    eprintln!("[stam translate] outputting complex translation {}", &translation_id);
                }
                let mut translation_selectors: Vec<SelectorBuilder<'static>> = Vec::new();
                let mut target_id_iter = config.target_side_ids.into_iter();

                // ID for the source annotation in the translation
                let source_id = if resegment {
                    generate_id("", "-translationsource")
                } else {
                    config
                        .source_side_id
                        .clone()
                        .unwrap_or_else(|| generate_id("", "-translationsource"))
                };
                for (side_i, selectors) in selectors_per_side.into_iter().enumerate() {
                    if source_side != Some(side_i) || !config.existing_source_side {
                        //create an annotation for this side of the translation
                        let side_id = if source_side == Some(side_i) {
                            source_id.clone()
                        } else {
                            target_id_iter
                                .next()
                                .unwrap_or_else(|| generate_id("", "-translated"))
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
                        if !config.no_translation {
                            translation_selectors
                                .push(SelectorBuilder::AnnotationSelector(side_id.into(), None));
                        }
                    } else {
                        //we are handling the source side
                        if resegment {
                            //resegmentation annotation needed (input annotation covers multiple text selections from the reference source)
                            let resegmentation_id = config
                                .resegmentation_id
                                .clone()
                                .unwrap_or_else(|| generate_id("resegmentation-", ""));
                            if config.debug {
                                eprintln!("[stam translate] outputting resegmentated annotation {}", &source_id);
                            }

                            //add the resegmented annotation (will be the source side of the translation):
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
                                    eprintln!("[stam translate] outputting resegmentation {} ({} -> {})", &resegmentation_id, &real_source_id, &source_id);
                                }
                                builders.push(
                                    AnnotationBuilder::new()
                                        .with_id(resegmentation_id)
                                        .with_data(
                                            "https://w3id.org/stam/extensions/stam-translate/",
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
                            //no resegmentation needed
                            if !self.rootstore().annotation(source_id.as_str()).is_some() {
                                if !config.existing_source_side {
                                    //add a copied source annotation (will be the source side of the translation):
                                    //this occurs if the original one has no ID to link to
                                    if config.debug {
                                        eprintln!("[stam translate] adding a copy of the source annotation (because original has no ID to link to)");
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
                                    return Err(StamError::TranslateError(
                                        format!(
                                            "Expected existing source annotation with ID {}",
                                            source_id.as_str(),
                                        ),
                                        "",
                                    ));
                                }
                            }
                        }
                        translation_selectors.push(SelectorBuilder::AnnotationSelector(
                            source_id.clone().into(),
                            None,
                        ));
                    }
                }
                if !config.no_translation {
                    if translation_selectors.len() < 2 {
                        return Err(StamError::TranslateError(
                            format!(
                                "Expected two sides for the translation, got only {}",
                                translation_selectors.len(),
                            ),
                            "",
                        ));
                    }
                    builders.push(
                        AnnotationBuilder::new()
                            .with_id(translation_id)
                            .with_data(
                                "https://w3id.org/stam/extensions/stam-translate/",
                                "Translation",
                                DataValue::Null,
                            )
                            .with_target(SelectorBuilder::DirectionalSelector(translation_selectors)),
                    );
                }
                if config.debug {
                    eprintln!("[stam translate] returning {} annotations", builders.len());
                }
                Ok(builders)
            }
        }
    }
}

/// Merges adjacent selectors
/// Used when doing translations with lose_segmentation
/// Leads to simpler output (but less powerful) 
fn merge_selectors(selectors_per_side: SmallVec<[Vec<SelectorBuilder<'static>>; 2]>, source_side: usize, debug: bool)  -> 
SmallVec<[Vec<SelectorBuilder<'static>>; 2]> {
    let mut mergable = Vec::new();
    let mut need_merge = false;

    // scan the source side for adjacent selectors to be merged:
    let mut cursor: Option<isize> = None;
    // index of the begin selector
    let mut begin_index: usize = 0;
    if let Some(selectors) = selectors_per_side.get(source_side) { 
        for (i, selector) in selectors.iter().enumerate() {
            if cursor.is_some() && Some(selector.offset().unwrap().begin.into()) != cursor {
                mergable.push((begin_index,i));
                begin_index = i + 1;
            }
            cursor = Some(selector.offset().unwrap().end.into());
        }
        mergable.push((begin_index,selectors.len() - 1)); //last one
        need_merge = selectors.len() > mergable.len()
    }

    if !need_merge {
        //nothing needs to be merged, just return input as is and save some time
        if debug {
            eprintln!("[stam translate] no merges needed");
        }
        return selectors_per_side;
    } else if debug {
        eprintln!("[stam translate] merging selectors (indices): {:?}", mergable);
    }

    // merge the selectors
    let mut merged_selectors: SmallVec<[Vec<SelectorBuilder<'static>>; 2]> = SmallVec::new();
    for (_side, selectors) in selectors_per_side.into_iter().enumerate() {
        let mut new_selectors: Vec<SelectorBuilder<'static>> = Vec::new();
        let resource = selectors.get(0).unwrap().resource().unwrap();
        for (begin_index, end_index) in mergable.iter() {
            let begin: isize = selectors.get(*begin_index).unwrap().offset().unwrap().begin.into();
            let end: isize = selectors.get(*end_index).unwrap().offset().unwrap().end.into();
            new_selectors.push(SelectorBuilder::textselector(resource.clone(), Offset::simple(
                begin as usize, end as usize
            )));
        }
        merged_selectors.push(new_selectors);
    }

    merged_selectors
}

impl<'store> ResultItem<'store, Annotation> {
    /// Tests if this annotaton is valid translation
    /// This is not an extensive test
    fn valid_translation(&self) -> Result<(), StamError> {
        if !self.as_ref().target().is_complex() {
            Err(StamError::TranslateError(
                format!(
                    "Annotation {} is not a valid translation. It must have a complex selector.",
                    self.id().unwrap_or("(no id)")
                ),
                "",
            ))
        } else {
            Ok(())
        }
    }
}
