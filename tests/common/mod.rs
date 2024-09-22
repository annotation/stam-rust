#![allow(dead_code)]
use stam::*;

pub fn setup_example_1() -> Result<AnnotationStore, StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new(Config::default().with_debug(true))
        .with_id("test")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("testres")
                .with_text("Hello world"),
        )?
        .with_dataset(
            AnnotationDataSetBuilder::new()
                .with_id("testdataset")
                .with_key("pos")
                .with_key_value_id("pos", "noun", "D1"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A1")
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(6, 11),
                ))
                .with_existing_data("testdataset", "D1"),
        )?;
    Ok(store)
}

pub fn setup_example_2() -> Result<AnnotationStore, StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::default()
        .with_id("test")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("testres")
                .with_text("Hello world"),
        )?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A1")
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(6, 11),
                ))
                .with_data_with_id("testdataset", "pos", "noun", "D1"),
        )?;
    Ok(store)
}

pub fn setup_example_3() -> Result<AnnotationStore, StamError> {
    //this example includes a higher-order annotation with relative offset
    let store = AnnotationStore::default()
        .with_id("test")
        .with_resource(TextResourceBuilder::new().with_id("testres").with_text(
            "I have no special talent. I am only passionately curious. -- Albert Einstein",
        ))?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("sentence2")
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(26, 57),
                ))
                .with_data("testdataset", "type", "sentence"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("sentence2word2")
                .with_target(SelectorBuilder::annotationselector(
                    "sentence2",
                    Some(Offset::simple(2, 4)),
                ))
                .with_data("testdataset", "type", "word"),
        )?;
    Ok(store)
}

pub fn setup_example_4() -> Result<AnnotationStore, StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::default()
        .with_id("test")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("testres")
                .with_text("Hello world"),
        )?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A1")
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(6, 11),
                ))
                .with_data_with_id("testdataset", "pos", "noun", "D1"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A2")
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(0, 5),
                ))
                .with_data_with_id("testdataset", "pos", "interjection", "D2"),
        )?;
    Ok(store)
}
pub fn setup_example_multiselector_notranged() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::default()
        .with_id("test")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("testres")
                .with_text("Hello world"),
        )?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(AnnotationBuilder::new().with_id("A1").with_target(
            SelectorBuilder::textselector("testres", Offset::simple(6, 11)),
        ))?
        .with_annotation(AnnotationBuilder::new().with_id("A2").with_target(
            SelectorBuilder::textselector("testres", Offset::simple(0, 5)),
        ))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("WordAnnotation")
                .with_target(SelectorBuilder::multiselector([
                    //because of the way we already constructed earlier annotations, this will NOT be an internal ranged selector
                    SelectorBuilder::textselector("testres", Offset::simple(0, 5)),
                    SelectorBuilder::textselector("testres", Offset::simple(6, 11)),
                ]))
                .with_data_with_id("testdataset", "type", "word", "WordAnnotationData"),
        )?;
    Ok(store)
}

pub fn setup_example_multiselector_ranged() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::default()
        .with_id("test")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("testres")
                .with_text("Hello world"),
        )?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("WordAnnotation")
                .with_target(SelectorBuilder::multiselector([
                    //this translates to an internal ranged selector internally!
                    SelectorBuilder::textselector("testres", Offset::simple(0, 5)),
                    SelectorBuilder::textselector("testres", Offset::simple(6, 11)),
                ]))
                .with_data_with_id("testdataset", "type", "word", "WordAnnotationData"),
        )?;
    Ok(store)
}

pub fn setup_example_multiselector_2() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::default()
        .with_id("test")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("testres")
                .with_text("Hello world"),
        )?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A1")
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(6, 11),
                ))
                .with_data_with_id("testdataset", "pos", "noun", "D1"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A2")
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(0, 5),
                ))
                .with_data_with_id("testdataset", "pos", "interjection", "D2"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("WordAnnotation")
                .with_target(SelectorBuilder::multiselector([
                    SelectorBuilder::textselector("testres", Offset::simple(0, 5)),
                    SelectorBuilder::textselector("testres", Offset::simple(6, 11)),
                ]))
                .with_data_with_id("testdataset", "type", "word", "WordAnnotationData"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("AllPosAnnotation")
                .with_target(SelectorBuilder::multiselector(vec![
                    SelectorBuilder::annotationselector("A1", Some(Offset::whole())),
                    SelectorBuilder::annotationselector("A2", Some(Offset::whole())),
                ]))
                .with_data_with_id("testdataset", "hastype", "pos", "AllPosAnnotationData"),
        )?;
    Ok(store)
}

const EXAMPLE5_TEXT: &str = "
Article 1

All human beings are born free and equal in dignity and rights. They are endowed with reason and conscience and should act towards one another in a spirit of brotherhood.

Article 2

Everyone is entitled to all the rights and freedoms set forth in this Declaration, without distinction of any kind, such as race, colour, sex, language, religion, political or other opinion, national or social origin, property, birth or other status. Furthermore, no distinction shall be made on the basis of the political, jurisdictional or international status of the country or territory to which a person belongs, whether it be independent, trust, non-self-governing or under any other limitation of sovereignty.

Article 3

Everyone has the right to life, liberty and security of person.

Article 4

No one shall be held in slavery or servitude; slavery and the slave trade shall be prohibited in all their forms.
";

pub fn setup_example_5() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::default()
        .with_id("example5")
        .add(TextResource::from_string(
            "humanrights",
            EXAMPLE5_TEXT,
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?;
    Ok(store)
}

pub fn setup_example_6() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::default()
        .with_id("example6")
        .add(TextResource::from_string(
            "humanrights",
            "All human beings are born free and equal in dignity and rights.",
            Config::default(),
        ))?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("Sentence1")
                .with_target(SelectorBuilder::textselector(
                    "humanrights",
                    Offset::whole(),
                ))
                .with_data("myset", "type", "sentence"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("Phrase1")
                .with_target(SelectorBuilder::textselector(
                    "humanrights",
                    Offset::simple(17, 40), //"are born free and equal"
                ))
                .with_data("myset", "type", "phrase"),
        )?;

    Ok(store)
}

pub fn annotate_phrases_for_example_6(
    store: &mut AnnotationStore,
) -> Result<AnnotationHandle, StamError> {
    store.annotate(
        AnnotationBuilder::new()
            .with_id("Phrase2")
            .with_target(
                SelectorBuilder::textselector("humanrights", Offset::simple(4, 25)), //"human beings are born",
            )
            .with_data("myset", "type", "phrase"),
    )?;
    store.annotate(
        AnnotationBuilder::new()
            .with_id("Phrase3")
            .with_target(
                SelectorBuilder::textselector("humanrights", Offset::simple(44, 62)), //"dignity and rights",
            )
            .with_data("myset", "type", "phrase"),
    )
}

pub fn annotate_regex_for_example_6(store: &mut AnnotationStore) -> Result<(), StamError> {
    let resource = store.resource("humanrights").unwrap();
    let annotations: Vec<_> = resource
        .find_text_regex(&[Regex::new(r"Article \d").unwrap()], None, true)?
        .into_iter()
        .map(|foundmatch| {
            let offset: Offset = foundmatch.textselections().first().unwrap().into();
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::textselector(resource.handle(), offset))
                .with_data("myset", "type", "header")
        })
        .collect();
    store.annotate_from_iter(annotations)?;
    Ok(())
}

pub fn setup_example_6b() -> Result<AnnotationStore, StamError> {
    // variant of example 6 (full version)
    // tree of annotations with larger annotations before smaller ones
    // using annotationselectors to point from smaller ones to relative offsets in the larger ones

    let mut store = AnnotationStore::default()
        .with_id("example6")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("humanrights")
                .with_text("All human beings are born free and equal in dignity and rights."),
        )?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("Sentence1")
                .with_target(SelectorBuilder::textselector(
                    "humanrights",
                    Offset::whole(),
                ))
                .with_data("myset", "type", "sentence"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("Phrase1")
                .with_target(SelectorBuilder::annotationselector(
                    "Sentence1",
                    Some(Offset::simple(17, 40)), //"are born free and equal"
                ))
                .with_data("myset", "type", "phrase"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("Phrase2")
                .with_target(
                    SelectorBuilder::annotationselector("Sentence1", Some(Offset::simple(4, 25))), //"human beings are born",
                )
                .with_data("myset", "type", "phrase"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("Phrase3")
                .with_target(
                    SelectorBuilder::annotationselector("Sentence1", Some(Offset::simple(44, 62))), //"dignity and rights",
                )
                .with_data("myset", "type", "phrase"),
        )?;

    let resource = store.resource("humanrights").expect("resource must exist");
    let annotations: Vec<_> = resource
        .split_text(" ")
        .filter_map(|word| {
            let word = word.trim_text_with(|x| x.is_ascii_punctuation()).unwrap();
            if !word.is_empty() {
                let sentence = store.annotation("Sentence1").unwrap();
                let builder = AnnotationBuilder::new()
                    .with_target(SelectorBuilder::annotationselector(
                        sentence.handle(),
                        word.relative_offset(
                            &sentence.textselections().next().unwrap(),
                            OffsetMode::default(),
                        ),
                    ))
                    .with_data("myset", "type", "word");
                Some(builder)
            } else {
                None
            }
        })
        .collect();

    store.annotate_from_iter(annotations)?;

    Ok(store)
}

pub fn setup_example_6c() -> Result<AnnotationStore, StamError> {
    // variant of example 6 (full version)
    // tree of annotations with smaller annotations before larger ones
    // using composite selectors to form larger ones from smaller ones

    //starts the same
    let mut store = AnnotationStore::default()
        .with_id("example6")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("humanrights")
                .with_text("All human beings are born free and equal in dignity and rights."),
        )?
        .with_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?;

    //we first annotate the words before anything else, the handles are returned in a vector:
    let words = annotate_words(&mut store, "humanrights")?;

    //then we form a sentence over all the words (we have only one sentence here anyway)
    let sentence = AnnotationBuilder::new()
        .with_id("Sentence1")
        .with_data("myset", "type", "sentence")
        .with_target(SelectorBuilder::CompositeSelector({
            let mut tokens: Vec<_> = words
                .iter()
                .map(|annotation| {
                    SelectorBuilder::annotationselector(*annotation, Some(Offset::whole()))
                })
                .collect();
            //the final punctuation is added directly on the text, we're allowed to mix in a CompositeSelector
            tokens.push(SelectorBuilder::textselector(
                "humanrights",
                Offset::new(Cursor::EndAligned(-1), Cursor::EndAligned(0)),
            ));
            tokens
        }));
    store.annotate(sentence)?;

    //Now we add the phrases, they point to the words too
    let phrase1 = AnnotationBuilder::new()
        .with_id("Phrase1")
        .with_data("myset", "type", "phrase")
        .with_target(SelectorBuilder::CompositeSelector(
            words[3..8] //we know which words to select ("are born free and equal")
                .iter()
                .map(|annotation| {
                    SelectorBuilder::annotationselector(*annotation, Some(Offset::whole()))
                })
                .collect(),
        ));
    store.annotate(phrase1)?;

    let phrase2 = AnnotationBuilder::new()
        .with_id("Phrase2")
        .with_data("myset", "type", "phrase")
        .with_target(SelectorBuilder::CompositeSelector(
            words[1..5] //we know which words to select ("human beings are born")
                .iter()
                .map(|annotation| {
                    SelectorBuilder::annotationselector(*annotation, Some(Offset::whole()))
                })
                .collect(),
        ));
    store.annotate(phrase2)?;

    let phrase3 = AnnotationBuilder::new()
        .with_id("Phrase3")
        .with_data("myset", "type", "phrase")
        .with_target(SelectorBuilder::CompositeSelector(
            words[9..] //we know which words to select
                .iter()
                .map(|annotation| {
                    SelectorBuilder::annotationselector(*annotation, Some(Offset::whole()))
                })
                .collect(),
        ));
    store.annotate(phrase3)?;

    Ok(store)
}

pub fn annotate_words(
    store: &mut AnnotationStore,
    resource: impl Request<TextResource>,
) -> Result<Vec<AnnotationHandle>, StamError> {
    let resource = store.resource(resource).expect("resource");

    //this is a very basic tokeniser:
    let annotations: Vec<_> = resource
        .split_text(" ")
        .filter_map(|word| {
            let word = word.trim_text_with(|x| x.is_ascii_punctuation()).unwrap();
            if !word.is_empty() {
                let builder = AnnotationBuilder::new()
                    .with_target(SelectorBuilder::textselector(resource.handle(), &word))
                    .with_data("myset", "type", "word");
                Some(builder)
            } else {
                None
            }
        })
        .collect();

    store.annotate_from_iter(annotations)
}

pub fn setup_example_7(n: usize) -> Result<AnnotationStore, StamError> {
    //note: this is very slow when not compiled with optimisations if n gets large!!

    let mut store = AnnotationStore::new(Config::default()).with_id("test");

    //artificial text with 100,000 Xs
    let mut text = String::with_capacity(n);
    for _ in 0..n {
        text.push('X');
    }
    store.add_resource(
        TextResourceBuilder::new()
            .with_id("testres")
            .with_text(text),
    )?;
    store.add_dataset(AnnotationDataSetBuilder::new().with_id("testdataset"))?;

    for i in 0..n {
        store.annotate(
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(i, i + 1),
                ))
                .with_id(format!("A{}", i))
                .with_data("testdataset", "type", "X")
                .with_data("testdataset", "n", i),
        )?;

        store.annotate(
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(i, i + 1),
                ))
                .with_id(format!("P{}", i))
                .with_data(
                    "testdataset",
                    "parity",
                    if i % 2 == 0 { "even" } else { "odd" },
                ),
        )?;
    }

    for i in 0..n - 1 {
        store.annotate(
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::textselector(
                    "testres",
                    Offset::simple(i, i + 2),
                ))
                .with_id(format!("B{}", i))
                .with_data("testdataset", "type", "bigram"),
        )?;

        let left = store.annotation(format!("A{}", i)).unwrap().handle();
        let right = store.annotation(format!("A{}", i + 1)).unwrap().handle();

        store.annotate(
            AnnotationBuilder::new()
                .with_target(SelectorBuilder::compositeselector([
                    SelectorBuilder::annotationselector(left, Some(Offset::whole())),
                    SelectorBuilder::annotationselector(right, Some(Offset::whole())),
                ]))
                .with_id(format!("C{}", i))
                .with_data("testdataset", "type", "composite_bigram"),
        )?;
    }

    Ok(store)
}

pub fn setup_example_8() -> Result<AnnotationStore, StamError> {
    //simple transposition
    let store = AnnotationStore::default()
        .with_id("example8")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("humanrights")
                .with_text("all human beings are born free and equal in dignity and rights."),
        )?
        .with_resource(
            TextResourceBuilder::new()
                .with_id("warhol")
                .with_text("human beings are born solitary, but everywhere they are in chains."),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("SimpleTransposition1")
                .with_target(SelectorBuilder::DirectionalSelector(vec![
                    SelectorBuilder::textselector("humanrights", Offset::simple(4, 25)), //"human beings are born",
                    SelectorBuilder::textselector("warhol", Offset::simple(0, 21)), //"human beings are born",
                ]))
                .with_data(
                    "https://w3id.org/stam/extensions/stam-transpose/",
                    "Transposition",
                    DataValue::Null,
                ),
        )?;
    Ok(store)
}

pub fn setup_example_8b() -> Result<AnnotationStore, StamError> {
    //complex transposition

    let store = AnnotationStore::default()
        .with_id("example8")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("humanrights")
                .with_text("all human beings are born free and equal in dignity and rights."),
        )?
        .with_resource(
            TextResourceBuilder::new()
                .with_id("warhol")
                .with_text("human beings are born solitary, but everywhere they are in chains."),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A1")
                .with_target(
                    SelectorBuilder::textselector("humanrights", Offset::simple(4, 25)), //"human beings are born",
                )
                .with_data("testdataset", "type", "phrase"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A2")
                .with_target(
                    SelectorBuilder::textselector("warhol", Offset::simple(0, 21)), //"human beings are born",
                )
                .with_data("testdataset", "type", "phrase"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("ComplexTransposition1")
                .with_target(SelectorBuilder::DirectionalSelector(vec![
                    SelectorBuilder::annotationselector("A1", None),
                    SelectorBuilder::annotationselector("A2", None),
                ]))
                .with_data(
                    "https://w3id.org/stam/extensions/stam-transpose/",
                    "Transposition",
                    DataValue::Null,
                ),
        )?;
    Ok(store)
}

pub fn setup_example_8c() -> Result<AnnotationStore, StamError> {
    //complex transposition (we'll use this to test the word 'human' which is split over two textselections in this transposition)

    let store =
        AnnotationStore::default()
            .with_id("example8")
            .with_resource(
                TextResourceBuilder::new()
                    .with_id("humanrights")
                    .with_text("all human beings are born free and equal in dignity and rights."),
            )?
            .with_resource(TextResourceBuilder::new().with_id("warhol").with_text(
                "hu-\nman beings are born solitary, but everywhere they are in chains.",
            ))?
            .with_annotation(
                AnnotationBuilder::new()
                    .with_id("A1")
                    .with_target(SelectorBuilder::DirectionalSelector(vec![
                        SelectorBuilder::textselector("humanrights", Offset::simple(4, 6)), //"hu",
                        SelectorBuilder::textselector("humanrights", Offset::simple(6, 25)), //"man beings are born",
                    ]))
                    .with_data("testdataset", "type", "phrase"),
            )?
            .with_annotation(
                AnnotationBuilder::new()
                    .with_id("A2")
                    .with_target(SelectorBuilder::DirectionalSelector(vec![
                        SelectorBuilder::textselector("warhol", Offset::simple(1, 3)), //"hu",
                        SelectorBuilder::textselector("warhol", Offset::simple(5, 24)), //"man beings are born",
                    ]))
                    .with_data("testdataset", "type", "phrase"),
            )?
            .with_annotation(
                AnnotationBuilder::new()
                    .with_id("ComplexTransposition1")
                    .with_target(SelectorBuilder::DirectionalSelector(vec![
                        SelectorBuilder::annotationselector("A1", None),
                        SelectorBuilder::annotationselector("A2", None),
                    ]))
                    .with_data(
                        "https://w3id.org/stam/extensions/stam-transpose/",
                        "Transposition",
                        DataValue::Null,
                    ),
            )?;
    Ok(store)
}

pub fn setup_example_9() -> Result<AnnotationStore, StamError> {
    let store = AnnotationStore::default()
        .with_id("example9")
        .with_resource(
            TextResourceBuilder::new()
                .with_id("example9")
                .with_text("the big dog licks the tiny cat"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("np1")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(0, 11)), //"the big dog",
                )
                .with_data("testdataset", "pos", "np"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("np2")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(18, 30)), //"the tiny cat",
                )
                .with_data("testdataset", "pos", "np"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("det1")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(0, 3)), //"the",
                )
                .with_data("testdataset", "pos", "det"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("det2")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(18, 21)), //"the",
                )
                .with_data("testdataset", "pos", "det"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("adj1")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(4, 7)), //"big",
                )
                .with_data("testdataset", "pos", "adj"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("adj2")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(22, 26)), //"tiny",
                )
                .with_data("testdataset", "pos", "adj"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("n1")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(8, 11)), //"dog",
                )
                .with_data("testdataset", "pos", "n"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("n2")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(27, 30)), //"cat",
                )
                .with_data("testdataset", "pos", "n"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("v1")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(12, 17)), //"licks",
                )
                .with_data("testdataset", "pos", "v"),
        )?
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("vp1")
                .with_target(
                    SelectorBuilder::textselector("example9", Offset::simple(12, 30)), //"licks the tiny cat",
                )
                .with_data("testdataset", "pos", "vp"),
        )?;
    Ok(store)
}
