use stam::*;
use std::ops::Deref;

const CARGO_MANIFEST_DIR: &'static str = env!("CARGO_MANIFEST_DIR");

pub fn setup_example_1() -> Result<AnnotationStore, StamError> {
    //instantiate with builder pattern
    let store = AnnotationStore::new(Config::default().with_debug(true))
        .with_id("test")
        .add(
            TextResourceBuilder::new()
                .with_id("testres")
                .with_text("Hello world")
                .build()?,
        )?
        .add(
            AnnotationDataSet::new(Config::default())
                .with_id("testdataset")
                .add(DataKey::new("pos"))?
                .with_data_with_id("pos", "noun", "D1")?,
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
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
        .add(TextResource::from_string(
            "testres",
            "I have no special talent. I am only passionately curious. -- Albert Einstein",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
            let offset: Offset = foundmatch.textselections().first().unwrap().deref().into();
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
        .add(TextResource::from_string(
            "humanrights",
            "All human beings are born free and equal in dignity and rights.",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?
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
        .add(TextResource::from_string(
            "humanrights",
            "All human beings are born free and equal in dignity and rights.",
            Config::default(),
        ))?
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))?;

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
    let mut store = AnnotationStore::new(Config::default()).with_id("test");

    //artificial text with 100,000 Xs
    let mut text = String::with_capacity(n);
    for _ in 0..n {
        text.push('X');
    }
    store = store
        .add(TextResource::from_string(
            "testres",
            text,
            Config::default(),
        ))
        .unwrap()
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))
        .unwrap();

    for i in 0..n {
        store = store
            .with_annotation(
                AnnotationBuilder::new()
                    .with_target(SelectorBuilder::textselector(
                        "testres",
                        Offset::simple(i, i + 1),
                    ))
                    .with_id(format!("A{}", i))
                    .with_data("testdataset", "type", "X")
                    .with_data("testdataset", "n", i),
            )
            .unwrap();
    }

    for i in 0..n - 1 {
        store = store
            .with_annotation(
                AnnotationBuilder::new()
                    .with_target(SelectorBuilder::textselector(
                        "testres",
                        Offset::simple(i, i + 2),
                    ))
                    .with_id(format!("B{}", i))
                    .with_data("testdataset", "type", "bigram"),
            )
            .unwrap();

        let left = store.annotation(format!("A{}", i)).unwrap().handle();
        let right = store.annotation(format!("A{}", i + 1)).unwrap().handle();

        store = store
            .with_annotation(
                AnnotationBuilder::new()
                    .with_target(SelectorBuilder::compositeselector([
                        SelectorBuilder::annotationselector(left, Some(Offset::whole())),
                        SelectorBuilder::annotationselector(right, Some(Offset::whole())),
                    ]))
                    .with_id(format!("C{}", i))
                    .with_data("testdataset", "type", "composite_bigram"),
            )
            .unwrap();
    }

    Ok(store)
}
