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

pub fn setup_example_multiselector() -> Result<AnnotationStore, StamError> {
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
        .with_annotation(AnnotationBuilder::new().with_id("Sentence1").with_target(
            SelectorBuilder::textselector("humanrights", Offset::whole()),
        ))?
        .with_annotation(AnnotationBuilder::new().with_id("Phrase1").with_target(
            SelectorBuilder::textselector(
                "humanrights",
                Offset::simple(17, 40), //"are born free and equal"
            ),
        ))?;

    Ok(store)
}

pub fn setup_example_6b(store: &mut AnnotationStore) -> Result<AnnotationHandle, StamError> {
    store.annotate(AnnotationBuilder::new().with_id("Phrase2").with_target(
        SelectorBuilder::textselector("humanrights", Offset::simple(4, 25)), //"human beings are born",
    ))?;
    store.annotate(AnnotationBuilder::new().with_id("Phrase3").with_target(
        SelectorBuilder::textselector("humanrights", Offset::simple(44, 62)), //"dignity and rights",
    ))
}

pub fn annotate_regex(store: &mut AnnotationStore) -> Result<(), StamError> {
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

pub fn annotate_words(
    store: &mut AnnotationStore,
    resource: impl Request<TextResource>,
) -> Result<(), StamError> {
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

    store.annotate_from_iter(annotations)?;
    Ok(())
}
