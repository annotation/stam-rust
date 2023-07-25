use criterion::{black_box, criterion_group, criterion_main, Criterion};

use stam::{
    Annotation, AnnotationBuilder, AnnotationDataSet, AnnotationHandle, AnnotationStore, BuildItem,
    Config, Handle, Offset, Regex, SelectorBuilder, StoreFor, Text, TextResource,
};

const CARGO_MANIFEST_DIR: &'static str = env!("CARGO_MANIFEST_DIR");

pub fn bench_textsearch(c: &mut Criterion) {
    //we use the GPL license as input text for benchmarks, since we have it anyway and it contains a fair body of text
    let filename = &format!("{}/LICENSE", CARGO_MANIFEST_DIR);

    let mut store = AnnotationStore::new(Config::default());

    let resource_handle = store.add_resource_from_file(filename).unwrap();
    let resource = store.resource(resource_handle).unwrap();

    let singleexpression = Regex::new(r"\w+(?:[-_]\w+)*").unwrap();

    let expressions: Vec<_> = vec![
        Regex::new(r"\w+(?:[-_]\w+)*").unwrap(),
        Regex::new(r"[\.\?,/]+").unwrap(),
        Regex::new(r"[0-9]+(?:[,\.][0-9]+)").unwrap(),
    ];

    let searchterm_regex = Regex::new("license").unwrap();
    let searchterm = "license";

    c.bench_function("textsearch/find_text_regex_single", |b| {
        b.iter(|| {
            let singleexpression = black_box(&singleexpression).clone();
            let mut sumlen = 0;
            for item in black_box(store.find_text_regex(&[singleexpression], &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("textsearch/find_text_regex_text_multi", |b| {
        b.iter(|| {
            let expressions = black_box(&expressions).clone();
            let mut sumlen = 0;
            for item in black_box(store.find_text_regex(&expressions, &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("textsearch/find_text_regex_single2", |b| {
        b.iter(|| {
            let singleexpression = black_box(&searchterm_regex).clone();
            let mut sumlen = 0;
            for item in black_box(store.find_text_regex(&[singleexpression], &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("textsearch/find_text_all", |b| {
        b.iter(|| {
            let mut sumlen = 0;
            for item in black_box(resource.find_text(&searchterm)) {
                sumlen += item.begin(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("textsearch/split_text", |b| {
        b.iter(|| {
            let mut sumlen = 0;
            for item in black_box(resource.split_text(&searchterm)) {
                sumlen += item.begin(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });
}

pub fn bench_storefor(c: &mut Criterion) {
    let store = AnnotationStore::new(Config::default())
        .with_id("test")
        .add(TextResource::from_string(
            "testres",
            "Hello world",
            Config::default(),
        ))
        .unwrap()
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset"))
        .unwrap()
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A1")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(6, 11),
                ))
                .with_data_with_id("testdataset", "pos", "noun", "D1"),
        )
        .unwrap()
        .with_annotation(
            AnnotationBuilder::new()
                .with_id("A2")
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(0, 5),
                ))
                .with_data_with_id("testdataset", "pos", "interjection", "D2"),
        )
        .unwrap();

    let item: BuildItem<Annotation> = BuildItem::from(0);
    let handle: AnnotationHandle = AnnotationHandle::new(0);
    let id: BuildItem<Annotation> = BuildItem::from("A1");

    c.bench_function("store_get_by_handle", |b| {
        b.iter(|| {
            black_box(store.get(&item)).ok();
        })
    });

    c.bench_function("store_get_by_handle_unchecked", |b| {
        b.iter(|| {
            black_box(unsafe {
                <AnnotationStore as StoreFor<Annotation>>::get_unchecked(&store, handle)
            });
        })
    });

    c.bench_function("store_get_by_idref", |b| {
        b.iter(|| {
            black_box(store.get(&id)).ok();
        })
    });

    c.bench_function("store_get_wrapped_by_handle", |b| {
        b.iter(|| {
            black_box(store.annotation(&item));
        })
    });

    c.bench_function("store_get_wrapped_by_id", |b| {
        b.iter(|| {
            black_box(store.annotation(&id));
        })
    });
}

criterion_group!(benches, bench_textsearch, bench_storefor);
criterion_main!(benches);
