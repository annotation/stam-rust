use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use stam::{
    Annotation, AnnotationDataSet, AnnotationHandle, AnnotationStore, Config, Handle, Item, Offset,
    Regex, SelectorBuilder, StoreFor, TextResource, TextSelection,
};

const CARGO_MANIFEST_DIR: &'static str = env!("CARGO_MANIFEST_DIR");

pub fn bench_textsearch(c: &mut Criterion) {
    //we use the GPL license as input text for benchmarks, since we have it anyway and it contains a fair body of text
    let filename = &format!("{}/LICENSE", CARGO_MANIFEST_DIR);

    let mut store = AnnotationStore::new();

    let resource_handle = store.add_resource_from_file(filename).unwrap();
    let resource = store.resource(&resource_handle.into()).unwrap();

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
            for item in black_box(store.find_text_regex(&[singleexpression], &None, &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("textsearch/find_text_regex_text_multi", |b| {
        b.iter(|| {
            let expressions = black_box(&expressions).clone();
            let mut sumlen = 0;
            for item in black_box(store.find_text_regex(&expressions, &None, &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("textsearch/find_text_regex_single2", |b| {
        b.iter(|| {
            let singleexpression = black_box(&searchterm_regex).clone();
            let mut sumlen = 0;
            for item in black_box(store.find_text_regex(&[singleexpression], &None, &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("textsearch/find_text_all", |b| {
        b.iter(|| {
            let mut sumlen = 0;
            for item in black_box(resource.find_text(&searchterm, None)) {
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
    let store = AnnotationStore::new()
        .with_id("test".into())
        .add(TextResource::from_string(
            "testres".to_string(),
            "Hello world".into(),
            Config::default(),
        ))
        .unwrap()
        .add(AnnotationDataSet::new(Config::default()).with_id("testdataset".into()))
        .unwrap()
        .with_annotation(
            Annotation::builder()
                .with_id("A1".into())
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(6, 11),
                ))
                .with_data_with_id(
                    "testdataset".into(),
                    "pos".into(),
                    "noun".into(),
                    "D1".into(),
                ),
        )
        .unwrap()
        .with_annotation(
            Annotation::builder()
                .with_id("A2".into())
                .with_target(SelectorBuilder::TextSelector(
                    "testres".into(),
                    Offset::simple(0, 5),
                ))
                .with_data_with_id(
                    "testdataset".into(),
                    "pos".into(),
                    "interjection".into(),
                    "D2".into(),
                ),
        )
        .unwrap();

    let item: Item<Annotation> = Item::from(0);
    let handle: AnnotationHandle = AnnotationHandle::new(0);
    let id: Item<Annotation> = Item::from("A1");

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
}

criterion_group!(benches, bench_textsearch, bench_storefor);
criterion_main!(benches);
