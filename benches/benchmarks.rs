use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use stam::{AnnotationStore, Regex};

const CARGO_MANIFEST_DIR: &'static str = env!("CARGO_MANIFEST_DIR");

pub fn bench_textresource(c: &mut Criterion) {
    //we use the GPL license as input text for benchmarks, since we have it anyway and it contains a fair body of text
    let filename = &format!("{}/LICENSE", CARGO_MANIFEST_DIR);

    let mut store = AnnotationStore::new();

    let handle = store.add_resource_from_file(filename).unwrap();

    let singleexpression = Regex::new(r"\w+(?:[-_]\w+)*").unwrap();

    let expressions: Vec<_> = vec![
        Regex::new(r"\w+(?:[-_]\w+)*").unwrap(),
        Regex::new(r"[\.\?,/]+").unwrap(),
        Regex::new(r"[0-9]+(?:[,\.][0-9]+)").unwrap(),
    ];

    c.bench_function("search_text_single", |b| {
        b.iter(|| {
            let singleexpression = black_box(&singleexpression).clone();
            let mut sumlen = 0;
            for item in black_box(store.search_text(&[singleexpression], &None, &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });

    c.bench_function("search_text_multi", |b| {
        b.iter(|| {
            let expressions = black_box(&expressions).clone();
            let mut sumlen = 0;
            for item in black_box(store.search_text(&expressions, &None, &None, false)) {
                sumlen += item.text().len(); //just so we have something silly to do with the item
            }
            assert!(sumlen > 0);
        })
    });
}

criterion_group!(benches, bench_textresource);
criterion_main!(benches);
