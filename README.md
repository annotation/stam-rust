<p align="center">
    <img src="https://github.com/annotation/stam/raw/master/logo.png" alt="stam logo" width="320" />
</p>

[![Crate](https://img.shields.io/crates/v/stam.svg)](https://crates.io/crates/stam)
[![Docs](https://docs.rs/stam/badge.svg)](https://docs.rs/stam/)
[![GitHub build](https://github.com/annotation/stam-rust/actions/workflows/stam.yml/badge.svg?branch=master)](https://github.com/annotation/stam-rust/actions/)
[![GitHub release](https://img.shields.io/github/release/annotation/stam-rust.svg)](https://GitHub.com/annotation/stam-rust/releases/)
[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
![Technology Readiness Level 7/9 - Release Candidate - Technology ready enough and in initial use by end-users in intended scholarly environments. Further validation in progress.](https://w3id.org/research-technology-readiness-levels/Level7ReleaseCandidate.svg)

# STAM Library

STAM is a standalone data model for stand-off text annotation. This is a
software library to work with the model from Rust, and is the primary
library/reference implementation for STAM. It aims to implement the full model
as per the [STAM specification](https://github.com/annotation/stam) and most of
the extensions.

**What can you do with this library?**

* Keep, build and manipulate an efficient in-memory store of texts and annotations on texts
* Search in annotations, data and text, either programmatically or via the [STAM Query Language](https://github.com/annotation/stam/tree/master/extensions/stam-query).
    * Search annotations by data, textual content, relations between text fragments (overlap, embedding, adjacency, etc),
    * Search in text (incl. via regular expressions) and find annotations targeting found text selections.
    * Search in data (set,key,value) and find annotations that use the data.
    * Elementary text operations with regard for text offsets (splitting text on a delimiter, stripping text).
    * Convert between different kind of offsets (absolute, relative to other structures, UTF-8 bytes vs unicode codepoints, etc)
* Read and write resources and annotations from/to STAM JSON, STAM CSV, or an optimised binary (CBOR) representation
    * The underlying [STAM model](https://github.com/annotation/stam) aims to be clear and simple. It is flexible and 
      does not commit to any vocabulary or annotation paradigm other than stand-off annotation.

This STAM library is intended as a foundation upon which further applications
can be built that deal with stand-off annotations on text. We implement all 
the low-level logic in dealing this so you no longer have to and can focus on your 
actual application. The library is written with performance in mind.

## Installation

Add `stam` to your project's `Cargo.toml`:

``$ cargo add stam``

## Usage

Import the library

```rust
use stam;
```

Or if you prefer losing the namespace:

```rust
use stam::*;
```

Loading a STAM JSON file containing an annotation store:

```rust
fn your_function() -> Result<(),stam::StamError> {
    let store = stam::AnnotationStore::from_file("example.stam.json", stam::Config::default())?;
    ...
}
```

*We assume some kind of function returning `Result<_,stam::StamError>` for all examples in this section.*

The annotation store is your workspace, it holds all resources, annotation sets
(i.e. keys and annotation data) and of course the actual annotations. It is a
memory-based store and you can as much as you like into it (as long as it fits
in memory:). 

When instantiating an annotation store, you can pass a configuration
(`stam::Config()`) which specifies various parameters, such as which indices to
generate. Use the various `with_()` methods (a builder pattern) to set the
various configuration options.

### Retrieving items

You can retrieve items by methods that are similarly named to the desired return type:

```rust
let annotation =  store.annotation("my-annotation").or_fail()?;
let resource = store.resource("my-resource").or_fail()?;
let annotationset: &stam::AnnotationDataSet = store.annotationset("my-annotationset").or_fail()?;
let key = annotationset.key("my-key").or_fail()?;
let data = annotationset.annotationdata("my-data").or_fail()?;
```

All of these methods return an `Option<ResultItem<T>>`, where `T` is a type in
the STAM model like `Annotation`, `TextResource`,`AnnotationDataSet`, `DataKey`
or `TextSelection`. The `or_fail()` method transforms it into a
`Result<T,StamError>` and the `?` unwraps it safely into `ResultItem<T>` or
propagates the error further.

The `ResultItem<T>` type holds *a reference* to T, with a lifetime equal to the
store, it also holds a reference to the store itself. You can call `as_ref()`
on all `ResultItem<T>` instances to a direct reference with a lifetime equal to
the store, this exposes a lower-level API. `ResultItem<T>` itself always
exposes a high-level API, which is what you want in most cases.

The wrapping of `TextSelection` is a bit special, instead of
`ResultItem<TextSelection>`, we typically use a more specialised type
`ResultTextSelection`.

### Adding items

Add a resource from an existing plain text file to an existing store:

```rust
let resource_handle = store.add_resource( stam::TextResourceBuilder::new().with_filename("my-text.txt")) )?;
```

*Here we see a `Builder` type that uses a builder pattern to construct
instances of their associated types. The actual instances will be built by the
underlying store.

A similar pattern works for `AnnotationDataSet`:

```rust
let annotationset_handle = store.add_dataset( stam::AnnotationDataSetBuilder::new().with_filename("myset.json") )?;
```

The `add_*` methods take take associated builder types and return handles. There is also a `with_*` variant which can be used in a chained builder pattern, as they return the modified `AnnotationStore` itself.

We use `annotate()` (or `with_annotation()`) to add annotations to an existing store:

```rust
let annotation_handle = store.annotate( stam::AnnotationBuilder::new()
           .with_target( stam::SelectorBuilder::TextSelector("testres", stam::Offset::simple(6,11))) 
           .with_data("testdataset", "pos", "noun") 
)?;
```

Let's now create a store and annotations from scratch, with an explicitly filled `AnnotationDataSet`:

```rust
let store = stam::AnnotationStore::new(stam::Config::default())
    .with_id("test")
    .with_resource( stam::TextResourceBuilder::new().with_id("testres").with_text("Hello world"))?
    .with_dataset( stam::AnnotationDataSetBuilder::new().with_id("testdataset")
           .with_key( "pos")
           .with_key_value_id("pos", "noun", "D1")
    )?
    .with_annotation( stam::Annotation::builder() 
            .with_id("A1")
            .with_target( stam::SelectorBuilder::textselector("testres", stam::Offset::simple(6,11))) 
            .with_existing_data("testdataset", "D1") )?;
```

And here is the very same thing but the `AnnotationDataSet` is filled implicitly here:

```rust
let store = stam::AnnotationStore::default().with_id("test")
    .with_resource( stam::TextResourceBuilder::new().with_id("testres").with_text("Hello world"))?
    .with_dataset( stam::AnnotationDataSetBuilder::new().with_id("testdataset"))
    .with_annotation( stam::AnnotationBuilder::new()
            .with_id("A1")
            .with_target( stam::SelectorBuilder::textselector("testres", stam::Offset::simple(6,11))) 
            .with_data_with_id("testdataset","pos","noun","D1")
    )?;
```

The implementation will ensure to reuse any already existing `AnnotationData` if possible, as not duplicating data is one of the core characteristics of the STAM model.

### Serialisation to file

You can serialize the entire annotation store (including all sets and annotations) to a [STAM JSON](https://github.com/annotation/stam#stam-json) file:

```rust
store.to_file("example.stam.json")?;
```

Or to a [STAM CSV](https://github.com/annotation/stam/tree/master/extensions/stam-csv) file (this will actually create separate derived CSV files for sets and and annotations):

```rust
store.to_file("example.stam.csv")?;
```

### Iterators & Searching

Iterating through all annotations in the store, and outputting a simple tab
separated format with the data by annotation and the text by annotation:

```rust
for annotation in store.annotations() {
    let id = annotation.id().unwrap_or("");
    for data in annotation.data() {
        // get the text to which this annotation refers (if any)
        let text: Vec<&str> = annotation.text().collect();
        print!("{}\t{}\t{}\t{}", id, data.key().id().unwrap(), data.value(), text.join(" "));
    }
}
```

Here is an overview of the most important methods that return an iterator, the
iterators in turn all return `ResultItem<T>` instances (or `ResultTextSelection`). The table is divided into two parts,
the top part simple methods that follows STAM's ownership model. Those in the bottom part leverage the various *reverse indices*
that are computed:


| Method                               |  T                    | Description                         |
| -------------------------------------| ----------------------| ------------------------------------|
| `AnnotationStore.annotations()`      | `Annotation`          | all annotations in the store        | 
| `AnnotationStore.resources()`        | `TextResource`        | all resources in the store          |
| `AnnotationStore.datasets()`         | `AnnotationDataSet`   | all annotation sets in the store    |
| `AnnotationDataSet.keys()`           | `DataKey`             | all keys in the set                 |
| `AnnotationDataSet.data()`           | `AnnotationData`      | all data in the set                 |
| `Annotation.data()`                  | `AnnotationData`      | the data pertaining to the annotation |
| -------------------------------------|-----------------------|-------------------------------------|
| `TextResource.textselections()`      | `TextSelection`       | all *known* text selections in the resource (1) |
| `TextResource.annotations()`         | `Annotation`          | Annotations referencing this text using a `TextSelector` or `AnnotationSelector` |
| `TextResource.annotations_as_metadata()`| `Annotation`          | Annotations referencing the resource via a `ResourceSelector` |
| `AnnotationDataSet.annotations()`         | `Annotation`          | All annotations making use of this set |
| `AnnotationDataSet.annotations_as_metadata()`| `Annotation`          | Annotations referencing the set via a `DataSetSelector` |
| `Annotation.annotations()`           | `Annotation`          | Annotations that reference the current one via an `AnnotationSelector` |
| `Annotation.annotations_in_targets()`  | `Annotation`        | Annotations referenced by the current one via an `AnnotationSelector` |
| `Annotation.textselections()`        | `Annotation`          | Targeted text selections (via `TextSelector` or `AnnotationSelector`) |
| `AnnotationData.annotations()`       | `Annotation`          | All annotations that use this data  |
| `DataKey.data()`                     | `AnnotationData`      | All annotation data that uses this key |
| `TextSelection.annotations()`        | `Annotation`          | All annotations that target this text selection |
| -------------------------------------|-----------------------|-------------------------------------|


Notes:
* (1) With *known* text selections, we refer to portions of the texts that have been referenced by an annotation.
* Most of the methods in the left column, second part of the table, are implemented only for `ResultItem<T>`, not `&T`.
* This library consistently uses iterators and therefore *lazy evaluation*.
  This is more efficient and less memory intensive because you don't need to
  wait for all results to be collected (and heap allocated) before you can do computation.

The main named iterator traits in STAM are:

| Iterator trait                       | T                      | Methods that produce the iterator             |
| ------------------------------------ | ---------------------- | --------------------------------------------- |
| `AnnotationIterator`                | `Annotation`           | `annotations()` / `annotations_in_targets()`  |
| `DataIterator`                       | `AnnotationData`       | `data()` / `find_data()`                      |
| `TextSelectionIterator`              | `TextSelection`        | `textselections()` / `related_text()`         |
| `ResourcesIterator`                  | `AnnotationData`       | `resources()`                                 |
| `KeyIterator`                        | `DataKey`              | `keys()`                                      |
| ------------------------------------ | -----------------------|-----------------------------------------------|

The iterators expose an API allowing various transformations and
filter actions: You can typically transform one type of iterator to another
using the methods in the third column. Similarly, you can obtain an iterator
from `ResultItem` instances through equally named methods.

All of these iterators have an owned collection counterpart (`Handles<T>`) that
holds an entire collection in memory, the items are held by reference to a
store, so the space-overhead is reduced. You can go from the former to the
latter with `.to_handles()` and from the latter to the format with `.items()`.


| Iterator Trait                       | Collection             |
| ------------------------------------ | ---------------------- |
| `AnnotationIterator`                 | `Annotations`          |
| `DataIterator`                       | `Data`                 |
| `ResourcesIterator`                  | `Resources`            |
| `TextSelectionsIter`                 | `TextSelections`       |
| `KeyIterator`                        | `Keys`                 |
| ------------------------------------ | -----------------------|

The iterators can be extended by filters, they are applied in a build pattern and return the an iterator that still implements the same trait, but with the filter applied:

| Filter method                                  | Description            |
| ---------------------------------------------- | ---------------------- |
| `filter_annotation(&ResultItem<Annotation>)`   | Filters on a single annotation  | 
| `filter_annotations(Annotations)`              | Filters on multiple annotations |
| `filter_annotationdata(&ResultItem<AnnotationData>)`     | Filters on a single data item  |
| `filter_data(Data)`                            | Filters on multiple data items |
| `filter_key(&ResultItem<DataKey>)`             | Filters on a data key  | 
| `filter_value(value)`                          | Filters on a data value, the parameter can be of various types |
| ---------------------------------------------- | ---------------------- |

All these iterators are lazy-iterator, that is to say they don't do anything unless consumed. Once they are being iterated over, internal buffers may be allocated.

When you are not interested in the actual items but merely want to test whether
there are results at all, then use the `test()` method that is available on these iterators.

For improved performance, you can add `.parallel()` to an iterator, any
subsequent iterator methods (generic ones like `map()` and `filter()`, not
STAM-specific), will then run in parallel over multiple cores.

#### Examples

Example retrieving all annotations for that have part-of-speech noun (fictitious model):

```rust
let dataset = store.dataset("linguistic-features").or_fail()?;
let key = dataset.key("part-of-speech").or_fail()?;
let annotationsiter = key.data().filter_value("noun".into()).annotations();
```

Alternatively, this can also be done as follows, following a slightly different path to get to the same results. Sometimes one version is more performant than the other, depending on how your data is modelled:

```rust
let annotationsiter = key.annotations().filter_value("noun".into());
```

Example testing whether a word is annotated with part-of-speech noun (fictitious model):

```rust
let dataset = store.dataset("linguistic-features").or_fail()?;
let key = dataset.key("part-of-speech").or_fail()?;
if word.annotations().filter_key(&key).filter_value("noun".into()).test() {
   ...    
}
```

### Searching data

The above methods already allow to find data, but there is `find_data()` method on AnnotationStore and AnnotationDataSet provide a shortcut to quickly get data instances (via a `DataIter`).

Example:

```rust
let data = store.find_data("linguistic-features", "part-of-speech", "noun".into()).next()
```

Here and in examples before we use the `into()` method to coerce a `&str` into
a `DataOperator::Equals(&str)`. There are also other data operators available
allowing for various types and various kinds of comparison (equality,
inequality, greater than, less than, logical and/or etc).

### Searching text

The following methods are available to search for text, they return iterators
producing `ResultItem<T>` items.

| Method                               |  T                    | Description                         |
| -------------------------------------| ----------------------| ------------------------------------|
| `TextResource.find_text()`           | `TextSelection`       | Finds a particular substring in the resource's text. | 
| `TextSelection.find_text()`          | `TextSelection`       | Finds a particular substring within the specified text selection. |
| `TextResource.find_text_regex()`     | `TextSelection`       | Idem, but as powerful regular expressed based search. | 
| `TextSelection.find_text_regex()`    | `TextSelection`       | Idem, but as powerful regular expressed based search. |
| -------------------------------------|-----------------------|-------------------------------------|

### Searching related text

The `related_text()` method allows for for finding text selections that are in a certain relation with the current one(s). It takes a `TextSelectionOperator` as parameter, which distinguishes various variants.

* `Equals` - Both sets occupy cover the exact same TextSelections, and all are covered (cf. textfabric's `==`), commutative, transitive
* `Overlaps` - Each TextSelection in A overlaps with a TextSelection in B (cf. textfabric's `&&`), commutative
* `Embeds` - All TextSelections in B are embedded by a TextSelection in A (cf. textfabric's `[[`)
* `Embedded` - All TextSelections in A are embedded by a TextSelection in B (cf. textfabric's `]]`)
* `Before` - Each TextSelection in A comes before a textselection in B  (cf. textfabric's `<<`)
* `After` - Each TextSelection In A comes after a textselection in B (cf. textfabric's `>>`)
* `Precedes` - Each TextSelection in A precedes B; it ends where at least one TextSelection in B begins.
* `Succeeds` - Each TextSelection in A succeeds B; it begins where at least one TextSelection in A ends.
* `SameBegin` - Each TextSelection in A starts where a TextSelection in B starts
* `SameEnd` - Each TextSelection in A starts where a TextSelection in B ends

The variants are typically constructed via a helper function on `TextSelectionOperator` (simply name of the variant in lowercase), e.g. `TextSelectionOperator::equals()`.

Example, select all words in a sentence (sentence may be either an `Annotation` or `TextSelection` in this case):

```rust
let dataset = store.dataset("structure-type").or_fail()?;
let key_word = dataset.key("word").or_fail()?;
for word in sentence.related_text(stam::TextSelectionOperator::embeds()).annotations().filter_key(key_word) {
    ...
}
```

### Querying

Rather than searching programmatically, you can also express queries via the
[STAM Query Language
(STAMQL)](https://github.com/annotation/stam/tree/master/extensions/stam-query).
Do note that this incurs a performance penalty due to extra overhead:

```rust
let query: Query = "SELECT ANNOTATION ?a WHERE DATA myset type = phrase;".try_into()?;
let iter = store.query(query);
let names = iter.names();
for results in iter {
    if let Ok(result) = results.get_by_name(&names, "a") {
       if let QueryResultItem::Annotation(annotation) = result {
          ...
        }
    }
}
```


## API Reference Documentation

Please consult the [API reference documentation](https://docs.rs/stam) for
in-depth explanation on all structures, traits and methods, along with some
examples.

## Extensions

This library implements the following STAM extensions:

* [STAM-CSV](https://github.com/annotation/stam/tree/master/extensions/stam-csv) - Defines an alternative serialisation format using CSV.
* [STAM-Query](https://github.com/annotation/stam/tree/master/extensions/stam-query) - Defines the STAM Query Language. 
* [STAM-Transpose](https://github.com/annotation/stam/tree/master/extensions/stam-transpose) -  Defines linking identical textual parts across resources. 
* [STAM-Textvalidation](https://github.com/annotation/stam/tree/master/extensions/stam-textvalidation) - Defines a mechanism to ensure annotation targets can be checked for changes. 

## Python binding

This library comes with a binding for Python, see [here](https://github.com/annotation/stam-python).

## Acknowledgements

This work is conducted at the [KNAW Humanities Cluster](https://huc.knaw.nl/)'s [Digital Infrastructure department](https://di.huc.knaw.nl/), and funded by the [CLARIAH](https://clariah.nl) project (CLARIAH-PLUS, NWO grant 184.034.023) as part of the FAIR Annotations track of the Shared Development Roadmap.
