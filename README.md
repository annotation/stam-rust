<p align="center">
    <img src="https://github.com/annotation/stam/raw/master/logo.png" alt="stam logo" width="320" />
</p>

[![Crate](https://img.shields.io/crates/v/stam.svg)](https://crates.io/crates/stam)
[![Docs](https://docs.rs/stam/badge.svg)](https://docs.rs/stam/)
[![GitHub build](https://github.com/annotation/stam-rust/actions/workflows/stam.yml/badge.svg?branch=master)](https://github.com/annotation/stam-rust/actions/)
[![GitHub release](https://img.shields.io/github/release/annotation/stam-rust.svg)](https://GitHub.com/annotation/stam-rust/releases/)
[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)

# STAM Library

[STAM](https://github.com/annotation/stam) is a data model for stand-off text annotation and described in detail [here](https://github.com/annotation/stam). This is a software library to work with the model, written in Rust.

This is the primary software library for working with the data model. It is currently in a preliminary stage. We aim to implement the full model and most extensions.

## Installation

Add `stam` to your project's `Cargo.toml`:

``$ cargo add stam``

## Usage

Import the library

```rust
use stam;
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

You can retrieve items by methods that are similarly named to the return type:

```rust
let annotation =   store.annotation("my-annotation");
let resource = store.resource("my-resource");
let annotationset: &stam::AnnotationDataSet = store.annotationset("my-annotationset");
let key = annotationset.key("my-key");
let data = annotationset.annotationdata("my-data");
```


All of these methods return an `Option<ResultItem<T>>`, where `T` is a STAM
type like `Annotation`, `TextResource`,`AnnotationDataSet`, `DataKey` or
`TextSelection`. If the item was not found, due to an invalid ID for instance,
the `Option<>` has the value `None`.

The `ResultItem<T>` type holds *a reference* to T,
with a lifetime equal to the store, it also holds a reference to the store
itself. You can call `as_ref()` on all `ResultItem<T>` instances to a direct reference with a lifetime equal to the store.

### Adding items

Add a resource to an existing store:

```rust
let resource_handle = store.add( stam::TextResource::from_file("my-text.txt", store.config()) )?;
```

A similar pattern works for `AnnotationDataSet`:

```rust
let annotationset_handle = store.add( stam::AnnotationDataSet::from_file("myset.json", store.config()) )?;
```

The `add` methods adds the items directly, which means they have to have been constructed already. 
Many STAM data structures, however, have an associated builder type and are not
instantiated directly. We use `annotate()` rather than `add()` to add annotations to an existing store:

```rust
let annotation_handle = store.annotate( stam::AnnotationBuilder::new()
           .with_target( SelectorBuilder::TextSelector("testres", stam::Offset::simple(6,11))) 
           .with_data("testdataset", "pos", "noun") 
)?;
```

*Here we see a `Builder` type that uses a builder pattern to construct
instances of their associated types. The actual instances will be built by the
underlying store.

Structures like `AnnotationDataSets` and `TextResource` also have builders, you
can use them with `add()` by invoking the `build()` method on the builder to
produce the final type:

```rust
let annotationset_handle = store.add(
                   stam::AnnotationDataSetBuilder::new().with_id("testdataset"))
                                                 .with_data_with_id("pos", "noun", "D1").build()?)?;
```

Let's now create a store and annotations from scratch, with an explicitly filled `AnnotationDataSet`:

```rust
let store = stam::AnnotationStore::new(stam::Config::default())
    .with_id("test")
    .add( stam::TextResource::from_string("testres", "Hello world"))?
    .add( stam::AnnotationDataSet::new().with_id("testdataset")
           .add( stam::DataKey::new("pos"))?
           .with_data_with_id("pos", "noun", "D1")?
    )?
    .with_annotation( stam::Annotation::builder() 
            .with_id("A1")
            .with_target( stam::SelectorBuilder::textselector("testres", stam::Offset::simple(6,11))) 
            .with_existing_data("testdataset", "D1") )?;
```

And here is the very same thing but the `AnnotationDataSet` is filled implicitly here:

```rust
let store = stam::AnnotationStore::default().with_id("test")
    .add( stam::TextResource::from_string("testres".to_string(),"Hello world"))?
    .add( stam::AnnotationDataSet::new().with_id("testdataset"))?
    .with_annotation( stam::AnnotationBuilder::new()
            .with_id("A1")
            .with_target( stam::SelectorBuilder::textselector("testres", stam::Offset::simple(6,11))) 
            .with_data_with_id("testdataset","pos","noun","D1")
    )?;
```


The implementation will ensure to reuse any already existing `AnnotationData` if possible, as not duplicating data is one of the core characteristics of the STAM model.

There is also an `AnnotationStoreBuilder` you can use with implements the builder pattern for the annotation store as a whole.

### Serialisation to file

You can serialize the entire annotation store (including all sets and annotations) to a [STAM JSON](https://github.com/annotation/stam#stam-json) file:

```rust
store.to_file("example.stam.json")?;
```

Or to a [STAM CSV](https://github.com/annotation/stam/tree/master/extensions/stam-csv) file (this will actually create separate derived CSV files for sets and and annotations):

```rust
store.to_file("example.stam.csv")?;
```

### Iterators

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
iterators in turn all return `ResultItem<T>` instances. The table is divided into two parts,
the top part simple methods that follows STAM's ownership model. Those in the bottom part leverage the various *reverse indices*
that are computed:


| Method                               |  T                    | Description                         |
| -------------------------------------| ----------------------| ------------------------------------|
| `AnnotationStore.annotations()`      | `Annotation`          | all annotations in the store        | 
| `AnnotationStore.resources()`        | `TextResource`        | all resources in the store          |
| `AnnotationStore.annotationsets()`   | `AnnotationDataSet`   | all annotation sets in the store    |
| `AnnotationDataSet.keys()`           | `DataKey`             | all keys in the set                 |
| `AnnotationDataSet.data()`           | `AnnotationData`      | all data in the set                 |
| `Annotation.data()`                  | `AnnotationData`      | the data pertaining to the annotation |
| -------------------------------------|-----------------------|-------------------------------------|
| `TextResource.textselections()`      | `TextSelection`       | all *known* text selections in the resource (1) |
| `TextResource.annotations()`         | `Annotation`          | Annotations referencing this text using a `TextSelector` or `AnnotationSelector` |
| `TextResource.annotations_metadata()`| `Annotation`          | Annotations referencing the resource via a `ResourceSelector` |
| `AnnotationDataSet.annotations()`         | `Annotation`          | All annotations making use of this set |
| `AnnotationDataSet.annotations_metadata()`| `Annotation`          | Annotations referencing the set via a `DataSetSelector` |
| `Annotation.annotations()`           | `Annotation`          | Annotations pointed at via a `AnnotationSelector` |
| `Annotation.annotations_reverse()`   | `Annotation`          | The reverse of the above (annotations that point back) |
| `Annotation.textselections()`        | `Annotation`          | Targeted text selections (via `TextSelector` or `AnnotationSelector`) |
| `AnnotationData.annotations()`       | `Annotation`          | All annotations that uses of this data  |
| `DataKey.data()`                     | `AnnotationData`      | All annotation data that uses this key |
| `TextSelection.annotations()`        | `Annotation`          | All annotations that target this text selection |
| -------------------------------------|-----------------------|-------------------------------------|

Notes:
* (1) With *known* text selections, we refer to portions of the texts that have been referenced by an annotation.
* Most of the methods in the left column, second part of the table, are implemented only for `WrappedItem<T>`, not `&T`.
* This library consistently uses iterators and therefore *lazy evaluation*.
  This is more efficient and less memory intensive because you don't need to
  wait for all results to be collected (and heap allocated) before you can do computation.

#### Iterators (advanced, low-level)

*(feel free to skip this subsection on a first reading!)*

Whereas all iterators in the above table produce a `WrappedItem<T>`, there are lower level methods available which only return handles.
We are not going to list them here, but they can be recognized by the keyword `_by_` in the method name, like `annotations_by_data()`.

### Searching

There are several methods available, on various objects, to enable searching. These all start with the prefix `find_`, and like those in the previous section, they return iterators producing `WrappedItem<T>` items.


| Method                               |  T                    | Description                         |
| -------------------------------------| ----------------------| ------------------------------------|
| `TextResource.find_text()`           | `TextSelection`       | Finds a particular substring in the resource's text. | 
| `TextSelection.find_text()`          | `TextSelection`       | Finds a particular substring within the specified text selection. |
| `TextResource.find_text_regex()`     | `TextSelection`       | Idem, but as powerful regular expressed based search. | 
| `TextSelection.find_text_regex()`    | `TextSelection`       | Idem, but as powerful regular expressed based search. |
| `TextSelection.find_textselections()` | `TextSelection`      | Finds text selections that are in a specific relation with this text selection, the relation is expressed via a `TextSelectionOperator`.
| `TextSelectionSet.find_textselections()`| `TextSelection`    | Finds text selections that are in a specific relation with these text selections, the relation is expressed via a `TextSelectionOperator`.
| `TextSelection.find_annotations()`   | `Annotation`          | Finds other annotations via a relationship that holds between its text selections and this text selection
| `Annotation.find_textselections()`   | `TextSelection`       | Finds other that selections that are in a specific relations with the text selections pertaining to the annotation.
| `Annotation.find_annotations()`      | `Annotation`          | Finds other annotations via a relationship that holds between the respective text selections.
| `TextSelectionSet.find_annotations()`| `Annotation`          | Finds annotations that are in a specific relation with these text selections.
| `AnnotationDataSet.find_data()`      | `AnnotationData`      | Finds `AnnotationData` in a set, matching the search criteria.
| `Annotation.find_data()`             | `AnnotationData`      | Finds `AnnotationData` in an annotation, matching the search criteria.
| -------------------------------------|-----------------------|-------------------------------------|

Many of these methods take a `TextSelectionOperator` as parameter, this expresses a relation between two text selections (or two sets of text selections). This library defines the following enum variants for `TextSelectionOperator`:

* `Equals` - Both sets occupy cover the exact same TextSelections, and all are covered (cf. textfabric's `==`), commutative, transitive
* `Overlaps` - Each TextSelection in A overlaps with a TextSelection in B (cf. textfabric's `&&`), commutative
* `Embeds` - All TextSelections in B are embedded by a TextSelection in A (cf. textfabric's `[[`)
* `Embedded` - All TextSelections in A are embedded by a TextSelection in B (cf. textfabric's `]]`)
* `Before` - Each TextSelection in A comes before a textselection in B  (cf. textfabric's `<<`)
* `After` - Each TextSelection In A comes after a textselection in B (cf. textfabric's `>>`)
* `Precedes` - Each TextSelection in A is precedes B; it ends where at least one TextSelection in B begins.
* `Succeeds` - Each TextSelection in A is succeeds B; it begins where at least one TextSelection in A ends.
* `SameBegin` - Each TextSelection in A starts where a TextSelection in B starts
* `SameEnd` - Each TextSelection in A starts where a TextSelection in B ends

There are some modifiers you can set for each operator

* `all` (bool) - If this is set, then for each `TextSelection` in A, the relationship must hold with **ALL** of the text selections in B. The normal behaviour, when this is set to false, is a match with any item suffices (and may be returned).
* `negate` (bool) - Inverses the operator (negation).


## API Reference Documentation

See [here](https://docs.rs/stam)

## Extensions

This library implements the following STAM extensions:

* [STAM-CSV](https://github.com/annotation/stam/tree/master/extensions/stam-csv) - Defines an alternative serialisation format using CSV.

## Python binding

This library comes with a binding for Python, see [here](https://github.com/annotation/stam-python)

## Acknowledgements

This work is conducted at the [KNAW Humanities Cluster](https://huc.knaw.nl/)'s [Digital Infrastructure department](https://di.huc.knaw.nl/), and funded by the [CLARIAH](https://clariah.nl) project (CLARIAH-PLUS, NWO grant 184.034.023) as part of the FAIR Annotations track.
