[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

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
let annotation =   store.annotation(&stam::Item::from("my-annotation"));
let resource = store.resource(&stam::Item::from("my-resource"));
let annotationset: &stam::AnnotationDataSet = store.annotationset(&stam::Item::from("my-annotationset"));
let key = annotationset.key(&stam::Item::from("my-key"));
let data = annotationset.annotationdata(&stam::Item::from("my-data"));
```

In our request to various methods in the STAM model, we use the `stam::Item<T>`
type. It abstracts over various ways by which an item can be requested, such as
by ID (either owned or borrowed), by handle, or by reference. The following are
all equivalent:

```rust
let annotation = store.annotation(&Item::from("my-annotation"));
let annotation = store.annotation(&"my-annotation".into());
let annotation = store.annotation(&Item::IdRef("my-annotation"));
```

All of these methods return an `Option<WrappedItem<T>>`, where `T` is a STAM
type like `Annotation`, `TextResource`,`AnnotationDataSet`, `DataKey` or
`TextSelection`. If the item was not found, due to an invalid ID for instance,
the `Option<>` has the value `None`.

Whereas we use `Item<T>` to *supply parameters*, `WrappedItem<T>` is a *return
type*. It is never the other way around. The latter holds, in most cases, *a reference* to T,
with a lifetime equal to the store, it also holds a reference to the store
itself.

**Advanced:** *You can call `unwrap()` on almost all `WrappedItem<T>` instances you obtain to return
a direct reference with a lifetime equal to the store. WrappedItem also implements `Deref` so you
can transparently access the underlying reference (albeit with a more limited lifetime!).*

#### Retrieving items (advanced, low-level)

*(feel free to skip this subsection on a first reading!)*

For lower-level, access we can use the `get()` method on  a store to retrieve
any item in the STAM model from the store, such as by public ID. It will return
a directly return a reference.

```rust
let annotation: &stam::Annotation = store.get(&stam::Item::from("my-annotation"))?;
let resource: &stam::TextResource = store.get(&stam::Item::from("my-resource"))?;
let annotationset: &stam::AnnotationDataSet = store.get(&stam::Item::from("my-annotationset"))?;
let key: &stam::DataKey = annotationset.get(&stam::Item::from("my-key"))?;
let data: &stam::AnnotationData = annotationset.get(&stam::Item::from("my-data"))?;
```

The directly returned reference is not as potent as `WrappedItem<T>`, as the
latter implements more higher-level methods. You can turn a reference `&T` to a `WrappedItem<T>` yourself though:

```rust
let annotation = store.wrap(annotation);
let resource = store.wrap(resource);
let annotationset = store.wrap(annotationset);
let key = annotationset.wrap(key);
let data = annotationset.wrap(data);
```

Low-level methods often return a so called *handle* instead of a reference. You
can use this handle to obtain a reference as shown in the next example, in
which we obtain a reference to the resource we just inserted. The following are
all equivalent:

```rust
let annotation: &stam::Annotation = store.get(&Item::from(handle))?;
let annotation: &stam::Annotation = store.get(&handle.into())?;
let annotation: &stam::Annotation = store.get(&Item::Handle(handle))?;
```

Retrieving items by handle is much faster than retrieval by public ID, as
handles encapsulate an internal numeric ID. Passing around handles is also
cheap and sometimes easier than passing around references, as it avoids
borrowing issues.

The ``get()`` method returns a `Result<&T, StamError>`. When using `get()` it is 
is important to specify the return type, as that's how the compiler can infer what you want to get.
(these methods are provided by the `ForStore<T>` trait.).

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
           .with_target( SelectorBuilder::TextSelector("testres".into(), stam::Offset::simple(6,11))) 
           .with_data("testdataset".into(), "pos".into(), stam::DataValue::String("noun".to_string())) 
)?;
```

*Here we see a `Builder` type that uses a builder pattern to construct
instances of their associated types. The actual instances will be built by the
underlying store. You can note the heavy use of `into()` to coerce the
parameters to the right type (via `Item<T>`, which you already encountered in
an earlier section). Rather than pass string parameters referring to public
IDs, you may just as well pass and coerce (again with `into()`) references like
`&Annotation`, `&AnnotationDataSet`, `&DataKey` or handles.*


Structures like `AnnotationDataSets` and `TextResource` also have builders, you
can use them with `add()` by invoking the `build()` method on the builder to
produce the final type:

```rust
let annotationset_handle = store.add(
                   stam::AnnotationDataSetBuilder::new().with_id("testdataset".into())
                                                 .with_key( stam::DataKey::new("pos".into()))?
                                                 .with_data("D1".into(), "pos".into() , "noun".into()).build()?)?;
```

Let's now create a store and annotations from scratch, with an explicitly filled `AnnotationDataSet`:

```rust
let store = stam::AnnotationStore::new()
    .with_config(stam::Config::default())
    .with_id("test".into())
    .add( stam::TextResource::from_string("testres".into(), "Hello world".into()))?
    .add( stam::AnnotationDataSet::new().with_id("testdataset".into())
           .add( stam::DataKey::new("pos".into()))?
           .with_data("D1".into(), "pos".into() , "noun".into())?
    )?
    .with_annotation( stam::Annotation::builder() 
            .with_id("A1".into())
            .target_text( "testres".into(), stam::Offset::simple(6,11)) 
            .with_data_by_id("testdataset".into(), "D1".into()) )?;
```

And here is the very same thing but the `AnnotationDataSet` is filled implicitly here:

```rust
let store = stam::AnnotationStore::new().with_id("test".into())
    .add( stam::TextResource::from_string("testres".to_string(),"Hello world".into()))?
    .add( stam::AnnotationDataSet::new().with_id("testdataset".into()))?
    .with_annotation( stam::AnnotationBuilder::new()
            .with_id("A1".into())
            .target_text( "testres".into(), stam::Offset::simple(6,11)) 
            .with_data_with_id("testdataset".into(),"pos".into(),"noun".into(),"D1".into())
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
iterators in turn all return `WrappedItem<T>` instances. The table is divided into two parts,
the top part follows STAM's ownership model. Those in the bottom part leverage the various *reverse indices*
that are computed:


| Method                               |  T                    | Description                         |
| -------------------------------------| ----------------------| ------------------------------------|
| `AnnotationStore.annotations()`      | `Annotation`          | all annotations in the store        | 
| `AnnotationStore.resources()`        | `TextResource`        | all resources in the store          |
| `AnnotationStore.annotationsets()`   | `AnnotationDataSet`   | all annotation sets in the store    |
| `AnnotationDataSet.keys()`           | `DataKey`             | all keys in the set                 |
| `AnnotationDataSet.data()`           | `AnnotationData`      | all data in the set                 |
| `Annotation.data()`                  | `AnnotationData`      | the data for the annotation         |
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
| `AnnotationDataSet.find_data()`      | `AnnotationData`      | Returns AnnotationData matching the search criteria, this is not an iterator but returns a single instance only
| -------------------------------------|-----------------------|-------------------------------------|

Many of these methods take a `TextSelectionOperator` as parameter, this expresses a relation between two text selections (or two sets of text selections). This library defines the following enum variants for `TextSelectionOperator`:

* `Equals` - Both sets occupy cover the exact same TextSelections, and all are covered (cf. textfabric's `==`), commutative, transitive
* `Overlaps` - Each TextSelection in A overlaps with a TextSelection in B (cf. textfabric's `&&`), commutative
* `Embeds` - All TextSelections in B are embedded by a TextSelection in A (cf. textfabric's `[[`)
* `Embedded` - All TextSelections in A are embedded by a TextSelection in B (cf. textfabric's `]]`)
* `Precedes` - Each TextSelection in A precedes (comes before) a textselection in B  (cf. textfabric's `<<`)
* `Succeeds` - Each TextSelection In A succeeds (comes after) a textselection in B (cf. textfabric's `>>`)
* `LeftAdjacent` - Each TextSelection in A is ends where at least one TextSelection in B begins.
* `RightAdjacent` - Each TextSelection in A is begis where at least one TextSelection in A ends.
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
