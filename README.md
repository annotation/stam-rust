[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

# STAM Library

[STAM](https:/github.com/annotation/stam) is a data model for stand-off text annotation and described in detail [here](https://github.com/annotation/stam). This is a software library to work with the model, written in Rust.

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
    let store = stam::AnnotationStore::from_file("example.stam.json")?;
    ...
}
```

*We assume some kind of function returning `Result<_,stam::StamError>` for all examples in this section.*

The annotation store is your workspace, it holds all resources, annotation sets (i.e. keys and annotation data) and of course the actual annotations. It is a memory-based store and you can as much as you like into it (as long as it fits in memory:).

Retrieving anything by ID:

```rust
let annotation: &stam::Annotation = store.get_by_id("my-annotation")?;
let resource: &stam::TextResource = store.get_by_id("my-resource")?;
let annotationset: &stam::AnnotationDataSet = store.get_by_id("my-annotationset")?;
let key: &stam::DataKey = annotationset.get_by_id("my-key")?;
let data: &stam::AnnotationData = annotationset.get_by_id("my-data")?;
```

*Note it is important to specify the return type, as that's how the compiler can infer what you want to get.
The methods are provided by the `ForStore<T>` trait.)*

Iterating through all annotations in the store, and outputting a simple tab separated format:

```rust
for annotation in store.annotations() {
    let id = annotation.id().unwrap_or("");
    for (key, data, dataset) in store.data(annotation) {
        // get the text to which this annotation refers (if any)
        let text: &str = match annotation.target().kind() {
            stam::SelectorKind::TextSelector => {
                store.select(annotation.target())?
            },
            _ => "",
        };
        print!("{}\t{}\t{}\t{}", id, key.id().unwrap(), data.value(), text);
    }
}
```

Add resources:

```rust
let resource_handle = store.insert( stam::TextResource::from_file("my-text.txt") )?;
```

Many methods return a so called *handle* instead of a reference. You can use this handle to obtain a reference as shown in the next example, in which we obtain a reference to the resource we just inserted:

```rust
let resource: &stam::Resource = store.get(resource_handle)?;
```

Retrieving items by handle is much faster than retrieval by public ID, as handles encapsulate an internal numeric ID. Passing around handles is also cheap and sometimes easier than passing around references, as it avoids borrowing issues.


Add annotations:

```rust
let annotation_handle = store.annotate( stam::Annotation::builder()
           .target_text( "testres".into(), stam::Offset::simple(6,11)) 
           .with_data("testdataset".into(), "pos".into(), stam::DataValue::String("noun".to_string())) 
)?;
```

*Here we see some `Builder` types that are use a builder pattern to construct instances of their respective types. The actual instances will be built by the underlying store. You can note the heavy use of `into()` to coerce the parameters to the right type. Rather than pass string parameters referring to public IDs, you may just as well pass and coerce (again with `into()`) references like `&Annotation`, `&AnnotationDataSet`, `&DataKey` or handles. We call the type of these parameters `AnyId<T>` and you will encounter them in more places.*


Create a store and annotations from scratch, with an explicitly filled `AnnotationDataSet`:


```rust
let store = stam::AnnotationStore::new().with_id("test".into())
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
    .with_annotation( stam::Annotation::builder()
            .with_id("A1".into())
            .target_text( "testres".into(), stam::Offset::simple(6,11)) 
            .with_data_with_id("testdataset".into(),"pos".into(),"noun".into(),"D1".into())
    )?;
```

The implementation will ensure to reuse any already existing `AnnotationData` if possible, as not duplicating data is one of the core characteristics of the STAM model.

You can serialize the entire annotation store (including all sets and annotations) to a STAM JSON file:

```rust
store.to_file("example.stam.json")?;
```


## API Reference Documentation

See [here](https://docs.rs/stam)

## Python binding

This library comes with binding for Python, see [here](https://github.com/annotation/stam-rust/tree/master/bindings/python)
