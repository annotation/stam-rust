[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

# STAM Library

[STAM](https:/github.com/annotation/stam) is a data model for stand-off text annotation and described in detail [here](https://github.com/annotation/stam). This is a sofware library to work with the model, written in Rust.

This is the primary software library for working with the data model. It is currently in a preliminary stage. We aim to implement the full model and most extensions.

## Installation

``$ cargo install stam``

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

Retrieving anything by ID:

```rust
let annotation: &stam::Annotation = store.get_by_id("my-annotation");
let resource: &stam::TextResource = store.get_by_id("my-resource");
let annotationset: &stam::AnnotationDataSet = store.get_by_id("my-annotationset");
let key: &stam::DataKey = annotationset.get_by_id("my-key");
let data: &stam::AnnotationData = annotationset.get_by_id("my-data");
```



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

Add annotations:


```rust
store.annotate( AnnotationBuilder::new()
           .with_target( SelectorBuilder::TextSelector( "testres".into(), Offset::simple(0,5) ) )
           .with_data("testdataset".into(),"pos".into(), DataValue::String("noun".to_string())) 
 )?;
```


Create annotations from scratch:


## API Reference Documentation

See [here](https://docs.rs/stam)


