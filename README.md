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
}
```

Iterating through all annotations in the store, and outputting a simple tab separated format:

```rust
for annotation in store.annotations() {
    let id = store.annotation.id().unwrap_or("");
    for key, data, dataset in annotation.data() {
        // get the text to which this annotation refers (if any)
        let text = match annotation.target.type() {
            stam::SelectorType::TextSelector => {
                store.select(annotation.target)?
            },
            _ => "",
        };
        print!("{}\t{}\t{}\t{}", id, key.id().unwrap(), data.value(), text);
    }
}
```


## API Reference Documentation

See [here](https://docs.rs/stam)


