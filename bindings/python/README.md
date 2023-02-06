[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

# STAM Python binding

[STAM](https:/github.com/annotation/stam) is a data model for stand-off text annotation and described in detail [here](https://github.com/annotation/stam). This is a python library (to be more specific; a python binding written in Rust) to work with the model.

This library offers a higher-level interface than the underlying Rust library. Implementation is currently in a preliminary stage. We aim to implement the full model and most extensions.

## Installation

``$ pip install stam``

## Usage

Import the library

```rust
import stam
```

Loading a STAM JSON file containing an annotation store:

```python
store = stam.AnnotationStore(file="example.stam.json")
```

The annotation store is your workspace, it holds all resources, annotation sets (i.e. keys and annotation data) and of course the actual annotations. It is a memory-based store and you can as much as you like into it (as long as it fits in memory:).

Retrieving anything by ID:

```python
annotation = store.annotation("my-annotation")
resource = store.resource("my-resource")
annotationset = store.annotationset("my-annotationset")
key = annotationset.key("my-key")
data = = annotationset.annotationdata("my-data")
```

Iterating through all annotations in the store, and outputting a simple tab separated format:

```python
for annotation in store.annotations():
    # get the text to which this annotation refers (if any)
    try:
        text = str(annotation)
    except stam.StamError:
        text = "n/a"
    for data in annotation:
        print("\t".join(( annotation.id, data.key().id(), str(data.value()), text)));
```




