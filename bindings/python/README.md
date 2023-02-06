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

The annotation store is your workspace, it holds all resources, annotation sets
(i.e. keys and annotation data) and of course the actual annotations. It is a
memory-based store and you can as much as you like into it (as long as it fits
in memory:).

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
        print("\t".join(( annotation.id, data.key().id, str(data.value()), text)));
```


Adding a resource:

```python
resource = store.add_resource(filename="my-text.txt")
```

Create a store and annotations from scratch:

```python
from stam import AnnotationStore, Selector, AnnotationDataBuilder

store = AnnotationStore(id="test")
resource = store.add_resource(id="testres", text="Hello world")
store.annotate(id="A1", 
                target=Selector.text(resource, Offset.simple(6,11)),
                data=[AnnotationDataBuilder(id="D1", key="pos", value="noun", annotationset="testdataset")])
```

In the above example, the `AnnotationDataSet` , `DataKey` and `AnnotationData`
are created on-the-fly. You can also create them explicitly, as shown in the
next snippet, results in the exact same store:


```python
store = AnnotationStore(id="test")
resource = store.add_resource(id="testres", text="Hello world")
annotationset = store.add_annotationset(id="testdataset")
annotationset.add_key("pos")
data = annotationset.add_data("pos","noun","D1")
self.store.annotate(id="A1", 
    target=Selector.text(resource, Offset.simple(6,11)),
    data=[AnnotationDataBuilder.link(data)])
```

Here we use `AnnotationDataBuilder.link()` to link to the existing annotation.
Providing the full `AnnotationDataBuilder` as in the example before would have
also worked fine with the same end result, but would be less performant. The
implementation will ensure to reuse any already existing `AnnotationData` if
possible, as not duplicating data is one of the core characteristics of the
STAM model.

You can serialize the entire annotation store (including all sets and annotations) to a STAM JSON file:

```python
store.to_file("example.stam.json")
```

## Differences between the rust library and python library and performance considerations

Although this Python binding builds on the Rust library, the API it exposes
differs in certain aspects to make it more pythonic and easier to work with.
This results in a higher-level API that hides some of the lower-level details
that are present in the Rust library. This approach does come at the cost of causing
some additional runtime overhead. 

In this Python binding, most classes of the model (`Annotation`,
`AnnotationData`, `DataKey`, etc..) are references to the annotation store
(self-containing also a reference to the store itself). None of them can be
instantiated directly, but always via an `add_*()` or `annotate()` method which
will add them and return the reference. 

These instances play a bigger role in the Python API than their equivalents in
the Rust API (which distinguishes owned data, borrowed data aka references and
so-called handles). In the Rust API, methods for search are mostly implemented on the main
`AnnotationStore` or `AnnotationDataSet`, reflecting the underlying ownership model more strictly.
In the Python API, they are implemented on the types themselves. Here's a comparison of some common methods:

+----------------------------------+-----------------------------------------------------+
| Python API                       | Rust API                                            |
+----------------------------------+-----------------------------------------------------+
| `Annotation.annotations()`       | `AnnotationStore::annotations_by_annotation()`      |
| `Annotation.resources()`         | `AnnotationStore::resources_by_annotation()`        |
| `Annotation.textselections()`    | `AnnotationStore::textselections_by_annotation()`   |
| `Annotation.text()`              | `AnnotationStore::text_by_annotation()`             |
| `TextResource.annotations()`     | `AnnotationStore::annotations_by_resource()`        |
| `TextSelection.annotations()`    | `AnnotationStore::annotations_by_textselection()`   |
| `DataKey.data()`                 | `AnnotationDataSet::data_by_key()`                  |
| `DataKey.annotationset()`        | n/a                                                 |
| `AnnotationData.annotationset()` | n/a                                                 |
+--------------------------------+-------------------------------------------------------+

The Rust methods will return iterators, references or handles whenever they
can, moreover it will do so safely. The Python API is often forced to make a
local copy. For iterators it sometimes decides to let the entire underlying Rust
iterator run its course and then return the result as a whole as a tuple, rather than
return a Python generator. Here you gain some speed at the cost of some memory.

Probably needless to say, but using Rust directly will always be more
performant than using this Python binding. However, using this Python binding
should still be way more performant than if the whole thing were implemented in
native Python. The trick is in letting the binding work for you as much as
possible, use higher-level methods whenever they are available rather than
implementing your logic in Python.













