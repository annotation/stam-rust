# v0.18.6 - 2025-11-10

* translate: fix in finding all embedded reference texts


# v0.18.5 - 2025-10-08

Previous release was premature, this minor release downgrades another dependency (minicbor) so we don't require edition2024 yet and also run on older rust compilers.


# v0.18.4 - 2025-10-08

This minor release downgrades a dependency (base16ct) so we don't require edition2024 yet and also run on older rust compilers.


# v0.18.3 - 2025-09-27

Fixes:

* webanno: fix in serialising multiple namespace prefixes
* webanno: when a value looks like an IRI where the base corresponds to one of the extra contexts, then strip this prefix and reduce the IRI value to an alias

# v0.18.2 - 2025-09-21

Various fixes and improvements

* webanno: properly translate multiple identical keys to a list
* webanno: allow multiple extra target templates
* webanno: added {resource_iri} variable for extra targets, and {resource} now yields only the ID
* fixed parsing IdStrategy
* implemented textselection_count() function on ResultItem<Annotation>
* translate/transpose: big performance improvement by pre-computing some things
* Added AnnotationStore.reannotate() method to modify existing annotations

# v0.18.1 - 2025-09-12

* fix: extra guards against empty textselectionsets
* fix: translate() and transpose() now return StamError::NoText() if called on annotations that have no text
* new: implemented deserialisation of IdStrategy from String

# v0.18.0 - 2025-09-08

This release implements STAM v1.3.0

## New

* [STAM translate](https://github.com/annotation/stam/tree/master/extensions/stam-translate): updated for changes in this extension - Added a `translate()` function

## Other

* Dependency updates

# v0.17.0 - 2025-07-18

This release implements STAM v1.2.

## New

* core: Added a `DataValue::Map`` type, supporting arbitrary nested maps
* core: Added `DataOperator::GetKey`` to access keys in maps
* query: Added `.` operator in to access keys in maps
* web annotations: implemented conversion of maps
* web annotations: allow specifying JSON-LD contexts and using STAM keys as aliases.
* web annotations: added `skip_context` parameter to skip serialisation of JSON-LD contexts, useful in situations where it is already done in a prior stage.

## Bugfixes

* web annotations: prevent duplicate anno.jsonld in context and map rdfs:type properly without full IRI

## Other 

* extra tests
* dependency updates

# v0.16.7 - 2025-07-08

## Minor update

* implemented From<(isize,isize)> for Offset,  facilitates quick conversion from tuples
* and added TryFrom<&str> for Offset, The latter parses a string to an offset. The string is in the form
  start:end or start,end where start and end are (possibly signed)
  integers. Either may also be omitted.   

# v0.16.6 - 2025-05-28

## New

* improved error feedback when IDs in queries can not be found,  this introduces the or_fail_for_id(id) method to be used instead of

## Bugfixes

* improved error feedback when IDs in queries can not be found
* allow OPTIONAL subqueries to fail with not found errors (instead of crashing the query engine)

## Updates

* most dependencies were updated to their latest versions

# v0.16.5 - 2024-11-18

* Minor API change in some `to_json_string()` methods  (no config parameter needed)
* Added `to_json_value()` method alongside `to_json_string()` (returns serde_json::Value)
* Fix in JSON serialisation of DataValue:null
* Implemented `to_json_string()`, `to_json_value()` and `text()` for QueryResultItem
* Minor dependency upgrades
* Added `extra_target_template` parameter in `WebAnnoConfig` for web annotation serialisation. This adds an extra target during serialisation of web annotations that resolves the TextPositionSelectors to a single IRI/URI (according to the template).


# v0.16.4 - 2024-10-18

Bugfix release and minor API change:

* implemented STAMQL serialisation of collection constraints that were not serialisable before
* DataOperator::Equals now takes Cow<'a, str> instead of &'a str, so it can be owned or borrowed: If in your code you use `DataOperator::Equals("blah")` you will now have to change it to `DataOperator::Equals("blah".into())`

# v0.16.3 - 2024-10-04

Bugfix release:

* query: added a missing constraint
* query: do not propagate NotFoundError and (new) VariableNotFoundError in UNION

# v0.16.2 - 2024-09-22

Dependency downgrade of minicbor so things keep running on slightly older rust versions

# v0.16.1 - 2024-09-22

Bugfix release:
* fix for annotations_no_substores() and similar functions


# v0.16.0 - 2024-09-22

* Breaking API changes:
     * `TextResourceBuilder` was simplified, the `build()` method is no longer public
     * `AnnotationStore.add()` got renamed to `AnnotationStore.with_item()` but is a lower-level function that should be used less.
     * Use the new `AnnotationStore.add_resource()` and `AnnotationStore.add_dataset()` instead (or the variants `with_resource()` and 
`with_dataset()`).
     * `@include` behaviour is now more strictly defined (STAM v1.1.1). Relative files are always interpreted relative to the working directory (either an explicitly set working directory in the configuration, or the actual system's cwd) ([annotation/stam#31](https://github.com/annotation/stam/issues/31))
* API changes
    * Expanded substore API, added `AnnotationStore.annotations_no_substores()`, `Annotation.substore()`, `TextResource.substores()`, `AnnotationDataSets.substores()`
    * Added `AnnotationDataSetBuilder`, to be used with `AnnotationStore.add_dataset()`
    * query: Added support for substores as contraints (`SUBSTORE` keyword).
* Fixes for [annotation/stam#31](https://github.com/annotation/stam/issues/31)
    * Fixed relative path handling (`@include mechanism`)
    * Deserialisation fixes for `@include` mechanism.
    * Substores can have multiple parents

# v0.15.0 - 2024-08-29

* Query 
    * Removed QueryNames, no longer needed
    * Allow multiple subqueries ([annotation/stam#28](https://github.com/annotation/stam/issues/28))
         * `subquery()` is now `subqueries()` (iterator).
    * Implemented support for custom attributes on queries and constraints (used e.g. by stam view in stam-tools)
    * Allow DATASET as primary constraint for KEY result type
    * Implemented OPTIONAL qualifier for SELECT subqueries
    * Added `Query.bind_from_result()` method to bind variables from a previous result
* DataValue/DataOperator: fixes in parsing disjunctions
* DataValue/DataOperator: allow matching some distinct types if the reference is a string
* Implemented substores; allows including other (stand-off) annotation stores as dependencies, keeping STAM JSON serialisations apart ([annotation/stam#29](https://github.com/annotation/stam/issues/29))


# v0.14.2 - 2024-07-15

fix in STAM JSON serialisation of resources and annotation data sets after deletions

# v0.14.1 - 2024-05-27

* Fixes a bug in STAMQL query parser

# v0.14.0 - 2024-05-26

New features:
* Implemented text validation extension [#5](https://github.com/annotation/stam-rust/issues/5)
* Support LIMIT keyword in query language (annotation/stam#25)

Fixes:
* Preceeds and Succeeds by default allow whitespace in between now ([#17](https://github.com/annotation/stam-rust/issues/17))

API improvements:
* added methods value() and value_as_str() on iterators over AnnotationData


# v0.13.0 - 2024-05-14

New features:

* Implemented deletion [#3](https://github.com/annotation/stam-rust/issues/3)
* Updates to the query language (STAMQL) and engine:
    * New `ADD` and `DELETE` statements in STAMQL queries for editing data. Added a method `AnnotationStore.query_mut()` to run such queries that require mutability.
    * New `OFFSET`modifier for `RESOURCE` and `ANNOTATION` result types in STAMQL.

Minor API improvements:

* Added conversion of Cursor to usize and isize
* Implemented conversion from std::io::Error for StamError

Miscellaneous:

* Refactored contextvars implementation in query engine
* Updated dependencies

Bugfixes:

* Fixed writing stand-off files
* Added missing implementations in resolving DataKeySelector, AnnotationDataSelector
* In segmentation(), do not include seegments that have no annotations (e.g. milestones)


# v0.12.0 - 2024-03-28

Fairly minor update release:

New features:

* Added a `segmentation()` method for ResultItem<TextResource> and ResultTextSelection, providing all non-overlapping text segments that together fully cover the resource/text selection
* Generate body IDs in Web Annotation output

Fixes:

* Query: Added DATASET primary constraint for ANNOTATION result type

# v0.11.0 - 2024-03-15

New features:
* Implemented [STAM Transpose extension](https://github.com/annotation/stam/blob/master/extensions/stam-transpose/README.md): a `transpose()` function is now available on Annotation and TextSelectionSets [#28](https://github.com/annotation/stam-rust/issues/28) 
* Implemented UNION constraint in the STAM Query Language [#26](https://github.com/annotation/stam-rust/issues/26)
* Exposed functions for ID (re)generation (`IdStrategy`, `generate_id()`, `regenerate_id()`)
* Implemented `TextSelection.intersection()`

Minor API improvements:
* Implemented missing high-level iterator for AnnotationDataSet
* added `Offset.len()`, `Cursor.shift()` and `Offset.shift()`
* added `ResultItem<Annotation>.textselectionsets()`, `ResultItem<Annotation>.textselectionset_in()`
* constrained the characters used in randomly generated IDs to include only alphanumeric ones
* better error feedback in querying
* extra inspection methods for AnnotationBuilder and SelectionBuilder

Bugfixes:
* Fixed serialisation of webannotations and added supported for internal range selectors
* Fixes for merging multiple annotation stores
* Implemented several missing constraint handler for the query language

There was also some refactoring and dependency upgrades.

# v0.10.1 - 2024-02-22

New features:

* Implemented DataValue::Datetime ([#27](https://github.com/annotation/stam-rust/issues/27)), which was in the official specification but not yet implemented until now
* Implemented support for W3C Web Annotation export (JSON-LD) of Annotations ([#4](https://github.com/annotation/stam-rust/issues/4))
* Implemented STAM JSON serialisation for TextSelection (even though it doesn't appear in a normal STAM JSON output)

Minor API improvements:

* Implemented `TextResource.to_json()`
* Implemented `TextSelection.absolute_offset()`
* Implemented a better API to get a textselection of a whole resource, e.g. using From trait
* Implemented `QueryResultItems.get_by_name_or_first()` and `QueryResultItems.get_by_name_or_last()`
* Exposed a common `generate_id()` function

Bugfixes:

* Fix for `AnnotationStore.add_resource(filename=)`

# v0.9.0 - 2024-01-24

This is a major update that significantly changes the high-level API and
introduces a query implementation. We have now entered a stage in which the API
is stabilising and can be actively used.

* New high-level API, with documentation.
    * Introduces traits `AnnotationsIterator`, `DataIterator`, `ResourcesIterator`, `TextSelectionIterator`.
    * Added `Handles<T>` structure to hold collections (type aliases `Annotations`,`Data`, `TextSelections`,`Resources`).
    * Many new `filter*` methods
    * Major refactoring in large code parts
* A query language (STAMQL) and query engine have been implemented. ([#14](https://github.com/annotation/stam-rust/issues/14))
* Two new selectors were added to the core model: `DataKeySelector` and `AnnotationDataSelector` ([#25](https://github.com/annotation/stam-rust/issues/25))
* Implemented `AnnotationStore.find_text_nocase()` method
* Various bugfixes, and other minor improvements.

**Important note:** This release breaks backward-compatibility in a number of ways as the API has been overhauled again. This should be the last such major overhaul. 

# v0.8.0 - 2023-10-19

This is a major new release that introduces a new high-level API, allowing for elaborate search of annotations, data, text selections:

* New high-level API, with documentation
      * New iterators `AnnotationsIter`, `DataIter` and `TextSelectionsIter`.
      * New collections `Annotations`, `Data`.
      * Iterators have a `parallel()` method to initiate parallelisation from that point on.
* Made a split between the low-level API and high-level API, certain parts of the low-level API are no longer exposed publicly.
* Implemented binary (de)serialisation using CBOR ([#13](https://github.com/annotation/stam-rust/issues/13) )
* CompositeSelectors and MultiSelectors adhere to textual order ([#21](https://github.com/annotation/stam-rust/issues/21))
* Internal ranged selectors reduce the memory footprint for complex selectors ([#15](https://github.com/annotation/stam-rust/issues/15)) 
* Implemented temporary public IDs in cases where no public IDs have been assigned; also added the option to strip IDs from existing annotations/data.
* Methods for introspection regarding memory usage
* Major improvements in memory consumption (contained an important bug)
* Major refactoring, numerous bugfixes, performance improvements, etc...

The above is just a limited summary of the most important changes, consult the git log for details.

**Important note**: This release breaks backward-compatibility in a number of ways as the API has been overhauled completely! We hope to avoid similar large breakages in the future.


# v0.7.2 - 2023-06-21

* bug fix for stack overflow error in various iterators
* implemented test_data() methods on Annotation and AnnotationDataSet
* AnnotationData::test() renamed to AnnotationData::test_value(), added a new higher-level test() method


# v0.7.1 - 2023-06-08

Minor correction, previous version tagged the wrong commit



# v0.7.0 - 2023-06-07

* find_data() has been reimplemented and now returns an iterator, the old-style version is renamed to `data_by_value()`
* implemented find_text_nocase()
* implemented find_text_sequence() for finding/aligning text sequences
* added TextResource.with_filename()
* minor fixes
* updated dependencies

# v0.6.0 - 2023-04-19

This release is now ready for use by the general public, even though there may still be API changes in the future, the biggest ones are now behind us:

* Implemented a higher-level API. This is generally implemented on the new `WrappedItem<T>` structure which wraps various STAM data structures in a fat pointer that also references the underlying store.
* Implemented `find_textselections()` and `find_annotations()` method that use `TextSelectionOperator` to find textselections/annotations that are in a specific textual relationship (overlap, embedding, adjacency, etc..).
* Implemented a `Text` trait that consolidates all operations on text. It is implemented for `TextResource` and `TextSelection`.
    * Contains methods like `split_text()`, `trim_text()`, `find_text()`, `find_text_regex()`... all returning text selections (retaining offset information)
* Renamed various data types:
     * `SearchTextIter` -> `FindRegexIter`
     * `AnyId` -> `Item`  , and major refactoring that allows borrowed as well as owned forms now.
* Implemented `Clone` for more data types.
* Major refactoring
    * Removed a decent amount of redundancy
    * Made various structure parameters private, use getters/setters to access them
    * Improved builder patterns
* Numerous bugfixes
* Added more benchmarks
* Added a cool logo



# v0.5.0 - 2023-04-02

This release is ready for experimentation, but not for production use yet. The API hasn't stabilized yet.

* Implemented STAM CSV support
* Major refactoring and various bugfixes


# v0.4.0 - 2023-03-27

This release is ready for experimentation, but not for production use yet. The API hasn't stabilized yet.

* reimplemented search_text() to return results in textual order and implemented support for disallowing overlapping results
* reimplemented utf8byte_to_charpos to be more efficient (using byte2charmap index now)
* changed passing Config , required in more places (and with ownership)
* renamed from_str() to from_json()  
* adding method positionindex_len() to TextResource 

# v0.3.0 - 2023-03-25

This third release makes the library ready for experimentation, but not for production use yet.

A lot has been implemented:

* auto-generated IDs, enabled by default
* support for reading from stdin and outputting to stdout instead of reading from/to file
* searching in working directory when loading/saving files
* add multiple annotations from a json file (annotate_from_file())  
* merge multiple annotation stores into one
* powerful regular expression based text search via search_text()
* unicode character position to utf8 byte conversion
* configurability via the Config class
* Serialisation/deserialisation to/from stand-off files via the `@include` mechanism
* Renamed to_string() to to_json()
* Ordered iteration over text selections in a resource
* a position index that aids in mapping text offsets to annotations and enforces an ordering
* Initial support for internal ranged selectors, improving the space efficiency of complex selectors  (annotation/stam#11)
* More efficient unicode points to utf8 byte conversion
* Improved JSON error feedback, and error feedback in general
* Many bugfixes and major refactoring

# v0.2.0 - 2023-02-06

This is a second initial release. This library is an an early stage of development. Not ready for production use yet.

A lot has been implemented in this release, iterators and reverse indices being one of the new features, and a lot from the previous release has been refactored and improved.


# v0.1.0 - 2023-01-13

Initial release. This library is an an early stage of development. Not ready for production use yet.

Implemented at this stage is the core model and serialisation from/to STAM json.
