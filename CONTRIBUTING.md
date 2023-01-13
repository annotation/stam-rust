## Contributor Guidelines 

* Any STAM library implementation should always be in sync with the [STAM specification](https://github.com/annotation/stam), read that first.
    * Make sure you're familiar with the model as explained in the specification
    * Propose new ideas to the specification first (as extensions)
    * Specifications should leave enough freedom to the implementations for details
* Formatting: Indentation style: 4 spaces, unix newlines
* STAM extensions may be implemented in the this library but need to be implemented as [features](https://doc.rust-lang.org/cargo/reference/features.html) that can be enabled/disabled. This allows users to compile a lighter version if certain extensions are not needed.
* We follow the [Rust API guidelines](https://rust-lang.github.io/api-guidelines/checklist.html) as much as possible 
    * [Variable naming](https://rust-lang.github.io/api-guidelines/naming.html)
* Recommended reading for Rust
    * [THE book](https://doc.rust-lang.org/book/)
    * [How not to learn rust](https://dystroy.org/blog/how-not-to-learn-rust/)
    * [Rust data structures with circular references](https://eli.thegreenplace.net/2021/rust-data-structures-with-circular-references/)

