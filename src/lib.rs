extern crate chrono;

pub mod types;
pub mod annotationstore;
pub mod annotationdata;
pub mod annotation;
pub mod resources;
pub mod selector;
pub mod error;

pub use types::*;
pub use annotationstore::*;
pub use annotationdata::*;
pub use annotation::*;
pub use resources::*;
pub use selector::*;
pub use error::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
