mod annotation;
mod annotationdata;
mod annotationdataset;
mod annotationstore;
mod datakey;
mod resources;
mod text;
mod textselection;

pub use annotation::*;
pub use annotationdata::*;
pub use annotationdataset::*;
pub use annotationstore::*;
pub use datakey::*;
pub use resources::*;
pub use text::*;
pub use textselection::*;

// This root module contains some common structures used by multiple parts of the higher-level API.
// See api/* for the high-level API implementations for each STAM object.
