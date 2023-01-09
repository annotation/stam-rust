use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;

use crate::types::*;
use crate::error::*;

use serde_json::{Result,Value};

pub fn parse(filename: &str) -> std::io::Result<()> {
    let f = File::open(filename)?;
    let reader = BufReader::new(f);
    let data: Value = serde_json::from_reader(reader)?;
    Ok(())
}
