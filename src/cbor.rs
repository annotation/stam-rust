/*
    STAM Library (Stand-off Text Annotation Model)
        by Maarten van Gompel <proycon@anaproy.nl>
        Digital Infrastucture, KNAW Humanities Cluster

        Licensed under the GNU General Public License v3

        https://github.com/annotation/stam-rust
*/

//! This module contain some helper functions for serialisation/deserialisation to/from CBOR (the binary representation).

use minicbor::{decode::ArrayIterWithCtx, CborLen, Encode};
use smallvec::SmallVec;
use std::sync::{Arc, RwLock};

use crate::config::SerializeMode;
use crate::textselection::TextSelectionHandle;

// we need these three functions and the CborLen implementation because there is no out of the box support for SmallVec
// these implementations are not very generic (constrainted to a very particular SmallVec usage), but that's okay as
// long as we don't need to (de)serialize others.

pub(crate) fn cbor_decode_positionitem_smallvec<'b, Ctx>(
    d: &mut minicbor::decode::Decoder<'b>,
    ctx: &mut Ctx,
) -> Result<SmallVec<[(usize, TextSelectionHandle); 1]>, minicbor::decode::Error> {
    let iter: ArrayIterWithCtx<Ctx, (usize, TextSelectionHandle)> = d.array_iter_with(ctx)?;
    let mut v = SmallVec::new();
    for x in iter {
        v.push(x?)
    }
    Ok(v)
}

pub(crate) fn cbor_encode_positionitem_smallvec<Ctx, W: minicbor::encode::Write>(
    v: &SmallVec<[(usize, TextSelectionHandle); 1]>,
    e: &mut minicbor::encode::Encoder<W>,
    ctx: &mut Ctx,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    e.array(v.len() as u64)?;
    for x in v {
        x.encode(e, ctx)?
    }
    Ok(())
}

//copied from u32 implementation in minicbor (make sure to update this if TextSelectionHandle ever adopts a bigger/smaller type)
impl<C> CborLen<C> for TextSelectionHandle {
    fn cbor_len(&self, _: &mut C) -> usize {
        match self.0 {
            0..=0x17 => 1,
            0x18..=0xff => 2,
            0x100..=0xffff => 3,
            _ => 5,
        }
    }
}

/*
pub(crate) fn cbor_len_positionitem_smallvec<Ctx, W: minicbor::encode::Write>(
    v: &SmallVec<[(usize, TextSelectionHandle); 1]>,
    ctx: &mut Ctx,
) -> usize {
    let n = v.len();
    n.cbor_len(ctx) + v.iter().map(|x| x.cbor_len(ctx)).sum::<usize>()
}
*/

// minicbor has no skip property unfortunately, we have to fake it for the 'changed' fields on AnnotationDataSet and TextResource:

pub(crate) fn cbor_decode_changed<'b, Ctx>(
    _d: &mut minicbor::decode::Decoder<'b>,
    _ctx: &mut Ctx,
) -> Result<Arc<RwLock<bool>>, minicbor::decode::Error> {
    Ok(Arc::new(RwLock::new(false)))
}

pub(crate) fn cbor_encode_changed<Ctx, W: minicbor::encode::Write>(
    _v: &Arc<RwLock<bool>>,
    _e: &mut minicbor::encode::Encoder<W>,
    _ctx: &mut Ctx,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    Ok(())
}

pub(crate) fn cbor_decode_serialize_mode<'b, Ctx>(
    _d: &mut minicbor::decode::Decoder<'b>,
    _ctx: &mut Ctx,
) -> Result<Arc<RwLock<SerializeMode>>, minicbor::decode::Error> {
    Ok(Arc::new(RwLock::new(SerializeMode::NoInclude)))
}

pub(crate) fn cbor_encode_serialize_mode<Ctx, W: minicbor::encode::Write>(
    _v: &Arc<RwLock<SerializeMode>>,
    _e: &mut minicbor::encode::Encoder<W>,
    _ctx: &mut Ctx,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    Ok(())
}
