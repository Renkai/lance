//  Copyright 2022 Lance Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Plain encoding

use arrow2::types::{NativeType};
use std::io::{Read, Result, Seek, SeekFrom};
use std::sync::Arc;
use arrow2::array::{Array, MutablePrimitiveArray};
use arrow2::array::Int32Array as Int32Array2;
use arrow2::array::{new_empty_array as new_empty_array2};
use arrow2::compute::arithmetics::basic::{sub_scalar};
use crate::encodings::Decoder;

/// Plain Decoder
pub struct PlainDecoder<'a, R: Read + Seek> {
    file: &'a mut R,
    position: u64,
    page_length: i64,
}

impl<'a, R: Read + Seek> PlainDecoder<'a, R> {
    pub fn new(file: &'a mut R, position: u64, page_length: i64) -> Self {
        PlainDecoder {
            file,
            position,
            page_length,
        }
    }
}

impl<'a, R: Read + Seek, T: NativeType> Decoder<T> for PlainDecoder<'a, R> {
    type ArrowType = T;

    fn decode(&mut self, offset: i32, length: &Option<i32>) -> Result<Arc<dyn Array>> {
        let read_len = length.unwrap_or((self.page_length - (offset as i64)) as i32) as usize;
        (*self.file).seek(SeekFrom::Start(self.position + offset as u64))?;
        // let mut mutable_buf = Buffer::new(read_len * T::get_byte_width());
        let byte_size = std::mem::size_of::<T>();
        let mut buf = vec![0u8; read_len * byte_size];//TODO is it right?
        (*self.file).read_exact(&mut buf)?;
        let mut builder = MutablePrimitiveArray::with_capacity(read_len);
        for i in 0..read_len {
            let slice = &buf[i * byte_size..(i + 1) * byte_size];
            let mut bytes = T::Bytes::default();
            for j in 0..slice.len() {//TODO is there a better way?
                bytes[j] = slice[j]
            }

            let v = T::from_le_bytes(bytes);//TODO is it right?
            builder.push(Option::Some(v));
        }

        Ok(builder.into_arc())
    }

    fn take(&mut self, indices: &Int32Array2) -> Result<Box<dyn Array>> {
        if indices.len() == 0 {
            return Ok(new_empty_array2(T::PRIMITIVE.into()));
        }

        let start = indices.value(0);
        let length = indices.values().last().map(|i| i - start);

        let values = self.decode2::<T>(start, &length)?;

        let reset_indices = sub_scalar(&indices, &start);

        use arrow2::compute::take::{take as take2};

        let res = take2(values.as_ref(), &reset_indices);
        res.into()
    }

    fn value(&self, i: usize) -> Result<T> {
        todo!()
    }
}
