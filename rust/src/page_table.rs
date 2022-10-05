use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::iter::Map;
use std::mem::size_of;
use arrow2::datatypes::DataType;
use byteorder::{LittleEndian, ReadBytesExt};
use prost::bytes::{Buf, BufMut};

pub struct PageInfo(i64, i64);

pub fn get_page_info(column_id: i32, batch_id: usize) -> PageInfo {
    todo!()
}

pub struct PageTable {
    page_info_map: HashMap<usize, HashMap<usize, PageInfo>>,
}

impl PageTable {
    pub fn make<R: Read + Seek>(file: &mut R, page_table_position: u64, num_columns: usize, num_batches: usize) -> PageTable {
        // ARROW_ASSIGN_OR_RAISE(
        //     auto buf, in->ReadAt(page_table_position, (num_columns * num_batches * 2 * sizeof(int64_t))));
        //
        // auto arr = ::arrow::Int64Array(num_columns * num_batches * 2, buf);
        file.seek(SeekFrom::Start(page_table_position)).unwrap();
        // let mut buf = vec![0u8; num_columns * num_batches * 2 * size_of::<i64>()];
        // file.read_exact(&mut buf).unwrap();
        // let buffer = arrow2::buffer::Buffer::from(buf);

        let mut vec = vec![0i64; num_columns * num_batches * 2];
        let mut buffer = arrow2::buffer::Buffer::from(vec);
        file.read_i64_into::<LittleEndian>(&mut buffer).unwrap(); //TODO is it right?
        let arr = arrow2::array::Int64Array::new(DataType::Int64, buffer, None);
        let mut lt = PageTable { page_info_map: HashMap::new() };
        for col in 0..num_columns {
            for batch in 0..num_batches {
                let idx = col * num_batches + batch;
                let position = arr.value(idx * 2);
                let length = arr.value(idx * 2 + 1);
                lt.set_page_info(col, batch, position, length);
            }
        }

        lt
    }

    fn set_page_info(&mut self, col: usize, batch: usize, position: i64, length: i64) {
        // page_info_map_[column_id][batch_id] = std::make_tuple(position, length)
        let mut map = &self.page_info_map;
        //TODO map is not a necessary structure
        map.insert()
        todo!()
    }
}
