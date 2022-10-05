use std::io::{Cursor, Error, ErrorKind, Read, Result, Seek, SeekFrom};

pub struct PageInfo(i64, i64);

pub fn get_page_info(column_id: i32, batch_id: usize) -> PageInfo {
    todo!()
}

pub struct PageTable {

}

impl PageTable {
    pub fn make <R: Read + Seek>(file:&R, page_table_position: u64, num_columns: usize, num_batches: usize) -> PageTable {
        // ARROW_ASSIGN_OR_RAISE(
        //     auto buf, in->ReadAt(page_table_position, (num_columns * num_batches * 2 * sizeof(int64_t))));
        //
        // auto arr = ::arrow::Int64Array(num_columns * num_batches * 2, buf);
        //
        // auto lt = std::make_shared<PageTable>();
        // for (int32_t col = 0; col < num_columns; col++) {
        //     for (int32_t batch = 0; batch < num_batches; batch++) {
        //         auto idx = col * num_batches + batch;
        //         auto position = arr.Value(idx * 2);
        //         auto length = arr.Value(idx * 2 + 1);
        //         lt->SetPageInfo(col, batch, position, length);
        //     }
        // }
        // return lt;

        todo!()
    }
}
