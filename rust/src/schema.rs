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

//! Lance Dataset Schema

use std::fmt;
use std::io::{Read, Seek};

use arrow::datatypes::{ArrowPrimitiveType, DataType};
use arrow2::datatypes::DataType as DType2;
use arrow2::datatypes::Field as Field2;

use crate::encodings::{Decoder, Encoding};
use crate::encodings::plain::PlainDecoder;
use crate::format::pb;

/// Lance Field.
///
/// Metadata of a column.
#[derive(Debug)]
pub struct Field {
    pub id: i32,
    pub parent_id: i32,
    pub name: String,
    logical_type: String,
    extension_name: String,
    pub encoding: Option<Encoding>,
    node_type: i32,

    children: Vec<Field>,
}

impl Field {
    pub fn new(field: &arrow::datatypes::Field) -> Field {
        Field {
            id: -1,
            parent_id: -1,
            name: field.name().clone(),
            logical_type: field.data_type().to_string(),
            extension_name: String::new(),
            encoding: match field.data_type() {
                t if DataType::is_numeric(t) => Some(Encoding::Plain),
                DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8 => {
                    Some(Encoding::VarBinary)
                }
                DataType::Dictionary(_, _) => Some(Encoding::Dictionary),
                _ => None,
            },
            node_type: 0,
            children: vec![],
        }
        // TODO Add subfields.
    }

    /// Build Field from protobuf.
    pub fn from_proto(pb: &pb::Field) -> Field {
        Field {
            id: pb.id,
            parent_id: pb.parent_id,
            name: pb.name.to_string(),
            logical_type: pb.logical_type.to_string(),
            extension_name: pb.extension_name.to_string(),
            encoding: match pb.encoding {
                1 => Some(Encoding::Plain),
                2 => Some(Encoding::VarBinary),
                3 => Some(Encoding::Dictionary),
                _ => None,
            },
            node_type: pb.r#type,

            children: vec![],
        }
    }

    /// Return Arrow Data Type.
    pub fn data_type(&self) -> DataType {
        match self.logical_type.as_str() {
            "bool" => DataType::Boolean,
            "uint8" => DataType::UInt8,
            "int8" => DataType::Int8,
            "uint16" => DataType::UInt16,
            "int16" => DataType::Int16,
            "uint32" => DataType::UInt32,
            "int32" => DataType::Int32,
            "uint64" => DataType::UInt64,
            "int64" => DataType::Int64,
            "halffloat" => DataType::Float16,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "binary" => DataType::Binary,
            "string" => DataType::Utf8,
            _ => panic!(),
        }
    }

    pub fn from_logical_type(logical_type: &String) -> DType2 {
        match logical_type.as_str() {
            "bool" => DType2::Boolean,
            "uint8" => DType2::UInt8,
            "int8" => DType2::Int8,
            "uint16" => DType2::UInt16,
            "int16" => DType2::Int16,
            "uint32" => DType2::UInt32,
            "int32" => DType2::Int32,
            "uint64" => DType2::UInt64,
            "int64" => DType2::Int64,
            "halffloat" => DType2::Float16,
            "float" => DType2::Float32,
            "double" => DType2::Float64,
            "binary" => DType2::Binary,
            "string" => DType2::Utf8,
            _ => panic!(),
        }
    }
    pub fn is_extension_type(&self) -> bool {
        return !self.extension_name.is_empty();
    }
    /// Return Arrow Data Type.
    pub fn data_type2(&self) -> DType2 {
        if self.is_extension_type() {
            //TODO don't know why shall we have this, just make compile pass
            let boxed_type = DType2::Binary;
            DType2::Extension(self.extension_name.clone(), Box::new(boxed_type), None)
        } else {
            self.get_arrow2_data_type()
        }
    }

    pub fn storage_type(&self) -> DType2 {
        self.get_arrow2_data_type()
    }

    pub fn get_arrow2_data_type(&self) -> DType2 {
        let logical_type = &self.logical_type;
        return if logical_type == "list" || logical_type == "list.struct" {
            assert_eq!(self.children.len(), 1);
            DType2::List(Box::from(Field2 {
                name: self.name.clone(),
                data_type: self.data_type2(),
                is_nullable: false, //TODO don't know what to fill
                metadata: Default::default(),
            }))
        } else if logical_type == "struct" {
            let fields: Vec<Field2> = self.children.iter().map(|field| {
                Field2 {
                    name: field.name.clone(),
                    data_type: self.data_type2(),
                    is_nullable: false,
                    metadata: Default::default(),
                }
            }).collect();
            DType2::Struct(fields)
        } else {
            Self::from_logical_type(&self.logical_type)
        };
    }

    fn insert(&mut self, child: Field) {
        self.children.push(child)
    }

    fn field_mut(&mut self, id: i32) -> Option<&mut Field> {
        for field in &mut self.children {
            if field.id == id {
                return Some(field);
            }
            match field.field_mut(id) {
                Some(c) => return Some(c),
                None => {}
            }
        }
        None
    }

    pub(crate) fn get_decoder<R: Read + Seek, T: ArrowPrimitiveType>(&self, file: &R) -> Box<dyn Decoder<T>> {
        //  std::shared_ptr<lance::encodings::Decoder> decoder;
        //   auto data_type = storage_type();
        let data_type = self.storage_type();
        let decoder :  Box<dyn Decoder<T>> =  if self.encoding == Some(Encoding::Plain) {
            PlainDecoder::new()
        } else if self.encoding == Some(Encoding::VarBinary) {

        } else if self.encoding == Some(Encoding::Dictionary) {

        } else {
            //not implemented in C++
            todo!()
        }
        //   if (encoding() == pb::Encoding::PLAIN) {
        //     if (logical_type_ == "list" || logical_type_ == "list.struct") {
        //       decoder = std::make_shared<lance::encodings::PlainDecoder>(infile, ::arrow::int32());
        //     } else if (data_type->id() == ::arrow::TimestampType::type_id ||
        //                data_type->id() == ::arrow::Time64Type::type_id ||
        //                data_type->id() == ::arrow::Date64Type::type_id) {
        //       decoder = std::make_shared<lance::encodings::PlainDecoder>(infile, ::arrow::int64());
        //     } else if (data_type->id() == ::arrow::Time32Type::type_id ||
        //                data_type->id() == ::arrow::Date32Type::type_id) {
        //       decoder = std::make_shared<lance::encodings::PlainDecoder>(infile, ::arrow::int32());
        //     } else {
        //       decoder = std::make_shared<lance::encodings::PlainDecoder>(infile, data_type);
        //     }
        //   } else if (encoding_ == pb::Encoding::VAR_BINARY) {
        //     if (logical_type_ == "string") {
        //       decoder = std::make_shared<lance::encodings::VarBinaryDecoder<::arrow::StringType>>(
        //           infile, data_type);
        //     } else if (logical_type_ == "binary") {
        //       decoder = std::make_shared<lance::encodings::VarBinaryDecoder<::arrow::BinaryType>>(
        //           infile, data_type);
        //     }
        //   } else if (encoding_ == pb::Encoding::DICTIONARY) {
        //     auto dict_type = std::static_pointer_cast<::arrow::DictionaryType>(data_type);
        //     if (!dictionary()) {
        //       {
        //         std::scoped_lock lock(lock_);
        //         if (!dictionary()) {
        //           /// Fetch dictionary on demand?
        //           ARROW_RETURN_NOT_OK(LoadDictionary(infile));
        //         }
        //       }
        //     }
        //     decoder =
        //         std::make_shared<lance::encodings::DictionaryDecoder>(infile, dict_type, dictionary());
        //   }
        //
        //   if (decoder) {
        //     auto status = decoder->Init();
        //     if (!status.ok()) {
        //       return status;
        //     }
        //     return decoder;
        //   } else {
        //     return ::arrow::Status::NotImplemented(
        //         fmt::format("Field::GetDecoder(): encoding={} logic_type={} is not supported.",
        //                     encoding(),
        //                     logical_type_));
        //   }
        todo!()
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Field({}, id={}, type={}, ext_type={}, encoding={})",
            self.name,
            self.id,
            self.logical_type,
            self.extension_name,
            match &self.encoding {
                Some(enc) => format!("{}", enc),
                None => String::from("N/A"),
            }
        )?;
        self.children.iter().for_each(|field| {
            write!(f, "{}", field).unwrap();
        });
        Ok(())
    }
}

/// Lance file Schema.
#[derive(Debug)]
pub struct Schema {
    pub(crate) fields: Vec<Field>,
}

impl Schema {
    /// Create a Schema from arrow schema.
    pub fn new(schema: &arrow::datatypes::Schema) -> Schema {
        Schema {
            fields: schema
                .fields()
                .iter()
                .map(Field::new)
                .collect(),
        }
    }
    /// Create a new schema from protobuf.
    pub fn from_proto(fields: &[crate::format::pb::Field]) -> Schema {
        let mut schema = Schema { fields: vec![] };
        fields.iter().for_each(|f| {
            let lance_field = Field::from_proto(f);
            if lance_field.parent_id < 0 {
                schema.fields.push(lance_field);
            } else {
                schema
                    .field_mut(lance_field.parent_id)
                    .map(|f| f.insert(lance_field));
            }
        });
        schema
    }

    fn field_mut(&mut self, id: i32) -> Option<&mut Field> {
        for field in &mut self.fields {
            if field.id == id {
                return Some(field);
            }
            match field.field_mut(id) {
                Some(c) => return Some(c),
                None => {}
            }
        }
        None
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Schema(")?;
        for field in &self.fields {
            write!(f, "{}", field)?
        }
        writeln!(f, ")")
    }
}
