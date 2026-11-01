use crate::models::{ArrowIpc, ArrowIpcBatch, Column, ExecuteRequest, QueryRequest, QueryResult};
use duckdb::Connection;
use duckdb::arrow::array::{
    Array as ArrowArray, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, StringArray,
    StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use duckdb::arrow::datatypes::DataType;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::display::{ArrayFormatter, FormatOptions};
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use std::env;
use std::ffi::CStr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tracing::info;
use zetasketch_rs::{Aggregator, HyperLogLogPlusPlus};

struct FlociByteLength;

impl VScalar for FlociByteLength {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let vector = input.flat_vector(0);
        let values = unsafe { vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let mut output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if vector.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let mut value = *value;
            unsafe {
                *output.as_mut_ptr::<i64>().add(index) =
                    DuckString::new(&mut value).as_bytes().len() as i64;
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![LogicalTypeId::Varchar, LogicalTypeId::Blob]
            .into_iter()
            .map(|input| {
                ScalarFunctionSignature::exact(vec![input.into()], LogicalTypeId::Bigint.into())
            })
            .collect()
    }
}

struct FlociLength;

impl VScalar for FlociLength {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use duckdb::ffi;

        let input_vector = unsafe { ffi::duckdb_data_chunk_get_vector(input.get_ptr(), 0) };
        let mut input_type = unsafe { ffi::duckdb_vector_get_column_type(input_vector) };
        let input_type_id = LogicalTypeId::from(unsafe { ffi::duckdb_get_type_id(input_type) });
        let vector = input.flat_vector(0);
        let values =
            unsafe { vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(input.len()) };
        let mut output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if vector.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let mut value = *value;
            let mut value = DuckString::new(&mut value);
            let length = if input_type_id == LogicalTypeId::Blob {
                value.as_bytes().len()
            } else {
                value.as_str().chars().count()
            };
            unsafe {
                *output.as_mut_ptr::<i64>().add(index) = length as i64;
            }
        }
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut input_type);
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![LogicalTypeId::Varchar, LogicalTypeId::Blob]
            .into_iter()
            .map(|input| {
                ScalarFunctionSignature::exact(vec![input.into()], LogicalTypeId::Bigint.into())
            })
            .collect()
    }
}

struct BlobShiftLeft;

impl VScalar for BlobShiftLeft {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let blobs = input.flat_vector(0);
        let shifts = input.flat_vector(1);
        let values = unsafe { blobs.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let shift_values = unsafe { shifts.as_slice_with_len::<i64>(len) };
        let mut output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if blobs.row_is_null(index as u64) || shifts.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let shift = shift_values[index];
            if shift < 0 {
                return Err("BYTES shift count must not be negative".into());
            }
            let mut value = *value;
            let source = DuckString::new(&mut value).as_bytes();
            let mut shifted = vec![0; source.len()];
            let byte_shift = (shift / 8) as usize;
            let bit_shift = (shift % 8) as u32;
            for (target, byte) in shifted.iter_mut().enumerate() {
                let source_index = target + byte_shift;
                if source_index >= source.len() {
                    break;
                }
                *byte = source[source_index] << bit_shift;
                if bit_shift != 0 && source_index + 1 < source.len() {
                    *byte |= source[source_index + 1] >> (8 - bit_shift);
                }
            }
            output.insert(index, &shifted);
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Blob.into(), LogicalTypeId::Bigint.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

struct BlobBitAnd;

impl VScalar for BlobBitAnd {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let left = input.flat_vector(0);
        let right = input.flat_vector(1);
        let left_values = unsafe { left.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let right_values = unsafe { right.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let mut output = output.flat_vector();
        for index in 0..len {
            if left.row_is_null(index as u64) || right.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let mut left_value = left_values[index];
            let mut right_value = right_values[index];
            let left_bytes = DuckString::new(&mut left_value).as_bytes();
            let right_bytes = DuckString::new(&mut right_value).as_bytes();
            if left_bytes.len() != right_bytes.len() {
                return Err("BYTES bitwise operands must have equal lengths".into());
            }
            let value: Vec<u8> = left_bytes
                .iter()
                .zip(right_bytes)
                .map(|(left, right)| left & right)
                .collect();
            output.insert(index, &value);
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Blob.into(), LogicalTypeId::Blob.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

struct BlobBitCount;

impl VScalar for BlobBitCount {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let vector = input.flat_vector(0);
        let values = unsafe { vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let mut output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if vector.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let mut value = *value;
            let count: i64 = DuckString::new(&mut value)
                .as_bytes()
                .iter()
                .map(|byte| i64::from(byte.count_ones()))
                .sum();
            unsafe { *output.as_mut_ptr::<i64>().add(index) = count };
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Blob.into()],
            LogicalTypeId::Bigint.into(),
        )]
    }
}

struct BlobRepeat;

impl VScalar for BlobRepeat {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let values_vector = input.flat_vector(0);
        let counts_vector = input.flat_vector(1);
        let values =
            unsafe { values_vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let counts = unsafe { counts_vector.as_slice_with_len::<i64>(len) };
        let mut output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if values_vector.row_is_null(index as u64) || counts_vector.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            if counts[index] < 0 {
                return Err("BYTES repeat count must not be negative".into());
            }
            let mut value = *value;
            output.insert(
                index,
                DuckString::new(&mut value)
                    .as_bytes()
                    .repeat(counts[index] as usize)
                    .as_slice(),
            );
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Blob.into(), LogicalTypeId::Bigint.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

struct BlobConcat;

impl VScalar for BlobConcat {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let vectors: Vec<_> = (0..input.num_columns())
            .map(|column| input.flat_vector(column))
            .collect();
        let values: Vec<_> = vectors
            .iter()
            .map(|vector| unsafe {
                vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(input.len())
            })
            .collect();
        let mut output = output.flat_vector();
        for index in 0..input.len() {
            if vectors
                .iter()
                .any(|vector| vector.row_is_null(index as u64))
            {
                output.set_null(index);
                continue;
            }
            let mut concatenated = Vec::new();
            for column in &values {
                let mut value = column[index];
                concatenated.extend_from_slice(DuckString::new(&mut value).as_bytes());
            }
            output.insert(index, &concatenated);
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        (2..=8)
            .map(|arity| {
                ScalarFunctionSignature::exact(
                    (0..arity).map(|_| LogicalTypeId::Blob.into()).collect(),
                    LogicalTypeId::Blob.into(),
                )
            })
            .collect()
    }
}

struct FlociHllInit;

impl VScalar for FlociHllInit {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use duckdb::ffi;

        let values_vector = unsafe { ffi::duckdb_data_chunk_get_vector(input.get_ptr(), 0) };
        let mut values_type = unsafe { ffi::duckdb_vector_get_column_type(values_vector) };
        let precision_vector = input.flat_vector(1);
        let precisions = unsafe { precision_vector.as_slice_with_len::<i32>(input.len()) };
        let mut output = output.flat_vector();

        let result: Result<(), Box<dyn std::error::Error>> = (|| {
            for (index, precision) in precisions.iter().enumerate().take(input.len()) {
                if raw_vector_is_null(values_vector, index)
                    || precision_vector.row_is_null(index as u64)
                {
                    output.set_null(index);
                    continue;
                }
                match hll_init_list(values_vector, values_type, index, *precision)? {
                    Some(sketch) => output.insert(index, &sketch),
                    None => output.set_null(index),
                }
            }
            Ok(())
        })();
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut values_type);
        }
        result
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Any.into(), LogicalTypeId::Integer.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

struct FlociHllExtract;

impl VScalar for FlociHllExtract {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input_vector = input.flat_vector(0);
        let values =
            unsafe { input_vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(input.len()) };
        let output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if input_vector.row_is_null(index as u64) {
                unsafe { *output.as_mut_ptr::<i64>().add(index) = 0 };
                continue;
            }
            let mut value = *value;
            let sketch = parse_hll(DuckString::new(&mut value).as_bytes())?;
            unsafe { *output.as_mut_ptr::<i64>().add(index) = sketch.result()? };
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Blob.into()],
            LogicalTypeId::Bigint.into(),
        )]
    }
}

struct FlociHllMerge;

impl VScalar for FlociHllMerge {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        invoke_hll_merge(input, output, false)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Any.into()],
            LogicalTypeId::Bigint.into(),
        )]
    }
}

struct FlociHllMergePartial;

impl VScalar for FlociHllMergePartial {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        invoke_hll_merge(input, output, true)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Any.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

fn hll_init_list(
    vector: duckdb::ffi::duckdb_vector,
    logical_type: duckdb::ffi::duckdb_logical_type,
    index: usize,
    precision: i32,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    use duckdb::ffi;

    if LogicalTypeId::from(unsafe { ffi::duckdb_get_type_id(logical_type) }) != LogicalTypeId::List
    {
        return Err("floci_hll_init requires a LIST argument".into());
    }
    let entry = unsafe { raw_vector_value::<ffi::duckdb_list_entry>(vector, index) };
    // HLL_COUNT.INIT is NULL for an aggregate group with no non-NULL inputs.
    // The translator removes NULLs before calling this scalar shim, so an empty
    // list represents both the all-NULL and empty-input aggregate cases.
    if entry.length == 0 {
        return Ok(None);
    }
    let child_vector = unsafe { ffi::duckdb_list_vector_get_child(vector) };
    let mut child_type = unsafe { ffi::duckdb_list_type_child_type(logical_type) };
    let child_type_id = LogicalTypeId::from(unsafe { ffi::duckdb_get_type_id(child_type) });

    let result =
        (|| {
            let builder = HyperLogLogPlusPlus::builder().normal_precision(precision);
            let mut sketch = match child_type_id {
                LogicalTypeId::Varchar => builder.build_for_string()?,
                LogicalTypeId::Blob => builder.build_for_bytes()?,
                LogicalTypeId::Tinyint
                | LogicalTypeId::Smallint
                | LogicalTypeId::Integer
                | LogicalTypeId::Bigint => builder.build_for_u64()?,
                other => return Err(format!("HLL_COUNT.INIT does not support {other:?}").into()),
            };

            for child_index in entry.offset..entry.offset + entry.length {
                let child_index = child_index as usize;
                if raw_vector_is_null(child_vector, child_index) {
                    continue;
                }
                match child_type_id {
                    LogicalTypeId::Varchar => {
                        let mut value = unsafe {
                            raw_vector_value::<ffi::duckdb_string_t>(child_vector, child_index)
                        };
                        sketch.add_string(DuckString::new(&mut value).as_str().as_ref())?;
                    }
                    LogicalTypeId::Blob => {
                        let mut value = unsafe {
                            raw_vector_value::<ffi::duckdb_string_t>(child_vector, child_index)
                        };
                        sketch.add_bytes(DuckString::new(&mut value).as_bytes())?;
                    }
                    LogicalTypeId::Tinyint => {
                        sketch.add_i64(
                            unsafe { raw_vector_value::<i8>(child_vector, child_index) } as i64,
                        )?
                    }
                    LogicalTypeId::Smallint => sketch.add_i64(unsafe {
                        raw_vector_value::<i16>(child_vector, child_index)
                    } as i64)?,
                    LogicalTypeId::Integer => sketch.add_i64(unsafe {
                        raw_vector_value::<i32>(child_vector, child_index)
                    } as i64)?,
                    LogicalTypeId::Bigint => sketch
                        .add_i64(unsafe { raw_vector_value::<i64>(child_vector, child_index) })?,
                    _ => unreachable!(),
                }
            }
            Ok(Some(sketch.serialize_to_bytes()?))
        })();
    unsafe {
        ffi::duckdb_destroy_logical_type(&mut child_type);
    }
    result
}

fn parse_hll(bytes: &[u8]) -> Result<HyperLogLogPlusPlus, Box<dyn std::error::Error>> {
    Ok(HyperLogLogPlusPlus::from_bytes(bytes)?)
}

fn merged_hll_list(
    vector: duckdb::ffi::duckdb_vector,
    logical_type: duckdb::ffi::duckdb_logical_type,
    index: usize,
) -> Result<Option<HyperLogLogPlusPlus>, Box<dyn std::error::Error>> {
    use duckdb::ffi;

    if LogicalTypeId::from(unsafe { ffi::duckdb_get_type_id(logical_type) }) != LogicalTypeId::List
    {
        return Err("HLL_COUNT.MERGE requires a LIST argument".into());
    }
    let entry = unsafe { raw_vector_value::<ffi::duckdb_list_entry>(vector, index) };
    let child_vector = unsafe { ffi::duckdb_list_vector_get_child(vector) };
    let mut child_type = unsafe { ffi::duckdb_list_type_child_type(logical_type) };
    let child_type_id = LogicalTypeId::from(unsafe { ffi::duckdb_get_type_id(child_type) });
    if child_type_id != LogicalTypeId::Blob {
        unsafe { ffi::duckdb_destroy_logical_type(&mut child_type) };
        return Err(format!("HLL_COUNT.MERGE requires BYTES, found {child_type_id:?}").into());
    }

    let result = (|| {
        let mut merged: Option<HyperLogLogPlusPlus> = None;
        for child_index in entry.offset..entry.offset + entry.length {
            let child_index = child_index as usize;
            if raw_vector_is_null(child_vector, child_index) {
                continue;
            }
            let mut value =
                unsafe { raw_vector_value::<ffi::duckdb_string_t>(child_vector, child_index) };
            let sketch = parse_hll(DuckString::new(&mut value).as_bytes())?;
            if let Some(current) = &mut merged {
                current.merge_aggregator(sketch)?;
            } else {
                merged = Some(sketch);
            }
        }
        Ok(merged)
    })();
    unsafe {
        ffi::duckdb_destroy_logical_type(&mut child_type);
    }
    result
}

fn invoke_hll_merge(
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
    partial: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use duckdb::ffi;

    let input_vector = unsafe { ffi::duckdb_data_chunk_get_vector(input.get_ptr(), 0) };
    let mut input_type = unsafe { ffi::duckdb_vector_get_column_type(input_vector) };
    let mut output = output.flat_vector();
    let result = (|| {
        for index in 0..input.len() {
            if raw_vector_is_null(input_vector, index) {
                if partial {
                    output.set_null(index);
                } else {
                    unsafe { *output.as_mut_ptr::<i64>().add(index) = 0 };
                }
                continue;
            }
            let merged = merged_hll_list(input_vector, input_type, index)?;
            if partial {
                match merged {
                    Some(sketch) => output.insert(index, &sketch.serialize_to_bytes()?),
                    None => output.set_null(index),
                }
            } else {
                let estimate = match merged {
                    Some(sketch) => sketch.result()?,
                    None => 0,
                };
                unsafe { *output.as_mut_ptr::<i64>().add(index) = estimate };
            }
        }
        Ok(())
    })();
    unsafe {
        ffi::duckdb_destroy_logical_type(&mut input_type);
    }
    result
}

struct IpToString;

impl VScalar for IpToString {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let vector = input.flat_vector(0);
        let values = unsafe { vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let mut output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if vector.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let mut value = *value;
            let bytes = DuckString::new(&mut value).as_bytes();
            let address = match bytes.len() {
                4 => IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(bytes)?)),
                16 => IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(bytes)?)),
                _ => return Err("NET.IP_TO_STRING requires a 4-byte or 16-byte address".into()),
            };
            output.insert(index, address.to_string().as_str());
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Blob.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct IpFromString;

impl VScalar for IpFromString {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input_len = input.len();
        let input_vector = input.flat_vector(0);
        let input_values =
            unsafe { input_vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(input_len) };
        let mut output_vector = output.flat_vector();

        for (index, input_value) in input_values.iter().enumerate() {
            if input_vector.row_is_null(index as u64) {
                output_vector.set_null(index);
                continue;
            }

            let mut input_value = *input_value;
            let address = DuckString::new(&mut input_value)
                .as_str()
                .parse::<IpAddr>()?;
            match address {
                IpAddr::V4(address) => output_vector.insert(index, address.octets().as_slice()),
                IpAddr::V6(address) => output_vector.insert(index, address.octets().as_slice()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

struct SafeIpFromString;

impl VScalar for SafeIpFromString {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let vector = input.flat_vector(0);
        let values = unsafe { vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let mut output = output.flat_vector();
        for (index, value) in values.iter().enumerate() {
            if vector.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let mut value = *value;
            match DuckString::new(&mut value).as_str().parse::<IpAddr>() {
                Ok(IpAddr::V4(address)) => output.insert(index, address.octets().as_slice()),
                Ok(IpAddr::V6(address)) => output.insert(index, address.octets().as_slice()),
                Err(_) => output.set_null(index),
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

struct IpTrunc;

impl VScalar for IpTrunc {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let address_vector = input.flat_vector(0);
        let prefix_vector = input.flat_vector(1);
        let addresses =
            unsafe { address_vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(len) };
        let prefixes = unsafe { prefix_vector.as_slice_with_len::<i64>(len) };
        let mut output = output.flat_vector();
        for (index, address) in addresses.iter().enumerate() {
            if address_vector.row_is_null(index as u64) || prefix_vector.row_is_null(index as u64) {
                output.set_null(index);
                continue;
            }
            let mut address = *address;
            let mut bytes = DuckString::new(&mut address).as_bytes().to_vec();
            let prefix = prefixes[index];
            let maximum_bits = match bytes.len() {
                4 => 32,
                16 => 128,
                _ => return Err("NET.IP_TRUNC requires a 4-byte or 16-byte address".into()),
            };
            if prefix < 0 || prefix > maximum_bits {
                return Err(format!(
                    "NET.IP_TRUNC prefix length {prefix} is invalid for a {}-byte address",
                    bytes.len()
                )
                .into());
            }
            let full_bytes = (prefix / 8) as usize;
            let remaining_bits = (prefix % 8) as u8;
            if remaining_bits != 0 && full_bytes < bytes.len() {
                bytes[full_bytes] &= u8::MAX << (8 - remaining_bits);
            }
            for byte in bytes
                .iter_mut()
                .skip(full_bytes + usize::from(remaining_bits != 0))
            {
                *byte = 0;
            }
            output.insert(index, &bytes);
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Blob.into(), LogicalTypeId::Bigint.into()],
            LogicalTypeId::Blob.into(),
        )]
    }
}

struct FarmFingerprint;

impl VScalar for FarmFingerprint {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input_len = input.len();
        let input_vector = input.flat_vector(0);
        let input_values =
            unsafe { input_vector.as_slice_with_len::<duckdb::ffi::duckdb_string_t>(input_len) };
        let mut output_vector = output.flat_vector();

        for (index, input_value) in input_values.iter().enumerate() {
            if input_vector.row_is_null(index as u64) {
                output_vector.set_null(index);
                continue;
            }

            let mut input_value = *input_value;
            let value = DuckString::new(&mut input_value).as_bytes();
            unsafe {
                *output_vector.as_mut_ptr::<i64>().add(index) =
                    farmhash::fingerprint64(value) as i64;
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeId::Varchar.into()],
                LogicalTypeId::Bigint.into(),
            ),
            ScalarFunctionSignature::exact(
                vec![LogicalTypeId::Blob.into()],
                LogicalTypeId::Bigint.into(),
            ),
        ]
    }
}

struct FlociToJsonString;

impl VScalar for FlociToJsonString {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input_vector = unsafe { duckdb::ffi::duckdb_data_chunk_get_vector(input.get_ptr(), 0) };
        let pretty_vector = (input.num_columns() == 2)
            .then(|| unsafe { duckdb::ffi::duckdb_data_chunk_get_vector(input.get_ptr(), 1) });
        let mut input_type = unsafe { duckdb::ffi::duckdb_vector_get_column_type(input_vector) };
        let mut output = output.flat_vector();

        let result: Result<(), Box<dyn std::error::Error>> = (|| {
            for index in 0..input.len() {
                if pretty_vector.is_some_and(|vector| raw_vector_is_null(vector, index)) {
                    output.set_null(index);
                    continue;
                }

                if raw_vector_is_null(input_vector, index) {
                    output.set_null(index);
                    continue;
                }
                let value = raw_vector_to_json(input_vector, input_type, index)?;
                let pretty = pretty_vector
                    .map(|vector| unsafe { raw_vector_value::<bool>(vector, index) })
                    .unwrap_or(false);
                let json = if pretty {
                    serde_json::to_string_pretty(&value)?
                } else {
                    serde_json::to_string(&value)?
                };
                output.insert(index, &json);
            }
            Ok(())
        })();
        unsafe {
            duckdb::ffi::duckdb_destroy_logical_type(&mut input_type);
        }

        result
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeId::Any.into()],
                LogicalTypeId::Varchar.into(),
            ),
            ScalarFunctionSignature::exact(
                vec![LogicalTypeId::Any.into(), LogicalTypeId::Boolean.into()],
                LogicalTypeId::Varchar.into(),
            ),
        ]
    }
}

fn raw_vector_is_null(vector: duckdb::ffi::duckdb_vector, index: usize) -> bool {
    unsafe {
        let validity = duckdb::ffi::duckdb_vector_get_validity(vector);
        !validity.is_null() && !duckdb::ffi::duckdb_validity_row_is_valid(validity, index as u64)
    }
}

unsafe fn raw_vector_value<T: Copy>(vector: duckdb::ffi::duckdb_vector, index: usize) -> T {
    unsafe { *(duckdb::ffi::duckdb_vector_get_data(vector) as *const T).add(index) }
}

fn raw_vector_to_json(
    vector: duckdb::ffi::duckdb_vector,
    logical_type: duckdb::ffi::duckdb_logical_type,
    index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use duckdb::ffi;

    if raw_vector_is_null(vector, index) {
        return Ok(serde_json::Value::Null);
    }

    let type_id = LogicalTypeId::from(unsafe { ffi::duckdb_get_type_id(logical_type) });
    match type_id {
        LogicalTypeId::Boolean => Ok(serde_json::Value::Bool(unsafe {
            raw_vector_value::<bool>(vector, index)
        })),
        LogicalTypeId::Tinyint => Ok(unsafe { raw_vector_value::<i8>(vector, index) }.into()),
        LogicalTypeId::Smallint => Ok(unsafe { raw_vector_value::<i16>(vector, index) }.into()),
        LogicalTypeId::Integer => Ok(unsafe { raw_vector_value::<i32>(vector, index) }.into()),
        LogicalTypeId::Bigint => {
            let value = unsafe { raw_vector_value::<i64>(vector, index) };
            Ok(json_int64(value))
        }
        LogicalTypeId::UTinyint => Ok(unsafe { raw_vector_value::<u8>(vector, index) }.into()),
        LogicalTypeId::USmallint => Ok(unsafe { raw_vector_value::<u16>(vector, index) }.into()),
        LogicalTypeId::UInteger => Ok(unsafe { raw_vector_value::<u32>(vector, index) }.into()),
        LogicalTypeId::UBigint => {
            let value = unsafe { raw_vector_value::<u64>(vector, index) };
            Ok(if value <= 9_007_199_254_740_992 {
                value.into()
            } else {
                serde_json::Value::String(value.to_string())
            })
        }
        LogicalTypeId::Date => {
            let date = unsafe { raw_vector_value::<ffi::duckdb_date>(vector, index) };
            let parts = unsafe { ffi::duckdb_from_date(date) };
            Ok(serde_json::Value::String(format!(
                "{:04}-{:02}-{:02}",
                parts.year, parts.month, parts.day
            )))
        }
        LogicalTypeId::Time => {
            let time = unsafe { raw_vector_value::<ffi::duckdb_time>(vector, index) };
            let parts = unsafe { ffi::duckdb_from_time(time) };
            Ok(serde_json::Value::String(format_time(parts)))
        }
        LogicalTypeId::TimeTZ => {
            let time = unsafe { raw_vector_value::<ffi::duckdb_time_tz>(vector, index) };
            let parts = unsafe { ffi::duckdb_from_time_tz(time) };
            Ok(serde_json::Value::String(format_time(parts.time)))
        }
        LogicalTypeId::Hugeint => {
            let value = unsafe { raw_vector_value::<ffi::duckdb_hugeint>(vector, index) };
            let value = ((value.upper as i128) << 64) + value.lower as i128;
            Ok(serde_json::Value::String(value.to_string()))
        }
        LogicalTypeId::UHugeint => {
            let value = unsafe { raw_vector_value::<ffi::duckdb_uhugeint>(vector, index) };
            let value = ((value.upper as u128) << 64) | value.lower as u128;
            Ok(serde_json::Value::String(value.to_string()))
        }
        LogicalTypeId::Uuid => {
            let value = unsafe { raw_vector_value::<ffi::duckdb_uhugeint>(vector, index) };
            let value = ((value.upper as u128) << 64) | value.lower as u128;
            Ok(serde_json::Value::String(
                uuid::Uuid::from_u128(value).to_string(),
            ))
        }
        LogicalTypeId::Float => {
            float_to_json(unsafe { raw_vector_value::<f32>(vector, index) } as f64)
        }
        LogicalTypeId::Double => float_to_json(unsafe { raw_vector_value::<f64>(vector, index) }),
        LogicalTypeId::Decimal => raw_decimal_to_json(vector, logical_type, index),
        LogicalTypeId::Varchar => {
            let mut value = unsafe { raw_vector_value::<ffi::duckdb_string_t>(vector, index) };
            let value = DuckString::new(&mut value).as_str().into_owned();
            if logical_type_has_alias(logical_type, "JSON") {
                Ok(serde_json::from_str(&value)?)
            } else {
                Ok(serde_json::Value::String(value))
            }
        }
        LogicalTypeId::Blob => {
            let mut value = unsafe { raw_vector_value::<ffi::duckdb_string_t>(vector, index) };
            Ok(base64_value(DuckString::new(&mut value).as_bytes()))
        }
        LogicalTypeId::Timestamp => {
            let timestamp = unsafe { raw_vector_value::<ffi::duckdb_timestamp>(vector, index) };
            Ok(serde_json::Value::String(format_datetime(
                timestamp.micros,
            )?))
        }
        LogicalTypeId::TimestampTZ => {
            let timestamp = unsafe { raw_vector_value::<ffi::duckdb_timestamp>(vector, index) };
            Ok(serde_json::Value::String(format!(
                "{}Z",
                format_datetime(timestamp.micros)?
            )))
        }
        LogicalTypeId::Struct => raw_struct_to_json(vector, logical_type, index),
        LogicalTypeId::List => raw_list_to_json(vector, logical_type, index),
        other => Err(format!("floci_to_json_string does not support {other:?} values").into()),
    }
}

fn logical_type_has_alias(logical_type: duckdb::ffi::duckdb_logical_type, expected: &str) -> bool {
    use duckdb::ffi;

    let alias = unsafe { ffi::duckdb_logical_type_get_alias(logical_type) };
    if alias.is_null() {
        return false;
    }
    let matches = unsafe { CStr::from_ptr(alias) }
        .to_str()
        .is_ok_and(|alias| alias.eq_ignore_ascii_case(expected));
    unsafe { ffi::duckdb_free(alias.cast()) };
    matches
}

fn raw_decimal_to_json(
    vector: duckdb::ffi::duckdb_vector,
    logical_type: duckdb::ffi::duckdb_logical_type,
    index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use duckdb::ffi;

    let scale = unsafe { ffi::duckdb_decimal_scale(logical_type) } as usize;
    let internal_type = unsafe { ffi::duckdb_decimal_internal_type(logical_type) };
    let raw = match internal_type {
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => unsafe {
            raw_vector_value::<i8>(vector, index) as i128
        },
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => unsafe {
            raw_vector_value::<i16>(vector, index) as i128
        },
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => unsafe {
            raw_vector_value::<i32>(vector, index) as i128
        },
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => unsafe {
            raw_vector_value::<i64>(vector, index) as i128
        },
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
            let value = unsafe { raw_vector_value::<ffi::duckdb_hugeint>(vector, index) };
            ((value.upper as i128) << 64) | (value.lower as u128 as i128)
        }
        other => return Err(format!("unsupported DECIMAL internal type {other}").into()),
    };

    let negative = raw < 0;
    let digits = raw.unsigned_abs().to_string();
    let number = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    let json_number = if negative {
        format!("-{number}")
    } else {
        number
    };
    Ok(serde_json::from_str(&json_number)?)
}

fn float_to_json(value: f64) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    if value.is_nan() {
        return Ok(serde_json::Value::String("NaN".into()));
    }
    if value == f64::INFINITY {
        return Ok(serde_json::Value::String("Infinity".into()));
    }
    if value == f64::NEG_INFINITY {
        return Ok(serde_json::Value::String("-Infinity".into()));
    }
    serde_json::Number::from_f64(value)
        .map(serde_json::Value::Number)
        .ok_or_else(|| "floci_to_json_string cannot encode this floating-point value".into())
}

fn json_int64(value: i64) -> serde_json::Value {
    if (-9_007_199_254_740_992..=9_007_199_254_740_992).contains(&value) {
        value.into()
    } else {
        serde_json::Value::String(value.to_string())
    }
}

fn format_time(time: duckdb::ffi::duckdb_time_struct) -> String {
    let fraction = format_fractional_micros(time.micros as u32);
    format!("{:02}:{:02}:{:02}{fraction}", time.hour, time.min, time.sec)
}

fn format_fractional_micros(micros: u32) -> String {
    if micros == 0 {
        String::new()
    } else if micros.is_multiple_of(1_000) {
        // BigQuery's canonical temporal JSON representation uses millisecond
        // precision when the remaining three microsecond digits are all zero.
        format!(".{:03}", micros / 1_000)
    } else {
        format!(".{micros:06}")
    }
}

fn format_datetime(micros_since_epoch: i64) -> Result<String, Box<dyn std::error::Error>> {
    let datetime =
        duckdb::arrow::temporal_conversions::timestamp_us_to_datetime(micros_since_epoch)
            .ok_or("TIMESTAMP value is out of range")?;
    let base = datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
    Ok(format!(
        "{base}{}",
        format_fractional_micros(datetime.and_utc().timestamp_subsec_micros())
    ))
}

fn raw_struct_to_json(
    vector: duckdb::ffi::duckdb_vector,
    logical_type: duckdb::ffi::duckdb_logical_type,
    index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use duckdb::ffi;

    let child_count = unsafe { ffi::duckdb_struct_type_child_count(logical_type) as usize };
    let mut value = serde_json::Map::with_capacity(child_count);
    for child_index in 0..child_count {
        let name_ptr =
            unsafe { ffi::duckdb_struct_type_child_name(logical_type, child_index as u64) };
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_str()?.to_owned();
        unsafe {
            ffi::duckdb_free(name_ptr.cast());
        }

        let mut child_type =
            unsafe { ffi::duckdb_struct_type_child_type(logical_type, child_index as u64) };
        let child_vector =
            unsafe { ffi::duckdb_struct_vector_get_child(vector, child_index as u64) };
        let child_value = raw_vector_to_json(child_vector, child_type, index);
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut child_type);
        }
        value.insert(name, child_value?);
    }

    Ok(serde_json::Value::Object(value))
}

fn raw_list_to_json(
    vector: duckdb::ffi::duckdb_vector,
    logical_type: duckdb::ffi::duckdb_logical_type,
    index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use duckdb::ffi;

    let entry = unsafe { raw_vector_value::<ffi::duckdb_list_entry>(vector, index) };
    let child_vector = unsafe { ffi::duckdb_list_vector_get_child(vector) };
    let mut child_type = unsafe { ffi::duckdb_list_type_child_type(logical_type) };
    let mut values = Vec::with_capacity(entry.length as usize);
    let result: Result<(), Box<dyn std::error::Error>> = (|| {
        for child_index in entry.offset..entry.offset + entry.length {
            values.push(raw_vector_to_json(
                child_vector,
                child_type,
                child_index as usize,
            )?);
        }
        Ok(())
    })();
    unsafe {
        ffi::duckdb_destroy_logical_type(&mut child_type);
    }
    result?;

    Ok(serde_json::Value::Array(values))
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

fn apply_variables(sql: &str, variables: &std::collections::HashMap<String, String>) -> String {
    let mut result = sql.to_string();
    for (key, value) in variables {
        result = result.replace(&format!("{{{{{}}}}}", key), value);
    }
    result
}

fn open_conn() -> anyhow::Result<Connection> {
    let conn = Connection::open_in_memory()?;
    // BigQuery's JSON_EXTRACT_ARRAY returns an unnestable array of JSON values.
    // DuckDB's equivalent returns one JSON value, so cast it to JSON[] for
    // compatibility with queries that immediately UNNEST the result.
    conn.execute_batch(
        "CREATE MACRO json_extract_array(input_json, json_path := '$') AS (
            cast_to_type(
              list_transform(json_extract(input_json, json_path)::JSON[], value -> CAST(value AS VARCHAR)),
              [input_json][1:0]
            )
        );",
    )?;
    conn.register_scalar_function::<IpToString>("ip_to_string")?;
    conn.register_scalar_function::<IpFromString>("ip_from_string")?;
    conn.register_scalar_function::<SafeIpFromString>("safe_ip_from_string")?;
    conn.register_scalar_function::<IpTrunc>("ip_trunc")?;
    conn.register_scalar_function::<FarmFingerprint>("farm_fingerprint")?;
    conn.register_scalar_function::<FlociToJsonString>("floci_to_json_string")?;
    conn.register_scalar_function::<FlociByteLength>("floci_byte_length")?;
    conn.register_scalar_function::<FlociLength>("floci_length")?;
    conn.register_scalar_function::<BlobShiftLeft>("<<")?;
    conn.register_scalar_function::<BlobBitAnd>("&")?;
    conn.register_scalar_function::<BlobBitCount>("bit_count")?;
    conn.register_scalar_function::<BlobRepeat>("repeat")?;
    conn.register_scalar_function::<BlobConcat>("concat")?;
    conn.register_scalar_function::<FlociHllInit>("floci_hll_init")?;
    conn.register_scalar_function::<FlociHllExtract>("floci_hll_extract")?;
    conn.register_scalar_function::<FlociHllMerge>("floci_hll_merge")?;
    conn.register_scalar_function::<FlociHllMergePartial>("floci_hll_merge_partial")?;
    if let Ok(ext_dir) = env::var("FLOCI_DUCK_EXT_DIR") {
        info!("Setting extension directory to: {}", ext_dir);
        conn.execute_batch(&format!(
            "SET extension_directory = '{}';",
            escape_sql(&ext_dir)
        ))?;
    }
    Ok(conn)
}

/// Called once at startup to ensure extensions used by queries are installed on disk.
/// Installing them serially avoids concurrent requests racing to auto-install the same extension.
pub fn preflight() -> anyhow::Result<()> {
    info!("Preflight: installing DuckDB extensions...");
    let conn = open_conn()?;
    conn.execute_batch("INSTALL httpfs; INSTALL icu;")?;
    info!("Preflight: DuckDB extensions installed successfully");
    Ok(())
}

/// Loads httpfs and configures S3 credentials on an open connection.
fn setup_s3(
    conn: &Connection,
    s3_endpoint: &str,
    s3_region: &str,
    access_key: &str,
    secret_key: &str,
    use_ssl: bool,
    url_style: &str,
) -> anyhow::Result<()> {
    conn.execute_batch("LOAD httpfs;")?;

    let endpoint = s3_endpoint
        .trim_end_matches('/')
        .trim_start_matches("https://")
        .trim_start_matches("http://");

    info!(
        "Configuring S3: endpoint={}, region={}",
        endpoint, s3_region
    );
    conn.execute_batch(&format!(
        "SET s3_endpoint = '{}';
         SET s3_region = '{}';
         SET s3_access_key_id = '{}';
         SET s3_secret_access_key = '{}';
         SET s3_use_ssl = {};
         SET s3_url_style = '{}';",
        escape_sql(endpoint),
        escape_sql(s3_region),
        escape_sql(access_key),
        escape_sql(secret_key),
        use_ssl,
        escape_sql(url_style),
    ))?;

    Ok(())
}

fn resolve_s3_params(
    s3_endpoint: &str,
    s3_region: Option<&str>,
    s3_access_key: Option<&str>,
    s3_secret_key: Option<&str>,
    s3_use_ssl: Option<bool>,
    s3_url_style: Option<&str>,
) -> (String, String, String, bool, String) {
    let region = s3_region
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());

    let access_key = s3_access_key
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_ACCESS_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());

    let secret_key = s3_secret_key
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_SECRET_KEY").ok())
        .unwrap_or_else(|| "flociadmin".to_string());

    let use_ssl = s3_use_ssl
        .or_else(|| {
            env::var("FLOCI_DUCK_S3_USE_SSL")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or_else(|| s3_endpoint.starts_with("https://"));

    let url_style = s3_url_style
        .map(String::from)
        .or_else(|| env::var("FLOCI_DUCK_S3_URL_STYLE").ok())
        .unwrap_or_else(|| "path".to_string());

    (region, access_key, secret_key, use_ssl, url_style)
}

/// Fire-and-forget executor used by Athena/Firehose.
pub fn execute_query(req: ExecuteRequest) -> anyhow::Result<()> {
    let conn = open_conn()?;

    let (region, access_key, secret_key, use_ssl, url_style) = resolve_s3_params(
        &req.s3_endpoint,
        req.s3_region.as_deref(),
        req.s3_access_key.as_deref(),
        req.s3_secret_key.as_deref(),
        req.s3_use_ssl,
        req.s3_url_style.as_deref(),
    );

    setup_s3(
        &conn,
        &req.s3_endpoint,
        &region,
        &access_key,
        &secret_key,
        use_ssl,
        &url_style,
    )?;

    let variables = req.variables.unwrap_or_default();

    if let Some(setup) = &req.setup_sql {
        if !setup.trim().is_empty() {
            info!("Executing setup SQL");
            let setup_sql = apply_variables(setup, &variables);
            conn.execute_batch(&setup_sql)?;
        }
    }

    let sql = apply_variables(&req.sql, &variables);

    let final_sql = if let Some(output_path) = &req.output_s3_path {
        info!("Athena mode detected. Output path: {}", output_path);
        format!("COPY ({}) TO '{}' (FORMAT CSV, HEADER);", sql, output_path)
    } else {
        info!("Firehose mode detected. Running raw SQL.");
        sql
    };

    info!("Executing final SQL: {}", final_sql);
    conn.execute_batch(&final_sql)?;

    Ok(())
}

/// Result of `/query`: the column names and DuckDB types, plus the rows, and the
/// result of the optional follow-up statement.
pub struct QueryOutput {
    pub columns: Vec<Column>,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub followup: Option<QueryResult>,
    pub arrow: Option<ArrowIpc>,
}

/// Query executor that returns rows as JSON maps, used by S3 Select and BigQuery.
///
/// Uses `query_arrow()` so schema (column names) and data are available together
/// without the borrow conflict that arises when calling `column_names()` on a
/// `Rows`-borrowed statement.
pub fn execute_query_returning(req: QueryRequest) -> anyhow::Result<QueryOutput> {
    let conn = open_conn()?;

    let (region, access_key, secret_key, use_ssl, url_style) = resolve_s3_params(
        &req.s3_endpoint,
        req.s3_region.as_deref(),
        req.s3_access_key.as_deref(),
        req.s3_secret_key.as_deref(),
        req.s3_use_ssl,
        req.s3_url_style.as_deref(),
    );

    setup_s3(
        &conn,
        &req.s3_endpoint,
        &region,
        &access_key,
        &secret_key,
        use_ssl,
        &url_style,
    )?;

    if let Some(setup) = &req.setup_sql {
        if !setup.trim().is_empty() {
            info!("Executing setup SQL");
            conn.execute_batch(setup)?;
        }
    }

    info!("Executing query SQL: {}", req.sql);
    let (columns, rows, arrow) = if req.arrow_ipc {
        let (columns, arrow) = run_statement_arrow(&conn, &req.sql)?;
        (columns, Vec::new(), Some(arrow))
    } else {
        let (columns, rows) = run_statement(&conn, &req.sql, req.typed_values)?;
        (columns, rows, None)
    };
    info!("Query returned {} rows", rows.len());

    let followup = match &req.followup_sql {
        Some(sql) if !sql.trim().is_empty() => {
            info!("Executing follow-up SQL: {}", sql);
            let (columns, rows) = run_statement(&conn, sql, req.typed_values)?;
            Some(QueryResult { columns, rows })
        }
        _ => None,
    };

    Ok(QueryOutput {
        columns,
        rows,
        followup,
        arrow,
    })
}

/// Runs one statement and returns its result as Arrow IPC messages instead of JSON rows.
fn run_statement_arrow(conn: &Connection, sql: &str) -> anyhow::Result<(Vec<Column>, ArrowIpc)> {
    use arrow_ipc::writer::{
        CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message,
    };

    let mut stmt = conn.prepare(sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    let schema = stmt.schema();
    let columns = with_described_types(conn, sql, result_columns(&stmt));

    let generator = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    let mut compression = CompressionContext::default();

    let mut schema_bytes = Vec::new();
    let encoded_schema =
        generator.schema_to_bytes_with_dictionary_tracker(&schema, &mut tracker, &options);
    write_message(&mut schema_bytes, encoded_schema, &options)?;

    let mut encoded_batches = Vec::with_capacity(batches.len());
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let (dictionaries, encoded) =
            generator.encode(batch, &mut tracker, &options, &mut compression)?;
        let mut data = Vec::new();
        for dictionary in dictionaries {
            write_message(&mut data, dictionary, &options)?;
        }
        write_message(&mut data, encoded, &options)?;
        encoded_batches.push(ArrowIpcBatch {
            data: base64_string(&data),
            row_count: batch.num_rows(),
        });
    }
    Ok((
        columns,
        ArrowIpc {
            schema: base64_string(&schema_bytes),
            batches: encoded_batches,
        },
    ))
}

type Rows = Vec<serde_json::Map<String, serde_json::Value>>;

/// Runs one statement and returns its result columns and rows.
fn run_statement(
    conn: &Connection,
    sql: &str,
    typed_values: bool,
) -> anyhow::Result<(Vec<Column>, Rows)> {
    let mut stmt = conn.prepare(sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    let columns = with_described_types(conn, sql, result_columns(&stmt));

    let mut rows = Vec::new();
    for batch in &batches {
        let field_names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        for row_idx in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for (col_idx, name) in field_names.iter().enumerate() {
                let col = batch.column(col_idx);
                let value = if typed_values {
                    arrow_value_to_typed_json(col.as_ref(), row_idx)
                } else {
                    arrow_value_to_json(col.as_ref(), row_idx)
                };
                map.insert(name.clone(), value);
            }
            rows.push(map);
        }
    }
    Ok((columns, rows))
}

/**
 * Replaces the Arrow-derived type names with the exact ones `DESCRIBE` reports (Arrow folds
 * aliases such as `JSON` into `VARCHAR`). `DESCRIBE` only binds the query, so this costs no
 * execution; statements it cannot describe (DML, DDL) keep the Arrow-derived names.
 */
fn with_described_types(conn: &Connection, sql: &str, columns: Vec<Column>) -> Vec<Column> {
    let described: Option<Vec<(String, String)>> = (|| {
        let mut stmt = conn.prepare(&format!("DESCRIBE {}", sql)).ok()?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .ok()?;
        rows.collect::<Result<Vec<_>, _>>().ok()
    })();
    match described {
        Some(described) if described.len() == columns.len() => columns
            .into_iter()
            .zip(described)
            .map(|(column, (_, type_name))| Column {
                name: column.name,
                type_name,
            })
            .collect(),
        _ => columns,
    }
}

fn result_columns(stmt: &duckdb::Statement<'_>) -> Vec<Column> {
    let schema = stmt.schema();
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| Column {
            name: field.name().clone(),
            type_name: column_type_sql(field.data_type(), stmt.column_logical_type(idx).id()),
        })
        .collect()
}

/// Renders a result column's type as DuckDB SQL, the way `DESCRIBE` prints it
/// (e.g. `DECIMAL(38,9)`, `TIMESTAMP WITH TIME ZONE`, `STRUCT("a" INTEGER[])`).
///
/// The structure comes from the Arrow schema; the column's DuckDB logical type id only
/// disambiguates types Arrow folds into another (HUGEINT exports as DECIMAL(38,0), UUID
/// and ENUM as strings).
pub fn column_type_sql(arrow: &DataType, logical: LogicalTypeId) -> String {
    match logical {
        LogicalTypeId::Hugeint => "HUGEINT".into(),
        LogicalTypeId::UHugeint => "UHUGEINT".into(),
        LogicalTypeId::Uuid => "UUID".into(),
        LogicalTypeId::Enum => "ENUM".into(),
        LogicalTypeId::TimeTZ => "TIME WITH TIME ZONE".into(),
        LogicalTypeId::Bit => "BIT".into(),
        _ => arrow_type_sql(arrow),
    }
}

fn arrow_type_sql(t: &DataType) -> String {
    match t {
        DataType::Null => "\"NULL\"".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt8 => "UTINYINT".into(),
        DataType::UInt16 => "USMALLINT".into(),
        DataType::UInt32 => "UINTEGER".into(),
        DataType::UInt64 => "UBIGINT".into(),
        DataType::Float16 | DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("DECIMAL({},{})", p, s),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB".into(),
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".into(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".into(),
        DataType::Timestamp(unit, None) => match unit {
            duckdb::arrow::datatypes::TimeUnit::Second => "TIMESTAMP_S".into(),
            duckdb::arrow::datatypes::TimeUnit::Millisecond => "TIMESTAMP_MS".into(),
            duckdb::arrow::datatypes::TimeUnit::Nanosecond => "TIMESTAMP_NS".into(),
            duckdb::arrow::datatypes::TimeUnit::Microsecond => "TIMESTAMP".into(),
        },
        DataType::Interval(_) | DataType::Duration(_) => "INTERVAL".into(),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("{}[]", arrow_type_sql(f.data_type()))
        }
        DataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "\"{}\" {}",
                        f.name().replace('"', "\"\""),
                        arrow_type_sql(f.data_type())
                    )
                })
                .collect();
            format!("STRUCT({})", parts.join(", "))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "MAP({}, {})",
                arrow_type_sql(kv[0].data_type()),
                arrow_type_sql(kv[1].data_type())
            ),
            _ => "MAP".into(),
        },
        DataType::Dictionary(_, value) => arrow_type_sql(value),
        other => format!("{:?}", other).to_uppercase(),
    }
}

/// Lossless JSON encoding of one Arrow value, used when a `/query` request sets
/// `typed_values`. Scalars that JSON can hold exactly stay numbers/booleans; decimals,
/// temporals and intervals are strings in Arrow's canonical (ISO-8601) text form; blobs
/// are base64; lists and structs nest.
pub fn arrow_value_to_typed_json(array: &dyn ArrowArray, idx: usize) -> serde_json::Value {
    if array.is_null(idx) {
        return serde_json::Value::Null;
    }
    match array.data_type() {
        DataType::Float32 | DataType::Float64 => {
            let v = match array.data_type() {
                DataType::Float32 => array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(idx) as f64,
                _ => array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(idx),
            };
            if v.is_nan() {
                serde_json::Value::String("NaN".into())
            } else if v.is_infinite() {
                serde_json::Value::String(if v > 0.0 { "Infinity" } else { "-Infinity" }.into())
            } else {
                arrow_value_to_json(array, idx)
            }
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8 => arrow_value_to_json(array, idx),
        DataType::Binary => base64_value(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(idx),
        ),
        DataType::LargeBinary => base64_value(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(idx),
        ),
        DataType::FixedSizeBinary(_) => base64_value(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(idx),
        ),
        DataType::List(_) => nested_list(
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(idx)
                .as_ref(),
        ),
        DataType::LargeList(_) => nested_list(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .value(idx)
                .as_ref(),
        ),
        DataType::FixedSizeList(_, _) => nested_list(
            array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .value(idx)
                .as_ref(),
        ),
        DataType::Struct(_) => {
            let s = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut map = serde_json::Map::new();
            for (i, name) in s.column_names().iter().enumerate() {
                map.insert(
                    name.to_string(),
                    arrow_value_to_typed_json(s.column(i).as_ref(), idx),
                );
            }
            serde_json::Value::Object(map)
        }
        DataType::Map(_, _) => {
            let entries = array
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap()
                .value(idx);
            let pairs = (0..entries.len())
                .map(|i| {
                    let mut pair = serde_json::Map::new();
                    pair.insert(
                        "key".into(),
                        arrow_value_to_typed_json(entries.column(0).as_ref(), i),
                    );
                    pair.insert(
                        "value".into(),
                        arrow_value_to_typed_json(entries.column(1).as_ref(), i),
                    );
                    serde_json::Value::Object(pair)
                })
                .collect();
            serde_json::Value::Array(pairs)
        }
        DataType::Timestamp(unit, tz) => {
            use duckdb::arrow::array::{
                TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
                TimestampSecondArray,
            };
            use duckdb::arrow::datatypes::TimeUnit;
            use duckdb::arrow::temporal_conversions::{
                timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
                timestamp_us_to_datetime,
            };
            let any = array.as_any();
            let datetime = match unit {
                TimeUnit::Second => timestamp_s_to_datetime(
                    any.downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(idx),
                ),
                TimeUnit::Millisecond => timestamp_ms_to_datetime(
                    any.downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(idx),
                ),
                TimeUnit::Microsecond => timestamp_us_to_datetime(
                    any.downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(idx),
                ),
                TimeUnit::Nanosecond => timestamp_ns_to_datetime(
                    any.downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(idx),
                ),
            };
            match datetime {
                // DuckDB stores TIMESTAMPTZ as a UTC instant; the zone only affects display.
                Some(dt) => serde_json::Value::String(format!(
                    "{}{}",
                    dt.format("%Y-%m-%dT%H:%M:%S%.f"),
                    if tz.is_some() { "Z" } else { "" }
                )),
                None => serde_json::Value::Null,
            }
        }
        // Decimals, dates, times, intervals, dictionaries (ENUM): Arrow's formatter
        // renders these exactly.
        _ => match ArrayFormatter::try_new(array, &FormatOptions::default()) {
            Ok(formatter) => serde_json::Value::String(formatter.value(idx).to_string()),
            Err(_) => serde_json::Value::String(format!("[{:?}]", array.data_type())),
        },
    }
}

fn nested_list(values: &dyn ArrowArray) -> serde_json::Value {
    serde_json::Value::Array(
        (0..values.len())
            .map(|i| arrow_value_to_typed_json(values, i))
            .collect(),
    )
}

fn base64_string(bytes: &[u8]) -> String {
    match base64_value(bytes) {
        serde_json::Value::String(s) => s,
        _ => unreachable!(),
    }
}

fn base64_value(bytes: &[u8]) -> serde_json::Value {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    serde_json::Value::String(out)
}

fn arrow_value_to_json(array: &dyn ArrowArray, idx: usize) -> serde_json::Value {
    if array.is_null(idx) {
        return serde_json::Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(idx);
            serde_json::Value::Bool(v)
        }
        DataType::Int8 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Int16 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Int32 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Int64 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt8 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt16 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt32 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::UInt64 => serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(idx)
                .into(),
        ),
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(idx);
            serde_json::Number::from_f64(v as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(idx);
            serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Decimal128(_, scale) => {
            let raw = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(idx);
            let divisor = 10_f64.powi(*scale as i32);
            serde_json::Number::from_f64(raw as f64 / divisor)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Utf8 => serde_json::Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(idx)
                .to_string(),
        ),
        DataType::LargeUtf8 => serde_json::Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(idx)
                .to_string(),
        ),
        // Dates, timestamps, intervals, structs, lists — stringify for now
        other => serde_json::Value::String(format!("[{:?}]", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_athena_mode_sql_wrap() {
        let req = ExecuteRequest {
            sql: "SELECT * FROM table".to_string(),
            s3_endpoint: "http://floci:9000".to_string(),
            s3_region: Some("us-east-1".to_string()),
            s3_access_key: None,
            s3_secret_key: None,
            s3_use_ssl: None,
            s3_url_style: None,
            output_s3_path: Some("s3://bucket/output.csv".to_string()),
            setup_sql: None,
            variables: None,
        };

        let variables = req.variables.clone().unwrap_or_default();
        let sql = apply_variables(&req.sql, &variables);
        let output_path = req.output_s3_path.as_ref().unwrap();
        let final_sql = format!("COPY ({}) TO '{}' (FORMAT CSV, HEADER);", sql, output_path);
        assert_eq!(
            final_sql,
            "COPY (SELECT * FROM table) TO 's3://bucket/output.csv' (FORMAT CSV, HEADER);"
        );
    }

    #[test]
    fn test_variable_substitution() {
        let mut vars = std::collections::HashMap::new();
        vars.insert("table".to_string(), "my_table".to_string());
        vars.insert("limit".to_string(), "100".to_string());
        let sql = apply_variables("SELECT * FROM {{table}} LIMIT {{limit}}", &vars);
        assert_eq!(sql, "SELECT * FROM my_table LIMIT 100");
    }

    #[test]
    fn test_escape_sql() {
        assert_eq!(escape_sql("O'Brien"), "O''Brien");
        assert_eq!(escape_sql("normal"), "normal");
    }

    #[test]
    fn test_farm_fingerprint_matches_bigquery_for_string_and_bytes() {
        let conn = open_conn().unwrap();
        let (first, second, third, bytes, null_string, null_bytes): (
            i64,
            i64,
            i64,
            i64,
            Option<i64>,
            Option<i64>,
        ) = conn
            .query_row(
                "SELECT
                    farm_fingerprint('1footrue'),
                    farm_fingerprint('2applefalse'),
                    farm_fingerprint('3true'),
                    farm_fingerprint(CAST('1footrue' AS BLOB)),
                    farm_fingerprint(NULL::VARCHAR),
                    farm_fingerprint(NULL::BLOB)",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();

        // Values from BigQuery's FARM_FINGERPRINT documentation. The first and third
        // vectors also ensure hashes with the high bit set are returned as signed INT64.
        assert_eq!(first, -1_541_654_101_129_638_711);
        assert_eq!(second, 2_794_438_866_806_483_259);
        assert_eq!(third, -4_880_158_226_897_771_312);
        assert_eq!(bytes, first);
        assert_eq!(null_string, None);
        assert_eq!(null_bytes, None);
    }

    #[test]
    fn test_hll_functions_use_bigquery_compatible_zetasketch_bytes() {
        let conn = open_conn().unwrap();
        let (serialized, extracted, merged_raw): (String, i64, i64) = conn
            .query_row(
                "SELECT
                    to_base64(floci_hll_init(['ip1'], 15)),
                    floci_hll_extract(from_base64('CHAQARgCIAuCBwsQARgPIBQyA9bwNQ==')),
                    floci_hll_merge([
                        from_base64('CHAQARgCIAiCBwsQARgPIBQyA5ajLg=='),
                        from_base64('CHAQAhgCIAiCBw8QAhgPIBQyB6z8INWlrwE=')
                    ])",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(serialized, "CHAQARgCIAuCBwsQARgPIBQyA9bwNQ==");
        assert_eq!(extracted, 1);
        assert_eq!(merged_raw, 3);
        assert!(parse_hll(b"CHAQARgCIAuCBwsQARgPIBQyA9bwNQ==").is_err());
    }

    #[test]
    fn test_length_functions_distinguish_characters_from_bytes() {
        let conn = open_conn().unwrap();
        let lengths: (i64, i64, i64, i64, i64, i64, i64, i64) = conn
            .query_row(
                "SELECT
                   floci_length('hé'),
                   floci_length(from_hex('00FF')),
                   floci_byte_length('hé'),
                   floci_byte_length(from_hex('00FF')),
                   floci_length('é'),
                   floci_byte_length('é'),
                   floci_length('👨‍👩‍👧‍👦'),
                   floci_byte_length('👨‍👩‍👧‍👦')",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(lengths, (2, 2, 3, 2, 2, 3, 7, 25));
    }

    #[test]
    fn test_hll_functions_compose_through_an_aggregate_cte() {
        let conn = open_conn().unwrap();
        let (extracted, merged): (i64, i64) = conn
            .query_row(
                "WITH users(name) AS (VALUES ('ana'), ('bo'), ('cyd')),
                 sketches AS (
                   SELECT floci_hll_init(
                     list_filter(list(DISTINCT name), value -> value IS NOT NULL), 20
                   ) AS sketch
                   FROM users
                 )
                 SELECT floci_hll_extract(sketch),
                   (SELECT floci_hll_merge(list(value))
                    FROM UNNEST([sketch, sketch]) AS values(value))
                 FROM sketches",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(extracted, 3);
        assert_eq!(merged, 3);
    }

    #[test]
    fn test_hll_init_returns_null_without_non_null_inputs() {
        let conn = open_conn().unwrap();
        let (all_null, no_rows): (Option<Vec<u8>>, Option<Vec<u8>>) = conn
            .query_row(
                "SELECT
                   floci_hll_init(list_filter(list(value), value -> value IS NOT NULL), 15),
                   (SELECT floci_hll_init(list_filter(list(value), value -> value IS NOT NULL), 15)
                    FROM (SELECT 'never' AS value WHERE FALSE))
                 FROM (VALUES (CAST(NULL AS VARCHAR))) AS inputs(value)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(all_null, None);
        assert_eq!(no_rows, None);
    }

    #[test]
    fn test_blob_bitwise_operators_preserve_width_and_use_big_endian_bits() {
        let conn = open_conn().unwrap();
        let (shifted, masked, bit_is_set): (Vec<u8>, Vec<u8>, bool) = conn
            .query_row(
                "SELECT FROM_HEX('0102') << 4,
                        FROM_HEX('12f0') & FROM_HEX('0ff0'),
                        BIT_COUNT(FROM_HEX('10') &
                          (concat(repeat(FROM_HEX('00'), 0), FROM_HEX('01')) << 4)) != 0",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(shifted, vec![0x10, 0x20]);
        assert_eq!(masked, vec![0x02, 0xf0]);
        assert!(bit_is_set);
    }

    #[test]
    fn test_floci_to_json_string_uses_canonical_fractional_second_widths() {
        let conn = open_conn().unwrap();
        let (millis, datetime, timestamp): (String, String, String) = conn
            .query_row(
                "SELECT
                   floci_to_json_string(TIMESTAMP '2021-07-11 06:07:08.120000'),
                   floci_to_json_string(TIMESTAMP '2021-07-11 06:07:08.123400'),
                   floci_to_json_string(TIMESTAMPTZ '2021-07-11 06:07:08.123400+00')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(millis, r#""2021-07-11T06:07:08.120""#);
        assert_eq!(datetime, r#""2021-07-11T06:07:08.123400""#);
        assert_eq!(timestamp, r#""2021-07-11T06:07:08.123400Z""#);
    }

    #[test]
    fn test_floci_to_json_string_honors_pretty_print_and_propagates_nulls() {
        let conn = open_conn().unwrap();
        let (null_value, pretty, null_pretty): (Option<String>, String, Option<String>) = conn
            .query_row(
                "SELECT
                   floci_to_json_string(CAST(NULL AS VARCHAR)),
                   floci_to_json_string({'id': 1, 'coordinates': [10, 20]}, TRUE),
                   floci_to_json_string({'id': 1}, CAST(NULL AS BOOLEAN))",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(null_value, None);
        assert_eq!(
            pretty,
            "{\n  \"id\": 1,\n  \"coordinates\": [\n    10,\n    20\n  ]\n}"
        );
        assert_eq!(null_pretty, None);
    }

    #[test]
    fn test_floci_to_json_string_preserves_timestamp_types_recursively() {
        let conn = open_conn().unwrap();
        let (timestamp, timestamptz, nested, ordinary_string, null, decimal, negative_decimal): (
            String,
            String,
            String,
            String,
            Option<String>,
            String,
            String,
        ) = conn
            .query_row(
                "SELECT
                    floci_to_json_string(TIMESTAMP '2024-01-02 03:04:05.123456'),
                    floci_to_json_string(TIMESTAMPTZ '2024-01-02 03:04:05.123456+00'),
                    floci_to_json_string({
                        'at': TIMESTAMPTZ '2024-01-02 03:04:05.123456+00',
                        'local_at': TIMESTAMP '2024-01-03 04:05:06',
                        'events': [
                            TIMESTAMPTZ '2024-01-03 04:05:06+00',
                            NULL::TIMESTAMPTZ
                        ]
                    }),
                    floci_to_json_string('2024-01-02 03:04:05+00'),
                    floci_to_json_string(NULL::TIMESTAMPTZ),
                    floci_to_json_string(CAST('123.4500' AS DECIMAL(10, 4))),
                    floci_to_json_string(CAST('-0.0012' AS DECIMAL(10, 4)))",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(timestamp, "\"2024-01-02T03:04:05.123456\"");
        assert_eq!(timestamptz, "\"2024-01-02T03:04:05.123456Z\"");
        assert_eq!(
            nested,
            r#"{"at":"2024-01-02T03:04:05.123456Z","local_at":"2024-01-03T04:05:06","events":["2024-01-03T04:05:06Z",null]}"#
        );
        assert_eq!(ordinary_string, "\"2024-01-02 03:04:05+00\"");
        assert_eq!(null, None);
        assert_eq!(decimal, "123.45");
        assert_eq!(negative_decimal, "-0.0012");
    }

    #[test]
    fn test_floci_to_json_string_preserves_nested_json_values() {
        let conn = open_conn().unwrap();
        let encoded: String = conn
            .query_row(
                "SELECT floci_to_json_string([
                    JSON '1',
                    JSON '20',
                    JSON '[20, \"This is a string\", {\"a\": 1}]'
                ])",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(encoded, r#"[1,20,[20,"This is a string",{"a":1}]]"#);
    }

    #[test]
    fn test_json_extract_array_supports_default_path_and_input_overloads() {
        let conn = open_conn().unwrap();
        let (string_type, json_type, first): (String, String, String) = conn
            .query_row(
                "SELECT
                   typeof(json_extract_array('[1, {\"a\": 2}]')),
                   typeof(json_extract_array(JSON '[1, {\"a\": 2}]')),
                   json_extract_array('[1, {\"a\": 2}]')[1]",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(string_type, "VARCHAR[]");
        assert_eq!(json_type, "JSON[]");
        assert_eq!(first, "1");
    }

    #[test]
    fn test_to_json_string_matches_bigquery_numeric_encoding_boundaries() {
        let conn = open_conn().unwrap();
        let values: (String, String, String, String, String) = conn
            .query_row(
                "SELECT
                   floci_to_json_string(9007199254740992::BIGINT),
                   floci_to_json_string(9007199254740993::BIGINT),
                   floci_to_json_string('NaN'::DOUBLE),
                   floci_to_json_string('Infinity'::DOUBLE),
                   floci_to_json_string('-Infinity'::DOUBLE)",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(values.0, "9007199254740992");
        assert_eq!(values.1, r#""9007199254740993""#);
        assert_eq!(values.2, r#""NaN""#);
        assert_eq!(values.3, r#""Infinity""#);
        assert_eq!(values.4, r#""-Infinity""#);
    }

    #[test]
    fn test_query_returning_in_memory() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT 42 AS answer, 'hello' AS greeting")
            .unwrap();
        let batches: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["answer", "greeting"]);
        assert_eq!(batch.num_rows(), 1);

        let answer = arrow_value_to_json(batch.column(0).as_ref(), 0);
        let greeting = arrow_value_to_json(batch.column(1).as_ref(), 0);
        assert_eq!(answer, serde_json::Value::Number(42.into()));
        assert_eq!(greeting, serde_json::Value::String("hello".to_string()));
    }

    fn run(sql: &str, typed_values: bool) -> QueryOutput {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("SET TimeZone = 'UTC';").unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        let batches: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
        let columns = result_columns(&stmt);
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let mut map = serde_json::Map::new();
                for (col, field) in batch.schema().fields().iter().enumerate() {
                    let array = batch.column(col);
                    let value = if typed_values {
                        arrow_value_to_typed_json(array.as_ref(), row)
                    } else {
                        arrow_value_to_json(array.as_ref(), row)
                    };
                    map.insert(field.name().clone(), value);
                }
                rows.push(map);
            }
        }
        QueryOutput {
            columns,
            rows,
            followup: None,
            arrow: None,
        }
    }

    const TYPED_SQL: &str = "SELECT 1.25::DECIMAL(38,9) AS num, DATE '2024-01-02' AS d, \
        TIMESTAMPTZ '2024-01-02 03:04:05.123456+00' AS ts, TIMESTAMP '2024-01-02 03:04:05' AS dt, \
        [1, 2] AS arr, {'x': 1, 'y': 'z'} AS st, 'hi'::BLOB AS b, count_if(true) AS c, \
        'NaN'::DOUBLE AS nan";

    #[test]
    fn test_columns_report_duckdb_types() {
        let out = run(TYPED_SQL, false);
        let types: Vec<(&str, &str)> = out
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.type_name.as_str()))
            .collect();
        assert_eq!(
            types,
            vec![
                ("num", "DECIMAL(38,9)"),
                ("d", "DATE"),
                ("ts", "TIMESTAMP WITH TIME ZONE"),
                ("dt", "TIMESTAMP"),
                ("arr", "INTEGER[]"),
                ("st", "STRUCT(\"x\" INTEGER, \"y\" VARCHAR)"),
                ("b", "BLOB"),
                ("c", "HUGEINT"),
                ("nan", "DOUBLE"),
            ]
        );
    }

    #[test]
    fn test_described_types_keep_type_aliases() {
        let conn = Connection::open_in_memory().unwrap();
        let sql = "SELECT '{\"a\": 1}'::JSON AS j, 1::HUGEINT AS h, 'x' AS s";
        let mut stmt = conn.prepare(sql).unwrap();
        let _: Vec<RecordBatch> = stmt.query_arrow([]).unwrap().collect();
        let columns = with_described_types(&conn, sql, result_columns(&stmt));
        let types: Vec<&str> = columns.iter().map(|c| c.type_name.as_str()).collect();
        assert_eq!(types, vec!["JSON", "HUGEINT", "VARCHAR"]);

        // DML cannot be described; the Arrow-derived names remain.
        conn.execute_batch("CREATE TABLE t (x INTEGER);").unwrap();
        let mut insert = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
        let _: Vec<RecordBatch> = insert.query_arrow([]).unwrap().collect();
        let columns =
            with_described_types(&conn, "INSERT INTO t VALUES (1)", result_columns(&insert));
        assert_eq!(columns[0].type_name, "BIGINT");
    }

    #[test]
    fn test_columns_present_for_empty_results() {
        let out = run("SELECT 1 AS a, 'x' AS b WHERE false", false);
        assert!(out.rows.is_empty());
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].type_name, "INTEGER");
    }

    #[test]
    fn test_typed_values_are_lossless() {
        let row = &run(TYPED_SQL, true).rows[0];
        assert_eq!(row["num"], serde_json::json!("1.250000000"));
        assert_eq!(row["d"], serde_json::json!("2024-01-02"));
        assert_eq!(row["ts"], serde_json::json!("2024-01-02T03:04:05.123456Z"));
        assert_eq!(row["dt"], serde_json::json!("2024-01-02T03:04:05"));
        assert_eq!(row["arr"], serde_json::json!([1, 2]));
        assert_eq!(row["st"], serde_json::json!({"x": 1, "y": "z"}));
        assert_eq!(row["b"], serde_json::json!("aGk="));
        assert_eq!(row["c"], serde_json::json!("1"));
        assert_eq!(row["nan"], serde_json::json!("NaN"));
    }

    #[test]
    fn test_untyped_values_keep_the_legacy_encoding() {
        // Existing /query callers (floci S3 Select) must see exactly the old output.
        let row = &run(TYPED_SQL, false).rows[0];
        assert_eq!(row["num"], serde_json::json!(1.25));
        assert_eq!(row["d"], serde_json::json!("[Date32]"));
        assert!(row["arr"].as_str().unwrap().starts_with("[List("));
        assert_eq!(row["nan"], serde_json::Value::Null);
    }

    #[test]
    fn test_ip_to_string_formats_ipv4_and_ipv6() {
        let conn = open_conn().unwrap();
        let (ipv4, ipv6): (String, String) = conn
            .query_row(
                "SELECT
                    ip_to_string(ip_from_string('123.123.123.123')),
                    ip_to_string(safe_ip_from_string('1111:1111:1111:1111::'))",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(ipv4, "123.123.123.123");
        assert_eq!(ipv6, "1111:1111:1111:1111::");
    }

    #[test]
    fn test_net_functions_require_raw_ip_bytes_and_accept_int64_prefixes() {
        let conn = open_conn().unwrap();
        let (address, truncated): (String, Vec<u8>) = conn
            .query_row(
                "SELECT ip_to_string(FROM_HEX('01020304')), ip_trunc(FROM_HEX('ffff1234'), 16::BIGINT)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(address, "1.2.3.4");
        assert_eq!(truncated, vec![0xff, 0xff, 0, 0]);
        assert!(
            conn.query_row("SELECT ip_to_string(CAST('AQIDBA==' AS BLOB))", [], |_| Ok(
                ()
            ))
            .is_err()
        );
    }

    #[test]
    fn test_followup_sees_the_changes_of_the_first_statement() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, name);",
        )
        .unwrap();
        let (columns, rows) =
            run_statement(&conn, "UPDATE t SET name = 'x' WHERE id = 2", true).unwrap();
        assert_eq!(columns[0].name, "Count");
        assert_eq!(rows[0]["Count"], serde_json::json!(1));

        let (columns, rows) = run_statement(&conn, "SELECT * FROM t ORDER BY id", true).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(rows[1]["name"], serde_json::json!("x"));
    }

    #[test]
    fn test_arrow_ipc_messages_round_trip() {
        use duckdb::arrow::datatypes::DataType as DT;
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("SET TimeZone = 'UTC';").unwrap();
        let (columns, arrow) = run_statement_arrow(
            &conn,
            "SELECT * FROM (VALUES (1::BIGINT, 'a', TIMESTAMPTZ '2024-01-02 03:04:05+00'), \
             (2, 'b', NULL)) v(id, name, ts)",
        )
        .unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(arrow.batches.iter().map(|b| b.row_count).sum::<usize>(), 2);
        // Every message is an encapsulated IPC message: continuation marker, then metadata length.
        let schema = decode_base64(&arrow.schema);
        assert_eq!(&schema[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
        let parsed = arrow_ipc::convert::try_schema_from_ipc_buffer(&schema).unwrap();
        assert_eq!(parsed.field(0).data_type(), &DT::Int64);
        assert_eq!(
            parsed.field(2).data_type(),
            &DT::Timestamp(
                duckdb::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into())
            )
        );
        let batch = decode_base64(&arrow.batches[0].data);
        assert_eq!(&batch[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
    }

    fn decode_base64(text: &str) -> Vec<u8> {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::new();
        let mut buffer = 0u32;
        let mut bits = 0;
        for byte in text.bytes().filter(|b| *b != b'=') {
            let value = ALPHABET.iter().position(|c| *c == byte).unwrap() as u32;
            buffer = (buffer << 6) | value;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buffer >> bits) as u8);
                buffer &= (1 << bits) - 1;
            }
        }
        out
    }

    #[test]
    fn test_base64() {
        assert_eq!(base64_value(b""), serde_json::json!(""));
        assert_eq!(base64_value(b"f"), serde_json::json!("Zg=="));
        assert_eq!(base64_value(b"fo"), serde_json::json!("Zm8="));
        assert_eq!(base64_value(b"foo"), serde_json::json!("Zm9v"));
    }
}
