// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod compacted_key_encoder;
mod compacted_row_encoder;
mod iceberg_key_encoder;
mod paimon_key_encoder;

use crate::error::{Error, Result};
use crate::metadata::{DataLakeFormat, KvFormat, RowType};
use crate::row::encode::compacted_key_encoder::CompactedKeyEncoder;
use crate::row::encode::compacted_row_encoder::CompactedRowEncoder;
use crate::row::encode::iceberg_key_encoder::IcebergKeyEncoder;
use crate::row::encode::paimon_key_encoder::PaimonKeyEncoder;
use crate::row::{Datum, InternalRow};
use bytes::Bytes;

/// An interface for encoding key of row into bytes.
#[allow(dead_code)]
pub trait KeyEncoder: Send + Sync {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes>;
}

pub struct KeyEncoderFactory;

impl KeyEncoderFactory {
    /// Create a key encoder for the primary key, mirroring Java's
    /// `KeyEncoder.ofPrimaryKeyEncoder`.
    ///
    /// Use this encoder when encoding the primary key for KV writes/lookups,
    /// or when encoding a prefix-lookup key (which is a strict prefix of the
    /// primary key and must share its byte layout to support lexicographic
    /// prefix matching).
    ///
    /// - `kv_format_version == 2` with `is_default_bucket_key == false`
    ///   (i.e. the primary key differs from the bucket key) →
    ///   `CompactedKeyEncoder::create_key_encoder` regardless of lake format
    ///   (primary key must be decodable by the server without lake-specific
    ///   format awareness).
    /// - Otherwise (`kv_format_version == 1`, or `kv_format_version == 2`
    ///   with `is_default_bucket_key == true` so the primary key matches the
    ///   bucket key) → align with the lake format via `Self::of`, producing
    ///   the same physical layout as the bucket key.
    /// - Any other `kv_format_version` → [`Error::UnsupportedOperation`].
    pub fn of_primary_key_encoder(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
        kv_format_version: i32,
        is_default_bucket_key: bool,
    ) -> Result<Box<dyn KeyEncoder>> {
        match kv_format_version {
            1 => Self::of(row_type, key_fields, data_lake_format),
            2 if is_default_bucket_key => Self::of(row_type, key_fields, data_lake_format),
            2 => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
            v => Err(Error::UnsupportedOperation {
                message: format!("Unsupported kv format version: {v}"),
            }),
        }
    }

    /// Create a key encoder for the bucket key, mirroring Java's
    /// `KeyEncoder.ofBucketKeyEncoder`.
    ///
    /// Use this encoder when computing the bucket id for a row. The bucket
    /// key encoding is bound to the configured data lake format (delegating
    /// to `Self::of`) because the resulting bucket id must stay consistent
    /// with the bucket id computed by the downstream lake (e.g. Paimon /
    /// Lance / Iceberg); otherwise the same row would land in different
    /// buckets on the Fluss side and on the lake side, breaking lake-tiering
    /// semantics.
    ///
    /// For Iceberg, only the first bucket key column is encoded, because
    /// Iceberg's `bucket[N]` transform takes a single column, mirroring Java's
    /// `IcebergBucketing.hashKey`.
    pub fn of_bucket_key_encoder(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyEncoder>> {
        match data_lake_format {
            Some(DataLakeFormat::Iceberg) => {
                let hash_key = std::slice::from_ref(iceberg_key_encoder::hash_key(key_fields)?);
                Ok(Box::new(IcebergKeyEncoder::new(row_type, hash_key)?))
            }
            _ => Self::of(row_type, key_fields, data_lake_format),
        }
    }

    /// Create a key encoder to encode the key bytes of the input row.
    /// # Arguments
    /// * `row_type` - the row type of the input row
    /// * `key_fields` - the key fields to encode
    /// * `lake_format` - the data lake format
    ///
    /// # Returns
    /// key encoder
    fn of(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyEncoder>> {
        match data_lake_format {
            Some(DataLakeFormat::Paimon) => {
                Ok(Box::new(PaimonKeyEncoder::new(row_type, key_fields)?))
            }
            Some(DataLakeFormat::Lance) => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
            Some(DataLakeFormat::Iceberg) => {
                Ok(Box::new(IcebergKeyEncoder::new(row_type, key_fields)?))
            }
            None => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
        }
    }
}

/// An encoder to write binary row data. It's used to write rows
/// one by one. When writing a new row:
///
/// 1. call method [`RowEncoder::start_new_row()`] to start the writing.
/// 2. call method [`RowEncoder::encode_field()`] to write the row's field.
/// 3. call method [`RowEncoder::finish_row()`] to finish the writing and get the written row.
#[allow(dead_code)]
pub trait RowEncoder: Send + Sync {
    /// Start to write a new row.
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn start_new_row(&mut self) -> Result<()>;

    /// Write the row's field in given pos with given value.
    ///
    /// # Arguments
    /// * pos - the position of the field to write.
    /// * value - the value of the field to write.
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn encode_field(&mut self, pos: usize, value: Datum) -> Result<()>;

    /// Finish write the row, returns the written row.
    ///
    /// Note that returned row borrows from [`RowEncoder`]'s internal buffer which is reused for subsequent rows
    /// [`RowEncoder::start_new_row()`] should only be called after the returned row goes out of scope.
    ///
    /// # Returns
    /// * the written row
    fn finish_row(&mut self) -> Result<Bytes>;

    /// Closes the row encoder
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn close(&mut self) -> Result<()>;
}

#[allow(dead_code)]
pub struct RowEncoderFactory {}

#[allow(dead_code)]
impl RowEncoderFactory {
    pub fn create(kv_format: KvFormat, row_type: RowType) -> Result<impl RowEncoder> {
        Self::create_for_field_types(kv_format, row_type)
    }

    pub fn create_for_field_types(
        kv_format: KvFormat,
        row_type: RowType,
    ) -> Result<impl RowEncoder> {
        match kv_format {
            KvFormat::INDEXED => {
                todo!()
            }
            KvFormat::COMPACTED => CompactedRowEncoder::new(row_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucketing::BucketingFunction;
    use crate::metadata::DataTypes;
    use crate::row::GenericRow;

    const ICEBERG: Option<DataLakeFormat> = Some(DataLakeFormat::Iceberg);
    const PRIMARY_KEY: [&str; 3] = ["id", "tenant", "item"];
    const BUCKET_KEY: [&str; 2] = ["id", "tenant"];

    fn order_items_row_type() -> RowType {
        RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string(), DataTypes::string()],
            vec!["id", "tenant", "item"],
        )
    }

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn row(id: i32, tenant: &'static str, item: &'static str) -> GenericRow<'static> {
        let data: Vec<Datum<'static>> = vec![id.into(), tenant.into(), item.into()];
        GenericRow::from_data(data)
    }

    fn iceberg_bucket_key_encoder(bucket_keys: &[&str]) -> Result<Box<dyn KeyEncoder>> {
        let row_type = order_items_row_type();
        KeyEncoderFactory::of_bucket_key_encoder(&row_type, &keys(bucket_keys), &ICEBERG)
    }

    fn iceberg_pk_encoder(fields: &[&str], is_default: bool) -> Result<Box<dyn KeyEncoder>> {
        let row_type = order_items_row_type();
        let fields = keys(fields);
        KeyEncoderFactory::of_primary_key_encoder(&row_type, &fields, &ICEBERG, 2, is_default)
    }

    #[test]
    fn iceberg_composite_bucket_key_routes_by_first_column() {
        // Frozen golden vectors of Java's FlussLookupInputPartitionerTest for an
        // Iceberg INT bucket key with 16 buckets: a composite bucket key must route
        // exactly like its first column, whatever the other columns are.
        let mut encoder = iceberg_bucket_key_encoder(&BUCKET_KEY).unwrap();
        let bucketing = <dyn BucketingFunction>::of(ICEBERG.as_ref());
        for (id, expected_bucket) in [(1, 4), (2, 4), (42, 14), (100, 0), (12345, 1)] {
            for tenant in ["a", "tenant-b", ""] {
                let encoded = encoder.encode_key(&row(id, tenant, "i")).unwrap();
                assert_eq!(
                    bucketing.bucketing(&encoded, 16).unwrap(),
                    expected_bucket,
                    "id={id} tenant={tenant}"
                );
            }
        }
    }

    #[test]
    fn iceberg_single_column_bucket_key_is_unchanged() {
        let row_type = order_items_row_type();
        let mut composite = iceberg_bucket_key_encoder(&BUCKET_KEY).unwrap();
        let mut single = IcebergKeyEncoder::new(&row_type, &keys(&["id"])).unwrap();
        let row = row(42, "tenant-b", "i");
        assert_eq!(
            composite.encode_key(&row).unwrap(),
            single.encode_key(&row).unwrap()
        );
    }

    #[test]
    fn iceberg_bucket_key_encoder_rejects_empty_keys() {
        let error = match iceberg_bucket_key_encoder(&[]) {
            Ok(_) => panic!("expected IllegalArgument"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Bucket keys must not be empty"));
    }

    #[test]
    fn iceberg_composite_primary_and_prefix_keys_are_compacted() {
        // A supported composite bucket key is a strict prefix of the primary key on
        // kv format version 2, so the primary key and the prefix lookup key use the
        // compacted encoding: the prefix bytes are a prefix of the primary key bytes.
        let mut primary_key_encoder = iceberg_pk_encoder(&PRIMARY_KEY, false).unwrap();
        let mut prefix_key_encoder = iceberg_pk_encoder(&BUCKET_KEY, false).unwrap();
        let row = row(1, "tenant-a", "i");
        let primary_key = primary_key_encoder.encode_key(&row).unwrap();
        let prefix_key = prefix_key_encoder.encode_key(&row).unwrap();
        assert!(primary_key.starts_with(&prefix_key));

        // ("ab", "c") and ("a", "bc") would collide in the Iceberg key encoding.
        let first = primary_key_encoder.encode_key(&row(1, "ab", "c")).unwrap();
        let second = primary_key_encoder.encode_key(&row(1, "a", "bc")).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn iceberg_composite_default_bucket_key_is_rejected() {
        // The server rejects composite default bucket keys on Iceberg tables. The
        // client fails too, instead of reusing the primary key encoder for bucketing.
        let error = match iceberg_pk_encoder(&BUCKET_KEY, true) {
            Ok(_) => panic!("expected IllegalArgument"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("Key fields must have exactly one field for iceberg format")
        );
    }
}
