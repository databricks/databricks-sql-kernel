// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Derive `num_modified_rows` for SEA DML statements from the result set.
//!
//! Thrift surfaces `numModifiedRows` on the operation **status**
//! (`TGetOperationStatusResp`, populated by the server on the terminal
//! status of a DML). The SEA REST API does **not** put a row count on its
//! status envelope at all — it returns the count as the statement's
//! *result set*, a single row of `num_*_rows` counters:
//!
//! | Statement | Result columns                                                          |
//! |-----------|-------------------------------------------------------------------------|
//! | INSERT    | `num_affected_rows`, `num_inserted_rows`                                |
//! | UPDATE    | `num_affected_rows`                                                      |
//! | DELETE    | `num_affected_rows`                                                      |
//! | MERGE     | `num_affected_rows`, `num_updated_rows`, `num_deleted_rows`, `num_inserted_rows` |
//! | COPY INTO, CTAS, RTAS | `num_affected_rows`, `num_inserted_rows`                     |
//!
//! `num_affected_rows` is the total, matching Thrift's `numModifiedRows`.
//! Reading it here lets the kernel's `num_modified_rows()` accessor reach
//! Thrift parity. The read is **non-consuming** (via
//! [`ResultStream::peek_first_batch`]) so the count row still flows to the
//! caller's fetch exactly as it does on the Thrift path — this only adds
//! the status value, it does not change which rows are returned.

use crate::kernel_error::Result;
use crate::result_stream::{ResultBatch, ResultStream};
use crate::types::sea::ResultManifest;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, Int8Type};
use arrow_array::Array;
use arrow_schema::DataType;

/// The exact set of column names a SEA DML count result is built from.
const DML_COUNT_COLUMNS: [&str; 4] = [
    "num_affected_rows",
    "num_inserted_rows",
    "num_updated_rows",
    "num_deleted_rows",
];

/// The total-affected column whose value maps to `num_modified_rows`.
const AFFECTED_ROWS_COLUMN: &str = "num_affected_rows";

/// SEA result aliases are ambiguous, so row-count extraction also requires a
/// statement form that can modify rows.
pub(crate) fn sql_may_modify_rows(sql: &str) -> bool {
    let mut cursor = SqlCursor::new(sql);
    let Some(first) = cursor.next_keyword() else {
        return false;
    };
    if first.eq_ignore_ascii_case("WITH") {
        if cursor.skip_ctes().is_none() {
            return false;
        }
        let Some(outer) = cursor.next_keyword() else {
            return false;
        };
        return cursor.statement_may_modify_rows(outer);
    }
    cursor.statement_may_modify_rows(first)
}

fn is_dml_keyword(keyword: &str) -> bool {
    ["INSERT", "UPDATE", "DELETE", "MERGE"]
        .iter()
        .any(|candidate| keyword.eq_ignore_ascii_case(candidate))
}

struct SqlCursor<'a> {
    sql: &'a str,
    index: usize,
}

fn is_identifier_char(character: char) -> bool {
    character == '_' || character.is_alphabetic() || character.is_ascii_digit()
}

impl<'a> SqlCursor<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, index: 0 }
    }

    fn current_char(&self) -> Option<char> {
        self.sql[self.index..].chars().next()
    }

    fn advance_char(&mut self) {
        if let Some(character) = self.current_char() {
            self.index += character.len_utf8();
        }
    }

    fn is_raw_string(&self) -> bool {
        let mut preceding = self.sql[..self.index].chars().rev();
        if !preceding.next().is_some_and(|c| matches!(c, 'R' | 'r')) {
            return false;
        }
        !preceding.next().is_some_and(is_identifier_char)
    }

    fn skip_quoted(&mut self, quote: char, backslash_escaped: bool, doubled_quote: bool) {
        self.index += 1;
        while let Some(character) = self.current_char() {
            if backslash_escaped && character == '\\' {
                self.advance_char();
                self.advance_char();
            } else if character == quote {
                self.advance_char();
                if doubled_quote && self.current_char() == Some(quote) {
                    self.advance_char();
                } else {
                    break;
                }
            } else {
                self.advance_char();
            }
        }
    }

    fn skip_string(&mut self, quote: char) {
        let raw = self.is_raw_string();
        self.skip_quoted(quote, !raw, !raw);
    }

    fn skip_line_comment(&mut self) {
        self.index += 2;
        while self.index < self.sql.len() {
            if self.sql.as_bytes()[self.index..].starts_with(b"\\\n") {
                self.index += 2;
            } else if matches!(self.current_char(), Some('\n' | '\r')) {
                let has_carriage_return = self.current_char() == Some('\r');
                self.advance_char();
                if has_carriage_return && self.current_char() == Some('\n') {
                    self.advance_char();
                }
                break;
            } else {
                self.advance_char();
            }
        }
    }

    fn skip_block_comment(&mut self) {
        let bytes = self.sql.as_bytes();
        self.index += 2;
        let mut comment_depth = 1usize;
        while self.index < bytes.len() && comment_depth > 0 {
            if bytes[self.index..].starts_with(b"/*") {
                comment_depth += 1;
                self.index += 2;
            } else if bytes[self.index..].starts_with(b"*/") {
                comment_depth -= 1;
                self.index += 2;
            } else {
                self.advance_char();
            }
        }
    }

    fn skip_trivia(&mut self) {
        while self.index < self.sql.len() {
            if self.sql.as_bytes()[self.index..].starts_with(b"--") {
                self.skip_line_comment();
                continue;
            }
            if self.sql.as_bytes()[self.index..].starts_with(b"/*") {
                self.skip_block_comment();
                continue;
            }
            if self.current_char().is_some_and(char::is_whitespace) {
                self.advance_char();
                continue;
            }
            break;
        }
    }

    fn next_keyword(&mut self) -> Option<&'a str> {
        self.skip_trivia();
        let start = self.index;
        if !self.current_char().is_some_and(is_identifier_char) {
            return None;
        }
        self.advance_char();
        while self.current_char().is_some_and(is_identifier_char) {
            self.advance_char();
        }
        Some(&self.sql[start..self.index])
    }

    fn next_top_level_keyword(&mut self) -> Option<&'a str> {
        loop {
            self.skip_trivia();
            match self.current_char()? {
                quote @ ('\'' | '"') => self.skip_string(quote),
                '`' => self.skip_quoted('`', false, true),
                '(' => self.skip_parenthesized()?,
                character if is_identifier_char(character) => return self.next_keyword(),
                _ => self.advance_char(),
            }
        }
    }

    fn statement_may_modify_rows(&mut self, keyword: &str) -> bool {
        if is_dml_keyword(keyword) {
            return true;
        }
        if keyword.eq_ignore_ascii_case("FROM") {
            return self.has_multi_insert_body();
        }
        if keyword.eq_ignore_ascii_case("COPY") {
            return self.consume_keyword("INTO");
        }
        if keyword.eq_ignore_ascii_case("CREATE") {
            return self.is_create_table_as_select();
        }
        if keyword.eq_ignore_ascii_case("REPLACE") {
            return self.consume_keyword("TABLE") && self.has_as_query();
        }
        if keyword.eq_ignore_ascii_case("EXECUTE") && self.consume_keyword("IMMEDIATE") {
            return self.take_string_literal().is_some_and(sql_may_modify_rows);
        }
        false
    }

    fn is_create_table_as_select(&mut self) -> bool {
        if self.consume_keyword("OR") && !self.consume_keyword("REPLACE") {
            return false;
        }
        if !self.consume_keyword("TEMP") {
            self.consume_keyword("TEMPORARY");
        }
        self.consume_keyword("TABLE") && self.has_as_query()
    }

    fn has_as_query(&mut self) -> bool {
        while let Some(keyword) = self.next_top_level_keyword() {
            if keyword.eq_ignore_ascii_case("AS") {
                return self.next_top_level_keyword().is_some_and(|query| {
                    ["SELECT", "TABLE", "VALUES", "WITH", "FROM"]
                        .iter()
                        .any(|candidate| query.eq_ignore_ascii_case(candidate))
                });
            }
        }
        false
    }

    fn take_string_literal(&mut self) -> Option<&'a str> {
        self.skip_trivia();
        if self.current_char().is_some_and(|c| matches!(c, 'R' | 'r')) {
            let prefix = self.index;
            self.advance_char();
            if !matches!(self.current_char(), Some('\'' | '"')) {
                self.index = prefix;
                return None;
            }
        }
        let quote = self.current_char()?;
        if !matches!(quote, '\'' | '"') {
            return None;
        }
        let raw = self.is_raw_string();
        self.advance_char();
        let start = self.index;
        while let Some(character) = self.current_char() {
            if !raw && character == '\\' {
                self.advance_char();
                self.advance_char();
            } else if character == quote {
                let end = self.index;
                self.advance_char();
                if !raw && self.current_char() == Some(quote) {
                    self.advance_char();
                } else {
                    return Some(&self.sql[start..end]);
                }
            } else {
                self.advance_char();
            }
        }
        None
    }

    fn has_multi_insert_body(&mut self) -> bool {
        while let Some(keyword) = self.next_top_level_keyword() {
            if keyword.eq_ignore_ascii_case("SELECT") {
                return false;
            }
            if keyword.eq_ignore_ascii_case("INSERT") {
                if self.consume_keyword("WITH")
                    && (!self.consume_keyword("SCHEMA") || !self.consume_keyword("EVOLUTION"))
                {
                    continue;
                }
                if self.consume_keyword("OVERWRITE") {
                    return true;
                }
                self.consume_keyword("ONCE");
                if self.consume_keyword("INTO") {
                    self.consume_keyword("ONCE");
                    return true;
                }
            }
        }
        false
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        let start = self.index;
        let matched = self
            .next_keyword()
            .is_some_and(|keyword| keyword.eq_ignore_ascii_case(expected));
        if !matched {
            self.index = start;
        }
        matched
    }

    fn skip_identifier(&mut self) -> Option<()> {
        self.skip_trivia();
        match self.current_char()? {
            quote @ ('"' | '`') => {
                self.skip_quoted(quote, quote == '"', true);
                Some(())
            }
            character if is_identifier_char(character) => {
                self.next_keyword()?;
                Some(())
            }
            _ => None,
        }
    }

    fn skip_parenthesized(&mut self) -> Option<()> {
        self.skip_trivia();
        if self.current_char() != Some('(') {
            return None;
        }
        self.advance_char();
        let mut depth = 1usize;
        while self.index < self.sql.len() {
            if self.sql.as_bytes()[self.index..].starts_with(b"--") {
                self.skip_line_comment();
            } else if self.sql.as_bytes()[self.index..].starts_with(b"/*") {
                self.skip_block_comment();
            } else {
                match self.current_char()? {
                    quote @ ('\'' | '"') => self.skip_string(quote),
                    '`' => self.skip_quoted('`', false, true),
                    '(' => {
                        depth += 1;
                        self.advance_char();
                    }
                    ')' => {
                        depth -= 1;
                        self.advance_char();
                        if depth == 0 {
                            return Some(());
                        }
                    }
                    _ => self.advance_char(),
                }
            }
        }
        None
    }

    fn skip_integer_value(&mut self) -> bool {
        self.skip_trivia();
        if self.current_char() == Some('?') {
            self.advance_char();
            return true;
        }
        if self.current_char() == Some(':') {
            self.advance_char();
            return self.skip_identifier().is_some();
        }
        let start = self.index;
        while self.current_char().is_some_and(|c| c.is_ascii_digit()) {
            self.advance_char();
        }
        self.index > start
    }

    fn skip_max_recursion_level(&mut self) -> Option<bool> {
        if !self.consume_keyword("MAX") {
            return Some(false);
        }
        if !self.consume_keyword("RECURSION")
            || !self.consume_keyword("LEVEL")
            || !self.skip_integer_value()
        {
            return None;
        }
        Some(true)
    }

    fn skip_ctes(&mut self) -> Option<()> {
        self.consume_keyword("RECURSIVE");
        loop {
            self.skip_identifier()?;
            self.skip_trivia();

            if self.current_char() == Some('(') {
                self.skip_parenthesized()?;
                self.skip_trivia();
                let body_follows = if self.skip_max_recursion_level()? {
                    self.consume_keyword("AS");
                    true
                } else {
                    self.consume_keyword("AS") || self.current_char() == Some('(')
                };
                if body_follows {
                    self.skip_parenthesized()?;
                }
            } else {
                self.skip_max_recursion_level()?;
                self.consume_keyword("AS");
                self.skip_parenthesized()?;
            }

            self.skip_trivia();
            if self.current_char() != Some(',') {
                return Some(());
            }
            self.advance_char();
        }
    }
}

/// Does this result `manifest` describe a DML row-count result?
///
/// Same strict signature as [`read_dml_affected_rows`], but checked against
/// the manifest's column metadata so a caller can decide whether the inline
/// result is worth decoding *before* building a reader. The submitted SQL must
/// still be checked because a SELECT can use these exact aliases.
pub(crate) fn manifest_is_dml_count(manifest: &ResultManifest) -> bool {
    let cols = &manifest.schema.columns;
    !cols.is_empty()
        && cols
            .iter()
            .all(|c| DML_COUNT_COLUMNS.contains(&c.name.as_str()))
        && cols.iter().any(|c| c.name == AFFECTED_ROWS_COLUMN)
}

/// If `stream`'s result set is a DML row-count result, return its
/// `num_affected_rows` value; otherwise `None`.
///
/// Detection is deliberately strict: **every** column must be one of the known
/// `num_*_rows` counters and `num_affected_rows` must be present. This shape is
/// still ambiguous with a SELECT that uses the same aliases, so callers must
/// also gate extraction on the submitted SQL. Thrift does not face this
/// ambiguity because its count comes from operation metadata.
///
/// Non-consuming: peeks the first batch and buffers it, so the caller's
/// subsequent fetch still sees the row.
pub(crate) async fn read_dml_affected_rows(stream: &mut ResultStream) -> Result<Option<i64>> {
    let schema = stream.schema();
    let fields = schema.fields();
    if fields.is_empty() {
        return Ok(None);
    }
    if !fields
        .iter()
        .all(|f| DML_COUNT_COLUMNS.contains(&f.name().as_str()))
    {
        return Ok(None);
    }
    let Some(col_idx) = fields.iter().position(|f| f.name() == AFFECTED_ROWS_COLUMN) else {
        return Ok(None);
    };

    let Some(ResultBatch::Arrow(batch)) = stream.peek_first_batch().await? else {
        return Ok(None);
    };
    // A DML count result is always exactly one summary row.
    if batch.num_rows() != 1 {
        return Ok(None);
    }
    let col = batch.column(col_idx);
    if col.is_null(0) {
        return Ok(None);
    }
    // The server types these BIGINT (Arrow Int64); accept the narrower
    // signed widths defensively, and ignore anything else rather than
    // mis-reporting.
    let value = match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().value(0),
        DataType::Int32 => col.as_primitive::<Int32Type>().value(0) as i64,
        DataType::Int16 => col.as_primitive::<Int16Type>().value(0) as i64,
        DataType::Int8 => col.as_primitive::<Int8Type>().value(0) as i64,
        _ => return Ok(None),
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::InMemoryReader;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn classifies_statements_that_may_modify_rows() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "-- comment\nUPDATE t SET value = 1",
            "/* outer /* nested */ comment */ DELETE FROM t",
            "merge into t using s on t.id = s.id when matched then delete",
            "WITH source AS (SELECT 1) INSERT INTO t SELECT * FROM source",
            "WITH source (SELECT 1) INSERT INTO t SELECT * FROM source",
            "WITH source(id) (SELECT 1) INSERT INTO t SELECT * FROM source",
            "WITH source MAX RECURSION LEVEL 10 AS (SELECT 1) UPDATE t SET value = 1",
            "WITH source(id) MAX RECURSION LEVEL :limit (SELECT 1) DELETE FROM t",
            "WITH source MAX RECURSION LEVEL ? AS (SELECT 1) MERGE INTO t USING source ON false WHEN NOT MATCHED THEN INSERT *",
            "WITH source AS (SELECT ') UPDATE t' AS value) UPDATE t SET value = 1",
            "WITH source AS (SELECT R'\\' AS value) UPDATE t SET value = 1",
            "WITH `source)`(`id)`) AS (SELECT 1) DELETE FROM t",
            "WITH café AS (SELECT 1) UPDATE t SET value = 1",
            "WITH `source\\` AS (SELECT 1) DELETE FROM t",
            "WITH table AS (SELECT 1) UPDATE t SET value = 1",
            "FROM source INSERT INTO target SELECT * FROM source",
            "WITH source AS (SELECT 1) FROM source INSERT INTO target SELECT * FROM source",
            "FROM source INSERT WITH SCHEMA EVOLUTION OVERWRITE target SELECT * FROM source",
            "FROM source INSERT ONCE INTO ONCE target SELECT * FROM source",
            "FROM insert INSERT INTO target SELECT * FROM insert",
            "FROM source AS insert INSERT INTO target SELECT * FROM source",
            "COPY INTO target FROM '/Volumes/catalog/schema/volume'",
            "CREATE TABLE target USING DELTA AS SELECT 1",
            "CREATE OR REPLACE TABLE target AS SELECT 1",
            "CREATE TEMPORARY TABLE target AS VALUES (1)",
            "REPLACE TABLE target AS WITH source AS (SELECT 1) SELECT * FROM source",
            "EXECUTE IMMEDIATE 'INSERT INTO target VALUES (1)'",
            "EXECUTE IMMEDIATE R'UPDATE target SET value = 1'",
        ] {
            assert!(sql_may_modify_rows(sql), "expected modifying SQL: {sql}");
        }
    }

    #[test]
    fn rejects_queries_and_dml_words_inside_ctes() {
        for sql in [
            "SELECT 7 AS num_affected_rows",
            "WITH source AS (SELECT 'INSERT INTO t' AS value) SELECT * FROM source",
            "WITH source AS (SELECT 1 -- \\\n) UPDATE t\n) SELECT * FROM source",
            "WITH update AS (SELECT 7 AS num_affected_rows) SELECT * FROM update",
            "WITH source AS (SELECT 1) VALUES (2)",
            "WITH source AS (SELECT 1) TABLE source",
            "WITH source AS (SELECT 1) (SELECT * FROM source)",
            "FROM source SELECT 'INSERT INTO target'",
            "FROM insert SELECT *",
            "WITH source AS (SELECT 1) FROM source SELECT *",
            "EXPLAIN UPDATE t SET value = 1",
            "CREATE TABLE t (value BIGINT)",
            "CREATE VIEW v AS SELECT 1",
            "EXECUTE IMMEDIATE 'SELECT 7 AS num_affected_rows'",
            "EXECUTE IMMEDIATE sql_variable",
            "WITH source AS (SELECT 1",
        ] {
            assert!(
                !sql_may_modify_rows(sql),
                "expected non-modifying SQL: {sql}"
            );
        }
    }

    /// Build a single-batch `ResultStream` from `(name, Int64 values)` columns.
    async fn count_stream(cols: &[(&str, &[i64])]) -> ResultStream {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, _)| Field::new(*n, DataType::Int64, false))
            .collect();
        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|(_, v)| Arc::new(Int64Array::from(v.to_vec())) as ArrayRef)
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap()
    }

    #[test]
    fn manifest_detection_gates_on_strict_signature() {
        let dml: ResultManifest = serde_json::from_str(
            r#"{"format":"ARROW_STREAM","schema":{"column_count":2,"columns":[
                {"name":"num_affected_rows","type_name":"LONG","type_text":"BIGINT","position":0},
                {"name":"num_inserted_rows","type_name":"LONG","type_text":"BIGINT","position":1}]},
                "total_row_count":1}"#,
        )
        .unwrap();
        assert!(manifest_is_dml_count(&dml));

        let select: ResultManifest = serde_json::from_str(
            r#"{"format":"ARROW_STREAM","schema":{"column_count":1,"columns":[
                {"name":"id","type_name":"INT","type_text":"INT","position":0}]},
                "total_row_count":3}"#,
        )
        .unwrap();
        assert!(!manifest_is_dml_count(&select));

        // Leads with the counter name but has a non-counter column → declined.
        let aliased: ResultManifest = serde_json::from_str(
            r#"{"format":"ARROW_STREAM","schema":{"column_count":2,"columns":[
                {"name":"num_affected_rows","type_name":"LONG","type_text":"BIGINT","position":0},
                {"name":"label","type_name":"STRING","type_text":"STRING","position":1}]},
                "total_row_count":1}"#,
        )
        .unwrap();
        assert!(!manifest_is_dml_count(&aliased));
    }

    #[tokio::test]
    async fn insert_shape_returns_affected_rows() {
        // INSERT → [num_affected_rows, num_inserted_rows].
        let mut s = count_stream(&[("num_affected_rows", &[3]), ("num_inserted_rows", &[3])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn update_delete_shape_returns_affected_rows() {
        let mut s = count_stream(&[("num_affected_rows", &[2])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn merge_shape_returns_affected_not_other_counters() {
        // MERGE → [num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows];
        // we report num_affected_rows (the total), not a sibling column.
        let mut s = count_stream(&[
            ("num_affected_rows", &[2]),
            ("num_updated_rows", &[1]),
            ("num_deleted_rows", &[0]),
            ("num_inserted_rows", &[1]),
        ])
        .await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn zero_affected_rows_is_a_real_value_not_none() {
        let mut s = count_stream(&[("num_affected_rows", &[0])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn peek_is_non_consuming_count_row_still_fetchable() {
        // The derive must not steal the row from the caller's fetch — parity
        // with Thrift, whose DML fetch also returns the count row.
        let mut s = count_stream(&[("num_affected_rows", &[7])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(7));
        // The single count row is still there for next_batch().
        let batch = s.next_batch().await.unwrap().expect("count row preserved");
        let ResultBatch::Arrow(rb) = batch;
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.column(0).as_primitive::<Int64Type>().value(0), 7);
        // And the stream is then exhausted.
        assert!(s.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn select_with_real_columns_is_not_a_dml_count() {
        // A genuine SELECT: not all columns are counters → None, and nothing
        // is peeked/consumed.
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut s = ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
        // Untouched: the real row is the first thing the caller fetches.
        assert!(s.next_batch().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn select_aliasing_a_counter_name_is_rejected_by_strict_match() {
        // `SELECT 1 AS num_affected_rows, 'x' AS label` — leads with a counter
        // name but `label` is not a counter, so the strict all-columns guard
        // declines rather than mis-reporting a row count.
        let fields = vec![
            Field::new("num_affected_rows", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut s = ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
    }

    #[tokio::test]
    async fn empty_schema_ddl_is_not_a_dml_count() {
        let schema = Arc::new(Schema::empty());
        let s_reader = InMemoryReader::new(schema.clone(), vec![]);
        let mut s = ResultStream::from_reader(Box::new(s_reader)).await.unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
    }

    #[tokio::test]
    async fn null_affected_rows_reports_none() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "num_affected_rows",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None as Option<i64>])) as ArrayRef],
        )
        .unwrap();
        let mut s = ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
    }
}
