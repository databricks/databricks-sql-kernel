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

//! SQL command builder for metadata queries.
//!
//! Builds SHOW SQL commands based on the Databricks SQL dialect, matching
//! the patterns used by the JDBC driver (`CommandConstants.java`).

/// Builds SQL commands for metadata queries.
///
/// Uses a builder pattern to set optional filters before generating the SQL.
#[derive(Default)]
pub(crate) struct SqlCommandBuilder {
    catalog: Option<String>,
    schema_pattern: Option<String>,
    table_pattern: Option<String>,
    column_pattern: Option<String>,
}

impl SqlCommandBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_catalog(mut self, catalog: Option<&str>) -> Self {
        self.catalog = catalog.map(|s| s.to_string());
        self
    }

    pub fn with_schema_pattern(mut self, pattern: Option<&str>) -> Self {
        self.schema_pattern = pattern.map(Self::jdbc_pattern_to_hive);
        self
    }

    pub fn with_table_pattern(mut self, pattern: Option<&str>) -> Self {
        self.table_pattern = pattern.map(Self::jdbc_pattern_to_hive);
        self
    }

    pub fn with_column_pattern(mut self, pattern: Option<&str>) -> Self {
        self.column_pattern = pattern.map(Self::jdbc_pattern_to_hive);
        self
    }

    /// Convert JDBC/ADBC-style pattern to Hive-style pattern for `SHOW` commands.
    ///
    /// Use this for `SHOW TABLES/SCHEMAS/COLUMNS` patterns, which use Hive-style wildcards.
    /// For `information_schema` SQL `LIKE` patterns, use [`escape_sql_string`] instead.
    ///
    /// Databricks SHOW commands use Hive-style wildcards, not SQL LIKE wildcards:
    /// - `%` → `*` (multi-char wildcard)
    /// - `_` → `.` (single-char wildcard)
    /// - `\%` → `%` (escaped literal percent)
    /// - `\_` → `_` (escaped literal underscore)
    /// - `\\` → `\\` (escaped backslash)
    ///
    /// Also escapes single quotes for safe SQL embedding.
    /// Matches JDBC driver's `WildcardUtil.jdbcPatternToHive`.
    fn jdbc_pattern_to_hive(pattern: &str) -> String {
        let mut result = String::with_capacity(pattern.len());
        let mut escape_next = false;
        for ch in pattern.chars() {
            if ch == '\\' && !escape_next {
                escape_next = true;
                continue;
            }
            if escape_next {
                // Escaped character: emit literally
                if ch == '\\' {
                    result.push_str("\\\\");
                } else {
                    result.push(ch);
                }
                escape_next = false;
            } else {
                match ch {
                    '%' => result.push('*'),
                    '_' => result.push('.'),
                    '\'' => result.push_str("''"),
                    _ => result.push(ch),
                }
            }
        }
        result
    }

    /// Whether an ODBC/JDBC-style pattern contains an *active* wildcard
    /// (`%` or `_`) — i.e. one that is not backslash-escaped.
    ///
    /// Used by `list_tables` to decide whether a catalog argument needs
    /// the enumerate-then-fan-out wildcard path (`SHOW CATALOGS`, filter
    /// names, then `SHOW TABLES IN CATALOG <name>` per match) or can take
    /// the fast exact single-catalog path. The Databricks `SHOW` grammar
    /// has no catalog-pattern position, and the SEA backend — unlike
    /// Thrift's `TGetTablesReq.catalogName` (a `TPatternOrIdentifier`) —
    /// cannot wildcard the catalog server-side, so the kernel resolves
    /// the pattern against catalog names client-side.
    pub fn pattern_has_wildcards(pattern: &str) -> bool {
        let mut escaped = false;
        for ch in pattern.chars() {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '%' | '_' => return true,
                _ => {}
            }
        }
        false
    }

    /// Unescape an ODBC/JDBC pattern into a literal identifier: `\%` →
    /// `%`, `\_` → `_`, `\\` → `\`. A lone trailing backslash is kept.
    ///
    /// Only valid to call on a pattern with no *active* wildcards (see
    /// [`pattern_has_wildcards`](Self::pattern_has_wildcards)); such a
    /// pattern denotes an exact catalog name, and the escapes must be
    /// removed before it goes into a backtick-quoted `IN CATALOG`
    /// identifier. Mirrors the JDBC driver's `WildcardUtil.stripJdbcEscapes`.
    pub fn strip_jdbc_escapes(pattern: &str) -> String {
        let mut result = String::with_capacity(pattern.len());
        let mut chars = pattern.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                // Consume the escaped char literally; a lone trailing
                // backslash (no following char) is emitted as-is.
                match chars.next() {
                    Some(next) => result.push(next),
                    None => result.push('\\'),
                }
            } else {
                result.push(ch);
            }
        }
        result
    }

    /// Escape identifier for use in SQL (backtick-quote).
    ///
    /// Use for catalog, schema, table names in SQL statements (e.g., `` `my_catalog` ``).
    /// For string literal values inside `LIKE` clauses, use [`escape_sql_string`] instead.
    fn escape_identifier(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Whether a catalog string, if handed to the `SHOW ... IN [CATALOG]
    /// …` builder as an exact scope, would instead be silently
    /// reinterpreted by the Databricks server as "all catalogs": the
    /// empty string, `%`, or `*`.
    ///
    /// The caller (`list_tables`) uses this to keep the kernel's
    /// `pattern_has_wildcards` decision from disagreeing with the server:
    /// a literal catalog that lands in this set must be matched
    /// client-side (enumerate-then-fan-out) instead of handed to the
    /// server as a scope, else e.g. a catalog literally named `*` would
    /// silently broaden to every catalog.
    ///
    /// Note this is *not* consulted by [`build_show_schemas`](Self::build_show_schemas)
    /// / [`build_show_tables`](Self::build_show_tables): those broaden to
    /// "all catalogs" only when the catalog is `None`, so a literal `%` /
    /// `*` / `""` that reaches them (e.g. an escaped `\%` unescaped to a
    /// literal `%`, or an exact `Identifier`) is scoped verbatim and
    /// matches nothing — matching JDBC's 0-row contract for a literal
    /// catalog that names no catalog.
    pub fn is_wildcard_or_empty_scope(s: &str) -> bool {
        s.is_empty() || s == "%" || s == "*"
    }

    /// Whether a LIKE pattern matches **every** name — i.e. it is one or
    /// more `%` and nothing else (`%`, `%%`, …). Such a catalog pattern
    /// is semantically identical to "no catalog filter", so `list_tables`
    /// routes it to a single `SHOW TABLES IN ALL CATALOGS` rather than
    /// enumerating catalogs and fanning out one query per match (which
    /// would be N round trips for what is really "everything", and would
    /// let one inaccessible catalog fail the whole call).
    ///
    /// A lone `_` does *not* match all (it requires exactly one char), and
    /// `*` is a literal char in ODBC LIKE (not a wildcard), so neither
    /// qualifies — only runs of `%`.
    pub fn pattern_matches_all(s: &str) -> bool {
        !s.is_empty() && s.chars().all(|c| c == '%')
    }

    pub fn build_show_catalogs(&self) -> String {
        "SHOW CATALOGS".to_string()
    }

    pub fn build_show_schemas(&self) -> String {
        // Broaden to "all catalogs" only when no catalog is scoped
        // (`None`). A `Some(name)` is an exact literal to scope on — even
        // when `name` is `%`, `*`, or `""` (a literal catalog matching no
        // real catalog), which must yield 0 rows, not every catalog.
        // Mirrors `build_show_columns`'s `Option`-based decision; the
        // caller resolves any real wildcards before this point.
        let mut sql = match self.catalog {
            None => "SHOW SCHEMAS IN ALL CATALOGS".to_string(),
            Some(ref catalog) => {
                format!("SHOW SCHEMAS IN {}", Self::escape_identifier(catalog))
            }
        };

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        sql
    }

    pub fn build_show_tables(&self) -> String {
        // Broaden to "all catalogs" only when no catalog is scoped
        // (`None`); see `build_show_schemas` for why a literal `%` / `*`
        // / `""` catalog is scoped verbatim (0 rows) rather than
        // broadened.
        let mut sql = match self.catalog {
            None => "SHOW TABLES IN ALL CATALOGS".to_string(),
            Some(ref catalog) => {
                format!(
                    "SHOW TABLES IN CATALOG {}",
                    Self::escape_identifier(catalog)
                )
            }
        };

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" SCHEMA LIKE '{}'", pattern));
        }

        if let Some(ref pattern) = self.table_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        sql
    }

    /// Build SHOW COLUMNS command.
    ///
    /// `catalog = None` emits `SHOW COLUMNS IN ALL CATALOGS …`, which
    /// the Databricks server supports (the response includes
    /// `catalogName` so callers can still attribute each row to its
    /// catalog). JDBC's `getColumns(null, …)` enumerates catalogs
    /// client-side rather than using this form; the kernel uses it
    /// directly for a single round trip.
    pub fn build_show_columns(&self, catalog: Option<&str>) -> String {
        let mut sql = match catalog {
            Some(c) => format!("SHOW COLUMNS IN CATALOG {}", Self::escape_identifier(c)),
            None => "SHOW COLUMNS IN ALL CATALOGS".to_string(),
        };

        if let Some(ref pattern) = self.schema_pattern {
            sql.push_str(&format!(" SCHEMA LIKE '{}'", pattern));
        }

        if let Some(ref pattern) = self.table_pattern {
            sql.push_str(&format!(" TABLE LIKE '{}'", pattern));
        }

        if let Some(ref pattern) = self.column_pattern {
            sql.push_str(&format!(" LIKE '{}'", pattern));
        }

        sql
    }

    /// Escape a value for use in a SQL string literal (inside single quotes).
    ///
    /// Use for `LIKE` pattern values in `information_schema` queries.
    /// For identifiers (catalog/schema/table names), use [`escape_identifier`] instead.
    /// For `SHOW` command patterns, use [`jdbc_pattern_to_hive`] instead.
    ///
    /// Databricks SQL treats backslash as an escape character in string literals
    /// by default, so both single quotes and backslashes must be escaped.
    ///
    /// Order matters: backslashes must be escaped first, then quotes.
    /// Reversing the order would double-escape `\'` → `\''` → `\\''`.
    fn escape_sql_string(value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "''")
    }

    /// Resolve the catalog prefix for `information_schema` queries.
    ///
    /// - `None` → `system` (cross-catalog query)
    /// - `Some("main")` → `` `main` `` (scoped to catalog)
    ///
    /// Empty string catalog must be handled by the caller before reaching
    /// the SQL builder (return an empty result at the FFI/client layer).
    fn resolve_catalog_prefix(catalog: Option<&str>) -> String {
        match catalog {
            None => "system".to_string(),
            Some(c) => Self::escape_identifier(c),
        }
    }

    /// Build a SELECT query against `information_schema.routines` for getProcedures.
    ///
    /// Catalog resolution:
    /// - `None` → queries `system.information_schema.routines` (cross-catalog)
    /// - `Some("main")` → queries `` `main`.information_schema.routines ``
    ///
    /// Empty string catalog must be handled by the caller (return empty result
    /// at the FFI/client layer) before calling this method.
    ///
    /// Schema and procedure patterns use SQL LIKE syntax directly (no Hive conversion).
    pub fn build_get_procedures(
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
    ) -> String {
        let prefix = Self::resolve_catalog_prefix(catalog);
        let mut sql = format!(
            "SELECT routine_catalog, routine_schema, routine_name, comment, specific_name \
             FROM {}.information_schema.routines \
             WHERE routine_type = 'PROCEDURE'",
            prefix
        );

        if let Some(pattern) = schema_pattern {
            sql.push_str(&format!(
                " AND routine_schema LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        if let Some(pattern) = procedure_pattern {
            sql.push_str(&format!(
                " AND routine_name LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        sql.push_str(" ORDER BY routine_catalog, routine_schema, routine_name");
        sql
    }

    /// Build a SELECT query against `information_schema.routines` for
    /// `getFunctions`.
    ///
    /// Mirrors [`SqlCommandBuilder::build_get_procedures`] but filters
    /// `routine_type = 'FUNCTION'`. Catalog resolution and pattern
    /// escaping match the procedures builder, so `escape_sql_string`
    /// and `resolve_catalog_prefix` are the single source of truth
    /// for both.
    pub fn build_get_functions(
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        function_pattern: Option<&str>,
    ) -> String {
        let prefix = Self::resolve_catalog_prefix(catalog);
        let mut sql = format!(
            "SELECT routine_catalog, routine_schema, routine_name, comment, specific_name \
             FROM {}.information_schema.routines \
             WHERE routine_type = 'FUNCTION'",
            prefix
        );

        if let Some(pattern) = schema_pattern {
            sql.push_str(&format!(
                " AND routine_schema LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        if let Some(pattern) = function_pattern {
            sql.push_str(&format!(
                " AND routine_name LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        sql.push_str(" ORDER BY routine_catalog, routine_schema, routine_name");
        sql
    }

    /// Build a SELECT query against `information_schema.parameters` JOINed to
    /// `information_schema.routines` for getProcedureColumns.
    ///
    /// `information_schema.parameters` lists arguments for *all* routines
    /// (functions and procedures), so it is JOINed to `routines` filtered to
    /// `routine_type = 'PROCEDURE'` to exclude function arguments — matching
    /// `java.sql.DatabaseMetaData.getProcedureColumns`, which is procedures-only.
    ///
    /// Catalog resolution matches [`SqlCommandBuilder::build_get_procedures`]:
    /// - `None` → `system.information_schema` (cross-catalog)
    /// - `Some("main")` → `` `main`.information_schema ``
    ///
    /// Empty-string catalog must be handled by the caller (return empty result
    /// at the FFI/client layer) before calling this method.
    ///
    /// Schema / procedure / column patterns use SQL LIKE syntax directly. The
    /// selected columns are the raw `information_schema.parameters` fields the
    /// kernel reshape (`reshape_procedure_columns`) maps into the JDBC
    /// getProcedureColumns column set.
    pub fn build_get_procedure_columns(
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> String {
        let prefix = Self::resolve_catalog_prefix(catalog);
        let mut sql = format!(
            "SELECT p.specific_catalog, p.specific_schema, p.specific_name, \
             p.parameter_name, p.parameter_mode, p.is_result, p.data_type, \
             p.numeric_precision, p.numeric_precision_radix, p.numeric_scale, \
             p.character_octet_length, p.ordinal_position, p.comment \
             FROM {prefix}.information_schema.parameters p \
             JOIN {prefix}.information_schema.routines r \
             ON p.specific_catalog = r.specific_catalog \
             AND p.specific_schema = r.specific_schema \
             AND p.specific_name = r.specific_name \
             WHERE r.routine_type = 'PROCEDURE'"
        );

        if let Some(pattern) = schema_pattern {
            sql.push_str(&format!(
                " AND p.specific_schema LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }
        if let Some(pattern) = procedure_pattern {
            sql.push_str(&format!(
                " AND p.specific_name LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }
        if let Some(pattern) = column_pattern {
            sql.push_str(&format!(
                " AND p.parameter_name LIKE '{}'",
                Self::escape_sql_string(pattern)
            ));
        }

        sql.push_str(
            " ORDER BY p.specific_catalog, p.specific_schema, \
             p.specific_name, p.ordinal_position",
        );
        sql
    }

    pub fn build_show_primary_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }

    pub fn build_show_foreign_keys(catalog: &str, schema: &str, table: &str) -> String {
        format!(
            "SHOW FOREIGN KEYS IN CATALOG {} IN SCHEMA {} IN TABLE {}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table)
        )
    }
}

/// A compiled, case-insensitive ODBC/JDBC LIKE matcher (`%` = any
/// sequence, `_` = any single char, `\` escapes the next char).
///
/// The pattern is lowercased and decomposed into `char`s **once** at
/// construction so a matcher can be reused across many candidates
/// without re-lowering the invariant pattern per call — the shape
/// `list_tables` needs when filtering catalog *names* from
/// `SHOW CATALOGS` before fanning out per-catalog queries.
///
/// Matching is anchored (the whole candidate must be consumed) and
/// case-insensitive, mirroring Databricks `SHOW` matching and the Thrift
/// server's catalog matching. Operates on Unicode scalar values (not
/// bytes) so `_` matches exactly one character.
pub struct LikeMatcher {
    pattern: Vec<char>,
}

impl LikeMatcher {
    /// Compile `pattern` (lowercased for case-insensitive matching).
    pub fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_lowercase().chars().collect(),
        }
    }

    /// Whether `candidate` matches the compiled pattern (case-insensitive,
    /// anchored: the whole candidate must be consumed).
    pub fn matches(&self, candidate: &str) -> bool {
        let cand: Vec<char> = candidate.to_lowercase().chars().collect();
        Self::match_inner(&self.pattern, &cand)
    }

    /// Recursive LIKE matcher over `char` slices with backtracking on
    /// `%`. `pattern` may contain `\`-escaped literals.
    fn match_inner(pattern: &[char], candidate: &[char]) -> bool {
        let mut pi = 0;
        let mut ci = 0;
        while pi < pattern.len() {
            match pattern[pi] {
                '%' => {
                    // Try to match the remainder at every suffix of
                    // `candidate` (handles trailing / consecutive `%`).
                    let rest = &pattern[pi + 1..];
                    if rest.is_empty() {
                        return true; // trailing % matches anything
                    }
                    for split in ci..=candidate.len() {
                        if Self::match_inner(rest, &candidate[split..]) {
                            return true;
                        }
                    }
                    return false;
                }
                '_' => {
                    if ci >= candidate.len() {
                        return false;
                    }
                    pi += 1;
                    ci += 1;
                }
                '\\' => {
                    // Escaped literal: match the next pattern char exactly.
                    // A trailing backslash matches a literal backslash.
                    let lit = if pi + 1 < pattern.len() {
                        pattern[pi + 1]
                    } else {
                        '\\'
                    };
                    if ci >= candidate.len() || candidate[ci] != lit {
                        return false;
                    }
                    pi += if pi + 1 < pattern.len() { 2 } else { 1 };
                    ci += 1;
                }
                lit => {
                    if ci >= candidate.len() || candidate[ci] != lit {
                        return false;
                    }
                    pi += 1;
                    ci += 1;
                }
            }
        }
        ci == candidate.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_catalogs();
        assert_eq!(sql, "SHOW CATALOGS");
    }

    #[test]
    fn test_show_schemas_all_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS");
    }

    #[test]
    fn test_show_schemas_with_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `main`");
    }

    #[test]
    fn test_show_schemas_with_pattern() {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("default%"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS LIKE 'default*'");
    }

    #[test]
    fn test_show_schemas_literal_percent_catalog_scopes_verbatim() {
        // A `Some("%")` reaching the builder is a *literal* catalog named
        // `%` (the caller resolves real wildcards before this point), so
        // it is scoped verbatim — matching no real catalog and yielding 0
        // rows — rather than broadened to "all catalogs". Regression guard
        // for the METADATA-036/037 catalog-wildcard conformance fix.
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("%"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `%`");
    }

    #[test]
    fn test_show_schemas_empty_catalog_scopes_verbatim() {
        // Likewise a literal empty-string catalog scopes to `` (0 rows),
        // not "all catalogs". Only `None` means "all catalogs".
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some(""))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ``");
    }

    #[test]
    fn test_show_schemas_star_catalog_scopes_verbatim() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("*"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `*`");
    }

    #[test]
    fn test_show_tables_literal_percent_catalog_scopes_verbatim() {
        // Same contract as `build_show_schemas`: a literal `%` catalog is
        // scoped verbatim (0 rows), not broadened to all catalogs.
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("%"))
            .build_show_tables();
        assert_eq!(sql, "SHOW TABLES IN CATALOG `%`");
    }

    #[test]
    fn test_show_tables_all_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_tables();
        assert_eq!(sql, "SHOW TABLES IN ALL CATALOGS");
    }

    #[test]
    fn test_show_tables_with_catalog() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .build_show_tables();
        assert_eq!(sql, "SHOW TABLES IN CATALOG `main`");
    }

    #[test]
    fn test_show_tables_with_patterns() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("main"))
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("my_table%"))
            .build_show_tables();
        assert_eq!(
            sql,
            "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 'default' LIKE 'my.table*'"
        );
    }

    #[test]
    fn test_show_columns_with_catalog() {
        let sql = SqlCommandBuilder::new().build_show_columns(Some("main"));
        assert_eq!(sql, "SHOW COLUMNS IN CATALOG `main`");
    }

    #[test]
    fn test_show_columns_with_all_patterns() {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("my_table"))
            .with_column_pattern(Some("id%"))
            .build_show_columns(Some("main"));
        assert_eq!(
            sql,
            "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 'default' TABLE LIKE 'my.table' LIKE 'id*'"
        );
    }

    #[test]
    fn test_show_columns_all_catalogs() {
        let sql = SqlCommandBuilder::new().build_show_columns(None);
        assert_eq!(sql, "SHOW COLUMNS IN ALL CATALOGS");
    }

    #[test]
    fn test_show_columns_all_catalogs_with_patterns() {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("default"))
            .with_table_pattern(Some("my_table"))
            .with_column_pattern(Some("id%"))
            .build_show_columns(None);
        assert_eq!(
            sql,
            "SHOW COLUMNS IN ALL CATALOGS SCHEMA LIKE 'default' TABLE LIKE 'my.table' LIKE 'id*'"
        );
    }

    #[test]
    fn test_escape_identifier_with_backtick() {
        let sql = SqlCommandBuilder::new()
            .with_catalog(Some("my`catalog"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN `my``catalog`");
    }

    #[test]
    fn test_escape_pattern_with_single_quote() {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(Some("it's"))
            .build_show_schemas();
        assert_eq!(sql, "SHOW SCHEMAS IN ALL CATALOGS LIKE 'it''s'");
    }

    #[test]
    fn test_show_primary_keys() {
        let sql = SqlCommandBuilder::build_show_primary_keys("main", "default", "my_table");
        assert_eq!(
            sql,
            "SHOW KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `my_table`"
        );
    }

    #[test]
    fn test_show_foreign_keys() {
        let sql = SqlCommandBuilder::build_show_foreign_keys("main", "default", "my_table");
        assert_eq!(
            sql,
            "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `default` IN TABLE `my_table`"
        );
    }

    // --- getProcedures / getProcedureColumns SQL builder tests ---

    #[test]
    fn test_get_procedures_null_catalog() {
        let sql = SqlCommandBuilder::build_get_procedures(None, None, None);
        assert!(sql.starts_with("SELECT routine_catalog"));
        assert!(sql.contains("FROM system.information_schema.routines"));
        assert!(sql.contains("WHERE routine_type = 'PROCEDURE'"));
        assert!(sql.ends_with("ORDER BY routine_catalog, routine_schema, routine_name"));
    }

    #[test]
    fn test_get_procedures_specific_catalog() {
        let sql = SqlCommandBuilder::build_get_procedures(Some("main"), None, None);
        assert!(sql.contains("FROM `main`.information_schema.routines"));
    }

    #[test]
    fn test_get_procedures_with_schema_pattern() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some("default%"), None);
        assert!(sql.contains("AND routine_schema LIKE 'default%'"));
    }

    #[test]
    fn test_get_procedures_with_procedure_pattern() {
        let sql = SqlCommandBuilder::build_get_procedures(None, None, Some("my_proc%"));
        assert!(sql.contains("AND routine_name LIKE 'my_proc%'"));
    }

    #[test]
    fn test_get_procedures_with_all_filters() {
        let sql =
            SqlCommandBuilder::build_get_procedures(Some("prod"), Some("public"), Some("sp_%"));
        assert!(sql.contains("FROM `prod`.information_schema.routines"));
        assert!(sql.contains("AND routine_schema LIKE 'public'"));
        assert!(sql.contains("AND routine_name LIKE 'sp_%'"));
    }

    #[test]
    fn test_get_procedures_sql_injection_escaped() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some("it's"), None);
        assert!(sql.contains("AND routine_schema LIKE 'it''s'"));
    }

    #[test]
    fn test_get_procedures_backslash_escaped() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some(r"foo\bar"), None);
        assert!(
            sql.contains(r"AND routine_schema LIKE 'foo\\bar'"),
            "Backslash should be escaped: {}",
            sql
        );
    }

    #[test]
    fn test_get_procedures_trailing_backslash_escaped() {
        let sql = SqlCommandBuilder::build_get_procedures(None, Some(r"foo\"), None);
        assert!(
            sql.contains(r"AND routine_schema LIKE 'foo\\'"),
            "Trailing backslash should be escaped: {}",
            sql
        );
    }

    #[test]
    fn test_get_procedures_catalog_with_backtick() {
        let sql = SqlCommandBuilder::build_get_procedures(Some("my`catalog"), None, None);
        assert!(sql.contains("FROM `my``catalog`.information_schema.routines"));
    }

    #[test]
    fn test_get_procedure_columns_null_catalog() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(None, None, None, None);
        assert!(sql.starts_with("SELECT p.specific_catalog"));
        assert!(sql.contains("FROM system.information_schema.parameters p"));
        assert!(sql.contains("JOIN system.information_schema.routines r"));
        assert!(sql.contains("WHERE r.routine_type = 'PROCEDURE'"));
        assert!(sql.ends_with(
            "ORDER BY p.specific_catalog, p.specific_schema, p.specific_name, p.ordinal_position"
        ));
    }

    #[test]
    fn test_get_procedure_columns_specific_catalog() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(Some("main"), None, None, None);
        assert!(sql.contains("FROM `main`.information_schema.parameters p"));
        assert!(sql.contains("JOIN `main`.information_schema.routines r"));
    }

    #[test]
    fn test_get_procedure_columns_join_predicate() {
        // The JOIN must key on all three specific_* columns so parameters are
        // matched to their owning routine.
        let sql = SqlCommandBuilder::build_get_procedure_columns(None, None, None, None);
        assert!(sql.contains("ON p.specific_catalog = r.specific_catalog"));
        assert!(sql.contains("AND p.specific_schema = r.specific_schema"));
        assert!(sql.contains("AND p.specific_name = r.specific_name"));
    }

    #[test]
    fn test_get_procedure_columns_all_filters() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(
            Some("prod"),
            Some("public"),
            Some("sp_%"),
            Some("arg%"),
        );
        assert!(sql.contains("FROM `prod`.information_schema.parameters p"));
        assert!(sql.contains("AND p.specific_schema LIKE 'public'"));
        assert!(sql.contains("AND p.specific_name LIKE 'sp_%'"));
        assert!(sql.contains("AND p.parameter_name LIKE 'arg%'"));
    }

    #[test]
    fn test_get_procedure_columns_column_pattern_only() {
        let sql = SqlCommandBuilder::build_get_procedure_columns(None, None, None, Some("id"));
        assert!(sql.contains("AND p.parameter_name LIKE 'id'"));
        // Absent filters must not emit a clause.
        assert!(!sql.contains("p.specific_schema LIKE"));
        assert!(!sql.contains("p.specific_name LIKE"));
    }

    #[test]
    fn test_get_procedure_columns_sql_injection_escaped() {
        let sql =
            SqlCommandBuilder::build_get_procedure_columns(None, Some("it's"), None, Some(r"a\b"));
        assert!(sql.contains("AND p.specific_schema LIKE 'it''s'"));
        assert!(
            sql.contains(r"AND p.parameter_name LIKE 'a\\b'"),
            "Backslash should be escaped: {sql}"
        );
    }

    #[test]
    fn test_get_procedure_columns_catalog_with_backtick() {
        let sql =
            SqlCommandBuilder::build_get_procedure_columns(Some("my`catalog"), None, None, None);
        assert!(sql.contains("FROM `my``catalog`.information_schema.parameters p"));
        assert!(sql.contains("JOIN `my``catalog`.information_schema.routines r"));
    }

    // --- resolve_catalog_prefix edge cases ---

    #[test]
    fn test_resolve_catalog_prefix_none() {
        assert_eq!(SqlCommandBuilder::resolve_catalog_prefix(None), "system");
    }

    #[test]
    fn test_resolve_catalog_prefix_specific() {
        assert_eq!(
            SqlCommandBuilder::resolve_catalog_prefix(Some("main")),
            "`main`"
        );
    }

    #[test]
    fn test_resolve_catalog_prefix_special_chars() {
        assert_eq!(
            SqlCommandBuilder::resolve_catalog_prefix(Some("my`catalog")),
            "`my``catalog`"
        );
    }

    // --- escape_sql_string edge cases ---

    #[test]
    fn test_escape_sql_string_single_quote() {
        assert_eq!(SqlCommandBuilder::escape_sql_string("it's"), "it''s");
    }

    #[test]
    fn test_escape_sql_string_backslash() {
        assert_eq!(
            SqlCommandBuilder::escape_sql_string(r"foo\bar"),
            r"foo\\bar"
        );
    }

    #[test]
    fn test_escape_sql_string_trailing_backslash() {
        assert_eq!(SqlCommandBuilder::escape_sql_string(r"foo\"), r"foo\\");
    }

    #[test]
    fn test_escape_sql_string_both_quote_and_backslash() {
        assert_eq!(
            SqlCommandBuilder::escape_sql_string(r"it's\here"),
            r"it''s\\here"
        );
    }

    #[test]
    fn test_escape_sql_string_percent_underscore_passthrough() {
        // % and _ are SQL LIKE wildcards — they should NOT be escaped by escape_sql_string.
        // They are meaningful in LIKE patterns and passed through as-is.
        assert_eq!(
            SqlCommandBuilder::escape_sql_string("foo%bar_baz"),
            "foo%bar_baz"
        );
    }

    #[test]
    fn test_escape_sql_string_empty() {
        assert_eq!(SqlCommandBuilder::escape_sql_string(""), "");
    }

    // --- getFunctions SQL builder tests ---

    #[test]
    fn test_get_functions_null_catalog() {
        let sql = SqlCommandBuilder::build_get_functions(None, None, None);
        assert!(sql.starts_with("SELECT routine_catalog"));
        assert!(sql.contains("FROM system.information_schema.routines"));
        assert!(sql.contains("WHERE routine_type = 'FUNCTION'"));
        assert!(sql.ends_with("ORDER BY routine_catalog, routine_schema, routine_name"));
    }

    #[test]
    fn test_get_functions_specific_catalog() {
        let sql = SqlCommandBuilder::build_get_functions(Some("main"), None, None);
        assert!(sql.contains("FROM `main`.information_schema.routines"));
    }

    #[test]
    fn test_get_functions_with_schema_pattern() {
        let sql = SqlCommandBuilder::build_get_functions(None, Some("default%"), None);
        assert!(sql.contains("AND routine_schema LIKE 'default%'"));
    }

    #[test]
    fn test_get_functions_with_function_pattern() {
        let sql = SqlCommandBuilder::build_get_functions(None, None, Some("avg%"));
        assert!(sql.contains("AND routine_name LIKE 'avg%'"));
    }

    #[test]
    fn test_get_functions_with_all_filters() {
        let sql =
            SqlCommandBuilder::build_get_functions(Some("prod"), Some("public"), Some("fn_%"));
        assert!(sql.contains("FROM `prod`.information_schema.routines"));
        assert!(sql.contains("AND routine_schema LIKE 'public'"));
        assert!(sql.contains("AND routine_name LIKE 'fn_%'"));
    }

    #[test]
    fn test_get_functions_sql_injection_escaped() {
        let sql = SqlCommandBuilder::build_get_functions(None, Some("it's"), None);
        assert!(sql.contains("AND routine_schema LIKE 'it''s'"));
    }

    // --- foreign keys with special characters ---

    #[test]
    fn test_show_foreign_keys_special_chars_in_catalog() {
        let sql = SqlCommandBuilder::build_show_foreign_keys("my`cat", "my`sch", "my`tbl");
        assert_eq!(
            sql,
            "SHOW FOREIGN KEYS IN CATALOG `my``cat` IN SCHEMA `my``sch` IN TABLE `my``tbl`"
        );
    }

    // --- jdbc_pattern_to_hive tests (matches JDBC WildcardUtil) ---

    #[test]
    fn test_pattern_percent_to_star() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("abc%"), "abc*");
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("%abc"), "*abc");
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("%abc%"), "*abc*");
    }

    #[test]
    fn test_pattern_underscore_to_dot() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("abc_"), "abc.");
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("_abc"), ".abc");
    }

    #[test]
    fn test_pattern_escaped_percent_literal() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive(r"abc\%"), "abc%");
    }

    #[test]
    fn test_pattern_escaped_underscore_literal() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive(r"abc\_"), "abc_");
    }

    #[test]
    fn test_pattern_escaped_backslash() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive(r"abc\\"), r"abc\\");
    }

    #[test]
    fn test_pattern_no_wildcards() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("exact"), "exact");
    }

    #[test]
    fn test_pattern_single_quote_escaped() {
        assert_eq!(SqlCommandBuilder::jdbc_pattern_to_hive("it's"), "it''s");
    }

    // --- catalog wildcard helpers (list_tables client-side path) ---

    #[test]
    fn test_pattern_has_wildcards() {
        assert!(SqlCommandBuilder::pattern_has_wildcards("comp_tests"));
        assert!(SqlCommandBuilder::pattern_has_wildcards("comp%"));
        assert!(SqlCommandBuilder::pattern_has_wildcards("%mid_dle%"));
        assert!(!SqlCommandBuilder::pattern_has_wildcards(
            "comparator_tests".replace('_', r"\_").as_str()
        ));
        assert!(!SqlCommandBuilder::pattern_has_wildcards("plaincatalog"));
        // Escaped wildcards are literals, not active wildcards.
        assert!(!SqlCommandBuilder::pattern_has_wildcards(r"comp\_tests"));
        assert!(!SqlCommandBuilder::pattern_has_wildcards(r"comp\%tests"));
        // A `%` after an escaped backslash is active again.
        assert!(SqlCommandBuilder::pattern_has_wildcards(r"comp\\%"));
    }

    #[test]
    fn test_strip_jdbc_escapes() {
        assert_eq!(
            SqlCommandBuilder::strip_jdbc_escapes(r"comp\_tests"),
            "comp_tests"
        );
        assert_eq!(SqlCommandBuilder::strip_jdbc_escapes(r"a\%b"), "a%b");
        assert_eq!(SqlCommandBuilder::strip_jdbc_escapes(r"a\\b"), r"a\b");
        assert_eq!(SqlCommandBuilder::strip_jdbc_escapes("plain"), "plain");
        // Lone trailing backslash is preserved.
        assert_eq!(SqlCommandBuilder::strip_jdbc_escapes(r"foo\"), r"foo\");
    }

    // Terse wrapper for the compiled matcher used across these tests.
    fn like(pattern: &str, candidate: &str) -> bool {
        LikeMatcher::new(pattern).matches(candidate)
    }

    #[test]
    fn test_like_match_ci_basic() {
        assert!(like("comparator_tests", "comparator-tests"));
        assert!(like("comparator_tests", "comparator_tests"));
        // `_` matches exactly one char, so a two-char gap must not match.
        assert!(!like("comparator_tests", "comparator--tests"));
    }

    #[test]
    fn test_like_match_ci_percent() {
        assert!(like("comp%", "comparator_tests"));
        assert!(like("%tests", "comparator_tests"));
        assert!(like("%para%", "comparator_tests"));
        assert!(like("%", "anything"));
        assert!(!like("comp%", "other"));
    }

    #[test]
    fn test_like_match_ci_case_insensitive() {
        assert!(like("MAIN", "main"));
        assert!(like("Comp%", "comparator"));
    }

    #[test]
    fn test_like_match_ci_escaped_literal() {
        // `\_` matches a literal underscore only, not a hyphen.
        assert!(like(r"comparator\_tests", "comparator_tests"));
        assert!(!like(r"comparator\_tests", "comparator-tests"));
    }

    #[test]
    fn test_like_match_ci_anchored() {
        // No implicit wildcards: pattern must match the whole candidate.
        assert!(!like("comp", "comparator"));
        assert!(like("comp", "comp"));
    }

    #[test]
    fn test_like_match_ci_empty_pattern_matches_only_empty() {
        // Empty pattern is "match nothing" for any non-empty name, and
        // matches only the empty string. `list_tables` never reaches the
        // matcher with `""` (it short-circuits to an empty result), but
        // the matcher's own contract should still be well-defined.
        assert!(!like("", "anything"));
        assert!(like("", ""));
    }

    #[test]
    fn test_matcher_reused_across_candidates() {
        // A single compiled matcher must give consistent results across
        // many candidates (the catalog-name fan-out reuse path).
        let m = LikeMatcher::new("comparator_tests");
        assert!(m.matches("comparator-tests"));
        assert!(m.matches("comparator_tests"));
        assert!(!m.matches("unrelated"));
    }

    #[test]
    fn test_pattern_matches_all() {
        // Runs of `%` match everything → single all-catalogs query.
        assert!(SqlCommandBuilder::pattern_matches_all("%"));
        assert!(SqlCommandBuilder::pattern_matches_all("%%"));
        // A lone `_` requires exactly one char, `*` is a literal, and any
        // anchoring literal narrows — none match all.
        assert!(!SqlCommandBuilder::pattern_matches_all("_"));
        assert!(!SqlCommandBuilder::pattern_matches_all("*"));
        assert!(!SqlCommandBuilder::pattern_matches_all("%a%"));
        assert!(!SqlCommandBuilder::pattern_matches_all(""));
    }

    #[test]
    fn test_is_wildcard_or_empty_scope() {
        // These are the strings the SHOW builder reinterprets as
        // "all catalogs"; `list_tables` must route them client-side.
        assert!(SqlCommandBuilder::is_wildcard_or_empty_scope(""));
        assert!(SqlCommandBuilder::is_wildcard_or_empty_scope("%"));
        assert!(SqlCommandBuilder::is_wildcard_or_empty_scope("*"));
        assert!(!SqlCommandBuilder::is_wildcard_or_empty_scope("main"));
        assert!(!SqlCommandBuilder::is_wildcard_or_empty_scope("a*b"));
    }
}
