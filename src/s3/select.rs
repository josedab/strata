//! S3 Select - SQL queries on object content.
//!
//! Implements S3 Select functionality allowing SQL expressions to query CSV, JSON, and Parquet data.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

/// S3 Select request parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectRequest {
    /// SQL expression to execute.
    pub expression: String,
    /// Expression type (always SQL for now).
    pub expression_type: ExpressionType,
    /// Input serialization format.
    pub input_serialization: InputSerialization,
    /// Output serialization format.
    pub output_serialization: OutputSerialization,
    /// Request progress enabled.
    pub request_progress: Option<RequestProgress>,
    /// Scan range (for partial object scans).
    pub scan_range: Option<ScanRange>,
}

impl Default for SelectRequest {
    fn default() -> Self {
        Self {
            expression: String::new(),
            expression_type: ExpressionType::Sql,
            input_serialization: InputSerialization::default(),
            output_serialization: OutputSerialization::default(),
            request_progress: None,
            scan_range: None,
        }
    }
}

/// Expression type for Select.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpressionType {
    /// SQL expression.
    Sql,
}

impl Default for ExpressionType {
    fn default() -> Self {
        Self::Sql
    }
}

/// Input serialization configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputSerialization {
    /// CSV format configuration.
    pub csv: Option<CsvInput>,
    /// JSON format configuration.
    pub json: Option<JsonInput>,
    /// Parquet format configuration.
    pub parquet: Option<ParquetInput>,
    /// Compression type.
    pub compression_type: CompressionType,
}

impl Default for InputSerialization {
    fn default() -> Self {
        Self {
            csv: Some(CsvInput::default()),
            json: None,
            parquet: None,
            compression_type: CompressionType::None,
        }
    }
}

/// Output serialization configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSerialization {
    /// CSV format configuration.
    pub csv: Option<CsvOutput>,
    /// JSON format configuration.
    pub json: Option<JsonOutput>,
}

impl Default for OutputSerialization {
    fn default() -> Self {
        Self {
            csv: Some(CsvOutput::default()),
            json: None,
        }
    }
}

/// CSV input configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvInput {
    /// Whether the file has a header row.
    pub file_header_info: FileHeaderInfo,
    /// Comment character.
    pub comments: Option<char>,
    /// Quote escape character.
    pub quote_escape_character: Option<char>,
    /// Record delimiter.
    pub record_delimiter: String,
    /// Field delimiter.
    pub field_delimiter: String,
    /// Quote character.
    pub quote_character: Option<char>,
    /// Allow quoted record delimiter.
    pub allow_quoted_record_delimiter: bool,
}

impl Default for CsvInput {
    fn default() -> Self {
        Self {
            file_header_info: FileHeaderInfo::Use,
            comments: None,
            quote_escape_character: Some('"'),
            record_delimiter: "\n".to_string(),
            field_delimiter: ",".to_string(),
            quote_character: Some('"'),
            allow_quoted_record_delimiter: false,
        }
    }
}

/// File header info for CSV.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileHeaderInfo {
    /// Use the first row as headers.
    Use,
    /// Ignore headers.
    Ignore,
    /// No headers present.
    None,
}

impl Default for FileHeaderInfo {
    fn default() -> Self {
        Self::Use
    }
}

impl FileHeaderInfo {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "USE" => Some(Self::Use),
            "IGNORE" => Some(Self::Ignore),
            "NONE" => Some(Self::None),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Use => "USE",
            Self::Ignore => "IGNORE",
            Self::None => "NONE",
        }
    }
}

/// JSON input configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonInput {
    /// JSON type (document or lines).
    pub json_type: JsonType,
}

impl Default for JsonInput {
    fn default() -> Self {
        Self {
            json_type: JsonType::Document,
        }
    }
}

/// JSON type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonType {
    /// JSON document (single object or array).
    Document,
    /// JSON lines (newline-delimited JSON).
    Lines,
}

impl JsonType {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "DOCUMENT" => Some(Self::Document),
            "LINES" => Some(Self::Lines),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Document => "DOCUMENT",
            Self::Lines => "LINES",
        }
    }
}

/// Parquet input configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParquetInput {
    // Parquet doesn't need additional configuration
}

/// Compression type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    /// No compression.
    None,
    /// GZIP compression.
    Gzip,
    /// BZIP2 compression.
    Bzip2,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::None
    }
}

impl CompressionType {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NONE" => Some(Self::None),
            "GZIP" => Some(Self::Gzip),
            "BZIP2" => Some(Self::Bzip2),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Gzip => "GZIP",
            Self::Bzip2 => "BZIP2",
        }
    }
}

/// CSV output configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvOutput {
    /// Quote fields setting.
    pub quote_fields: QuoteFields,
    /// Quote escape character.
    pub quote_escape_character: Option<char>,
    /// Record delimiter.
    pub record_delimiter: String,
    /// Field delimiter.
    pub field_delimiter: String,
    /// Quote character.
    pub quote_character: Option<char>,
}

impl Default for CsvOutput {
    fn default() -> Self {
        Self {
            quote_fields: QuoteFields::AsNeeded,
            quote_escape_character: Some('"'),
            record_delimiter: "\n".to_string(),
            field_delimiter: ",".to_string(),
            quote_character: Some('"'),
        }
    }
}

/// Quote fields setting for CSV output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuoteFields {
    /// Quote as needed.
    AsNeeded,
    /// Always quote.
    Always,
}

impl QuoteFields {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ASNEEDED" | "AS_NEEDED" => Some(Self::AsNeeded),
            "ALWAYS" => Some(Self::Always),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AsNeeded => "ASNEEDED",
            Self::Always => "ALWAYS",
        }
    }
}

/// JSON output configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonOutput {
    /// Record delimiter.
    pub record_delimiter: String,
}

impl Default for JsonOutput {
    fn default() -> Self {
        Self {
            record_delimiter: "\n".to_string(),
        }
    }
}

/// Request progress configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestProgress {
    /// Enable progress reporting.
    pub enabled: bool,
}

/// Scan range for partial object scans.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRange {
    /// Start byte offset.
    pub start: Option<u64>,
    /// End byte offset.
    pub end: Option<u64>,
}

/// Select result.
#[derive(Debug, Clone)]
pub struct SelectResult {
    /// Output records.
    pub records: Vec<String>,
    /// Statistics.
    pub stats: SelectStats,
}

/// Select statistics.
#[derive(Debug, Clone, Default)]
pub struct SelectStats {
    /// Bytes scanned.
    pub bytes_scanned: u64,
    /// Bytes processed.
    pub bytes_processed: u64,
    /// Bytes returned.
    pub bytes_returned: u64,
}

/// S3 Select errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SelectError {
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Invalid input format: {0}")]
    InvalidInputFormat(String),

    #[error("Invalid output format: {0}")]
    InvalidOutputFormat(String),

    #[error("Unsupported expression type: {0}")]
    UnsupportedExpressionType(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Invalid XML: {0}")]
    InvalidXml(String),
}

/// Simple SQL expression parser for S3 Select.
pub struct SqlParser;

impl SqlParser {
    /// Parse a simple SELECT expression.
    pub fn parse(expression: &str) -> Result<ParsedQuery, SelectError> {
        let expression = expression.trim();

        // Must start with SELECT
        if !expression.to_uppercase().starts_with("SELECT") {
            return Err(SelectError::InvalidExpression(
                "Expression must start with SELECT".to_string(),
            ));
        }

        let upper = expression.to_uppercase();

        // Find FROM clause
        let from_pos = upper.find(" FROM ").ok_or_else(|| {
            SelectError::InvalidExpression("Missing FROM clause".to_string())
        })?;

        // Extract columns
        let columns_str = &expression[6..from_pos].trim();
        let columns = parse_columns(columns_str)?;

        // Find optional WHERE clause
        let (table, where_clause) = if let Some(where_pos) = upper.find(" WHERE ") {
            let table = expression[from_pos + 6..where_pos].trim().to_string();
            let where_expr = expression[where_pos + 7..].trim().to_string();
            (table, Some(where_expr))
        } else {
            (expression[from_pos + 6..].trim().to_string(), None)
        };

        Ok(ParsedQuery {
            columns,
            table,
            where_clause,
        })
    }
}

/// Parsed SQL query.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Selected columns.
    pub columns: Vec<SelectColumn>,
    /// Table/object reference (S3Object or s).
    pub table: String,
    /// WHERE clause expression.
    pub where_clause: Option<String>,
}

/// Selected column.
#[derive(Debug, Clone)]
pub enum SelectColumn {
    /// All columns (*).
    All,
    /// Named column.
    Named(String),
    /// Column with alias.
    Aliased { column: String, alias: String },
    /// Aggregate function.
    Aggregate { function: AggregateFunction, column: String, alias: Option<String> },
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunction {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "COUNT" => Some(Self::Count),
            "SUM" => Some(Self::Sum),
            "AVG" => Some(Self::Avg),
            "MIN" => Some(Self::Min),
            "MAX" => Some(Self::Max),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

/// Parse column list from SELECT clause.
fn parse_columns(columns_str: &str) -> Result<Vec<SelectColumn>, SelectError> {
    let columns_str = columns_str.trim();

    if columns_str == "*" {
        return Ok(vec![SelectColumn::All]);
    }

    let mut columns = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;

    for ch in columns_str.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
            ',' if paren_depth == 0 => {
                if !current.trim().is_empty() {
                    columns.push(parse_single_column(current.trim())?);
                }
                current = String::new();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.trim().is_empty() {
        columns.push(parse_single_column(current.trim())?);
    }

    Ok(columns)
}

/// Parse a single column expression.
fn parse_single_column(col: &str) -> Result<SelectColumn, SelectError> {
    let col = col.trim();

    // Check for aggregate function
    let upper = col.to_uppercase();
    for func_name in &["COUNT", "SUM", "AVG", "MIN", "MAX"] {
        if upper.starts_with(func_name) && col.contains('(') {
            let func = AggregateFunction::from_str(func_name).unwrap();

            // Extract column name from function
            let paren_start = col.find('(').unwrap();
            let paren_end = col.rfind(')').ok_or_else(|| {
                SelectError::InvalidExpression("Unclosed parenthesis".to_string())
            })?;

            let inner = col[paren_start + 1..paren_end].trim().to_string();

            // Check for alias
            let rest = col[paren_end + 1..].trim();
            let alias = if rest.to_uppercase().starts_with("AS ") {
                Some(rest[3..].trim().to_string())
            } else if !rest.is_empty() {
                Some(rest.to_string())
            } else {
                None
            };

            return Ok(SelectColumn::Aggregate {
                function: func,
                column: inner,
                alias,
            });
        }
    }

    // Check for alias
    if let Some(as_pos) = upper.find(" AS ") {
        let column = col[..as_pos].trim().to_string();
        let alias = col[as_pos + 4..].trim().to_string();
        return Ok(SelectColumn::Aliased { column, alias });
    }

    // Simple column name
    Ok(SelectColumn::Named(col.to_string()))
}

/// Execute S3 Select query on CSV data.
pub fn execute_csv_select(
    data: &[u8],
    request: &SelectRequest,
) -> Result<SelectResult, SelectError> {
    let csv_config = request.input_serialization.csv.as_ref().ok_or_else(|| {
        SelectError::InvalidInputFormat("CSV configuration required".to_string())
    })?;

    let content = std::str::from_utf8(data)
        .map_err(|e| SelectError::ParseError(format!("Invalid UTF-8: {}", e)))?;

    // Parse the query
    let query = SqlParser::parse(&request.expression)?;

    debug!(
        expression = %request.expression,
        columns = ?query.columns,
        where_clause = ?query.where_clause,
        "Executing S3 Select query"
    );

    // Parse CSV
    let lines: Vec<&str> = content
        .split(&csv_config.record_delimiter)
        .filter(|l| !l.is_empty())
        .collect();

    if lines.is_empty() {
        return Ok(SelectResult {
            records: Vec::new(),
            stats: SelectStats::default(),
        });
    }

    // Parse headers
    let headers: Vec<String> = if csv_config.file_header_info == FileHeaderInfo::Use {
        lines[0]
            .split(&csv_config.field_delimiter)
            .map(|s| s.trim().to_string())
            .collect()
    } else {
        // Generate column names _1, _2, etc.
        let first_line_cols = lines[0].split(&csv_config.field_delimiter).count();
        (1..=first_line_cols).map(|i| format!("_{}", i)).collect()
    };

    let data_start = if csv_config.file_header_info == FileHeaderInfo::Use { 1 } else { 0 };

    // Execute query on each row
    let mut output_records = Vec::new();
    let mut bytes_scanned = 0u64;
    let mut bytes_processed = 0u64;

    for line in lines.iter().skip(data_start) {
        bytes_scanned += line.len() as u64;

        let fields: Vec<&str> = line.split(&csv_config.field_delimiter).collect();

        // Build row map
        let row: HashMap<String, String> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let value = fields.get(i).map(|s| s.trim().to_string()).unwrap_or_default();
                (h.clone(), value)
            })
            .collect();

        // Evaluate WHERE clause
        if let Some(ref where_expr) = query.where_clause {
            if !evaluate_where(&row, where_expr)? {
                continue;
            }
        }

        bytes_processed += line.len() as u64;

        // Select columns
        let output = format_output_row(&query.columns, &row, &headers, &request.output_serialization)?;
        output_records.push(output);
    }

    let bytes_returned = output_records.iter().map(|s| s.len() as u64).sum();

    Ok(SelectResult {
        records: output_records,
        stats: SelectStats {
            bytes_scanned,
            bytes_processed,
            bytes_returned,
        },
    })
}

/// Evaluate WHERE clause against a row.
fn evaluate_where(row: &HashMap<String, String>, where_expr: &str) -> Result<bool, SelectError> {
    let expr = where_expr.trim();

    // Simple equality check: column = 'value'
    if let Some(eq_pos) = expr.find('=') {
        let left = expr[..eq_pos].trim();
        let right = expr[eq_pos + 1..].trim();

        // Get column value
        let col_name = left.trim_start_matches("s.").trim_start_matches("S3Object.");
        let col_value = row.get(col_name).map(|s| s.as_str()).unwrap_or("");

        // Get comparison value (strip quotes)
        let compare_value = right.trim_matches('\'').trim_matches('"');

        return Ok(col_value == compare_value);
    }

    // Simple comparison: column > value, column < value, etc.
    // Use function pointers instead of closures for uniform typing
    fn gt(a: f64, b: f64) -> bool { a > b }
    fn ge(a: f64, b: f64) -> bool { a >= b }
    fn lt(a: f64, b: f64) -> bool { a < b }
    fn le(a: f64, b: f64) -> bool { a <= b }

    let comparisons: &[(&str, fn(f64, f64) -> bool)] = &[
        (" >= ", ge),
        (" <= ", le),
        (" > ", gt),
        (" < ", lt),
    ];

    for (op, cmp_fn) in comparisons {
        if let Some(op_pos) = expr.find(op) {
            let left = expr[..op_pos].trim();
            let right = expr[op_pos + op.len()..].trim();

            let col_name = left.trim_start_matches("s.").trim_start_matches("S3Object.");
            let col_value: f64 = row
                .get(col_name)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let compare_value: f64 = right
                .trim_matches('\'')
                .trim_matches('"')
                .parse()
                .map_err(|_| SelectError::ExecutionError("Invalid numeric comparison".to_string()))?;

            return Ok(cmp_fn(col_value, compare_value));
        }
    }

    // Default to true if we can't parse the expression
    Ok(true)
}

/// Format output row based on selected columns.
fn format_output_row(
    columns: &[SelectColumn],
    row: &HashMap<String, String>,
    headers: &[String],
    output: &OutputSerialization,
) -> Result<String, SelectError> {
    let values: Vec<String> = columns
        .iter()
        .flat_map(|col| match col {
            SelectColumn::All => {
                headers.iter().map(|h| row.get(h).cloned().unwrap_or_default()).collect()
            }
            SelectColumn::Named(name) => {
                let clean_name = name.trim_start_matches("s.").trim_start_matches("S3Object.");
                vec![row.get(clean_name).cloned().unwrap_or_default()]
            }
            SelectColumn::Aliased { column, .. } => {
                let clean_name = column.trim_start_matches("s.").trim_start_matches("S3Object.");
                vec![row.get(clean_name).cloned().unwrap_or_default()]
            }
            SelectColumn::Aggregate { .. } => {
                // Aggregates are handled separately
                vec![]
            }
        })
        .collect();

    if let Some(ref csv_output) = output.csv {
        Ok(values.join(&csv_output.field_delimiter))
    } else if output.json.is_some() {
        // Output as JSON object
        let json_obj: HashMap<&str, &str> = columns
            .iter()
            .filter_map(|col| match col {
                SelectColumn::Named(name) => {
                    let clean = name.trim_start_matches("s.").trim_start_matches("S3Object.");
                    row.get(clean).map(|v| (clean, v.as_str()))
                }
                SelectColumn::Aliased { column, alias } => {
                    let clean = column.trim_start_matches("s.").trim_start_matches("S3Object.");
                    row.get(clean).map(|v| (alias.as_str(), v.as_str()))
                }
                _ => None,
            })
            .collect();

        Ok(format!("{:?}", json_obj)) // Simplified JSON output
    } else {
        Ok(values.join(","))
    }
}

/// Parse S3 Select request from XML.
pub fn parse_select_xml(xml: &str) -> Result<SelectRequest, SelectError> {
    let mut request = SelectRequest::default();

    // Extract Expression
    if let Some(expr) = extract_xml_value(xml, "Expression") {
        request.expression = expr;
    } else {
        return Err(SelectError::InvalidXml("Missing Expression".to_string()));
    }

    // Extract ExpressionType
    if let Some(expr_type) = extract_xml_value(xml, "ExpressionType") {
        if expr_type.to_uppercase() != "SQL" {
            return Err(SelectError::UnsupportedExpressionType(expr_type));
        }
    }

    // Extract InputSerialization
    if let Some(input_start) = xml.find("<InputSerialization>") {
        if let Some(input_end) = xml[input_start..].find("</InputSerialization>") {
            let input_xml = &xml[input_start..input_start + input_end + 21];
            request.input_serialization = parse_input_serialization(input_xml)?;
        }
    }

    // Extract OutputSerialization
    if let Some(output_start) = xml.find("<OutputSerialization>") {
        if let Some(output_end) = xml[output_start..].find("</OutputSerialization>") {
            let output_xml = &xml[output_start..output_start + output_end + 22];
            request.output_serialization = parse_output_serialization(output_xml)?;
        }
    }

    Ok(request)
}

/// Parse input serialization from XML.
fn parse_input_serialization(xml: &str) -> Result<InputSerialization, SelectError> {
    let mut input = InputSerialization {
        csv: None,
        json: None,
        parquet: None,
        compression_type: CompressionType::None,
    };

    // Check for CSV
    if xml.contains("<CSV>") {
        let mut csv = CsvInput::default();

        if let Some(header_info) = extract_xml_value(xml, "FileHeaderInfo") {
            csv.file_header_info = FileHeaderInfo::from_str(&header_info)
                .unwrap_or(FileHeaderInfo::Use);
        }
        if let Some(delim) = extract_xml_value(xml, "FieldDelimiter") {
            csv.field_delimiter = delim;
        }
        if let Some(delim) = extract_xml_value(xml, "RecordDelimiter") {
            csv.record_delimiter = delim;
        }

        input.csv = Some(csv);
    }

    // Check for JSON
    if xml.contains("<JSON>") {
        let mut json = JsonInput::default();

        if let Some(json_type) = extract_xml_value(xml, "Type") {
            json.json_type = JsonType::from_str(&json_type).unwrap_or(JsonType::Document);
        }

        input.json = Some(json);
    }

    // Check for Parquet
    if xml.contains("<Parquet>") {
        input.parquet = Some(ParquetInput::default());
    }

    // Compression type
    if let Some(compression) = extract_xml_value(xml, "CompressionType") {
        input.compression_type = CompressionType::from_str(&compression)
            .unwrap_or(CompressionType::None);
    }

    Ok(input)
}

/// Check if XML contains a tag (handles both <Tag> and <Tag /> formats).
fn has_xml_tag(xml: &str, tag: &str) -> bool {
    xml.contains(&format!("<{}>", tag))
        || xml.contains(&format!("<{} ", tag))
        || xml.contains(&format!("<{}/>", tag))
}

/// Parse output serialization from XML.
fn parse_output_serialization(xml: &str) -> Result<OutputSerialization, SelectError> {
    let mut output = OutputSerialization {
        csv: None,
        json: None,
    };

    // Check for CSV (handles both <CSV> and <CSV /> formats)
    if has_xml_tag(xml, "CSV") {
        let mut csv = CsvOutput::default();

        if let Some(quote_fields) = extract_xml_value(xml, "QuoteFields") {
            csv.quote_fields = QuoteFields::from_str(&quote_fields)
                .unwrap_or(QuoteFields::AsNeeded);
        }
        if let Some(delim) = extract_xml_value(xml, "FieldDelimiter") {
            csv.field_delimiter = delim;
        }
        if let Some(delim) = extract_xml_value(xml, "RecordDelimiter") {
            csv.record_delimiter = delim;
        }

        output.csv = Some(csv);
    }

    // Check for JSON (handles both <JSON> and <JSON /> formats)
    if has_xml_tag(xml, "JSON") {
        let mut json = JsonOutput::default();

        if let Some(delim) = extract_xml_value(xml, "RecordDelimiter") {
            json.record_delimiter = delim;
        }

        output.json = Some(json);
    }

    Ok(output)
}

/// Extract a single XML element value.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let start = xml.find(&start_tag)? + start_tag.len();
    let end = xml[start..].find(&end_tag)?;

    Some(xml[start..start + end].trim().to_string())
}

/// Generate XML response for Select stats.
pub fn select_stats_to_xml(stats: &SelectStats) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Stats>
    <BytesScanned>{}</BytesScanned>
    <BytesProcessed>{}</BytesProcessed>
    <BytesReturned>{}</BytesReturned>
</Stats>"#,
        stats.bytes_scanned, stats.bytes_processed, stats.bytes_returned
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_parser_basic() {
        let query = SqlParser::parse("SELECT * FROM S3Object").unwrap();
        assert_eq!(query.columns.len(), 1);
        assert!(matches!(query.columns[0], SelectColumn::All));
        assert_eq!(query.table, "S3Object");
        assert!(query.where_clause.is_none());
    }

    #[test]
    fn test_sql_parser_with_columns() {
        let query = SqlParser::parse("SELECT name, age FROM S3Object").unwrap();
        assert_eq!(query.columns.len(), 2);
        assert!(matches!(&query.columns[0], SelectColumn::Named(n) if n == "name"));
        assert!(matches!(&query.columns[1], SelectColumn::Named(n) if n == "age"));
    }

    #[test]
    fn test_sql_parser_with_where() {
        let query = SqlParser::parse("SELECT * FROM S3Object WHERE age > 30").unwrap();
        assert_eq!(query.where_clause, Some("age > 30".to_string()));
    }

    #[test]
    fn test_sql_parser_with_aggregate() {
        let query = SqlParser::parse("SELECT COUNT(*) FROM S3Object").unwrap();
        assert_eq!(query.columns.len(), 1);
        assert!(matches!(
            &query.columns[0],
            SelectColumn::Aggregate { function: AggregateFunction::Count, .. }
        ));
    }

    #[test]
    fn test_execute_csv_select() {
        let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF";

        let request = SelectRequest {
            expression: "SELECT name, age FROM S3Object".to_string(),
            input_serialization: InputSerialization {
                csv: Some(CsvInput::default()),
                ..Default::default()
            },
            output_serialization: OutputSerialization {
                csv: Some(CsvOutput::default()),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = execute_csv_select(csv_data, &request).unwrap();
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0], "Alice,30");
        assert_eq!(result.records[1], "Bob,25");
        assert_eq!(result.records[2], "Charlie,35");
    }

    #[test]
    fn test_execute_csv_select_with_where() {
        let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF";

        let request = SelectRequest {
            expression: "SELECT name FROM S3Object WHERE age > 28".to_string(),
            input_serialization: InputSerialization {
                csv: Some(CsvInput::default()),
                ..Default::default()
            },
            output_serialization: OutputSerialization {
                csv: Some(CsvOutput::default()),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = execute_csv_select(csv_data, &request).unwrap();
        assert_eq!(result.records.len(), 2);
        assert!(result.records.contains(&"Alice".to_string()));
        assert!(result.records.contains(&"Charlie".to_string()));
    }

    #[test]
    fn test_execute_csv_select_all() {
        let csv_data = b"name,age\nAlice,30\nBob,25";

        let request = SelectRequest {
            expression: "SELECT * FROM S3Object".to_string(),
            input_serialization: InputSerialization {
                csv: Some(CsvInput::default()),
                ..Default::default()
            },
            output_serialization: OutputSerialization {
                csv: Some(CsvOutput::default()),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = execute_csv_select(csv_data, &request).unwrap();
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0], "Alice,30");
    }

    #[test]
    fn test_file_header_info() {
        assert_eq!(FileHeaderInfo::from_str("USE"), Some(FileHeaderInfo::Use));
        assert_eq!(FileHeaderInfo::from_str("ignore"), Some(FileHeaderInfo::Ignore));
        assert_eq!(FileHeaderInfo::from_str("NONE"), Some(FileHeaderInfo::None));
        assert_eq!(FileHeaderInfo::from_str("invalid"), None);
    }

    #[test]
    fn test_json_type() {
        assert_eq!(JsonType::from_str("DOCUMENT"), Some(JsonType::Document));
        assert_eq!(JsonType::from_str("lines"), Some(JsonType::Lines));
    }

    #[test]
    fn test_compression_type() {
        assert_eq!(CompressionType::from_str("NONE"), Some(CompressionType::None));
        assert_eq!(CompressionType::from_str("gzip"), Some(CompressionType::Gzip));
        assert_eq!(CompressionType::from_str("BZIP2"), Some(CompressionType::Bzip2));
    }

    #[test]
    fn test_aggregate_function() {
        assert_eq!(AggregateFunction::from_str("COUNT"), Some(AggregateFunction::Count));
        assert_eq!(AggregateFunction::from_str("sum"), Some(AggregateFunction::Sum));
        assert_eq!(AggregateFunction::from_str("AVG"), Some(AggregateFunction::Avg));
        assert_eq!(AggregateFunction::from_str("min"), Some(AggregateFunction::Min));
        assert_eq!(AggregateFunction::from_str("MAX"), Some(AggregateFunction::Max));
    }

    #[test]
    fn test_parse_select_xml() {
        let xml = r#"
            <SelectObjectContentRequest>
                <Expression>SELECT * FROM S3Object</Expression>
                <ExpressionType>SQL</ExpressionType>
                <InputSerialization>
                    <CSV>
                        <FileHeaderInfo>USE</FileHeaderInfo>
                    </CSV>
                </InputSerialization>
                <OutputSerialization>
                    <CSV />
                </OutputSerialization>
            </SelectObjectContentRequest>
        "#;

        let request = parse_select_xml(xml).unwrap();
        assert_eq!(request.expression, "SELECT * FROM S3Object");
        assert!(request.input_serialization.csv.is_some());
        assert!(request.output_serialization.csv.is_some());
    }

    #[test]
    fn test_select_stats_to_xml() {
        let stats = SelectStats {
            bytes_scanned: 1000,
            bytes_processed: 500,
            bytes_returned: 200,
        };

        let xml = select_stats_to_xml(&stats);
        assert!(xml.contains("<BytesScanned>1000</BytesScanned>"));
        assert!(xml.contains("<BytesProcessed>500</BytesProcessed>"));
        assert!(xml.contains("<BytesReturned>200</BytesReturned>"));
    }

    #[test]
    fn test_quote_fields() {
        assert_eq!(QuoteFields::from_str("ASNEEDED"), Some(QuoteFields::AsNeeded));
        assert_eq!(QuoteFields::from_str("AS_NEEDED"), Some(QuoteFields::AsNeeded));
        assert_eq!(QuoteFields::from_str("ALWAYS"), Some(QuoteFields::Always));
    }
}
