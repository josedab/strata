//! S3 XML request/response parsing and generation.
//!
//! This module handles parsing S3 XML request bodies (like CompleteMultipartUpload)
//! and generating XML response bodies for S3 operations.

use serde::{Deserialize, Serialize};

/// Parse the CompleteMultipartUpload XML request body.
///
/// Example input:
/// ```xml
/// <CompleteMultipartUpload>
///   <Part>
///     <PartNumber>1</PartNumber>
///     <ETag>"etag1"</ETag>
///   </Part>
///   <Part>
///     <PartNumber>2</PartNumber>
///     <ETag>"etag2"</ETag>
///   </Part>
/// </CompleteMultipartUpload>
/// ```
pub fn parse_complete_multipart_upload(xml: &str) -> Result<Vec<CompletedPart>, XmlParseError> {
    let mut parts = Vec::new();

    // Find all <Part>...</Part> blocks
    let mut search_start = 0;
    while let Some(part_start) = xml[search_start..].find("<Part>") {
        let part_start = search_start + part_start;
        let part_content_start = part_start + 6; // len("<Part>")

        if let Some(part_end) = xml[part_content_start..].find("</Part>") {
            let part_end = part_content_start + part_end;
            let part_content = &xml[part_content_start..part_end];

            // Extract PartNumber and ETag from the part content
            let part_number = extract_tag_value(part_content, "PartNumber")
                .and_then(|s| s.parse::<u32>().ok());
            let etag = extract_tag_value(part_content, "ETag").map(|s| s.to_string());

            match (part_number, etag) {
                (Some(pn), Some(e)) => {
                    parts.push(CompletedPart {
                        part_number: pn,
                        etag: e,
                    });
                }
                _ => {
                    return Err(XmlParseError::MissingField(
                        "Part requires PartNumber and ETag".to_string(),
                    ));
                }
            }

            search_start = part_end + 7; // len("</Part>")
        } else {
            break;
        }
    }

    if parts.is_empty() {
        return Err(XmlParseError::InvalidFormat(
            "No parts found in CompleteMultipartUpload".to_string(),
        ));
    }

    Ok(parts)
}

/// Extract value from a simple XML tag like <Tag>value</Tag>
fn extract_tag_value<'a>(line: &'a str, tag: &str) -> Option<&'a str> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    if let Some(start) = line.find(&start_tag) {
        let value_start = start + start_tag.len();
        if let Some(end) = line.find(&end_tag) {
            if end > value_start {
                return Some(&line[value_start..end]);
            }
        }
    }
    None
}

/// A completed part from the CompleteMultipartUpload request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPart {
    pub part_number: u32,
    pub etag: String,
}

/// Error type for XML parsing.
#[derive(Debug)]
pub enum XmlParseError {
    InvalidFormat(String),
    MissingField(String),
}

impl std::fmt::Display for XmlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XmlParseError::InvalidFormat(msg) => write!(f, "Invalid XML format: {}", msg),
            XmlParseError::MissingField(msg) => write!(f, "Missing required field: {}", msg),
        }
    }
}

impl std::error::Error for XmlParseError {}

// XML response generators

/// Generate InitiateMultipartUploadResult XML response.
pub fn initiate_multipart_upload_result(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    )
}

/// Generate CompleteMultipartUploadResult XML response.
pub fn complete_multipart_upload_result(
    bucket: &str,
    key: &str,
    location: &str,
    etag: &str,
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Location>{}</Location>
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <ETag>{}</ETag>
</CompleteMultipartUploadResult>"#,
        xml_escape(location),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag)
    )
}

/// Generate ListPartsResult XML response.
pub fn list_parts_result(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[PartInfo],
) -> String {
    let parts_xml: String = parts
        .iter()
        .map(|p| {
            format!(
                r#"    <Part>
        <PartNumber>{}</PartNumber>
        <ETag>{}</ETag>
        <Size>{}</Size>
    </Part>"#,
                p.part_number,
                xml_escape(&p.etag),
                p.size
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <UploadId>{}</UploadId>
    <IsTruncated>false</IsTruncated>
{}
</ListPartsResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
        parts_xml
    )
}

/// Generate ListMultipartUploadsResult XML response.
pub fn list_multipart_uploads_result(
    bucket: &str,
    uploads: &[UploadInfo],
) -> String {
    let uploads_xml: String = uploads
        .iter()
        .map(|u| {
            format!(
                r#"    <Upload>
        <Key>{}</Key>
        <UploadId>{}</UploadId>
        <Initiated>{}</Initiated>
    </Upload>"#,
                xml_escape(&u.key),
                xml_escape(&u.upload_id),
                u.initiated
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{}</Bucket>
    <IsTruncated>false</IsTruncated>
{}
</ListMultipartUploadsResult>"#,
        xml_escape(bucket),
        uploads_xml
    )
}

/// Part information for ListParts response.
#[derive(Debug, Clone)]
pub struct PartInfo {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
}

/// Upload information for ListMultipartUploads response.
#[derive(Debug, Clone)]
pub struct UploadInfo {
    pub key: String,
    pub upload_id: String,
    pub initiated: String, // ISO 8601 timestamp
}

/// Generate S3 error response XML.
pub fn error_response(code: &str, message: &str, resource: Option<&str>) -> String {
    let resource_xml = resource
        .map(|r| format!("\n    <Resource>{}</Resource>", xml_escape(r)))
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>{}
</Error>"#,
        xml_escape(code),
        xml_escape(message),
        resource_xml
    )
}

/// Escape special XML characters.
pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_complete_multipart_upload() {
        let xml = r#"
        <CompleteMultipartUpload>
            <Part>
                <PartNumber>1</PartNumber>
                <ETag>"abc123"</ETag>
            </Part>
            <Part>
                <PartNumber>2</PartNumber>
                <ETag>"def456"</ETag>
            </Part>
        </CompleteMultipartUpload>
        "#;

        let parts = parse_complete_multipart_upload(xml).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"abc123\"");
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[1].etag, "\"def456\"");
    }

    #[test]
    fn test_parse_complete_multipart_upload_compact() {
        let xml = r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"etag1"</ETag></Part></CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_upload(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
    }

    #[test]
    fn test_initiate_multipart_upload_result() {
        let result = initiate_multipart_upload_result("mybucket", "mykey", "upload-123");
        assert!(result.contains("<Bucket>mybucket</Bucket>"));
        assert!(result.contains("<Key>mykey</Key>"));
        assert!(result.contains("<UploadId>upload-123</UploadId>"));
    }

    #[test]
    fn test_complete_multipart_upload_result() {
        let result = complete_multipart_upload_result(
            "mybucket",
            "mykey",
            "http://localhost/mybucket/mykey",
            "\"combined-etag\"",
        );
        assert!(result.contains("<Bucket>mybucket</Bucket>"));
        assert!(result.contains("<Key>mykey</Key>"));
        // ETag quotes are escaped in XML output
        assert!(result.contains("<ETag>&quot;combined-etag&quot;</ETag>"));
    }

    #[test]
    fn test_error_response() {
        let result = error_response("NoSuchUpload", "The specified upload does not exist", Some("upload-123"));
        assert!(result.contains("<Code>NoSuchUpload</Code>"));
        assert!(result.contains("<Message>The specified upload does not exist</Message>"));
        assert!(result.contains("<Resource>upload-123</Resource>"));
    }

    #[test]
    fn test_xml_escape() {
        assert_eq!(xml_escape("hello"), "hello");
        assert_eq!(xml_escape("<test>"), "&lt;test&gt;");
        assert_eq!(xml_escape("a & b"), "a &amp; b");
        assert_eq!(xml_escape("\"quoted\""), "&quot;quoted&quot;");
    }
}
