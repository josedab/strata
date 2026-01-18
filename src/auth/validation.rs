//! Input validation for security-sensitive operations.
//!
//! This module provides validation utilities to prevent common attacks
//! such as path traversal, symlink attacks, and injection vulnerabilities.

use crate::error::{Result, StrataError};
use std::path::{Component, Path, PathBuf};
use tracing::warn;

/// Maximum allowed path length.
pub const MAX_PATH_LENGTH: usize = 4096;

/// Maximum allowed filename length.
pub const MAX_FILENAME_LENGTH: usize = 255;

/// Maximum symlink resolution depth.
pub const MAX_SYMLINK_DEPTH: usize = 40;

/// Path validation configuration.
#[derive(Debug, Clone)]
pub struct PathValidationConfig {
    /// Whether to allow absolute paths.
    pub allow_absolute: bool,
    /// Whether to allow symlinks.
    pub allow_symlinks: bool,
    /// Whether to allow parent directory traversal (..).
    pub allow_parent_traversal: bool,
    /// Base directory for relative path resolution.
    pub base_dir: Option<PathBuf>,
    /// Maximum path depth.
    pub max_depth: usize,
}

impl Default for PathValidationConfig {
    fn default() -> Self {
        Self {
            allow_absolute: false,
            allow_symlinks: true,
            allow_parent_traversal: false,
            base_dir: None,
            max_depth: 100,
        }
    }
}

impl PathValidationConfig {
    /// Create a strict configuration (no traversal, no absolute paths).
    pub fn strict() -> Self {
        Self {
            allow_absolute: false,
            allow_symlinks: false,
            allow_parent_traversal: false,
            base_dir: None,
            max_depth: 50,
        }
    }

    /// Create a permissive configuration for internal operations.
    pub fn permissive() -> Self {
        Self {
            allow_absolute: true,
            allow_symlinks: true,
            allow_parent_traversal: true,
            base_dir: None,
            max_depth: 200,
        }
    }
}

/// Validate a path for security concerns.
pub fn validate_path(path: &str, config: &PathValidationConfig) -> Result<PathBuf> {
    // Check length
    if path.len() > MAX_PATH_LENGTH {
        return Err(StrataError::InvalidPath(format!(
            "Path exceeds maximum length of {} bytes",
            MAX_PATH_LENGTH
        )));
    }

    // Check for null bytes
    if path.contains('\0') {
        warn!(path_len = path.len(), "Path contains null byte");
        return Err(StrataError::InvalidPath(
            "Path contains null byte".to_string(),
        ));
    }

    let path_obj = Path::new(path);

    // Check absolute path
    if path_obj.is_absolute() && !config.allow_absolute {
        return Err(StrataError::InvalidPath(
            "Absolute paths not allowed".to_string(),
        ));
    }

    // Normalize and validate components
    let mut normalized = PathBuf::new();
    let mut depth = 0;

    for component in path_obj.components() {
        match component {
            Component::Normal(name) => {
                let name_str = name.to_string_lossy();

                // Check filename length
                if name_str.len() > MAX_FILENAME_LENGTH {
                    return Err(StrataError::InvalidPath(format!(
                        "Filename exceeds maximum length of {} bytes",
                        MAX_FILENAME_LENGTH
                    )));
                }

                // Check for hidden control characters
                if name_str.chars().any(|c| c.is_control() && c != '\t') {
                    warn!(name = %name_str, "Filename contains control characters");
                    return Err(StrataError::InvalidPath(
                        "Filename contains control characters".to_string(),
                    ));
                }

                normalized.push(name);
                depth += 1;

                if depth > config.max_depth {
                    return Err(StrataError::InvalidPath(format!(
                        "Path depth exceeds maximum of {}",
                        config.max_depth
                    )));
                }
            }
            Component::ParentDir => {
                if !config.allow_parent_traversal {
                    return Err(StrataError::InvalidPath(
                        "Parent directory traversal not allowed".to_string(),
                    ));
                }

                // Check if we'd escape base directory
                if let Some(ref base) = config.base_dir {
                    let resolved = base.join(&normalized).join("..");
                    if !resolved.starts_with(base) {
                        warn!(
                            base = %base.display(),
                            path = %normalized.display(),
                            "Path traversal attempt detected"
                        );
                        return Err(StrataError::InvalidPath(
                            "Path escapes base directory".to_string(),
                        ));
                    }
                }

                normalized.push("..");
            }
            Component::CurDir => {
                // Skip current directory references
            }
            Component::RootDir => {
                if config.allow_absolute {
                    normalized.push("/");
                }
            }
            Component::Prefix(_) => {
                // Windows prefix handling
                if config.allow_absolute {
                    normalized.push(component.as_os_str());
                }
            }
        }
    }

    Ok(normalized)
}

/// Validate a filename (single component, no slashes).
pub fn validate_filename(name: &str) -> Result<()> {
    // Check length
    if name.is_empty() {
        return Err(StrataError::InvalidPath("Filename cannot be empty".to_string()));
    }

    if name.len() > MAX_FILENAME_LENGTH {
        return Err(StrataError::InvalidPath(format!(
            "Filename exceeds maximum length of {} bytes",
            MAX_FILENAME_LENGTH
        )));
    }

    // Check for null bytes
    if name.contains('\0') {
        return Err(StrataError::InvalidPath(
            "Filename contains null byte".to_string(),
        ));
    }

    // Check for path separators
    if name.contains('/') || name.contains('\\') {
        return Err(StrataError::InvalidPath(
            "Filename contains path separator".to_string(),
        ));
    }

    // Check for reserved names
    if name == "." || name == ".." {
        return Err(StrataError::InvalidPath(
            "Reserved filename".to_string(),
        ));
    }

    // Check for control characters
    if name.chars().any(|c| c.is_control()) {
        return Err(StrataError::InvalidPath(
            "Filename contains control characters".to_string(),
        ));
    }

    Ok(())
}

/// Validate a symlink target.
pub fn validate_symlink_target(
    target: &str,
    base_dir: Option<&Path>,
    allow_absolute: bool,
) -> Result<PathBuf> {
    // Check length
    if target.len() > MAX_PATH_LENGTH {
        return Err(StrataError::InvalidPath(
            "Symlink target exceeds maximum length".to_string(),
        ));
    }

    // Check for null bytes
    if target.contains('\0') {
        return Err(StrataError::InvalidPath(
            "Symlink target contains null byte".to_string(),
        ));
    }

    let target_path = Path::new(target);

    // Check absolute symlinks
    if target_path.is_absolute() && !allow_absolute {
        return Err(StrataError::InvalidPath(
            "Absolute symlinks not allowed".to_string(),
        ));
    }

    // Count parent directory traversals
    let traversal_count = target_path
        .components()
        .filter(|c| matches!(c, Component::ParentDir))
        .count();

    // Warn on excessive traversals
    if traversal_count > 10 {
        warn!(
            target,
            traversals = traversal_count,
            "Excessive parent directory traversals in symlink"
        );
    }

    // If base directory specified, ensure target doesn't escape
    if let Some(base) = base_dir {
        // This is a heuristic - actual resolution requires following the symlink
        let normalized = normalize_path(target_path);
        if normalized.is_absolute() && !normalized.starts_with(base) {
            return Err(StrataError::InvalidPath(
                "Symlink target escapes base directory".to_string(),
            ));
        }
    }

    Ok(target_path.to_path_buf())
}

/// Normalize a path without filesystem access.
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            _ => {
                normalized.push(component);
            }
        }
    }

    normalized
}

/// Check if a path is within a base directory (without filesystem access).
pub fn is_within_base(path: &Path, base: &Path) -> bool {
    let normalized = normalize_path(path);
    let base_normalized = normalize_path(base);

    normalized.starts_with(base_normalized)
}

/// Sanitize a filename by removing dangerous characters.
pub fn sanitize_filename(name: &str) -> String {
    let mut sanitized = String::with_capacity(name.len());

    for c in name.chars() {
        match c {
            // Replace dangerous characters
            '/' | '\\' | '\0' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => {
                sanitized.push('_');
            }
            // Remove control characters
            c if c.is_control() => {}
            // Keep everything else
            c => sanitized.push(c),
        }
    }

    // Handle reserved names
    if sanitized == "." || sanitized == ".." {
        sanitized = "_".to_string();
    }

    // Ensure non-empty
    if sanitized.is_empty() {
        sanitized = "_".to_string();
    }

    // Truncate if needed
    if sanitized.len() > MAX_FILENAME_LENGTH {
        sanitized.truncate(MAX_FILENAME_LENGTH);
    }

    sanitized
}

/// Validate extended attribute name.
pub fn validate_xattr_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(StrataError::InvalidPath(
            "Extended attribute name cannot be empty".to_string(),
        ));
    }

    if name.len() > 255 {
        return Err(StrataError::InvalidPath(
            "Extended attribute name too long".to_string(),
        ));
    }

    if name.contains('\0') {
        return Err(StrataError::InvalidPath(
            "Extended attribute name contains null byte".to_string(),
        ));
    }

    // Check for valid xattr namespace prefix
    let valid_prefixes = ["user.", "security.", "system.", "trusted."];
    if !valid_prefixes.iter().any(|p| name.starts_with(p)) && !name.starts_with("com.") {
        warn!(name, "Extended attribute has non-standard namespace");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_path_normal() {
        let config = PathValidationConfig::default();
        let result = validate_path("foo/bar/baz.txt", &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_traversal() {
        let config = PathValidationConfig::default();
        let result = validate_path("foo/../../../etc/passwd", &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_path_absolute() {
        let config = PathValidationConfig::default();
        let result = validate_path("/etc/passwd", &config);
        assert!(result.is_err());

        let permissive = PathValidationConfig::permissive();
        let result = validate_path("/etc/passwd", &permissive);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_null_byte() {
        let config = PathValidationConfig::default();
        let result = validate_path("foo\0bar", &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_filename() {
        assert!(validate_filename("normal.txt").is_ok());
        assert!(validate_filename("").is_err());
        assert!(validate_filename("foo/bar").is_err());
        assert!(validate_filename(".").is_err());
        assert!(validate_filename("..").is_err());
    }

    #[test]
    fn test_validate_symlink_target() {
        assert!(validate_symlink_target("../other/file", None, false).is_ok());
        assert!(validate_symlink_target("/absolute/path", None, false).is_err());
        assert!(validate_symlink_target("/absolute/path", None, true).is_ok());
    }

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("normal.txt"), "normal.txt");
        assert_eq!(sanitize_filename("foo/bar"), "foo_bar");
        assert_eq!(sanitize_filename("file\0name"), "file_name");
        assert_eq!(sanitize_filename(".."), "_");
        assert_eq!(sanitize_filename(""), "_");
    }

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path(Path::new("foo/bar/baz")), PathBuf::from("foo/bar/baz"));
        assert_eq!(normalize_path(Path::new("foo/./bar")), PathBuf::from("foo/bar"));
        assert_eq!(normalize_path(Path::new("foo/bar/../baz")), PathBuf::from("foo/baz"));
    }

    #[test]
    fn test_is_within_base() {
        assert!(is_within_base(Path::new("/home/user/file"), Path::new("/home/user")));
        assert!(!is_within_base(Path::new("/etc/passwd"), Path::new("/home/user")));
    }

    #[test]
    fn test_validate_xattr_name() {
        assert!(validate_xattr_name("user.foo").is_ok());
        assert!(validate_xattr_name("security.bar").is_ok());
        assert!(validate_xattr_name("").is_err());
        assert!(validate_xattr_name("name\0with\0null").is_err());
    }
}
