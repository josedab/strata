// Test fixtures and data generators for integration tests

use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::path::PathBuf;

/// Deterministic random data generator for reproducible tests
pub struct TestDataGenerator {
    rng: StdRng,
}

impl TestDataGenerator {
    /// Creates a new generator with a fixed seed for reproducibility
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Generates random bytes of specified length
    pub fn random_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; len];
        self.rng.fill(&mut bytes[..]);
        bytes
    }

    /// Generates a random string of specified length
    pub fn random_string(&mut self, len: usize) -> String {
        (&mut self.rng)
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    /// Generates a random file name
    pub fn random_filename(&mut self) -> String {
        format!("{}.dat", self.random_string(8))
    }

    /// Generates a random directory path
    pub fn random_path(&mut self, depth: usize) -> PathBuf {
        let mut path = PathBuf::new();
        for _ in 0..depth {
            path.push(self.random_string(6));
        }
        path
    }

    /// Generates test file content with a pattern that can be verified
    pub fn verifiable_content(&mut self, size: usize) -> VerifiableContent {
        let seed = self.rng.gen();
        VerifiableContent::new(seed, size)
    }
}

impl Default for TestDataGenerator {
    fn default() -> Self {
        Self::new(42)
    }
}

/// Content that can be generated and verified deterministically
#[derive(Debug, Clone)]
pub struct VerifiableContent {
    seed: u64,
    size: usize,
}

impl VerifiableContent {
    pub fn new(seed: u64, size: usize) -> Self {
        Self { seed, size }
    }

    /// Generates the content
    pub fn generate(&self) -> Vec<u8> {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let mut bytes = vec![0u8; self.size];
        rng.fill(&mut bytes[..]);
        bytes
    }

    /// Verifies that data matches the expected content
    pub fn verify(&self, data: &[u8]) -> bool {
        if data.len() != self.size {
            return false;
        }
        let expected = self.generate();
        data == expected.as_slice()
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

/// Standard test file sizes
pub mod file_sizes {
    pub const TINY: usize = 1;
    pub const SMALL: usize = 1024;
    pub const MEDIUM: usize = 1024 * 1024;
    pub const LARGE: usize = 10 * 1024 * 1024;
    pub const HUGE: usize = 100 * 1024 * 1024;

    // Edge cases
    pub const ZERO: usize = 0;
    pub const ONE_BYTE: usize = 1;
    pub const CHUNK_SIZE: usize = 4 * 1024 * 1024; // Default chunk size
    pub const CHUNK_MINUS_ONE: usize = 4 * 1024 * 1024 - 1;
    pub const CHUNK_PLUS_ONE: usize = 4 * 1024 * 1024 + 1;
    pub const TWO_CHUNKS: usize = 8 * 1024 * 1024;
}

/// Test file fixture
#[derive(Debug, Clone)]
pub struct TestFile {
    pub name: String,
    pub path: PathBuf,
    pub content: VerifiableContent,
    pub metadata: FileMetadata,
}

impl TestFile {
    pub fn new(name: impl Into<String>, size: usize) -> Self {
        let name = name.into();
        Self {
            path: PathBuf::from(&name),
            name,
            content: VerifiableContent::new(rand::random(), size),
            metadata: FileMetadata::default(),
        }
    }

    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }

    pub fn with_metadata(mut self, metadata: FileMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// File metadata for tests
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub custom: HashMap<String, String>,
}

impl Default for FileMetadata {
    fn default() -> Self {
        Self {
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            custom: HashMap::new(),
        }
    }
}

/// Test directory structure
#[derive(Debug, Clone)]
pub struct TestDirectory {
    pub name: String,
    pub path: PathBuf,
    pub files: Vec<TestFile>,
    pub subdirs: Vec<TestDirectory>,
}

impl TestDirectory {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            path: PathBuf::from(&name),
            name,
            files: Vec::new(),
            subdirs: Vec::new(),
        }
    }

    pub fn with_file(mut self, file: TestFile) -> Self {
        self.files.push(file);
        self
    }

    pub fn with_subdir(mut self, subdir: TestDirectory) -> Self {
        self.subdirs.push(subdir);
        self
    }

    /// Creates a standard test directory structure
    pub fn standard_structure() -> Self {
        TestDirectory::new("root")
            .with_file(TestFile::new("readme.txt", file_sizes::SMALL))
            .with_file(TestFile::new("data.bin", file_sizes::MEDIUM))
            .with_subdir(
                TestDirectory::new("docs")
                    .with_file(TestFile::new("doc1.txt", file_sizes::SMALL))
                    .with_file(TestFile::new("doc2.txt", file_sizes::SMALL)),
            )
            .with_subdir(
                TestDirectory::new("data")
                    .with_file(TestFile::new("large.bin", file_sizes::LARGE))
                    .with_subdir(
                        TestDirectory::new("nested")
                            .with_file(TestFile::new("nested.txt", file_sizes::SMALL)),
                    ),
            )
    }

    /// Counts total files in the directory tree
    pub fn total_files(&self) -> usize {
        let mut count = self.files.len();
        for subdir in &self.subdirs {
            count += subdir.total_files();
        }
        count
    }

    /// Gets total size of all files
    pub fn total_size(&self) -> usize {
        let mut size: usize = self.files.iter().map(|f| f.content.size()).sum();
        for subdir in &self.subdirs {
            size += subdir.total_size();
        }
        size
    }
}

/// Test bucket configuration
#[derive(Debug, Clone)]
pub struct TestBucket {
    pub name: String,
    pub versioning: bool,
    pub encryption: bool,
    pub objects: Vec<TestObject>,
}

impl TestBucket {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            versioning: false,
            encryption: false,
            objects: Vec::new(),
        }
    }

    pub fn with_versioning(mut self) -> Self {
        self.versioning = true;
        self
    }

    pub fn with_encryption(mut self) -> Self {
        self.encryption = true;
        self
    }

    pub fn with_object(mut self, object: TestObject) -> Self {
        self.objects.push(object);
        self
    }
}

/// Test S3 object
#[derive(Debug, Clone)]
pub struct TestObject {
    pub key: String,
    pub content: VerifiableContent,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
}

impl TestObject {
    pub fn new(key: impl Into<String>, size: usize) -> Self {
        Self {
            key: key.into(),
            content: VerifiableContent::new(rand::random(), size),
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Test user/credentials
#[derive(Debug, Clone)]
pub struct TestUser {
    pub name: String,
    pub access_key: String,
    pub secret_key: String,
    pub is_admin: bool,
}

impl TestUser {
    pub fn new(name: impl Into<String>) -> Self {
        let mut gen = TestDataGenerator::new(rand::random());
        Self {
            name: name.into(),
            access_key: gen.random_string(20),
            secret_key: gen.random_string(40),
            is_admin: false,
        }
    }

    pub fn admin(name: impl Into<String>) -> Self {
        let mut user = Self::new(name);
        user.is_admin = true;
        user
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verifiable_content() {
        let content = VerifiableContent::new(12345, 1024);
        let data = content.generate();
        assert_eq!(data.len(), 1024);
        assert!(content.verify(&data));

        // Modified data should not verify
        let mut modified = data.clone();
        modified[0] ^= 0xFF;
        assert!(!content.verify(&modified));
    }

    #[test]
    fn test_deterministic_generation() {
        let mut gen1 = TestDataGenerator::new(42);
        let mut gen2 = TestDataGenerator::new(42);

        let bytes1 = gen1.random_bytes(100);
        let bytes2 = gen2.random_bytes(100);
        assert_eq!(bytes1, bytes2);

        let str1 = gen1.random_string(20);
        let str2 = gen2.random_string(20);
        assert_eq!(str1, str2);
    }

    #[test]
    fn test_directory_structure() {
        let dir = TestDirectory::standard_structure();
        assert!(dir.total_files() > 0);
        assert!(dir.total_size() > 0);
    }
}
