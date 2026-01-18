//! Unix daemonization support.
//!
//! Provides functionality for running processes as background daemons
//! using the standard Unix double-fork approach.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

/// Configuration for daemon process.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Working directory for the daemon (default: /).
    pub working_directory: Option<String>,
    /// File to write the PID to.
    pub pid_file: Option<String>,
    /// File to redirect stdout to.
    pub stdout_file: Option<String>,
    /// File to redirect stderr to.
    pub stderr_file: Option<String>,
    /// Umask for the daemon process.
    pub umask: Option<u32>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            working_directory: Some("/".to_string()),
            pid_file: None,
            stdout_file: None,
            stderr_file: None,
            umask: Some(0o027),
        }
    }
}

impl DaemonConfig {
    /// Creates a new daemon configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the working directory.
    pub fn working_directory(mut self, dir: impl Into<String>) -> Self {
        self.working_directory = Some(dir.into());
        self
    }

    /// Sets the PID file path.
    pub fn pid_file(mut self, path: impl Into<String>) -> Self {
        self.pid_file = Some(path.into());
        self
    }

    /// Sets the stdout redirect file.
    pub fn stdout(mut self, path: impl Into<String>) -> Self {
        self.stdout_file = Some(path.into());
        self
    }

    /// Sets the stderr redirect file.
    pub fn stderr(mut self, path: impl Into<String>) -> Self {
        self.stderr_file = Some(path.into());
        self
    }
}

/// Daemonizes the current process using the Unix double-fork approach.
///
/// This function:
/// 1. Forks the process
/// 2. Creates a new session (setsid)
/// 3. Forks again to ensure the daemon cannot acquire a controlling terminal
/// 4. Changes the working directory
/// 5. Sets the umask
/// 6. Redirects standard file descriptors
///
/// # Errors
///
/// Returns an error if any of the system calls fail.
#[cfg(unix)]
pub fn daemonize(config: &DaemonConfig) -> io::Result<()> {
    // First fork
    // SAFETY: fork() is safe to call; we handle the return value properly.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(io::Error::last_os_error());
    }
    if pid > 0 {
        // Parent process exits
        std::process::exit(0);
    }

    // Create new session and become session leader
    // SAFETY: setsid() is safe to call after fork.
    if unsafe { libc::setsid() } < 0 {
        return Err(io::Error::last_os_error());
    }

    // Second fork to prevent acquiring a controlling terminal
    // SAFETY: fork() is safe to call.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(io::Error::last_os_error());
    }
    if pid > 0 {
        // First child exits
        std::process::exit(0);
    }

    // Now we're the daemon (grandchild)

    // Set umask
    if let Some(mask) = config.umask {
        // SAFETY: umask() is always safe to call.
        unsafe { libc::umask(mask as libc::mode_t) };
    }

    // Change working directory
    if let Some(ref dir) = config.working_directory {
        std::env::set_current_dir(Path::new(dir))?;
    }

    // Write PID file
    if let Some(ref pid_path) = config.pid_file {
        let pid = std::process::id();
        std::fs::write(pid_path, format!("{}\n", pid))?;
    }

    // Redirect standard file descriptors
    redirect_std_streams(config)?;

    Ok(())
}

/// Redirects standard file descriptors to the configured files or /dev/null.
#[cfg(unix)]
fn redirect_std_streams(config: &DaemonConfig) -> io::Result<()> {

    // Open /dev/null for stdin
    let dev_null = File::open("/dev/null")?;

    // SAFETY: dup2 is safe when called with valid file descriptors.
    unsafe {
        libc::dup2(dev_null.as_raw_fd(), libc::STDIN_FILENO);
    }

    // Handle stdout
    if let Some(ref stdout_path) = config.stdout_file {
        let stdout_file = File::create(stdout_path)?;
        // SAFETY: dup2 is safe when called with valid file descriptors.
        unsafe {
            libc::dup2(stdout_file.as_raw_fd(), libc::STDOUT_FILENO);
        }
        std::mem::forget(stdout_file);
    } else {
        // SAFETY: dup2 is safe when called with valid file descriptors.
        unsafe {
            libc::dup2(dev_null.as_raw_fd(), libc::STDOUT_FILENO);
        }
    }

    // Handle stderr
    if let Some(ref stderr_path) = config.stderr_file {
        let stderr_file = File::create(stderr_path)?;
        // SAFETY: dup2 is safe when called with valid file descriptors.
        unsafe {
            libc::dup2(stderr_file.as_raw_fd(), libc::STDERR_FILENO);
        }
        std::mem::forget(stderr_file);
    } else {
        // SAFETY: dup2 is safe when called with valid file descriptors.
        unsafe {
            libc::dup2(dev_null.as_raw_fd(), libc::STDERR_FILENO);
        }
    }

    Ok(())
}

/// Non-Unix stub implementation.
#[cfg(not(unix))]
pub fn daemonize(_config: &DaemonConfig) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Daemonization is only supported on Unix systems",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_config_builder() {
        let config = DaemonConfig::new()
            .working_directory("/tmp")
            .pid_file("/tmp/test.pid")
            .stdout("/tmp/stdout.log")
            .stderr("/tmp/stderr.log");

        assert_eq!(config.working_directory, Some("/tmp".to_string()));
        assert_eq!(config.pid_file, Some("/tmp/test.pid".to_string()));
        assert_eq!(config.stdout_file, Some("/tmp/stdout.log".to_string()));
        assert_eq!(config.stderr_file, Some("/tmp/stderr.log".to_string()));
    }

    #[test]
    fn test_daemon_config_defaults() {
        let config = DaemonConfig::default();
        assert_eq!(config.working_directory, Some("/".to_string()));
        assert_eq!(config.umask, Some(0o027));
        assert!(config.pid_file.is_none());
    }
}
