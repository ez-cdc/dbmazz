use std::fs;
use std::time::Instant;

/// CPU tracker that reads directly from /proc/[pid]/stat
///
/// This tracker provides consistent CPU metrics across:
/// - Bare metal Linux
/// - Docker containers
/// - Kubernetes pods
///
/// It works by reading /proc/[pid]/stat directly and calculating
/// the CPU delta between samples, exactly like `ps` and `top` do.
pub struct CpuTracker {
    pid: u32,
    last_utime: u64,
    last_stime: u64,
    last_time: Instant,
    clock_ticks: f64,
    initialized: bool,
}

impl CpuTracker {
    /// Create a new tracker for the current process
    pub fn new() -> Self {
        let pid = std::process::id();

        // CLK_TCK is the system clock frequency (typically 100 Hz on Linux)
        // This allows us to convert CPU ticks to seconds
        // SAFETY: sysconf(_SC_CLK_TCK) is always safe to call on POSIX systems
        // and returns the number of clock ticks per second.
        let clock_ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) as f64 };
        
        Self {
            pid,
            last_utime: 0,
            last_stime: 0,
            last_time: Instant::now(),
            clock_ticks,
            initialized: false,
        }
    }

    /// Read utime and stime from /proc/[pid]/stat
    ///
    /// Returns (utime, stime) in CPU ticks
    /// utime = time in user mode
    /// stime = time in kernel mode
    fn read_cpu_times(&self) -> Option<(u64, u64)> {
        // Read /proc/[pid]/stat
        // Format: pid (comm) state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime ...
        let stat = fs::read_to_string(format!("/proc/{}/stat", self.pid)).ok()?;

        // The command name can contain spaces and parentheses, so we need to
        // find the last closing parenthesis and start from there
        let stat = stat.trim_end();
        let rpar_pos = stat.rfind(')')?;
        let parts: Vec<&str> = stat[rpar_pos + 1..].split_whitespace().collect();

        // After the closing parenthesis:
        // 0=state 1=ppid 2=pgrp 3=session 4=tty_nr 5=tpgid 6=flags
        // 7=minflt 8=cminflt 9=majflt 10=cmajflt
        // 11=utime 12=stime
        if parts.len() < 13 {
            return None;
        }
        
        let utime: u64 = parts[11].parse().ok()?;
        let stime: u64 = parts[12].parse().ok()?;
        
        Some((utime, stime))
    }

    /// Get CPU consumption in millicores
    ///
    /// Returns:
    /// - 1000 millicores = 100% of 1 core
    /// - 500 millicores = 50% of 1 core
    /// - 35 millicores = 3.5% of 1 core
    ///
    /// The value is consistent between Docker and bare metal.
    pub fn cpu_millicores(&mut self) -> u64 {
        let Some((utime, stime)) = self.read_cpu_times() else {
            return 0;
        };
        
        let now = Instant::now();
        let elapsed_secs = now.duration_since(self.last_time).as_secs_f64();

        // First read or very short interval: just initialize state
        if !self.initialized || elapsed_secs < 0.1 {
            self.last_utime = utime;
            self.last_stime = stime;
            self.last_time = now;
            self.initialized = true;
            return 0;
        }

        // Calculate CPU tick delta since last read
        let delta_utime = utime.saturating_sub(self.last_utime);
        let delta_stime = stime.saturating_sub(self.last_stime);
        let delta_ticks = delta_utime + delta_stime;

        // Convert ticks to CPU seconds used
        // cpu_seconds = ticks / clock_ticks
        let cpu_seconds = delta_ticks as f64 / self.clock_ticks;

        // Calculate millicores
        // millicores = (cpu_seconds / elapsed_seconds) * 1000
        //
        // Example: If the process used 0.2 CPU seconds in 1.0 real seconds:
        // millicores = (0.2 / 1.0) * 1000 = 200 millicores = 20% of 1 core
        let millicores = (cpu_seconds / elapsed_secs) * 1000.0;

        // Update state for next read
        self.last_utime = utime;
        self.last_stime = stime;
        self.last_time = now;

        // Ensure the value is not negative or excessively high due to read errors
        millicores.clamp(0.0, 100000.0) as u64
    }
}

impl Default for CpuTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_cpu_tracker_initialization() {
        let tracker = CpuTracker::new();
        assert!(tracker.pid > 0);
        assert!(tracker.clock_ticks > 0.0);
        assert!(!tracker.initialized);
    }
    
    #[test]
    fn test_cpu_tracker_first_read_returns_zero() {
        let mut tracker = CpuTracker::new();
        // First read should return 0 (no delta yet)
        let millicores = tracker.cpu_millicores();
        assert_eq!(millicores, 0);
        assert!(tracker.initialized);
    }
    
    #[test]
    fn test_cpu_tracker_measures_cpu() {
        let mut tracker = CpuTracker::new();

        // First read (initialization)
        let _ = tracker.cpu_millicores();

        // Wait a bit for measurable activity
        thread::sleep(Duration::from_millis(100));

        // Second read should return a reasonable value
        let millicores = tracker.cpu_millicores();

        // The value should be in a reasonable range (0-1000 millicores typically)
        // In tests it may be low because the process is idle
        assert!(millicores < 10000, "CPU millicores too high: {}", millicores);
    }
}

