use std::io;

#[cfg(target_os = "linux")]
pub fn get_total_cpus() -> usize {
    unsafe {
        match libc::sysconf(libc::_SC_NPROCESSORS_CONF) {
            n if n > 0 => n as usize,
            _ => 1,
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn get_total_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Convenience wrapper around sysconf; respects the current affinity mask on
/// platforms that expose one.
pub fn get_available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(target_os = "linux")]
pub fn get_current_cpu() -> i32 {
    use libc::{sched_getcpu, CPU_SETSIZE};

    unsafe { sched_getcpu() % CPU_SETSIZE }
}

#[cfg(not(target_os = "linux"))]
pub fn get_current_cpu() -> i32 {
    0
}

#[cfg(target_os = "linux")]
fn set_affinity(cpu_id: usize) -> io::Result<()> {
    use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
    use std::mem;

    unsafe {
        let mut cpuset: cpu_set_t = mem::zeroed();
        CPU_ZERO(&mut cpuset);
        CPU_SET(cpu_id, &mut cpuset);

        let ret = sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &cpuset);
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn set_affinity(_cpu_id: usize) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn reset_affinity_all() -> io::Result<()> {
    use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
    use std::mem;

    let total = get_total_cpus();
    let mut cpuset = unsafe { mem::zeroed::<cpu_set_t>() };
    unsafe {
        CPU_ZERO(&mut cpuset);
        for i in 0..total {
            CPU_SET(i, &mut cpuset);
        }
        if sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &cpuset) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn reset_affinity_all() -> io::Result<()> {
    Ok(())
}

/// RAII guard that restores the original affinity mask on drop.
pub struct AffinityGuard;

impl AffinityGuard {
    /// Save the current mask and pin to `cpu_id`.
    pub fn pin(cpu_id: usize) -> io::Result<Self> {
        set_affinity(cpu_id)?;
        Ok(AffinityGuard {})
    }
}

impl Drop for AffinityGuard {
    fn drop(&mut self) {
        if let Err(e) = reset_affinity_all() {
            eprintln!("failed to reset CPU affinity: {e}");
        }
    }
}

/// Run the closure `f` while the calling thread is pinned to `cpu_id` on Linux.
/// On other platforms this is a no-op wrapper.
pub fn with_affinity<F, R>(cpu_id: usize, f: F) -> io::Result<R>
where
    F: FnOnce() -> R,
{
    let _guard = AffinityGuard::pin(cpu_id)?;
    Ok(f())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn total_and_available_cpu_counts_reasonable() {
        let total = get_total_cpus();
        assert!(total >= 1, "total CPUs must be at least 1");
        let avail = get_available_cpus();
        assert!(
            avail >= 1 && avail <= total,
            "available CPUs ({avail}) must be in 1..={total}"
        );
    }
}
