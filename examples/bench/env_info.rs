//! Environment metadata embedded in the benchmark's summary output, so a reading names the
//! machine, build and commit it came from instead of asking the reader to trust a bare number -
//! the ticket's own requirement that every measurement travel with enough context to re-run and
//! compare.

use std::process::Command;

use serde::Serialize;

/// What this benchmark's numbers depend on: the machine, the disk, the build and the code under
/// test.
#[derive(Serialize, Debug, Clone)]
pub struct Environment {
    pub os: String,
    pub cpu: String,
    pub logical_cpus: usize,
    pub filesystem: String,
    pub profile: &'static str,
    pub commit: String,
    pub rustc: String,
}

/// Run `cmd args`, discarding stderr, and return trimmed stdout - `None` on any failure
/// (missing binary, non-zero exit, non-UTF-8 output). Every caller in this module treats a
/// missing fact as `"unknown"` rather than failing the whole benchmark over one absent tool.
fn run(cmd: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn cpu_model() -> String {
    run("sysctl", &["-n", "machdep.cpu.brand_string"])
        .or_else(|| {
            let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
            let line = cpuinfo.lines().find(|line| line.starts_with("model name"))?;
            let (_, value) = line.split_once(':')?;
            Some(value.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// The mount point `.` (the current directory) lives on, per `df -P`'s POSIX-stable column
/// layout (`Filesystem ... Mounted on`, the last column).
fn mount_point() -> Option<String> {
    let df = run("df", &["-P", "."])?;
    let data_line = df.lines().nth(1)?;
    data_line.split_whitespace().last().map(str::to_string)
}

/// The filesystem type of `.`'s mount point - `diskutil` on macOS, `findmnt` on Linux; `"unknown"`
/// if neither is present.
fn filesystem_type() -> String {
    let Some(mount_point) = mount_point() else {
        return "unknown".to_string();
    };

    run("diskutil", &["info", &mount_point])
        .and_then(|info| {
            let line = info.lines().find(|line| line.contains("Type (Bundle)"))?;
            let (_, value) = line.split_once(':')?;
            Some(value.trim().to_string())
        })
        .or_else(|| run("findmnt", &["-no", "FSTYPE", &mount_point]))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Collect what this benchmark's numbers depend on. Every fact-gathering command here is
/// best-effort: a missing tool degrades that one field to `"unknown"` instead of failing the run.
pub fn collect() -> Environment {
    Environment {
        os: run("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string()),
        cpu: cpu_model(),
        logical_cpus: std::thread::available_parallelism().map(std::num::NonZero::get).unwrap_or(1),
        filesystem: filesystem_type(),
        profile: "release",
        commit: run("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string()),
        rustc: run("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string()),
    }
}
