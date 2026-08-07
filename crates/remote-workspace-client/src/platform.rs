use std::path::PathBuf;

use tokio::process::{Child, Command};

#[cfg(unix)]
pub(crate) const HOME_ENV: &str = "HOME";
#[cfg(windows)]
pub(crate) const HOME_ENV: &str = "USERPROFILE";

pub(crate) fn home_dir(option: &str) -> anyhow::Result<PathBuf> {
    let home = std::env::var_os(HOME_ENV)
        .ok_or_else(|| anyhow::anyhow!("{HOME_ENV} is not set; pass {option}"))?;
    Ok(PathBuf::from(home))
}

#[cfg(unix)]
pub(crate) fn configure_parent_death(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
}

#[cfg(windows)]
pub(crate) fn configure_parent_death(_command: &mut Command) {}

#[cfg(unix)]
pub(crate) fn attach_parent_death(_child: &mut Child) -> std::io::Result<()> {
    Ok(())
}

#[cfg(windows)]
pub(crate) fn attach_parent_death(child: &mut Child) -> std::io::Result<()> {
    use std::mem::{size_of, zeroed};
    use std::ptr::null;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    static JOB: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

    let job = if let Some(job) = JOB.get() {
        *job as HANDLE
    } else {
        let created = unsafe { CreateJobObjectW(null(), null()) };
        if created.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let configured = unsafe {
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            SetInformationJobObject(
                created,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if configured == 0 {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(created) };
            return Err(error);
        }
        match JOB.set(created as usize) {
            Ok(()) => created,
            Err(_) => {
                unsafe { CloseHandle(created) };
                *JOB.get().expect("job initialized by another thread") as HANDLE
            }
        }
    };

    let Some(process) = child.raw_handle() else {
        return Err(std::io::Error::other(
            "spawned process has no Windows handle",
        ));
    };
    let assigned = unsafe { AssignProcessToJobObject(job, process as HANDLE) };
    if assigned == 0 {
        let error = std::io::Error::last_os_error();
        return Err(error);
    }

    // The process-lifetime handle is intentionally retained. Windows closes it
    // if this client exits for any reason, which kills every assigned child.
    Ok(())
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
    };

    const HELPER_ENV: &str = "REMOTE_WORKSPACE_JOB_TEST_HELPER";

    #[test]
    fn windows_job_kills_child_when_owner_dies() {
        if let Some(pid_file) = std::env::var_os(HELPER_ENV) {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let mut command = Command::new("powershell.exe");
                command.args([
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "Start-Sleep -Seconds 300",
                ]);
                let mut child = command.spawn().unwrap();
                attach_parent_death(&mut child).unwrap();
                std::fs::write(pid_file, child.id().unwrap().to_string()).unwrap();
                std::future::pending::<()>().await;
            });
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("child.pid");
        let mut owner = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "platform::tests::windows_job_kills_child_when_owner_dies",
                "--nocapture",
            ])
            .env(HELPER_ENV, &pid_file)
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let child_pid = loop {
            if let Ok(text) = std::fs::read_to_string(&pid_file) {
                break text.parse::<u32>().unwrap();
            }
            if Instant::now() >= deadline {
                let _ = owner.kill();
                panic!("job test helper did not report its child process");
            }
            std::thread::sleep(Duration::from_millis(50));
        };

        owner.kill().unwrap();
        owner.wait().unwrap();

        let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, child_pid) };
        if process.is_null() {
            return;
        }
        let wait = unsafe { WaitForSingleObject(process, 5_000) };
        unsafe { CloseHandle(process) };
        assert_eq!(wait, WAIT_OBJECT_0, "child process {child_pid} survived");
    }
}
