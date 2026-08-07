use tokio::process::{Child, Command};

#[cfg(unix)]
pub(crate) struct ProcessGroup {
    pid: Option<u32>,
}

#[cfg(unix)]
impl ProcessGroup {
    pub(crate) fn configure(command: &mut Command) {
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    pub(crate) fn attach(child: &mut Child) -> std::io::Result<Self> {
        Ok(Self { pid: child.id() })
    }

    pub(crate) fn kill(&self) {
        if let Some(pid) = self.pid {
            unsafe {
                libc::killpg(pid as i32, libc::SIGKILL);
            }
        }
    }
}

#[cfg(windows)]
pub(crate) struct ProcessGroup {
    job: usize,
}

#[cfg(windows)]
impl ProcessGroup {
    pub(crate) fn configure(command: &mut Command) {
        use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;

        command.creation_flags(CREATE_SUSPENDED);
    }

    pub(crate) fn attach(child: &mut Child) -> std::io::Result<Self> {
        use std::ptr::null;
        use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
        use windows_sys::Win32::System::JobObjects::{AssignProcessToJobObject, CreateJobObjectW};

        let job = unsafe { CreateJobObjectW(null(), null()) };
        if job.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let Some(process) = child.raw_handle() else {
            unsafe { CloseHandle(job) };
            return Err(std::io::Error::other(
                "spawned process has no Windows handle",
            ));
        };
        if unsafe { AssignProcessToJobObject(job, process as HANDLE) } == 0 {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(job) };
            return Err(error);
        }
        if let Err(error) = resume_process(child.id()) {
            unsafe { CloseHandle(job) };
            return Err(error);
        }
        Ok(Self { job: job as usize })
    }

    pub(crate) fn kill(&self) {
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;

        unsafe {
            TerminateJobObject(self.job as HANDLE, 1);
        }
    }
}

#[cfg(windows)]
fn resume_process(process_id: Option<u32>) -> std::io::Result<()> {
    use std::mem::size_of;
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    let process_id = process_id.ok_or_else(|| {
        std::io::Error::other("spawned process has no Windows process identifier")
    })?;
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }

    let mut entry = THREADENTRY32 {
        dwSize: size_of::<THREADENTRY32>() as u32,
        ..Default::default()
    };
    let mut found = false;
    let mut has_entry = unsafe { Thread32First(snapshot, &mut entry) } != 0;
    while has_entry {
        if entry.th32OwnerProcessID == process_id {
            found = true;
            let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if thread.is_null() {
                let error = std::io::Error::last_os_error();
                unsafe { CloseHandle(snapshot) };
                return Err(error);
            }
            let result = unsafe { ResumeThread(thread) };
            unsafe { CloseHandle(thread) };
            if result == u32::MAX {
                let error = std::io::Error::last_os_error();
                unsafe { CloseHandle(snapshot) };
                return Err(error);
            }
        }
        has_entry = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
    }
    unsafe { CloseHandle(snapshot) };

    if !found {
        return Err(std::io::Error::other(
            "spawned process has no Windows thread",
        ));
    }
    Ok(())
}

#[cfg(windows)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};

        unsafe {
            CloseHandle(self.job as HANDLE);
        }
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::ProcessGroup;
    use std::time::Duration;
    use tokio::process::Command;

    #[tokio::test]
    async fn child_stays_suspended_until_attached_to_job() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("started");
        let marker_arg = marker.display().to_string().replace('\'', "''");
        let mut command = Command::new("powershell.exe");
        command
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!("Set-Content -LiteralPath '{marker_arg}' -Value started"),
            ])
            .kill_on_drop(true);
        ProcessGroup::configure(&mut command);

        let mut child = command.spawn().unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!marker.exists());

        let _group = ProcessGroup::attach(&mut child).unwrap();
        assert!(child.wait().await.unwrap().success());
        assert!(marker.exists());
    }
}
