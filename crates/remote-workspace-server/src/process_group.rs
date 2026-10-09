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
    pub(crate) fn configure(_command: &mut Command) {}

    /// The child runs briefly before it joins the job, so a process it starts
    /// in that moment escapes the job. Joining at creation takes
    /// PROC_THREAD_ATTRIBUTE_JOB_LIST, which std offers only on nightly;
    /// starting the child suspended instead is what process injection looks
    /// like to endpoint security.
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
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};

        unsafe {
            CloseHandle(self.job as HANDLE);
        }
    }
}
