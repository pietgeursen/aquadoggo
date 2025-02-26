// SPDX-License-Identifier: AGPL-3.0-or-later

use std::error::Error;
use std::future::Future;

use futures::future;
use log::{debug, error};
use tokio::task;

/// Generic Result type for all async tasks used by TaskManager.
pub type FutureResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Handles multiple concurrent tasks and exists them gracefully on shutdown.
pub struct TaskManager {
    on_exit: exit_future::Exit,
    exit_signal: Option<exit_future::Signal>,
    tasks: Vec<task::JoinHandle<()>>,
}

impl TaskManager {
    /// Returns a new TaskManager instance.
    pub fn new() -> Self {
        let (exit_signal, on_exit) = exit_future::signal();

        Self {
            on_exit,
            exit_signal: Some(exit_signal),
            tasks: Vec::new(),
        }
    }

    /// Spawn a new task and register it in the task manager.
    pub fn spawn(
        &mut self,
        name: &'static str,
        task: impl Future<Output = FutureResult<()>> + Send + 'static,
    ) {
        let on_exit = self.on_exit.clone();

        let task_with_error_log = async move {
            if let Err(e) = task.await {
                error!("[{}]: ERROR @ {}", name, e)
            }
        };

        let run_task_until_exit = async move {
            futures::pin_mut!(task_with_error_log);
            future::select(on_exit, task_with_error_log).await;
            debug!("[{}]: Completed", name);
        };

        debug!("[{}]: Spawn", name);

        let task_handle = task::spawn(run_task_until_exit);
        self.tasks.push(task_handle);
    }

    /// Signal all tasks to exit and wait until they are actually shut down.
    pub async fn shutdown(mut self) {
        if let Some(exit_signal) = self.exit_signal.take() {
            let _ = exit_signal.fire();
        }

        futures::future::join_all(self.tasks).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::time;

    use super::{FutureResult, TaskManager};

    #[derive(Clone, Debug)]
    struct DropTester(Arc<Mutex<usize>>);
    struct DropTesterRef(DropTester);

    impl DropTester {
        fn new() -> DropTester {
            DropTester(Arc::new(Mutex::new(0)))
        }

        fn new_ref(&self) -> DropTesterRef {
            *self.0.lock().unwrap() += 1;
            DropTesterRef(self.clone())
        }
    }

    impl PartialEq<usize> for DropTester {
        fn eq(&self, other: &usize) -> bool {
            &*self.0.lock().unwrap() == other
        }
    }

    impl Drop for DropTesterRef {
        fn drop(&mut self) {
            *(self.0).0.lock().unwrap() -= 1;
        }
    }

    async fn run_background_task(_keep_alive: impl std::any::Any) -> FutureResult<()> {
        loop {
            time::sleep(Duration::from_millis(1000)).await;
        }
    }

    #[test]
    fn test_dropped_references() {
        let drop_tester = DropTester::new();
        assert_eq!(drop_tester, 0);

        let drop_tester_ref_1 = drop_tester.new_ref();
        assert_eq!(drop_tester, 1);
        let drop_tester_ref_2 = drop_tester.new_ref();
        assert_eq!(drop_tester, 2);

        drop(drop_tester_ref_1);
        assert_eq!(drop_tester, 1);
        drop(drop_tester_ref_2);
        assert_eq!(drop_tester, 0);
    }

    #[tokio::test]
    async fn drop_running_tasks_on_shutdown() {
        let mut task_manager = TaskManager::new();
        let drop_tester = DropTester::new();

        task_manager.spawn("task1", run_background_task(drop_tester.new_ref()));
        task_manager.spawn("task2", run_background_task(drop_tester.new_ref()));
        assert_eq!(drop_tester, 2);

        task_manager.shutdown().await;
        assert_eq!(drop_tester, 0);
    }
}
