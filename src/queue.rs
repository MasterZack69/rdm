use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cli;
use crate::priority_queue::PriorityQueue;
use crate::queue_state::{self, JobState, PersistedJob, Priority, QueueSnapshot};

#[derive(Debug, Clone)]
pub struct DownloadJob {
    pub id: u64,
    pub url: String,
    pub output_path: String,
    pub connections: usize,
    pub priority: Priority,
}

#[derive(Debug)]
pub enum JobOutcome {
    Success,
    Failed(String),
    Cancelled,
    Paused,
}

impl JobOutcome {
    pub fn is_success(&self) -> bool { matches!(self, JobOutcome::Success) }
}

#[derive(Debug)]
pub struct JobResult {
    pub id: u64,
    pub url: String,
    pub output_path: String,
    pub priority: Priority,
    pub outcome: JobOutcome,
}

struct Completion {
    id: u64,
    url: String,
    output_path: String,
    outcome: JobOutcome,
}

struct ActiveJob {
    token: CancellationToken,
    handle: JoinHandle<()>,
    job: DownloadJob,
}

enum QueueCommand {
    Pause(u64),
    Resume(u64),
    SetPriority(u64, Priority),
}

pub struct QueueController {
    tx: mpsc::UnboundedSender<QueueCommand>,
}

impl QueueController {
    pub fn pause_job(&self, id: u64) -> bool { self.tx.send(QueueCommand::Pause(id)).is_ok() }
    pub fn resume_job(&self, id: u64) -> bool { self.tx.send(QueueCommand::Resume(id)).is_ok() }
    pub fn set_priority(&self, id: u64, priority: Priority) -> bool { self.tx.send(QueueCommand::SetPriority(id, priority)).is_ok() }
}

pub struct DownloadQueue {
    pending: PriorityQueue<DownloadJob>,
    active: HashMap<u64, ActiveJob>,
    paused: HashMap<u64, DownloadJob>,
    pausing: HashSet<u64>,
    max_concurrent: usize,
    next_id: u64,
    state_path: PathBuf,
    cmd_tx: mpsc::UnboundedSender<QueueCommand>,
    cmd_rx: Option<mpsc::UnboundedReceiver<QueueCommand>>,
}

impl DownloadQueue {
    pub fn new(max_concurrent: usize) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        Self {
            pending: PriorityQueue::new(),
            active: HashMap::new(),
            paused: HashMap::new(),
            pausing: HashSet::new(),
            max_concurrent: max_concurrent.max(1),
            next_id: 1,
            state_path: queue_state::default_queue_path(),
            cmd_tx,
            cmd_rx: Some(cmd_rx),
        }
    }

    pub fn with_state_path(mut self, path: PathBuf) -> Self { self.state_path = path; self }

    pub async fn restore(max_concurrent: usize) -> Self {
        Self::restore_from(max_concurrent, queue_state::default_queue_path()).await
    }

    pub async fn restore_from(max_concurrent: usize, path: PathBuf) -> Self {
        let snapshot = queue_state::load_or_default(&path, max_concurrent).await;
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let mut pending = PriorityQueue::new();
        let mut paused = HashMap::new();

        for pj in &snapshot.jobs {
            let job = DownloadJob {
                id: pj.id, url: pj.url.clone(), output_path: pj.output_path.clone(),
                connections: pj.connections, priority: pj.priority,
            };
            match pj.state {
                JobState::Pending | JobState::Active => pending.push_back(job.priority, job),
                JobState::Paused => { paused.insert(job.id, job); }
            }
        }

        let restored = pending.len() + paused.len();
        if restored > 0 {
            eprintln!("  [Queue] Restored {} jobs ({} pending, {} paused)", restored, pending.len(), paused.len());
        }

        Self {
            pending, active: HashMap::new(), paused, pausing: HashSet::new(),
            max_concurrent: max_concurrent.max(1), next_id: snapshot.next_id,
            state_path: path, cmd_tx, cmd_rx: Some(cmd_rx),
        }
    }

    pub fn controller(&self) -> QueueController { QueueController { tx: self.cmd_tx.clone() } }

    pub fn add(&mut self, url: String, output_path: String, connections: usize) -> u64 {
        self.add_with_priority(url, output_path, connections, Priority::Normal)
    }

    pub fn add_with_priority(&mut self, url: String, output_path: String, connections: usize, priority: Priority) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        let job = DownloadJob { id, url, output_path, connections: connections.max(1), priority };
        self.pending.push_back(priority, job);
        id
    }

    pub fn add_job(&mut self, job: DownloadJob) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        let priority = job.priority;
        let job = DownloadJob { id, url: job.url, output_path: job.output_path, connections: job.connections.max(1), priority };
        self.pending.push_back(priority, job);
        id
    }

    pub fn remove(&mut self, id: u64) -> bool { self.pending.remove_by(|j| j.id == id).is_some() }

    pub fn set_priority(&mut self, id: u64, new_priority: Priority) -> bool {
        if let Some(current) = self.pending.find_lane(|j| j.id == id) {
            if current == new_priority { return true; }
            if let Some(mut job) = self.pending.remove_by(|j| j.id == id) {
                job.priority = new_priority;
                self.pending.push_back(new_priority, job);
                return true;
            }
        }
        if let Some(job) = self.paused.get_mut(&id) { job.priority = new_priority; return true; }
        if let Some(active) = self.active.get_mut(&id) { active.job.priority = new_priority; return true; }
        false
    }

    pub fn pause_job(&mut self, id: u64) -> bool {
        if let Some(job) = self.pending.remove_by(|j| j.id == id) { self.paused.insert(id, job); return true; }
        if let Some(active) = self.active.get(&id) { active.token.cancel(); self.pausing.insert(id); return true; }
        false
    }

    pub fn resume_job(&mut self, id: u64) -> bool {
        if let Some(job) = self.paused.remove(&id) { let p = job.priority; self.pending.push_back(p, job); return true; }
        false
    }

    pub fn pending_count(&self) -> usize { self.pending.len() }
    pub fn active_count(&self) -> usize { self.active.len() }
    pub fn paused_count(&self) -> usize { self.paused.len() }
    pub fn peek_pending(&self) -> Vec<&DownloadJob> { self.pending.iter().collect() }
    pub fn peek_paused(&self) -> Vec<&DownloadJob> { self.paused.values().collect() }

    pub fn cancel_job(&mut self, id: u64) -> bool {
        if let Some(active) = self.active.get(&id) { active.token.cancel(); return true; }
        self.remove(id)
    }

    fn snapshot(&self) -> QueueSnapshot {
        let mut jobs = Vec::new();
        for j in self.pending.iter() {
            jobs.push(PersistedJob { id: j.id, url: j.url.clone(), output_path: j.output_path.clone(), connections: j.connections, priority: j.priority, state: JobState::Pending });
        }
        for (_, j) in &self.paused {
            jobs.push(PersistedJob { id: j.id, url: j.url.clone(), output_path: j.output_path.clone(), connections: j.connections, priority: j.priority, state: JobState::Paused });
        }
        for (_, active) in &self.active {
            let j = &active.job;
            jobs.push(PersistedJob {
                id: j.id,
                url: j.url.clone(),
                output_path: j.output_path.clone(),
                connections: j.connections,
                priority: j.priority,
                // FIX 4: Persist active jobs as Active, not Pending
                state: JobState::Active,
            });
        }
        jobs.sort_by_key(|j| (j.priority as u8, j.id));
        QueueSnapshot { next_id: self.next_id, max_concurrent: self.max_concurrent, jobs }
    }

    async fn autosave(&self) {
        if let Err(e) = queue_state::save(&self.state_path, &self.snapshot()).await {
            eprintln!("  ⚠ Failed to save queue state: {:#}", e);
        }
    }

    pub async fn process(&mut self, cancel: CancellationToken) -> Vec<JobResult> {
        let (comp_tx, mut comp_rx) = mpsc::unbounded_channel::<Completion>();
        let mut cmd_rx = match self.cmd_rx.take() {
            Some(rx) => rx,
            None => { let (tx, rx) = mpsc::unbounded_channel(); self.cmd_tx = tx; rx }
        };
        let mut results: Vec<JobResult> = Vec::new();
        let mut shutting_down = false;

        self.autosave().await;

        loop {
            if !shutting_down {
                let mut spawned = false;
                while self.active.len() < self.max_concurrent {
                    match self.pending.pop_front() {
                        Some(job) => { self.spawn_job(job, &comp_tx, &cancel); spawned = true; }
                        None => break,
                    }
                }
                if spawned { self.autosave().await; }
            }

            if self.active.is_empty() && (self.pending.is_empty() || shutting_down) {
                break;
            }

            tokio::select! {
                biased;

                _ = cancel.cancelled(), if !shutting_down => {
                    shutting_down = true;

                    eprintln!(
                        "  [Queue] Cancelling {} active, {} pending",
                        self.active.len(),
                        self.pending.len()
                    );

                    for active in self.active.values() {
                        active.token.cancel();
                    }

                    for job in self.pending.drain() {
                        results.push(JobResult {
                            id: job.id,
                            url: job.url,
                            output_path: job.output_path,
                            priority: job.priority,
                            outcome: JobOutcome::Cancelled,
                        });
                    }

                    self.autosave().await;
                }

                cmd = cmd_rx.recv() => {
                    if let Some(c) = cmd {
                        match c {
                            QueueCommand::Pause(id) => self.handle_pause(id),
                            QueueCommand::Resume(id) => self.handle_resume(id),
                            QueueCommand::SetPriority(id, p) => { if self.set_priority(id, p) { eprintln!("  [Queue] #{} priority → {}", id, p); } }
                        }
                        self.autosave().await;
                    }
                }

                recv = comp_rx.recv() => {
                    if let Some(c) = recv { self.handle_completion(c, &mut results, shutting_down).await; self.autosave().await; }
                }
            }
        }

        for (_, job) in &self.paused {
            results.push(JobResult { id: job.id, url: job.url.clone(), output_path: job.output_path.clone(), priority: job.priority, outcome: JobOutcome::Paused });
        }
        self.autosave().await;
        if self.pending.is_empty() && self.paused.is_empty() && self.active.is_empty() { let _ = queue_state::delete(&self.state_path).await; }
        self.cmd_rx = Some(cmd_rx);
        results
    }

    fn handle_pause(&mut self, id: u64) {
        if let Some(job) = self.pending.remove_by(|j| j.id == id) { eprintln!("  [Queue] #{} ⏸ paused (was pending)", id); self.paused.insert(id, job); return; }
        if let Some(active) = self.active.get(&id) { active.token.cancel(); self.pausing.insert(id); eprintln!("  [Queue] #{} ⏸ pausing...", id); }
    }

    fn handle_resume(&mut self, id: u64) {
        if let Some(job) = self.paused.remove(&id) { eprintln!("  [Queue] #{} ▶ resumed", id); let p = job.priority; self.pending.push_back(p, job); }
    }

    async fn handle_completion(&mut self, completion: Completion, results: &mut Vec<JobResult>, shutting_down: bool) {
        let id = completion.id;
        let job_info = if let Some(active) = self.active.remove(&id) { let _ = active.handle.await; Some(active.job) } else { None };
        let priority = job_info.as_ref().map(|j| j.priority).unwrap_or_default();

        if self.pausing.remove(&id) && !shutting_down {
            if !matches!(completion.outcome, JobOutcome::Success) {
                if let Some(job) = job_info { eprintln!("  [Queue] #{} ⏸ paused: {}", id, job.url); self.paused.insert(id, job); return; }
            }
        }

        let icon = match &completion.outcome { JobOutcome::Success => "✅", JobOutcome::Failed(_) => "❌", JobOutcome::Cancelled => "⚠️", JobOutcome::Paused => "⏸" };
        eprintln!("  [Queue] #{} {} {}", id, icon, completion.url);

        let outcome = if shutting_down && !matches!(completion.outcome, JobOutcome::Success) { JobOutcome::Cancelled } else { completion.outcome };
        results.push(JobResult { id, url: completion.url, output_path: completion.output_path, priority, outcome });
    }

    fn spawn_job(&mut self, job: DownloadJob, tx: &mpsc::UnboundedSender<Completion>, parent: &CancellationToken) {
        let id = job.id;
        let url = job.url.clone();
        let output = job.output_path.clone();
        let conns = job.connections;
        let tx = tx.clone();
        let child = parent.child_token();
        let parent_check = parent.clone();

        eprintln!("  [Queue] #{} {} started: {} → {}", id, job.priority.icon(), url, output);

        let task_token = child.clone();
        let handle = tokio::spawn(async move {
            let outcome = match cli::run_download(url.clone(), Some(output.clone()), conns, task_token).await {
                Ok(()) => JobOutcome::Success,
                Err(_) if parent_check.is_cancelled() => JobOutcome::Cancelled,
                Err(e) => JobOutcome::Failed(format!("{:#}", e)),
            };
            let _ = tx.send(Completion { id, url, output_path: output, outcome });
        });

        self.active.insert(id, ActiveJob { token: child, handle, job });
    }
}

pub fn print_summary(results: &[JobResult]) {
    let succeeded = results.iter().filter(|r| r.outcome.is_success()).count();
    let failed = results.iter().filter(|r| matches!(r.outcome, JobOutcome::Failed(_))).count();
    let cancelled = results.iter().filter(|r| matches!(r.outcome, JobOutcome::Cancelled)).count();
    let paused = results.iter().filter(|r| matches!(r.outcome, JobOutcome::Paused)).count();

    eprintln!();
    eprintln!("  ────────────────────────────────────────");
    eprintln!("  Queue Summary");
    eprintln!("  ────────────────────────────────────────");
    eprintln!("  Total    : {}", results.len());
    eprintln!("  Success  : {}", succeeded);
    if failed > 0 { eprintln!("  Failed   : {}", failed); }
    if cancelled > 0 { eprintln!("  Cancelled: {}", cancelled); }
    if paused > 0 { eprintln!("  Paused   : {}", paused); }
    eprintln!();

    for r in results {
        let icon = match &r.outcome { JobOutcome::Success => "✅", JobOutcome::Failed(_) => "❌", JobOutcome::Cancelled => "⚠️", JobOutcome::Paused => "⏸" };
        eprintln!("  {} #{} [{}] {}", icon, r.id, r.priority, r.output_path);
        if let JobOutcome::Failed(msg) = &r.outcome { eprintln!("       {}", msg); }
    }
}
