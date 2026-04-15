#![deny(warnings)]

use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

const DEFAULT_BUSY_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Parser)]
#[command(name = "probe_sqlite_queue")]
#[command(about = "Probe SQLite multi-process job claiming with BEGIN IMMEDIATE transactions")]
struct Cli {
    #[command(subcommand)]
    command: Option<CommandKind>,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    Run(RunArgs),
    #[command(hide = true)]
    Worker(WorkerArgs),
}

#[derive(Debug, Clone, Args)]
struct RunArgs {
    /// Path to the SQLite database file. Defaults to a temp file.
    #[arg(long = "db-path")]
    db_path: Option<PathBuf>,

    /// Number of jobs to seed into the probe table.
    #[arg(long = "jobs", default_value_t = 2_000)]
    jobs: usize,

    /// Number of worker processes to spawn.
    #[arg(long = "workers", default_value_t = 4)]
    workers: usize,

    /// Simulated per-job processing delay in milliseconds.
    #[arg(long = "process-ms", default_value_t = 5)]
    process_ms: u64,

    /// SQLite busy timeout in milliseconds.
    #[arg(long = "busy-timeout-ms", default_value_t = DEFAULT_BUSY_TIMEOUT_MS)]
    busy_timeout_ms: u64,

    /// Keep the generated database file after the probe finishes.
    #[arg(long = "keep-db", default_value_t = false)]
    keep_db: bool,
}

#[derive(Debug, Clone, Args)]
struct WorkerArgs {
    #[arg(long = "db-path")]
    db_path: PathBuf,

    #[arg(long = "worker-id")]
    worker_id: String,

    #[arg(long = "process-ms")]
    process_ms: u64,

    #[arg(long = "busy-timeout-ms", default_value_t = DEFAULT_BUSY_TIMEOUT_MS)]
    busy_timeout_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerStats {
    worker_id: String,
    claimed_jobs: usize,
    busy_retries: usize,
    claim_latencies_ms: Vec<f64>,
    complete_latencies_ms: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LatencySummary {
    avg: f64,
    p50: f64,
    p95: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    db_path: String,
    jobs_seeded: usize,
    workers: usize,
    process_ms: u64,
    elapsed_ms: u128,
    completed_jobs: usize,
    total_attempt_count: i64,
    total_busy_retries: usize,
    claim_latency_ms: LatencySummary,
    complete_latency_ms: LatencySummary,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command.unwrap_or_else(|| {
        CommandKind::Run(RunArgs {
            db_path: None,
            jobs: 2_000,
            workers: 4,
            process_ms: 5,
            busy_timeout_ms: DEFAULT_BUSY_TIMEOUT_MS,
            keep_db: false,
        })
    }) {
        CommandKind::Run(args) => run_probe(args),
        CommandKind::Worker(args) => worker_entry(args),
    }
}

fn run_probe(args: RunArgs) -> Result<()> {
    let db_path = args
        .db_path
        .unwrap_or_else(|| std::env::temp_dir().join(temp_db_name()));
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    init_db(&db_path, args.jobs, args.busy_timeout_ms)?;

    let start = Instant::now();
    let exe = std::env::current_exe().context("failed to resolve probe executable path")?;
    let mut children = Vec::with_capacity(args.workers);
    for worker_ix in 0..args.workers {
        children.push(
            Command::new(&exe)
                .arg("worker")
                .arg("--db-path")
                .arg(&db_path)
                .arg("--worker-id")
                .arg(format!("worker-{}", worker_ix))
                .arg("--process-ms")
                .arg(args.process_ms.to_string())
                .arg("--busy-timeout-ms")
                .arg(args.busy_timeout_ms.to_string())
                .output()
                .with_context(|| format!("failed to run worker {}", worker_ix))?,
        );
    }

    let mut worker_stats = Vec::with_capacity(children.len());
    for output in children {
        if !output.status.success() {
            return Err(anyhow!(
                "worker failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stats: WorkerStats = serde_json::from_slice(&output.stdout)
            .context("failed to parse worker probe output")?;
        worker_stats.push(stats);
    }

    let elapsed = start.elapsed();
    let conn = open_connection(&db_path, args.busy_timeout_ms)?;
    let completed_jobs: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM oa_probe_job WHERE job_status = 'completed'",
            [],
            |row| row.get(0),
        )
        .context("failed to count completed probe jobs")?;
    let total_attempt_count: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(attempt_count), 0) FROM oa_probe_job",
            [],
            |row| row.get(0),
        )
        .context("failed to sum attempt counts")?;

    if completed_jobs != args.jobs {
        return Err(anyhow!(
            "probe completed {} jobs but expected {}",
            completed_jobs,
            args.jobs
        ));
    }
    if total_attempt_count != args.jobs as i64 {
        return Err(anyhow!(
            "probe recorded {} total attempts but expected {}",
            total_attempt_count,
            args.jobs
        ));
    }

    let claim_latencies = worker_stats
        .iter()
        .flat_map(|stats| stats.claim_latencies_ms.iter().copied())
        .collect::<Vec<_>>();
    let complete_latencies = worker_stats
        .iter()
        .flat_map(|stats| stats.complete_latencies_ms.iter().copied())
        .collect::<Vec<_>>();
    let total_busy_retries = worker_stats
        .iter()
        .map(|stats| stats.busy_retries)
        .sum::<usize>();

    let summary = RunSummary {
        db_path: db_path.display().to_string(),
        jobs_seeded: args.jobs,
        workers: args.workers,
        process_ms: args.process_ms,
        elapsed_ms: elapsed.as_millis(),
        completed_jobs,
        total_attempt_count,
        total_busy_retries,
        claim_latency_ms: latency_summary(&claim_latencies),
        complete_latency_ms: latency_summary(&complete_latencies),
    };

    println!("SQLite queue probe");
    println!("  db_path: {}", summary.db_path);
    println!("  jobs_seeded: {}", summary.jobs_seeded);
    println!("  workers: {}", summary.workers);
    println!("  process_ms: {}", summary.process_ms);
    println!("  elapsed_ms: {}", summary.elapsed_ms);
    println!("  completed_jobs: {}", summary.completed_jobs);
    println!("  total_attempt_count: {}", summary.total_attempt_count);
    println!("  total_busy_retries: {}", summary.total_busy_retries);
    println!(
        "  claim_latency_ms: avg {:.2} | p50 {:.2} | p95 {:.2} | max {:.2}",
        summary.claim_latency_ms.avg,
        summary.claim_latency_ms.p50,
        summary.claim_latency_ms.p95,
        summary.claim_latency_ms.max
    );
    println!(
        "  complete_latency_ms: avg {:.2} | p50 {:.2} | p95 {:.2} | max {:.2}",
        summary.complete_latency_ms.avg,
        summary.complete_latency_ms.p50,
        summary.complete_latency_ms.p95,
        summary.complete_latency_ms.max
    );

    if !args.keep_db {
        let _ = std::fs::remove_file(&db_path);
    }

    Ok(())
}

fn worker_entry(args: WorkerArgs) -> Result<()> {
    let mut conn = open_connection(&args.db_path, args.busy_timeout_ms)?;
    let mut claim_latencies = Vec::new();
    let mut complete_latencies = Vec::new();
    let mut busy_retries = 0usize;
    let mut claimed_jobs = 0usize;

    loop {
        let claim_started = Instant::now();
        match claim_next_job(&mut conn, &args.worker_id) {
            Ok(Some(job_id)) => {
                claim_latencies.push(claim_started.elapsed().as_secs_f64() * 1000.0);
                claimed_jobs += 1;
                if args.process_ms > 0 {
                    thread::sleep(Duration::from_millis(args.process_ms));
                }
                let complete_started = Instant::now();
                complete_job(&mut conn, &args.worker_id, job_id)?;
                complete_latencies.push(complete_started.elapsed().as_secs_f64() * 1000.0);
            }
            Ok(None) => {
                break;
            }
            Err(err) if is_busy_error(&err) => {
                busy_retries += 1;
                thread::sleep(Duration::from_millis(2));
            }
            Err(err) => return Err(err).context("worker claim failed"),
        }
    }

    let stats = WorkerStats {
        worker_id: args.worker_id,
        claimed_jobs,
        busy_retries,
        claim_latencies_ms: claim_latencies,
        complete_latencies_ms: complete_latencies,
    };
    let json = serde_json::to_vec(&stats).context("failed to serialize worker stats")?;
    print!("{}", String::from_utf8_lossy(&json));
    Ok(())
}

fn init_db(path: &PathBuf, jobs: usize, busy_timeout_ms: u64) -> Result<()> {
    if path.exists() {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    let conn = open_connection(path, busy_timeout_ms)?;
    conn.execute_batch(
        "CREATE TABLE oa_probe_job (
             job_id         INTEGER PRIMARY KEY,
             job_status     TEXT NOT NULL,
             available_at   INTEGER NOT NULL,
             priority_no    INTEGER NOT NULL,
             claimed_by     TEXT,
             claimed_at     INTEGER,
             attempt_count  INTEGER NOT NULL DEFAULT 0
         );
         CREATE INDEX ix_oa_probe_job_claim
             ON oa_probe_job (job_status, available_at, priority_no, job_id);",
    )
    .context("failed to initialize SQLite probe schema")?;

    let tx = conn
        .unchecked_transaction()
        .context("failed to open SQLite probe seed transaction")?;
    for job_id in 1..=jobs {
        tx.execute(
            "INSERT INTO oa_probe_job (job_id, job_status, available_at, priority_no)
             VALUES (?1, 'pending', unixepoch(), 100)",
            params![job_id as i64],
        )
        .with_context(|| format!("failed to insert probe job {}", job_id))?;
    }
    tx.commit().context("failed to commit SQLite probe seed")?;
    Ok(())
}

fn open_connection(path: &PathBuf, busy_timeout_ms: u64) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open SQLite database {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("failed to enable WAL mode")?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("failed to set synchronous mode")?;
    conn.busy_timeout(Duration::from_millis(busy_timeout_ms))
        .context("failed to configure busy timeout")?;
    Ok(conn)
}

fn claim_next_job(conn: &mut Connection, worker_id: &str) -> Result<Option<i64>> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to begin immediate transaction")?;
    let job_id = tx
        .query_row(
            "SELECT job_id
               FROM oa_probe_job
              WHERE job_status IN ('pending', 'retryable')
                AND available_at <= unixepoch()
              ORDER BY priority_no ASC, available_at ASC, job_id ASC
              LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .context("failed to select next probe job")?;

    if let Some(job_id) = job_id {
        tx.execute(
            "UPDATE oa_probe_job
                SET job_status = 'running',
                    claimed_by = ?2,
                    claimed_at = unixepoch(),
                    attempt_count = attempt_count + 1
              WHERE job_id = ?1",
            params![job_id, worker_id],
        )
        .context("failed to claim probe job")?;
        tx.commit().context("failed to commit probe claim")?;
        Ok(Some(job_id))
    } else {
        tx.commit().context("failed to commit empty probe claim")?;
        Ok(None)
    }
}

fn complete_job(conn: &mut Connection, worker_id: &str, job_id: i64) -> Result<()> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to begin completion transaction")?;
    let updated = tx
        .execute(
            "UPDATE oa_probe_job
                SET job_status = 'completed',
                    claimed_by = NULL,
                    claimed_at = NULL
              WHERE job_id = ?1
                AND claimed_by = ?2
                AND job_status = 'running'",
            params![job_id, worker_id],
        )
        .context("failed to complete probe job")?;
    if updated != 1 {
        return Err(anyhow!(
            "probe completion affected {} rows for job {}",
            updated,
            job_id
        ));
    }
    tx.commit().context("failed to commit probe completion")?;
    Ok(())
}

fn is_busy_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        if let Some(sqlite_err) = cause.downcast_ref::<rusqlite::Error>() {
            matches!(
                sqlite_err,
                rusqlite::Error::SqliteFailure(failure, _)
                    if failure.code == rusqlite::ErrorCode::DatabaseBusy
                        || failure.code == rusqlite::ErrorCode::DatabaseLocked
            )
        } else {
            false
        }
    })
}

fn temp_db_name() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("open_archive_sqlite_probe_{ts}.db")
}

fn latency_summary(samples: &[f64]) -> LatencySummary {
    if samples.is_empty() {
        return LatencySummary {
            avg: 0.0,
            p50: 0.0,
            p95: 0.0,
            max: 0.0,
        };
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let sum = sorted.iter().sum::<f64>();
    LatencySummary {
        avg: sum / sorted.len() as f64,
        p50: percentile(&sorted, 0.50),
        p95: percentile(&sorted, 0.95),
        max: *sorted.last().unwrap_or(&0.0),
    }
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let rank = ((sorted_values.len() - 1) as f64 * percentile).round() as usize;
    sorted_values[rank]
}
