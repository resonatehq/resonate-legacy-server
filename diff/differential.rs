// Differential random testing: drive the same random operation sequence through
// dispatch() on multiple backends simultaneously and assert identical responses
// and state snapshots at every step.
//
// Coverage requirement: the test runs until every operation kind has produced
// at least one 2xx response, guaranteeing that we are not trivially passing by
// only exercising failure paths.
//
// Backends:
//   SQLite   — always active (in-memory, :memory:)
//   Postgres — active when TEST_POSTGRES_URL env var is set
//   MySQL    — active when TEST_MYSQL_URL env var is set
//
// Run:
//   docker compose -f diff/docker-compose-diff.yml up --build
//   TEST_POSTGRES_URL=postgres://resonate:resonate@localhost:5432/resonate \
//   TEST_MYSQL_URL=mysql://resonate:resonate@localhost:3306/resonate \
//     cargo test --test differential -- --nocapture

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

// Serializes tests that share the same Postgres database so that concurrent
// debug.reset calls from one test cannot truncate another test's data.
static PG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
fn pg_lock() -> &'static Mutex<()> {
    PG_LOCK.get_or_init(|| Mutex::new(()))
}

use resonate::{
    config::Config,
    persistence::{
        persistence_mysql::MysqlStorage,
        persistence_postgres::PostgresStorage,
        persistence_sqlite::SqliteStorage,
        Storage,
    },
    server::{dispatch, Server},
    types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS},
};
use serde_json::{json, Value};

const TASK_RETRY_TIMEOUT_MS: i64 = 30_000;
// Fixed epoch anchor; all test times are offsets from here (ms).
const T0: i64 = 1_000_000_000;
// Fake worker URL — passes is_valid_address but no actual delivery attempted.
const WORKER_URL: &str = "http://diff-test-worker:9999";
const PID: &str = "diff-test-pid";
const TTL: i64 = 60_000;

// All operation kinds that must produce at least one 2xx before the test ends.
const ALL_OPS: &[&str] = &[
    "promise.create",
    "promise.get",
    "promise.settle",
    "promise.register_callback",
    "promise.register_listener",
    "promise.search",
    "task.create",
    "task.get",
    "task.acquire",
    "task.release",
    "task.fulfill",
    "task.suspend",
    "task.fence",
    "task.heartbeat",
    "task.halt",
    "task.continue",
    "task.search",
    "schedule.create",
    "schedule.get",
    "schedule.delete",
    "schedule.search",
    "debug.tick",
];

fn debug_config() -> Config {
    serde_json::from_value(json!({ "debug": true })).expect("valid default config")
}

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: fastrand::u64(..).to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn differential_random() {
    // Catch Op enum / ALL_OPS drift: every variant must have a matching coverage entry.
    debug_assert_eq!(22, ALL_OPS.len(), "Op has 22 variants; ALL_OPS must match");

    let sqlite = SqliteStorage::open(":memory:", TASK_RETRY_TIMEOUT_MS).expect("sqlite open");
    let sqlite_srv = Arc::new(Server::new(debug_config(), None, Storage::Sqlite(sqlite)));

    let pg_url = std::env::var("TEST_POSTGRES_URL").ok();
    let my_url = std::env::var("TEST_MYSQL_URL").ok();

    // Acquire lock before init() calls to prevent concurrent schema creation races.
    // unwrap_or_else recovers from a poisoned mutex (caused by a previous test panic).
    let _pg_guard = pg_url.as_ref().map(|_| pg_lock().lock().unwrap_or_else(|e| e.into_inner()));

    let pg_srv: Option<Arc<Server>> = match pg_url {
        Some(url) => {
            let pg = PostgresStorage::connect(&url, 5, TASK_RETRY_TIMEOUT_MS)
                .await
                .expect("postgres connect");
            pg.init().await.expect("postgres schema init");
            Some(Arc::new(Server::new(debug_config(), None, Storage::Postgres(pg))))
        }
        None => {
            eprintln!("[diff] TEST_POSTGRES_URL not set — PostgreSQL skipped");
            None
        }
    };

    let my_srv: Option<Arc<Server>> = match my_url {
        Some(url) => {
            let my = MysqlStorage::connect(&url, 5, TASK_RETRY_TIMEOUT_MS)
                .await
                .expect("mysql connect");
            my.init().await.expect("mysql schema init");
            Some(Arc::new(Server::new(debug_config(), None, Storage::Mysql(my))))
        }
        None => {
            eprintln!("[diff] TEST_MYSQL_URL not set — MySQL skipped");
            None
        }
    };

    let mut servers: Vec<(String, Arc<Server>)> = vec![("sqlite".into(), sqlite_srv)];
    if let Some(pg) = pg_srv { servers.push(("postgres".into(), pg)); }
    if let Some(my) = my_srv { servers.push(("mysql".into(), my)); }

    if servers.len() < 2 {
        eprintln!("[diff] fewer than 2 backends — set TEST_POSTGRES_URL or TEST_MYSQL_URL");
        return;
    }

    let names: Vec<&str> = servers.iter().map(|(n, _)| n.as_str()).collect();
    eprintln!("[diff] backends: {}", names.join(", "));

    setup_all(&servers, T0).await;

    // Run until (op, status, state_class) signatures plateau for PLATEAU_BATCHES consecutive
    // batches after all ops have produced a 2xx. MAX_STEPS is a hard safety cap.
    const MAX_STEPS: usize = 10_000;
    const BATCH_SIZE: usize = 50;
    const PLATEAU_BATCHES: usize = 5;

    let mut rng = fastrand::Rng::with_seed(0xc0ffee_dead_beef);
    let mut now = T0;
    let mut w;
    // kind → first 2xx step; also used to track which ops are still uncovered.
    let mut covered: HashMap<String, usize> = HashMap::new();
    let mut total_steps = 0usize;
    // (op_kind, response_status_code, pre-step state_class bitmask) — proxy for behavioral coverage.
    let mut seen_sigs: HashSet<(String, u16, u8)> = HashSet::new();
    let mut plateau_count = 0usize;

    'outer: loop {
        reset_all(&servers, now).await;
        w = World::default();
        now = T0;

        let sigs_before = seen_sigs.len();

        for _ in 0..BATCH_SIZE {
            if total_steps >= MAX_STEPS {
                break 'outer;
            }

            let op = pick_op(&mut rng, &w, &covered);
            total_steps += 1;

            let (envelope, now_after) = build_envelope(op, &mut rng, &w, now);
            now = now_after;

            let kind = envelope.kind.clone();
            let ctx = format!("step={total_steps} op={kind}");

            // Verify backends agree before this step; catch divergence as close to its
            // cause as possible.
            let pre_snaps = snap_all(&servers, now).await;
            assert_no_divergence(
                &pre_snaps,
                &["messages", "tasks", "callbacks", "taskTimeouts"],
                &format!("BEFORE {ctx}"),
            );

            let mut results = send_all(&servers, &envelope, now).await;
            for (_, _, data) in &mut results {
                normalize_resp(data);
            }

            let (_, status, ref resp_data) = results[0];
            let resp_data = resp_data.clone();

            if status < 300 {
                covered.entry(kind.clone()).or_insert(total_steps);
            }

            let sc = state_class(&pre_snaps[0].1);
            seen_sigs.insert((kind.clone(), status as u16, sc));

            assert_resps_agree(&results, &ctx);

            w.apply(&kind, status, &envelope.data, &resp_data);

            // Per-step message check: catch outgoing_execute divergences immediately.
            let mid_snaps = snap_all(&servers, now).await;
            assert_no_divergence(&mid_snaps, &["messages"], &format!("AFTER {ctx}"));
        }

        // Full snapshot comparison at the end of each batch.
        let snaps = snap_all(&servers, now).await;
        assert_snaps_agree(&snaps, &format!("step={total_steps}"));

        // Plateau check: stop once all ops are covered and signatures stop growing.
        let new_sigs = seen_sigs.len().saturating_sub(sigs_before);
        if covered.len() == ALL_OPS.len() {
            if new_sigs == 0 {
                plateau_count += 1;
                eprintln!(
                    "[diff] plateau {plateau_count}/{PLATEAU_BATCHES} — {} total signatures, no new in this batch",
                    seen_sigs.len()
                );
            } else {
                plateau_count = 0;
            }
            if plateau_count >= PLATEAU_BATCHES {
                eprintln!("[diff] coverage plateau reached after {total_steps} steps ({} signatures)", seen_sigs.len());
                break 'outer;
            }
        }
    }

    let snaps = snap_all(&servers, now).await;
    assert_snaps_agree(&snaps, "final");

    eprintln!("[diff] coverage after {total_steps} steps:");
    let mut missing = Vec::new();
    for op in ALL_OPS {
        if let Some(step) = covered.get(*op) {
            eprintln!("  [OK ] {op} (first 2xx at step {step})");
        } else {
            eprintln!("  [MISS] {op}");
            missing.push(*op);
        }
    }

    if !missing.is_empty() {
        panic!(
            "Coverage incomplete after {total_steps} steps — these ops never produced a 2xx: {:?}",
            missing
        );
    }

    eprintln!(
        "[diff] PASSED — {total_steps} steps, {} backends, all {} ops covered, {} behavioral signatures",
        servers.len(),
        ALL_OPS.len(),
        seen_sigs.len(),
    );
}

// Confirms that a tick past a task's TTL inserts an outgoing_execute message on every backend.
#[tokio::test(flavor = "multi_thread")]
async fn targeted_outgoing_execute() {
    let url = match std::env::var("TEST_POSTGRES_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("[targeted] TEST_POSTGRES_URL not set — skipping");
            return;
        }
    };
    let _pg_guard = pg_lock().lock().unwrap_or_else(|e| e.into_inner());

    let sqlite = SqliteStorage::open(":memory:", TASK_RETRY_TIMEOUT_MS).expect("sqlite open");
    let sqlite_srv = Arc::new(Server::new(debug_config(), None, Storage::Sqlite(sqlite)));

    let pg = PostgresStorage::connect(&url, 5, TASK_RETRY_TIMEOUT_MS).await.expect("postgres connect");
    pg.init().await.expect("postgres schema init");
    let pg_srv = Arc::new(Server::new(debug_config(), None, Storage::Postgres(pg)));

    let servers: Vec<(String, Arc<Server>)> = vec![
        ("sqlite".into(), sqlite_srv),
        ("postgres".into(), pg_srv),
    ];
    setup_all(&servers, T0).await;

    let task_id = "t99";
    let results = send_all(&servers, &req("task.create", json!({
        "pid": PID, "ttl": TTL,
        "action": {
            "kind": "promise.create", "head": {},
            "data": {
                "id": task_id,
                "timeoutAt": T0 + 600_000,
                "param": {},
                "tags": { "resonate:target": WORKER_URL }
            }
        }
    })), T0).await;
    assert_resps_agree(&results, "task.create");

    // Tick past the TTL; the expired task lease must trigger an outgoing_execute message.
    let tick_time = T0 + TTL + 10_000;
    let results = send_all(&servers, &req("debug.tick", json!({ "time": tick_time })), tick_time).await;
    assert_resps_agree(&results, "debug.tick");

    let snaps = snap_all(&servers, tick_time).await;
    assert_snaps_agree(&snaps, "after_tick");

    let msg_count = snaps[0].1.get("messages").and_then(|m| m.as_array()).map(|a| a.len()).unwrap_or(0);
    assert!(msg_count > 0, "expected at least one outgoing_execute message after tick, got none");

    // If the tick did not auto-release the task (implementation-dependent), release it
    // explicitly and verify backends still agree.
    let task_entry = snaps[0].1
        .get("tasks").and_then(|t| t.as_array())
        .and_then(|arr| arr.iter().find(|t| t.get("id").and_then(|v| v.as_str()) == Some(task_id)));
    let task_state = task_entry.and_then(|t| t.get("state")).and_then(|s| s.as_str()).unwrap_or("");
    let task_v    = task_entry.and_then(|t| t.get("version")).and_then(|v| v.as_i64()).unwrap_or(1);

    if task_state == "acquired" {
        let results = send_all(&servers, &req("task.release", json!({ "id": task_id, "version": task_v })), tick_time).await;
        assert_resps_agree(&results, "task.release");
        let post_snaps = snap_all(&servers, tick_time).await;
        assert_snaps_agree(&post_snaps, "after_release");
    }
}

// ---------------------------------------------------------------------------
// Infrastructure
// ---------------------------------------------------------------------------

async fn setup_all(servers: &[(String, Arc<Server>)], now: i64) {
    // debug.start must precede debug.reset: reset requires the server to be in started state
    // to safely truncate tables. If the server is already started, debug.start is a no-op.
    for env in &[req("debug.start", json!({})), req("debug.reset", json!({}))] {
        for (name, srv) in servers.iter() {
            let resp = dispatch(srv, env, now).await;
            assert_eq!(resp.head.status, 200, "{} failed on {name}", env.kind);
        }
    }
}

async fn reset_all(servers: &[(String, Arc<Server>)], now: i64) {
    let envelope = req("debug.reset", json!({}));
    for (name, srv) in servers {
        let resp = dispatch(srv, &envelope, now).await;
        assert_eq!(resp.head.status, 200, "debug.reset failed on {name}");
    }
}

async fn send_all(
    servers: &[(String, Arc<Server>)],
    envelope: &RequestEnvelope,
    now: i64,
) -> Vec<(String, i32, Value)> {
    let mut out = Vec::new();
    for (name, srv) in servers {
        let resp = dispatch(srv, envelope, now).await;
        out.push((name.clone(), resp.head.status, resp.data));
    }
    out
}

async fn snap_all(servers: &[(String, Arc<Server>)], now: i64) -> Vec<(String, Value)> {
    let envelope = req("debug.snap", json!({}));
    let mut out = Vec::new();
    for (name, srv) in servers {
        let resp = dispatch(srv, &envelope, now).await;
        assert_eq!(resp.head.status, 200, "debug.snap failed on {name}");
        let mut data = resp.data;
        normalize_snap(&mut data);
        out.push((name.clone(), data));
    }
    out
}

fn assert_resps_agree(results: &[(String, i32, Value)], ctx: &str) {
    let (first_name, first_status, first_data) = &results[0];
    for (name, status, data) in &results[1..] {
        assert_eq!(
            first_status, status,
            "{ctx}: status mismatch  {first_name}={first_status}  {name}={status}"
        );
        assert_eq!(
            first_data, data,
            "{ctx}: data mismatch between {first_name} and {name}\n  {first_name}: {first_data:#}\n  {name}: {data:#}"
        );
    }
}

fn assert_snaps_agree(snaps: &[(String, Value)], ctx: &str) {
    let (first_name, first_snap) = &snaps[0];
    for (name, snap) in &snaps[1..] {
        assert_eq!(
            first_snap, snap,
            "{ctx}: snapshot mismatch {first_name} vs {name}\n  {first_name}: {first_snap:#}\n  {name}: {snap:#}"
        );
    }
}

// Checks that specific keys agree across all snapshots; used for targeted
// pre/post-step divergence detection with a narrow scope.
fn assert_no_divergence(snaps: &[(String, Value)], keys: &[&str], ctx: &str) {
    let (first_name, first_snap) = &snaps[0];
    for (name, snap) in &snaps[1..] {
        for &key in keys {
            // Use Null (not []) as default: a missing key is a distinct state from an empty array,
            // and we want to catch that divergence rather than silently treating them as equal.
            let first_val = first_snap.get(key).cloned().unwrap_or(Value::Null);
            let val = snap.get(key).cloned().unwrap_or(Value::Null);
            assert_eq!(
                first_val, val,
                "{ctx}: `{key}` diverged\n  {first_name}: {first_val:#}\n  {name}: {val:#}"
            );
        }
    }
}

// Sort every top-level array in the snapshot by "id" so ordering differences
// across backends don't cause spurious failures.
fn normalize_snap(snap: &mut Value) {
    if let Some(obj) = snap.as_object_mut() {
        for v in obj.values_mut() {
            if let Some(arr) = v.as_array_mut() {
                sort_by_id(arr);
            }
        }
    }
}

// Sort list-response fields so ordering differences don't cause spurious failures.
fn normalize_resp(data: &mut Value) {
    for key in &["promises", "tasks", "schedules"] {
        if let Some(arr) = data.get_mut(*key).and_then(|v| v.as_array_mut()) {
            sort_by_id(arr);
        }
    }
}

fn sort_by_id(arr: &mut Vec<Value>) {
    arr.sort_by(|a, b| {
        let ak = a.get("id").and_then(|x| x.as_str()).unwrap_or("");
        let bk = b.get("id").and_then(|x| x.as_str()).unwrap_or("");
        ak.cmp(bk)
    });
}

// Coarse summary of which table groups are non-empty. Used as part of the
// behavioral signature to detect when coverage has plateaued.
fn state_class(snap: &Value) -> u8 {
    let mut c = 0u8;
    let non_empty = |key: &str| snap.get(key).and_then(|v| v.as_array()).is_some_and(|a| !a.is_empty());
    if non_empty("promises")        { c |= 1 << 0; }
    if non_empty("tasks")           { c |= 1 << 1; }
    if non_empty("callbacks")       { c |= 1 << 2; }
    if non_empty("listeners")       { c |= 1 << 3; }
    if non_empty("messages")        { c |= 1 << 4; }
    if non_empty("promiseTimeouts") { c |= 1 << 5; }
    if non_empty("schedules")       { c |= 1 << 6; }
    if non_empty("taskTimeouts")    { c |= 1 << 7; }
    c
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

enum Op {
    PromiseCreate,
    PromiseGet,
    PromiseSettle,
    PromiseRegisterCallback,
    PromiseRegisterListener,
    PromiseSearch,
    TaskCreate,
    TaskGet,
    TaskAcquire,
    TaskRelease,
    TaskFulfill,
    TaskSuspend,
    TaskFence,
    TaskHeartbeat,
    TaskHalt,
    TaskContinue,
    TaskSearch,
    ScheduleCreate,
    ScheduleGet,
    ScheduleDelete,
    ScheduleSearch,
    DebugTick,
}

fn pick_op(rng: &mut fastrand::Rng, w: &World, covered: &HashMap<String, usize>) -> Op {
    let uncovered = |kind: &str| !covered.contains_key(kind);

    // Force uncovered ops when their preconditions are met.
    if uncovered("task.suspend") && !w.acquired_tasks.is_empty() && !w.pending_promises.is_empty() {
        return Op::TaskSuspend;
    }
    if uncovered("task.continue") && !w.halted_tasks.is_empty() {
        return Op::TaskContinue;
    }
    if uncovered("task.release") && !w.acquired_tasks.is_empty() {
        return Op::TaskRelease;
    }
    if uncovered("task.fulfill") && !w.acquired_tasks.is_empty() {
        return Op::TaskFulfill;
    }
    if uncovered("task.halt") && (!w.acquired_tasks.is_empty() || !w.pending_tasks.is_empty()) {
        return Op::TaskHalt;
    }
    if uncovered("task.acquire") && !w.pending_tasks.is_empty() {
        return Op::TaskAcquire;
    }
    if uncovered("promise.register_callback") && !w.pending_promises.is_empty()
        && (!w.acquired_tasks.is_empty() || !w.pending_tasks.is_empty())
    {
        return Op::PromiseRegisterCallback;
    }
    if uncovered("promise.register_listener") && !w.pending_promises.is_empty() {
        return Op::PromiseRegisterListener;
    }
    if uncovered("schedule.delete") && !w.schedules.is_empty() {
        return Op::ScheduleDelete;
    }
    if uncovered("task.fence") && !w.acquired_tasks.is_empty() {
        return Op::TaskFence;
    }

    match rng.u32(0..100) {
        0..=14  => Op::PromiseCreate,
        15..=19 => Op::PromiseGet,
        20..=24 => Op::PromiseSettle,
        25..=27 => Op::PromiseRegisterCallback,
        28..=29 => Op::PromiseRegisterListener,
        30..=31 => Op::PromiseSearch,
        32..=39 => Op::TaskCreate,
        40..=41 => Op::TaskGet,
        42..=44 => Op::TaskAcquire,
        45..=47 => Op::TaskRelease,
        48..=52 => Op::TaskFulfill,
        53..=57 => Op::TaskSuspend,
        58..=60 => Op::TaskFence,
        61..=63 => Op::TaskHeartbeat,
        64..=66 => Op::TaskHalt,
        67..=69 => Op::TaskContinue,
        70..=71 => Op::TaskSearch,
        72..=77 => Op::ScheduleCreate,
        78..=80 => Op::ScheduleGet,
        81..=83 => Op::ScheduleDelete,
        84..=85 => Op::ScheduleSearch,
        _       => Op::DebugTick,
    }
}

// Dispatches an Op to its generator. Returns (envelope, updated now); only
// DebugTick advances now.
fn build_envelope(op: Op, rng: &mut fastrand::Rng, w: &World, now: i64) -> (RequestEnvelope, i64) {
    match op {
        Op::PromiseCreate           => (gen_promise_create(rng, w, now), now),
        Op::PromiseGet              => (gen_promise_get(rng, w), now),
        Op::PromiseSettle           => (gen_promise_settle(rng, w), now),
        Op::PromiseRegisterCallback => (gen_promise_register_callback(rng, w), now),
        Op::PromiseRegisterListener => (gen_promise_register_listener(rng, w), now),
        Op::PromiseSearch           => (gen_promise_search(rng), now),
        Op::TaskCreate              => (gen_task_create(rng, now), now),
        Op::TaskGet                 => (gen_task_get(rng, w), now),
        Op::TaskAcquire             => (gen_task_acquire(rng, w), now),
        Op::TaskRelease             => (gen_task_release(rng, w), now),
        Op::TaskFulfill             => (gen_task_fulfill(rng, w), now),
        Op::TaskSuspend             => (gen_task_suspend(rng, w), now),
        Op::TaskFence               => (gen_task_fence(rng, w, now), now),
        Op::TaskHeartbeat           => (gen_task_heartbeat(rng, w), now),
        Op::TaskHalt                => (gen_task_halt(rng, w), now),
        Op::TaskContinue            => (gen_task_continue(rng, w), now),
        Op::TaskSearch              => (gen_task_search(rng), now),
        Op::ScheduleCreate          => (gen_schedule_create(rng, now), now),
        Op::ScheduleGet             => (gen_schedule_get(rng, w), now),
        Op::ScheduleDelete          => (gen_schedule_delete(rng, w), now),
        Op::ScheduleSearch          => (gen_schedule_search(rng), now),
        Op::DebugTick               => gen_debug_tick(rng, now),
    }
}

fn gen_promise_create(rng: &mut fastrand::Rng, w: &World, now: i64) -> RequestEnvelope {
    // Prefer IDs already in the pool (idempotency) or new random ones.
    let id = w.pick_any_promise(rng).unwrap_or_else(|| random_promise_id(rng));
    let timeout_at = now + rng.i64(30_000..300_000);
    req("promise.create", json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": {} }))
}

fn gen_promise_get(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let id = w.pick_any_promise(rng).unwrap_or_else(|| random_promise_id(rng));
    req("promise.get", json!({ "id": id }))
}

fn gen_promise_settle(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let id = w.pick_pending_promise(rng).unwrap_or_else(|| random_promise_id(rng));
    let state = if rng.bool() { "resolved" } else { "rejected" };
    req("promise.settle", json!({ "id": id, "state": state, "value": {} }))
}

fn gen_promise_register_callback(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    // Awaiter must have resonate:target — use a task ID.
    let awaiter = w.pick_acquired_task(rng)
        .or_else(|| w.pick_pending_task(rng))
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    // Awaited must be a different existing promise.
    let awaited = w.pending_promises.iter()
        .find(|p| **p != awaiter)
        .cloned()
        .unwrap_or_else(|| promise_id_different_from(rng, &awaiter));
    req("promise.register_callback", json!({ "awaited": awaited, "awaiter": awaiter }))
}

fn gen_promise_register_listener(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let id = w.pick_pending_promise(rng).unwrap_or_else(|| random_promise_id(rng));
    req("promise.register_listener", json!({ "awaited": id, "address": WORKER_URL }))
}

fn gen_promise_search(rng: &mut fastrand::Rng) -> RequestEnvelope {
    let data = match rng.u32(0..4) {
        0 => json!({ "state": "pending",  "limit": 10 }),
        1 => json!({ "state": "resolved", "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("promise.search", data)
}

// Creates a task-promise pair with resonate:target tag.
fn gen_task_create(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
    let id = World::task_id(rng.u32(0..8));
    let timeout_at = now + rng.i64(60_000..600_000);
    req("task.create", json!({
        "pid": PID,
        "ttl": TTL,
        "action": {
            "kind": "promise.create",
            "head": {},
            "data": {
                "id": id,
                "timeoutAt": timeout_at,
                "param": {},
                "tags": { "resonate:target": WORKER_URL }
            }
        }
    }))
}

fn gen_task_get(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let id = w.pick_acquired_task(rng)
        .or_else(|| w.pick_pending_task(rng))
        .or_else(|| w.pick_suspended_task(rng))
        .or_else(|| w.pick_halted_task(rng))
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.get", json!({ "id": id }))
}

fn gen_task_acquire(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let (id, version) = w.pick_pending_task(rng).unwrap_or_else(|| (random_task_id(rng), 0));
    req("task.acquire", json!({ "id": id, "version": version, "pid": PID, "ttl": TTL }))
}

fn gen_task_release(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let (id, version) = w.pick_acquired_task(rng).unwrap_or_else(|| (random_task_id(rng), 1));
    req("task.release", json!({ "id": id, "version": version }))
}

fn gen_task_fulfill(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let (id, version) = w.pick_acquired_task(rng).unwrap_or_else(|| (random_task_id(rng), 1));
    let state = if rng.bool() { "resolved" } else { "rejected" };
    req("task.fulfill", json!({
        "id": id,
        "version": version,
        "action": {
            "kind": "promise.settle",
            "head": {},
            "data": { "id": id, "state": state, "value": {} }
        }
    }))
}

// Suspends an acquired task, awaiting one of the pending plain promises.
fn gen_task_suspend(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let (task_id, version) = w.pick_acquired_task(rng).unwrap_or_else(|| (random_task_id(rng), 1));
    let awaited = w.pending_promises.iter()
        .find(|p| **p != task_id)
        .cloned()
        .unwrap_or_else(|| promise_id_different_from(rng, &task_id));
    req("task.suspend", json!({
        "id": task_id,
        "version": version,
        "actions": [{
            "kind": "promise.register_callback",
            "head": {},
            "data": { "awaited": awaited, "awaiter": task_id }
        }]
    }))
}

fn gen_task_fence(rng: &mut fastrand::Rng, w: &World, now: i64) -> RequestEnvelope {
    let (task_id, version) = w.pick_acquired_task(rng).unwrap_or_else(|| (random_task_id(rng), 1));
    // Bias toward the settle path when pending promises exist — it's more likely to succeed
    // and exercises the suspend-unblock codepath. Fall back to create when nothing is pending.
    let do_settle = !w.pending_promises.is_empty() && rng.u32(0..4) != 0;
    if !do_settle {
        let new_promise_id = World::promise_id(rng.u32(0..8));
        let timeout_at = now + rng.i64(30_000..300_000);
        req("task.fence", json!({
            "id": task_id,
            "version": version,
            "action": {
                "kind": "promise.create",
                "head": {},
                "data": { "id": new_promise_id, "timeoutAt": timeout_at, "param": {}, "tags": {} }
            }
        }))
    } else {
        let promise_id = w.pick_pending_promise(rng).unwrap_or_else(|| random_promise_id(rng));
        let state = if rng.bool() { "resolved" } else { "rejected" };
        req("task.fence", json!({
            "id": task_id,
            "version": version,
            "action": {
                "kind": "promise.settle",
                "head": {},
                "data": { "id": promise_id, "state": state, "value": {} }
            }
        }))
    }
}

fn gen_task_heartbeat(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    // Shuffle a copy so every acquired task gets a fair chance of being included,
    // not just the ones at the front of the insertion-order list.
    let mut acquired = w.acquired_tasks.clone();
    let take = acquired.len().min(3);
    for i in 0..take {
        let j = rng.usize(i..acquired.len());
        acquired.swap(i, j);
    }
    let tasks: Vec<Value> = acquired.into_iter()
        .take(3)
        .map(|(id, version)| {
            // 25% of the time send a stale version to exercise the version guard.
            let v = if rng.u32(0..4) == 0 { version - 1 } else { version };
            json!({ "id": id, "version": v })
        })
        .collect();
    let tasks = if tasks.is_empty() {
        vec![json!({ "id": random_task_id(rng), "version": rng.i64(0..3) })]
    } else {
        tasks
    };
    // 15% of the time send a wrong PID to exercise the process_id guard.
    let pid = if rng.u32(0..7) == 0 { "wrong-pid" } else { PID };
    req("task.heartbeat", json!({ "pid": pid, "tasks": tasks }))
}

fn gen_task_halt(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    // task.halt only requires the task ID; version is not part of the protocol.
    let id = w.pick_acquired_task(rng)
        .or_else(|| w.pick_suspended_task(rng))
        .or_else(|| w.pick_pending_task(rng))
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.halt", json!({ "id": id }))
}

fn gen_task_continue(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let id = w.pick_halted_task(rng).map(|(id, _)| id).unwrap_or_else(|| random_task_id(rng));
    req("task.continue", json!({ "id": id }))
}

fn gen_task_search(rng: &mut fastrand::Rng) -> RequestEnvelope {
    let data = match rng.u32(0..5) {
        0 => json!({ "state": "acquired",  "limit": 10 }),
        1 => json!({ "state": "pending",   "limit": 10 }),
        2 => json!({ "state": "suspended", "limit": 10 }),
        3 => json!({ "state": "halted",    "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("task.search", data)
}

fn gen_schedule_create(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
    let id = World::schedule_id(rng.u32(0..4));
    let promise_timeout = now + rng.i64(60_000..600_000);
    req("schedule.create", json!({
        "id": id,
        "cron": "* * * * *",
        "promiseId": format!("sched-promise-{{{{.id}}}}-{{{{.timestamp}}}}"),
        "promiseTimeout": promise_timeout,
        "promiseParam": {},
        "promiseTags": {}
    }))
}

fn gen_schedule_get(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let id = w.pick_schedule(rng).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.get", json!({ "id": id }))
}

fn gen_schedule_delete(rng: &mut fastrand::Rng, w: &World) -> RequestEnvelope {
    let id = w.pick_schedule(rng).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.delete", json!({ "id": id }))
}

fn gen_schedule_search(rng: &mut fastrand::Rng) -> RequestEnvelope {
    let limit = if rng.bool() { 10 } else { 5 };
    req("schedule.search", json!({ "limit": limit }))
}

fn gen_debug_tick(rng: &mut fastrand::Rng, now: i64) -> (RequestEnvelope, i64) {
    let new_now = now + rng.i64(0..50_000);
    (req("debug.tick", json!({ "time": new_now })), new_now)
}

fn random_promise_id(rng: &mut fastrand::Rng) -> String  { World::promise_id(rng.u32(0..8)) }
fn random_task_id(rng: &mut fastrand::Rng) -> String     { World::task_id(rng.u32(0..8)) }
fn random_schedule_id(rng: &mut fastrand::Rng) -> String { World::schedule_id(rng.u32(0..4)) }

// Returns a promise ID drawn from the pool that is guaranteed to differ from `other`.
// Uses (n+1)%8 as the fallback so the result is always distinct without a second rng draw.
fn promise_id_different_from(rng: &mut fastrand::Rng, other: &str) -> String {
    let n = rng.u32(0..8);
    let candidate = World::promise_id(n);
    if candidate == other { World::promise_id((n + 1) % 8) } else { candidate }
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

fn remove_from(list: &mut Vec<(String, i64)>, id: &str) {
    list.retain(|(tid, _)| tid != id);
}

// Tracks what we believe is in the store so generators can produce valid
// (and therefore 2xx-eligible) operations. This is a best-effort shadow model,
// not an exact replica — it biases the generator toward interesting paths.
#[derive(Default)]
struct World {
    // Plain pending promises (no resonate:target — not tasks).
    pending_promises: Vec<String>,
    all_promises: Vec<String>,

    // Task promises: same ID for both the promise and the task.
    // Stored as (id, version).
    acquired_tasks:  Vec<(String, i64)>,
    pending_tasks:   Vec<(String, i64)>,
    suspended_tasks: Vec<(String, i64)>,
    halted_tasks:    Vec<(String, i64)>,

    schedules: Vec<String>,
}

impl World {
    fn promise_id(n: u32) -> String  { format!("p{n}") }
    fn task_id(n: u32) -> String     { format!("t{n}") }
    fn schedule_id(n: u32) -> String { format!("s{n}") }

    fn pick<T: Clone>(&self, rng: &mut fastrand::Rng, v: &[T]) -> Option<T> {
        if v.is_empty() { None } else { Some(v[rng.usize(0..v.len())].clone()) }
    }
    fn pick_pending_promise(&self, rng: &mut fastrand::Rng) -> Option<String> {
        self.pick(rng, &self.pending_promises)
    }
    fn pick_any_promise(&self, rng: &mut fastrand::Rng) -> Option<String> {
        self.pick(rng, &self.all_promises)
    }
    fn pick_acquired_task(&self, rng: &mut fastrand::Rng) -> Option<(String, i64)> {
        self.pick(rng, &self.acquired_tasks)
    }
    fn pick_pending_task(&self, rng: &mut fastrand::Rng) -> Option<(String, i64)> {
        self.pick(rng, &self.pending_tasks)
    }
    fn pick_suspended_task(&self, rng: &mut fastrand::Rng) -> Option<(String, i64)> {
        self.pick(rng, &self.suspended_tasks)
    }
    fn pick_halted_task(&self, rng: &mut fastrand::Rng) -> Option<(String, i64)> {
        self.pick(rng, &self.halted_tasks)
    }
    fn pick_schedule(&self, rng: &mut fastrand::Rng) -> Option<String> {
        self.pick(rng, &self.schedules)
    }

    fn remove_acquired(&mut self, id: &str)      { remove_from(&mut self.acquired_tasks, id); }
    fn remove_pending_task(&mut self, id: &str)  { remove_from(&mut self.pending_tasks, id); }
    fn remove_suspended(&mut self, id: &str)     { remove_from(&mut self.suspended_tasks, id); }
    fn remove_halted(&mut self, id: &str)        { remove_from(&mut self.halted_tasks, id); }

    // Looks up the tracked version for a task across all state lists.
    // Falls back to 1 when the ID is not tracked — safe because the only callers are
    // task.halt and task.continue apply branches, which only fire on a 200 response,
    // meaning the server accepted the operation and the task was in a known state.
    fn version_of(&self, id: &str) -> i64 {
        self.acquired_tasks.iter()
            .chain(&self.pending_tasks)
            .chain(&self.suspended_tasks)
            .chain(&self.halted_tasks)
            .find(|(tid, _)| tid == id)
            .map(|(_, v)| *v)
            .unwrap_or(1)
    }

    // Updates shadow state from a completed operation. No-ops on non-2xx.
    // Takes both the request and response bodies so it can extract IDs from
    // whichever carries them (responses for read-your-writes ops, requests for
    // ops whose responses are empty `{}`).
    fn apply(&mut self, kind: &str, status: i32, req: &Value, data: &Value) {
        if status >= 300 { return; }
        match kind {
            "promise.create" => {
                if let Some(id) = data.get("promise").and_then(|p| p.get("id")).and_then(|v| v.as_str()) {
                    let s = id.to_string();
                    if !self.all_promises.contains(&s) { self.all_promises.push(s.clone()); }
                    let state = data.get("promise").and_then(|p| p.get("state")).and_then(|v| v.as_str()).unwrap_or("");
                    if state == "pending" && !self.pending_promises.contains(&s) {
                        self.pending_promises.push(s);
                    }
                }
            }
            "promise.settle" => {
                if let Some(id) = data.get("promise").and_then(|p| p.get("id")).and_then(|v| v.as_str()) {
                    self.pending_promises.retain(|p| p != id);
                }
            }
            "task.create" => {
                if let Some(id) = data.get("task").and_then(|t| t.get("id")).and_then(|v| v.as_str()) {
                    let version = data.get("task").and_then(|t| t.get("version")).and_then(|v| v.as_i64()).unwrap_or(1);
                    let task_state = data.get("task").and_then(|t| t.get("state")).and_then(|v| v.as_str()).unwrap_or("");
                    let s = id.to_string();
                    self.remove_acquired(&s);
                    self.remove_pending_task(&s);
                    if task_state != "fulfilled" {
                        self.acquired_tasks.push((s, version));
                    }
                }
            }
            "task.acquire" => {
                if let Some(id) = data.get("task").and_then(|t| t.get("id")).and_then(|v| v.as_str()) {
                    let version = data.get("task").and_then(|t| t.get("version")).and_then(|v| v.as_i64()).unwrap_or(1);
                    let s = id.to_string();
                    self.remove_pending_task(&s);
                    if !self.acquired_tasks.iter().any(|(tid, _)| tid == &s) {
                        self.acquired_tasks.push((s, version));
                    }
                }
            }
            "task.release" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                let version = req.get("version").and_then(|v| v.as_i64()).unwrap_or(1);
                self.remove_acquired(id);
                // task_release sets state = 'pending' without incrementing version;
                // task_acquire increments via version = version + 1 in the UPDATE.
                self.pending_tasks.push((id.to_string(), version));
            }
            "task.fulfill" => {
                if let Some(id) = data.get("promise").and_then(|p| p.get("id")).and_then(|v| v.as_str()) {
                    self.remove_acquired(id);
                    self.pending_promises.retain(|p| p != id);
                }
            }
            "task.suspend" => {
                // status >= 300 is handled by the early return above.
                // A domain-specific 3xx from task.suspend means "immediate resume": the task
                // stays acquired and we make no change here.
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                let version = req.get("version").and_then(|v| v.as_i64()).unwrap_or(1);
                self.remove_acquired(id);
                self.suspended_tasks.push((id.to_string(), version));
            }
            "task.halt" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                let version = self.version_of(id);
                self.remove_acquired(id);
                self.remove_pending_task(id);
                self.remove_suspended(id);
                self.halted_tasks.push((id.to_string(), version));
            }
            "task.continue" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                let version = self.version_of(id);
                self.remove_halted(id);
                self.pending_tasks.push((id.to_string(), version));
            }
            "schedule.create" => {
                if let Some(id) = data.get("schedule").and_then(|s| s.get("id")).and_then(|v| v.as_str()) {
                    let s = id.to_string();
                    if !self.schedules.contains(&s) { self.schedules.push(s); }
                }
            }
            "schedule.delete" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                self.schedules.retain(|s| s != id);
            }
            _ => {}
        }
    }
}
