use std::{
    env,
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

use runner_reconstruction_legacy::RunnerConfig;

const ENV_VARS: &[&str] = &[
    RunnerConfig::ENV_WORKSPACE_ROOT,
    RunnerConfig::ENV_PYTHON_BIN,
    RunnerConfig::ENV_PYTHON_SCRIPT,
    RunnerConfig::ENV_PYTHON_ARGS,
    RunnerConfig::ENV_CPU_WORKERS,
];

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn clear_env() {
    for key in ENV_VARS {
        env::remove_var(key);
    }
}

#[test]
fn defaults_without_env_variables() {
    let _guard = env_lock().lock().unwrap();
    clear_env();

    let cfg = RunnerConfig::from_env().expect("config");
    assert_eq!(cfg.workspace_root, None);
    assert_eq!(
        cfg.python_bin,
        PathBuf::from(RunnerConfig::DEFAULT_PYTHON_BIN)
    );
    assert_eq!(
        cfg.python_script,
        PathBuf::from(RunnerConfig::DEFAULT_PYTHON_SCRIPT)
    );
    assert!(cfg.python_args.is_empty());
    assert_eq!(cfg.cpu_workers, RunnerConfig::DEFAULT_CPU_WORKERS);
    clear_env();
}

#[test]
fn env_overrides_are_respected() {
    let _guard = env_lock().lock().unwrap();
    clear_env();
    env::set_var(RunnerConfig::ENV_WORKSPACE_ROOT, "/tmp/runner-jobs");
    env::set_var(RunnerConfig::ENV_PYTHON_BIN, "/usr/local/bin/python");
    env::set_var(RunnerConfig::ENV_PYTHON_SCRIPT, "/opt/run.py");
    env::set_var(RunnerConfig::ENV_PYTHON_ARGS, "--foo bar");
    env::set_var(RunnerConfig::ENV_CPU_WORKERS, "8");

    let cfg = RunnerConfig::from_env().expect("config");
    assert_eq!(cfg.workspace_root, Some(PathBuf::from("/tmp/runner-jobs")));
    assert_eq!(cfg.python_bin, PathBuf::from("/usr/local/bin/python"));
    assert_eq!(cfg.python_script, PathBuf::from("/opt/run.py"));
    assert_eq!(
        cfg.python_args,
        vec![String::from("--foo"), String::from("bar")]
    );
    assert_eq!(cfg.cpu_workers, 8);
    clear_env();
}

#[test]
fn invalid_cpu_workers_surface_error() {
    let _guard = env_lock().lock().unwrap();
    clear_env();
    env::set_var(RunnerConfig::ENV_CPU_WORKERS, "not-a-number");

    let err = RunnerConfig::from_env().expect_err("should error");
    assert!(
        err.to_string().contains(RunnerConfig::ENV_CPU_WORKERS),
        "error message should reference the env var"
    );
    clear_env();
}
