use std::process::{Command, Stdio};
use std::time::Duration;

#[test]
fn a_second_qdb_process_exits_before_opening_the_database() {
    let n = std::process::id();
    let dir = std::env::temp_dir().join(format!("qdb-one-instance-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let lock = dir.join("instance.lock");
    let port = 25000 + (n % 10000);
    let mut first = Command::new(env!("CARGO_BIN_EXE_qdb"))
        .arg(&dir)
        .env("QDB_INSTANCE_LOCK", &lock)
        .env("QDB_FEED", "0")
        .env("QDB_HTTP_PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));
    assert!(first.try_wait().unwrap().is_none(), "first instance exited");
    let second = Command::new(env!("CARGO_BIN_EXE_qdb"))
        .arg(dir.join("other"))
        .env("QDB_INSTANCE_LOCK", &lock)
        .env("QDB_FEED", "0")
        .env("QDB_HTTP_PORT", (port + 1).to_string())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(!second.status.success(), "{stderr}");
    assert!(stderr.contains("已有一个 qdb 实例"), "{stderr}");
    assert!(
        !dir.join("other").exists(),
        "second process opened a database"
    );
    let _ = first.kill();
    let _ = first.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
