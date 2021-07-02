use std::fs;
use std::io::Write;
use std::path::PathBuf;

use log_writer::*;
use mktemp::Temp;

#[test]
fn write_one_line() {
    let temp = Temp::new_dir().unwrap();
    println!("{}", temp.display());
    let config = LogWriterConfig {
        target_dir: temp.to_path_buf(),
        prefix: "test".to_string(),
        suffix: ".txt".to_string(),
        max_use_of_total: None,
        min_avail_of_total: None,
        warn_if_avail_reached: false,
        min_avail_bytes: Some(8192),
        max_file_size: 4096,
    };

    let mut writer = LogWriter::new(config).unwrap();

    let num = writeln!(writer, "logwriter-integration-test: one line").unwrap();
    writer.flush().unwrap();

    let mut dir = (&temp).read_dir().unwrap();

    let file = dir.next().unwrap().unwrap();
    // Only one file expected
    assert!(dir.next().is_none());

    let content = fs::read_to_string(file.path()).unwrap();
    assert_eq!(content, "logwriter-integration-test: one line\n");
    drop(writer);
    temp.release();
}
