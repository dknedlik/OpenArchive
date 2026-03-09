/// Minimal Oracle probe - no open_archive lib dependency.
/// Tests whether the segfault is caused by something in the lib crate.
fn main() {
    eprintln!("minimal_probe: start");
    let username = std::env::var("DB_USERNAME").expect("DB_USERNAME");
    let password = std::env::var("DB_PASSWORD").expect("DB_PASSWORD");
    let tns_alias = std::env::var("TNS_ALIAS").unwrap_or_else(|_| "cleanengine_medium".to_string());
    eprintln!("minimal_probe: connecting as {username}@{tns_alias}");
    let conn = oracle::Connection::connect(&username, &password, &tns_alias)
        .expect("connect failed");
    let (n,): (i32,) = conn.query_row_as("SELECT 1 FROM dual", &[]).expect("query failed");
    eprintln!("minimal_probe: connected, got {n}");
}
