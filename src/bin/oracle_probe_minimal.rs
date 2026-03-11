/// Minimal Oracle probe - no open_archive lib dependency.
/// Tests whether the segfault is caused by something in the lib crate.
fn main() {
    eprintln!("minimal_probe: start");
    let username = std::env::var("OA_ORACLE_USERNAME").expect("OA_ORACLE_USERNAME");
    let password = std::env::var("OA_ORACLE_PASSWORD").expect("OA_ORACLE_PASSWORD");
    let connect_string =
        std::env::var("OA_ORACLE_CONNECT_STRING").expect("OA_ORACLE_CONNECT_STRING");
    eprintln!("minimal_probe: connecting as {username}@{connect_string}");
    let conn =
        oracle::Connection::connect(&username, &password, &connect_string).expect("connect failed");
    let (n,): (i32,) = conn
        .query_row_as("SELECT 1 FROM dual", &[])
        .expect("query failed");
    eprintln!("minimal_probe: connected, got {n}");
}
