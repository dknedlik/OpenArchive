use std::env;

fn main() {
    println!("OpenArchive brainstorming bootstrap");
    println!("  status: early project incubation");
    println!("  note: current files capture hypotheses, not final decisions");

    let config = [
        ("WALLET_DIR", false),
        ("TNS_ALIAS", true),
        ("DB_USERNAME", true),
        ("DB_PASSWORD", true),
        ("TNS_ADMIN", false),
    ];

    println!("  adb_env_if_revisited:");
    for (key, sensitive) in config {
        match env::var(key) {
            Ok(value) if !value.trim().is_empty() => {
                if sensitive {
                    println!("    {key}=set");
                } else {
                    println!("    {key}={value}");
                }
            }
            _ => println!("    {key}=missing"),
        }
    }
}
