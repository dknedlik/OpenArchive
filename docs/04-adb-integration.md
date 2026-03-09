# OpenArchive ADB Integration Notes

## What Seems De-Risked

- Reusing the existing Oracle Autonomous Database via a new schema looks like a
  sensible default to evaluate first
- Rust appears viable for the service if Oracle connectivity uses
  `rust-oracle`
- The reliable connection path found in the spike was Oracle Instant Client
  plus wallet plus `TNS_ADMIN`
- The pure-Rust Oracle driver path was not sufficient for the tested ADB
  handshake
- Local development against ADB is viable on this machine and in this broader
  project context; it is not just a deployment-only option

## Environment Facts From The Earlier Spike

- Existing app instance shape: `VM.Standard.A1.Flex`
- Existing app size: `1 OCPU`, `6 GB RAM`
- Boot volume: `50 GB`
- Additional block volume: `50 GB`
- Local Instant Client from the spike: `~/Downloads/instantclient_23_3`

## Operational Pattern Worth Reusing

From the existing application and spike:

- wallet-backed ADB connection
- TNS alias in `tnsnames.ora`
- least-privileged schema user at runtime
- OCI Vault for password retrieval in deployed environments

The current evidence for local viability is concrete:

- the existing fitness-tracker app connects to ADB locally
- the Rust ADB spike connects to ADB locally

So the local-development question is not "can this work locally at all?" It is
"how much friction are we willing to accept in local setup and iteration?"

If this project uses Rust plus ADB, the runtime shape would likely be:

- `WALLET_DIR` points at the unzipped ADB wallet
- `TNS_ADMIN` points at that wallet directory
- `TNS_ALIAS` selects the service, likely `_tp` for OLTP-oriented requests
- `DB_USERNAME` and `DB_PASSWORD` provide schema credentials
- Oracle Instant Client is available to the process

## Implications If We Go This Route

- Treat Oracle client installation as an explicit runtime prerequisite
- Avoid pretending the service is pure static Rust when ADB is mandatory
- Keep database access behind a small internal boundary so the rest of the
  service is not Oracle-driver-shaped
- Accept that local development is possible, while still recognizing that it is
  a more opinionated setup than SQLite or local Postgres
- Plan for schema migrations separately from the initial connectivity spike

## Possible Follow-Up

1. decide whether ADB-in-existing-schema-space is still the preferred default
2. define a first-pass canonical archive schema
3. build a minimal service only after the shape of the first import path is
   clearer
4. wire ADB connectivity using the proven wallet pattern if Rust remains the
   chosen stack
