use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Write};
use std::sync::Mutex;

use tempfile::TempDir;
use zip::write::SimpleFileOptions;

use super::chatgpt::{sniff_chatgpt_payload, ChatGptPayloadShape};
use super::ids::sha256_hex;
use super::obsidian::obsidian_note_identity_hash;
use super::*;
use crate::error::ParserError;
use crate::object_store::{NewObject, ObjectStore, PutObjectResult, StoredObject};
use crate::storage::{
    ArtifactIngestResult, EnrichmentStatus, ImportStatus, ImportWriteResult, ImportedArtifact,
    JobStatus, WriteImportSet,
};

struct MockStore {
    captured: Mutex<Vec<WriteImportSet>>,
}

impl MockStore {
    fn new() -> Self {
        Self {
            captured: Mutex::new(Vec::new()),
        }
    }
}

impl ImportWriteStore for MockStore {
    fn write_import(
        &self,
        import_set: WriteImportSet,
    ) -> crate::error::StorageResult<ImportWriteResult> {
        let artifact_ids = import_set
            .artifact_sets
            .iter()
            .map(|set| set.artifact.artifact_id.clone())
            .collect::<Vec<_>>();
        self.captured.lock().unwrap().push(import_set);
        Ok(ImportWriteResult {
            import_id: "import-test".to_string(),
            import_status: ImportStatus::Completed,
            artifacts: artifact_ids
                .into_iter()
                .map(|artifact_id| ImportedArtifact {
                    artifact_id,
                    enrichment_status: EnrichmentStatus::Pending,
                    ingest_result: ArtifactIngestResult::Created,
                })
                .collect(),
            failed_artifact_ids: Vec::new(),
            segments_written: 0,
        })
    }
}

struct FailingImportStore;

impl ImportWriteStore for FailingImportStore {
    fn write_import(
        &self,
        import_set: WriteImportSet,
    ) -> crate::error::StorageResult<ImportWriteResult> {
        Err(crate::error::StorageError::InsertImport {
            import_id: import_set.import.import_id,
            source: Box::new(std::io::Error::other("synthetic precommit failure")),
        })
    }
}

struct MockObjectStore {
    puts: Mutex<Vec<NewObject>>,
    deletes: Mutex<Vec<StoredObject>>,
}

impl MockObjectStore {
    fn new() -> Self {
        Self {
            puts: Mutex::new(Vec::new()),
            deletes: Mutex::new(Vec::new()),
        }
    }
}

impl ObjectStore for MockObjectStore {
    fn put_object(&self, object: NewObject) -> crate::error::ObjectStoreResult<PutObjectResult> {
        let stored = StoredObject {
            object_id: object.object_id.clone(),
            provider: "mock".to_string(),
            storage_key: format!("{}/{}", object.namespace, object.sha256),
            mime_type: object.mime_type.clone(),
            size_bytes: object.bytes.len() as i64,
            sha256: object.sha256.clone(),
        };
        self.puts.lock().unwrap().push(object);
        Ok(PutObjectResult {
            stored_object: stored,
            was_created: true,
        })
    }

    fn get_object_bytes(&self, object: &StoredObject) -> crate::error::ObjectStoreResult<Vec<u8>> {
        Ok(object.storage_key.as_bytes().to_vec())
    }

    fn delete_object(&self, object: &StoredObject) -> crate::error::ObjectStoreResult<()> {
        self.deletes.lock().unwrap().push(object.clone());
        Ok(())
    }
}

#[test]
fn import_chatgpt_payload_returns_compact_response() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let response = import_chatgpt_payload(
        &store,
        &object_store,
        single_conversation_export().as_bytes(),
    )
    .unwrap();

    assert_eq!(response.import_id, "import-test");
    assert_eq!(response.import_status, "completed");
    assert_eq!(response.artifacts.len(), 1);
    assert_eq!(response.artifacts[0].enrichment_status, "pending");
    assert_eq!(response.artifacts[0].ingest_result, "created");
}

#[test]
fn import_chatgpt_payload_builds_multi_conversation_write_set() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    import_chatgpt_payload(
        &store,
        &object_store,
        multi_conversation_export().as_bytes(),
    )
    .unwrap();

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();
    assert_eq!(import_set.import.conversation_count_detected, 2);
    assert_eq!(import_set.artifact_sets.len(), 2);
    assert_eq!(
        import_set.import.payload_object_id,
        import_set.payload_object.object_id
    );
    assert_eq!(
        import_set.artifact_sets[0].job.job_status,
        JobStatus::Pending
    );
    assert_eq!(
        import_set.artifact_sets[1].artifact.enrichment_status,
        EnrichmentStatus::Pending
    );

    for artifact_set in &import_set.artifact_sets {
        let payload =
            crate::storage::ArtifactExtractPayload::from_json(&artifact_set.job.payload_json)
                .expect("payload must deserialize to ArtifactExtractPayload");
        assert_eq!(payload.schema_version, "1");
        assert_eq!(
            payload.source_type,
            super::SourceType::ChatGptExport.as_str()
        );
        assert!(payload.conversation_windows.is_empty());
        assert!(payload.topic_threads.is_empty());
    }
}

#[test]
fn import_text_payload_builds_document_artifact_without_participants() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let response = import_text_payload(
        &store,
        &object_store,
        b"First paragraph.\n\nSecond paragraph.",
    )
    .unwrap();

    assert_eq!(response.import_status, "completed");

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();
    assert_eq!(import_set.import.source_type, SourceType::TextFile);
    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::TextPlain
    );
    assert_eq!(import_set.payload_object.mime_type, "text/plain");
    assert_eq!(import_set.artifact_sets.len(), 1);
    assert!(import_set.artifact_sets[0].participants.is_empty());
    assert_eq!(
        import_set.artifact_sets[0].artifact.artifact_class,
        ArtifactClass::Document
    );
    assert_eq!(import_set.artifact_sets[0].segments.len(), 2);
}

#[test]
fn import_markdown_payload_preserves_heading_title_and_markdown_mime_type() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    import_markdown_payload(
        &store,
        &object_store,
        b"# Project Notes\n\nParagraph text.\n\n- one\n- two",
    )
    .unwrap();

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();
    assert_eq!(import_set.import.source_type, SourceType::MarkdownFile);
    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::MarkdownText
    );
    assert_eq!(import_set.payload_object.mime_type, "text/markdown");
    assert_eq!(
        import_set.artifact_sets[0].artifact.title.as_deref(),
        Some("Project Notes")
    );
    assert_eq!(import_set.artifact_sets[0].segments.len(), 3);
}

#[test]
fn import_obsidian_payload_builds_first_class_note_metadata() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response = import_obsidian_vault_payload(&store, &object_store, &obsidian_fixture_zip())
        .expect("obsidian import should succeed");

    assert_eq!(response.artifacts.len(), 2);

    let captured = store.captured.lock().unwrap();
    let import_set = captured.last().expect("import set should exist");
    assert_eq!(import_set.import.source_type, SourceType::ObsidianVault);
    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::ObsidianVaultZip
    );
    assert_eq!(import_set.payload_object.mime_type, "application/zip");

    let artifact_ids_by_path = import_set
        .artifact_sets
        .iter()
        .map(|artifact_set| {
            (
                artifact_set
                    .artifact
                    .source_conversation_key
                    .clone()
                    .expect("note path should exist"),
                artifact_set.artifact.artifact_id.clone(),
            )
        })
        .collect::<HashMap<_, _>>();

    let inbox = import_set
        .artifact_sets
        .iter()
        .find(|artifact_set| {
            artifact_set.artifact.source_conversation_key.as_deref() == Some("Inbox.md")
        })
        .expect("Inbox.md artifact should exist");
    assert_eq!(inbox.artifact.artifact_class, ArtifactClass::Document);
    assert!(inbox.participants.is_empty());
    assert_eq!(inbox.imported_note_metadata.properties.len(), 5);
    assert_eq!(inbox.imported_note_metadata.tags.len(), 2);
    assert_eq!(inbox.imported_note_metadata.aliases.len(), 1);
    assert_eq!(inbox.imported_note_metadata.links.len(), 2);
    assert_eq!(
        inbox.imported_note_metadata.links[0]
            .resolved_artifact_id
            .as_deref(),
        artifact_ids_by_path
            .get("Projects/Acme.md")
            .map(String::as_str)
    );

    let payload = crate::storage::ArtifactExtractPayload::from_json(&inbox.job.payload_json)
        .expect("payload must deserialize");
    let note_metadata = payload
        .imported_note_metadata
        .expect("obsidian payload should include imported note metadata");
    assert_eq!(payload.source_type, SourceType::ObsidianVault.as_str());
    assert_eq!(note_metadata.note_path.as_deref(), Some("Inbox.md"));
    assert_eq!(note_metadata.tags.len(), 2);
    assert_eq!(note_metadata.aliases[0].normalized_alias, "oa inbox");
    assert_eq!(
        note_metadata.outbound_links[0]
            .resolved_artifact_id
            .as_deref(),
        artifact_ids_by_path
            .get("Projects/Acme.md")
            .map(String::as_str)
    );
}

#[test]
fn obsidian_source_hash_includes_note_path_identity() {
    let first = obsidian_note_identity_hash("Inbox.md", "abc123");
    let second = obsidian_note_identity_hash("Projects/Inbox.md", "abc123");

    assert_ne!(first, second);
}

#[test]
fn import_obsidian_payload_keeps_distinct_note_paths_with_same_content() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response =
        import_obsidian_vault_payload(&store, &object_store, &obsidian_same_content_fixture_zip())
            .expect("obsidian import should succeed");

    assert_eq!(response.artifacts.len(), 2);

    let captured = store.captured.lock().unwrap();
    let import_set = captured.last().expect("import set should exist");
    assert_eq!(import_set.artifact_sets.len(), 2);

    let mut source_hashes = import_set
        .artifact_sets
        .iter()
        .map(|artifact_set| artifact_set.artifact.source_conversation_hash.clone())
        .collect::<Vec<_>>();
    source_hashes.sort();
    source_hashes.dedup();
    assert_eq!(
        source_hashes.len(),
        2,
        "same-content notes under different paths must not collapse"
    );
}

fn create_test_vault_dir() -> TempDir {
    let temp_dir = TempDir::new().expect("temp dir should create");
    let root = temp_dir.path();

    fs::create_dir(root.join("Projects")).expect("dir should create");
    fs::create_dir(root.join("Projects").join("Acme")).expect("dir should create");
    fs::create_dir(root.join(".obsidian")).expect("hidden dir should create");

    fs::write(
        root.join("Inbox.md"),
        "---\ntags: [project/acme]\naliases: [Inbox Note]\n---\n# Inbox\nLink to [[Projects/Acme|Acme]].",
    )
    .expect("note should write");

    fs::write(root.join("Projects").join("Acme.md"), "# Acme\nContent").expect("note should write");

    fs::write(
        root.join("Projects").join("Acme").join("Tasks.md"),
        "# Tasks\n- [ ] Do something",
    )
    .expect("note should write");

    fs::write(root.join(".obsidian").join("config.md"), "# Config").ok();

    temp_dir
}

#[test]
fn import_obsidian_vault_directory_stores_manifest_and_blobs() {
    let vault_dir = create_test_vault_dir();
    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response = import_obsidian_vault_directory(&store, &object_store, vault_dir.path())
        .expect("directory import should succeed");

    assert_eq!(response.artifacts.len(), 3);

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().expect("import set should exist");
    assert_eq!(import_set.import.source_type, SourceType::ObsidianVault);
    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::ObsidianVaultDirectory
    );
    assert_eq!(import_set.payload_object.mime_type, "application/json");

    let puts = object_store.puts.lock().unwrap();
    assert_eq!(puts.len(), 4, "should store 3 file blobs + 1 manifest");

    let manifest_object = puts
        .iter()
        .find(|o| o.mime_type == "application/json")
        .expect("manifest should exist");
    assert!(!manifest_object.bytes.is_empty());

    let markdown_objects: Vec<_> = puts
        .iter()
        .filter(|o| o.mime_type == "text/markdown")
        .collect();
    assert_eq!(markdown_objects.len(), 3);
}

#[test]
fn import_obsidian_vault_directory_manifest_is_deterministic() {
    use crate::parser::obsidian::ObsidianVaultManifest;

    let vault_dir = create_test_vault_dir();

    let store1 = MockStore::new();
    let object_store1 = MockObjectStore::new();
    import_obsidian_vault_directory(&store1, &object_store1, vault_dir.path())
        .expect("first import should succeed");

    let store2 = MockStore::new();
    let object_store2 = MockObjectStore::new();
    import_obsidian_vault_directory(&store2, &object_store2, vault_dir.path())
        .expect("second import should succeed");

    let puts1 = object_store1.puts.lock().unwrap();
    let manifest1_bytes = &puts1
        .iter()
        .find(|o| o.mime_type == "application/json")
        .expect("manifest1 should exist")
        .bytes;

    let puts2 = object_store2.puts.lock().unwrap();
    let manifest2_bytes = &puts2
        .iter()
        .find(|o| o.mime_type == "application/json")
        .expect("manifest2 should exist")
        .bytes;

    let manifest1: ObsidianVaultManifest =
        serde_json::from_slice(manifest1_bytes).expect("manifest1 should parse");
    let manifest2: ObsidianVaultManifest =
        serde_json::from_slice(manifest2_bytes).expect("manifest2 should parse");

    assert_eq!(manifest1.version, manifest2.version);
    assert_eq!(
        manifest1.files.len(),
        manifest2.files.len(),
        "file counts should match"
    );

    let paths1: Vec<_> = manifest1.files.keys().collect();
    let paths2: Vec<_> = manifest2.files.keys().collect();
    assert_eq!(paths1, paths2, "file ordering must be deterministic");

    for (path, entry1) in &manifest1.files {
        let entry2 = manifest2
            .files
            .get(path)
            .expect("file should exist in both manifests");
        assert_eq!(
            entry1.content_hash, entry2.content_hash,
            "content hash should match for {path}"
        );
        assert_eq!(entry1.size, entry2.size, "size should match for {path}");
    }
}

#[test]
fn import_obsidian_vault_directory_cleans_up_payload_on_precommit_failure() {
    let vault_dir = create_test_vault_dir();
    let store = FailingImportStore;
    let object_store = MockObjectStore::new();

    let result = import_obsidian_vault_directory(&store, &object_store, vault_dir.path());
    assert!(result.is_err(), "should fail on write error");

    let deletes = object_store.deletes.lock().unwrap();
    assert_eq!(
        deletes.len(),
        1,
        "should clean up only the payload for pre-commit failures"
    );
}

#[test]
fn import_obsidian_vault_directory_cleans_up_blobs_on_manifest_storage_failure() {
    let vault_dir = create_test_vault_dir();
    let store = MockStore::new();
    let failing_object_store = FailingObjectStore::new(3);

    let result = import_obsidian_vault_directory(&store, &failing_object_store, vault_dir.path());
    assert!(result.is_err(), "should fail on manifest storage error");

    let deletes = failing_object_store.deletes.lock().unwrap();
    assert_eq!(
        deletes.len(),
        3,
        "should clean up file blobs when manifest storage fails"
    );
}

struct FailingObjectStore {
    puts: Mutex<Vec<NewObject>>,
    deletes: Mutex<Vec<StoredObject>>,
    fail_after: usize,
    put_count: Mutex<usize>,
}

impl FailingObjectStore {
    fn new(fail_after: usize) -> Self {
        Self {
            puts: Mutex::new(Vec::new()),
            deletes: Mutex::new(Vec::new()),
            fail_after,
            put_count: Mutex::new(0),
        }
    }
}

impl ObjectStore for FailingObjectStore {
    fn put_object(&self, object: NewObject) -> crate::error::ObjectStoreResult<PutObjectResult> {
        let mut count = self.put_count.lock().unwrap();
        *count += 1;

        if *count > self.fail_after {
            return Err(crate::error::ObjectStoreError::WriteObject {
                object_id: object.object_id.clone(),
                path: std::path::PathBuf::from("/mock/fail"),
                source: Box::new(std::io::Error::other("simulated storage failure")),
            });
        }

        let stored = StoredObject {
            object_id: object.object_id.clone(),
            provider: "mock".to_string(),
            storage_key: format!("{}/{}", "obsidian-files", object.sha256),
            mime_type: object.mime_type.clone(),
            size_bytes: object.bytes.len() as i64,
            sha256: object.sha256.clone(),
        };
        self.puts.lock().unwrap().push(object);
        Ok(PutObjectResult {
            stored_object: stored,
            was_created: true,
        })
    }

    fn get_object_bytes(&self, object: &StoredObject) -> crate::error::ObjectStoreResult<Vec<u8>> {
        Ok(object.storage_key.as_bytes().to_vec())
    }

    fn delete_object(&self, object: &StoredObject) -> crate::error::ObjectStoreResult<()> {
        self.deletes.lock().unwrap().push(object.clone());
        Ok(())
    }
}

#[test]
fn import_obsidian_vault_directory_produces_same_artifacts_as_zip() {
    let vault_dir = create_test_vault_dir();

    let dir_store = MockStore::new();
    let dir_object_store = MockObjectStore::new();
    let dir_response =
        import_obsidian_vault_directory(&dir_store, &dir_object_store, vault_dir.path())
            .expect("directory import should succeed");

    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file("Inbox.md", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(
            b"---\n\
              tags: [project/acme]\n\
              aliases: [Inbox Note]\n\
              ---\n\
              # Inbox\n\
              Link to [[Projects/Acme|Acme]].",
        )
        .expect("note should write");
    writer
        .start_file("Projects/Acme.md", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(b"# Acme\nContent")
        .expect("note should write");
    writer
        .start_file("Projects/Acme/Tasks.md", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(b"# Tasks\n- [ ] Do something")
        .expect("note should write");
    let zip_bytes = writer.finish().expect("zip should finish").into_inner();

    let zip_store = MockStore::new();
    let zip_object_store = MockObjectStore::new();
    let zip_response = import_obsidian_vault_payload(&zip_store, &zip_object_store, &zip_bytes)
        .expect("zip import should succeed");

    assert_eq!(dir_response.artifacts.len(), zip_response.artifacts.len());
    assert_eq!(dir_response.artifacts.len(), 3);

    let dir_captured = dir_store.captured.lock().unwrap();
    let dir_set = dir_captured.first().unwrap();
    let dir_paths: std::collections::BTreeSet<_> = dir_set
        .artifact_sets
        .iter()
        .map(|a| a.artifact.source_conversation_key.clone().unwrap())
        .collect();

    let zip_captured = zip_store.captured.lock().unwrap();
    let zip_set = zip_captured.first().unwrap();
    let zip_paths: std::collections::BTreeSet<_> = zip_set
        .artifact_sets
        .iter()
        .map(|a| a.artifact.source_conversation_key.clone().unwrap())
        .collect();

    assert_eq!(dir_paths, zip_paths);
}

#[test]
fn import_obsidian_vault_directory_link_resolution_works() {
    let vault_dir = create_test_vault_dir();
    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    import_obsidian_vault_directory(&store, &object_store, vault_dir.path())
        .expect("import should succeed");

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();

    let inbox = import_set
        .artifact_sets
        .iter()
        .find(|a| a.artifact.source_conversation_key.as_deref() == Some("Inbox.md"))
        .expect("Inbox should exist");

    let link = inbox
        .imported_note_metadata
        .links
        .iter()
        .find(|l| l.raw_target.starts_with("Projects/Acme"))
        .expect("link should exist");
    assert_eq!(link.target_path.as_deref(), Some("Projects/Acme.md"));
    assert!(
        matches!(
            link.resolution_status,
            crate::storage::ImportedNoteLinkResolutionStatus::Resolved
        ),
        "link should be resolved"
    );
}

fn single_conversation_export() -> &'static str {
    r#"[{
      "id": "conv-1",
      "title": "First",
      "create_time": 1710000000,
      "update_time": 1710000060,
      "current_node": "m2",
      "mapping": {
        "root": {"id": "root", "message": null, "parent": null, "children": ["m1"]},
        "m1": {
          "id": "m1",
          "parent": "root",
          "children": ["m2"],
          "message": {
            "author": {"role": "user", "name": "David"},
            "create_time": 1710000001,
            "content": {"content_type": "text", "parts": ["Hello"]},
            "metadata": {}
          }
        },
        "m2": {
          "id": "m2",
          "parent": "m1",
          "children": [],
          "message": {
            "author": {"role": "assistant", "name": "ChatGPT"},
            "create_time": 1710000002,
            "content": {"content_type": "text", "parts": ["Hi"]},
            "metadata": {"model_slug": "gpt-4"}
          }
        }
      },
      "default_model_slug": "gpt-4"
    }]"#
}

fn multi_conversation_export() -> &'static str {
    r#"[{
      "id": "conv-1",
      "title": "First",
      "create_time": 1710000000,
      "update_time": 1710000060,
      "current_node": "m1",
      "mapping": {
        "root": {"id": "root", "message": null, "parent": null, "children": ["m1"]},
        "m1": {
          "id": "m1",
          "parent": "root",
          "children": [],
          "message": {
            "author": {"role": "user", "name": "David"},
            "create_time": 1710000001,
            "content": {"content_type": "text", "parts": ["One"]},
            "metadata": {}
          }
        }
      },
      "default_model_slug": null
    },
    {
      "id": "conv-2",
      "title": "Second",
      "create_time": 1710000100,
      "update_time": 1710000160,
      "current_node": "n2",
      "mapping": {
        "root2": {"id": "root2", "message": null, "parent": null, "children": ["n1"]},
        "n1": {
          "id": "n1",
          "parent": "root2",
          "children": ["n2"],
          "message": {
            "author": {"role": "user", "name": "David"},
            "create_time": 1710000101,
            "content": {"content_type": "text", "parts": ["Two"]},
            "metadata": {}
          }
        },
        "n2": {
          "id": "n2",
          "parent": "n1",
          "children": [],
          "message": {
            "author": {"role": "assistant", "name": "ChatGPT"},
            "create_time": 1710000102,
            "content": {"content_type": "text", "parts": ["Three"]},
            "metadata": {}
          }
        }
      },
      "default_model_slug": null
    }]"#
}

fn obsidian_fixture_zip() -> Vec<u8> {
    let root = format!(
        "{}/tests/fixtures/obsidian_vault",
        env!("CARGO_MANIFEST_DIR")
    );
    let files = [
        ("Inbox.md", format!("{root}/Inbox.md")),
        ("Projects/Acme.md", format!("{root}/Projects/Acme.md")),
        (
            ".obsidian/workspace.json",
            format!("{root}/.obsidian/workspace.json"),
        ),
    ];
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    for (zip_path, file_path) in files {
        writer
            .start_file(zip_path, SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(&fs::read(file_path).expect("fixture should read"))
            .expect("fixture bytes should write");
    }
    writer.finish().expect("zip should finish").into_inner()
}

fn obsidian_same_content_fixture_zip() -> Vec<u8> {
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let files = [
        (
            "Templates/Header 1.md",
            "# Repeated Header\n\nShared body.\n",
        ),
        (
            "Templates/Header 2.md",
            "# Repeated Header\n\nShared body.\n",
        ),
        (".obsidian/workspace.json", "{}"),
    ];
    for (zip_path, contents) in files {
        writer
            .start_file(zip_path, SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(contents.as_bytes())
            .expect("fixture bytes should write");
    }
    writer.finish().expect("zip should finish").into_inner()
}

#[test]
fn import_chatgpt_payload_rejects_malformed_json() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let err = import_chatgpt_payload(&store, &object_store, br#"{"bad":true}"#).unwrap_err();
    assert!(matches!(err, OpenArchiveError::Parser(_)));
    assert!(
        store.captured.lock().unwrap().is_empty(),
        "parser failures must not call the storage boundary"
    );
}

#[test]
fn import_chatgpt_payload_parser_failure_does_not_mutate_prior_store_state() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    import_chatgpt_payload(
        &store,
        &object_store,
        single_conversation_export().as_bytes(),
    )
    .unwrap();

    let err = import_chatgpt_payload(&store, &object_store, br#"{"bad":true}"#).unwrap_err();
    assert!(matches!(err, OpenArchiveError::Parser(_)));

    let captured = store.captured.lock().unwrap();
    assert_eq!(
        captured.len(),
        1,
        "malformed payloads must not enqueue a second write"
    );
}

#[test]
fn import_chatgpt_payload_cleans_up_new_object_on_safe_precommit_failure() {
    let store = FailingImportStore;
    let object_store = MockObjectStore::new();

    let err = import_chatgpt_payload(
        &store,
        &object_store,
        single_conversation_export().as_bytes(),
    )
    .unwrap_err();

    assert!(matches!(err, OpenArchiveError::Storage(_)));
    assert_eq!(object_store.puts.lock().unwrap().len(), 1);
    assert_eq!(object_store.deletes.lock().unwrap().len(), 1);
}

#[test]
fn import_claude_payload_uses_claude_source_metadata() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response = import_claude_payload(
        &store,
        &object_store,
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/claude_export.json"
        )),
    )
    .unwrap();

    assert_eq!(response.artifacts.len(), 1);
    let captured = store.captured.lock().unwrap();
    let import_set = captured.last().unwrap();
    assert_eq!(import_set.import.source_type, SourceType::ClaudeExport);
    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::ClaudeExportJson
    );
    assert_eq!(
        import_set.artifact_sets[0].artifact.normalization_version,
        crate::parser::claude::CLAUDE_NORMALIZATION_VERSION
    );
}

#[test]
fn import_grok_payload_uses_grok_source_metadata() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response = import_grok_payload(
        &store,
        &object_store,
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/grok_export.json"
        )),
    )
    .unwrap();

    assert_eq!(response.artifacts.len(), 1);
    let captured = store.captured.lock().unwrap();
    let import_set = captured.last().unwrap();
    assert_eq!(import_set.import.source_type, SourceType::GrokExport);
    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::GrokExportJson
    );
    assert_eq!(
        import_set.artifact_sets[0].artifact.content_hash_version,
        crate::parser::grok::GROK_CONTENT_HASH_VERSION
    );
}

#[test]
fn import_gemini_payload_skips_empty_items_and_imports_usable_entries() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response = import_gemini_payload(
        &store,
        &object_store,
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_export.json"
        )),
    )
    .unwrap();

    assert_eq!(response.artifacts.len(), 1);
    let captured = store.captured.lock().unwrap();
    let import_set = captured.last().unwrap();
    assert_eq!(import_set.import.source_type, SourceType::GeminiTakeout);
    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::GeminiTakeoutJson
    );
    assert_eq!(import_set.artifact_sets[0].segments.len(), 2);
}

#[test]
fn import_gemini_payload_rejects_payloads_without_usable_entries() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let err = import_gemini_payload(
        &store,
        &object_store,
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_empty_export.json"
        )),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        OpenArchiveError::Parser(ParserError::EmptyExport)
    ));
}

fn chatgpt_export_zip() -> Vec<u8> {
    let json_content = single_conversation_export();
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file("conversations.json", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(json_content.as_bytes())
        .expect("content should write");
    writer.finish().expect("zip should finish").into_inner()
}

fn chatgpt_export_zip_with_extra_files() -> Vec<u8> {
    let json_content = single_conversation_export();
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file("README.txt", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(b"This is a ChatGPT export")
        .expect("content should write");
    writer
        .start_file("conversations.json", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(json_content.as_bytes())
        .expect("content should write");
    writer.finish().expect("zip should finish").into_inner()
}

fn chatgpt_export_zip_missing_conversations() -> Vec<u8> {
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file("README.txt", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(b"Missing conversations.json")
        .expect("content should write");
    writer.finish().expect("zip should finish").into_inner()
}

#[test]
fn import_chatgpt_payload_accepts_zip_export() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let zip_bytes = chatgpt_export_zip();

    let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

    assert_eq!(response.artifacts.len(), 1);

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();

    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::ChatGptExportZip
    );
    assert_eq!(import_set.payload_object.mime_type, "application/zip");

    let expected_sha256 = sha256_hex(&zip_bytes);
    assert_eq!(import_set.payload_object.sha256, expected_sha256);

    let artifact = &import_set.artifact_sets[0].artifact;
    assert_eq!(artifact.source_conversation_key.as_deref(), Some("conv-1"));
    assert_eq!(import_set.artifact_sets[0].segments.len(), 2);
}

#[test]
fn import_chatgpt_payload_accepts_zip_with_extra_files() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let zip_bytes = chatgpt_export_zip_with_extra_files();

    let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

    assert_eq!(response.artifacts.len(), 1);

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();

    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::ChatGptExportZip
    );
    assert_eq!(import_set.payload_object.mime_type, "application/zip");
}

#[test]
fn import_chatgpt_payload_zip_missing_conversations_json_errors() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let zip_bytes = chatgpt_export_zip_missing_conversations();

    let err = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap_err();

    assert!(
        matches!(
            &err,
            OpenArchiveError::Parser(ParserError::UnsupportedPayload { detail })
            if detail.contains("conversations.json")
        ),
        "expected UnsupportedPayload error mentioning conversations.json, got: {err:?}"
    );

    assert!(object_store.puts.lock().unwrap().is_empty());
}

#[test]
fn import_chatgpt_payload_zip_with_malformed_conversations_json_fails_on_parse() {
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file("conversations.json", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(br#"{"invalid": json}"#)
        .expect("content should write");
    let zip_bytes = writer.finish().expect("zip should finish").into_inner();

    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let err = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap_err();

    assert!(matches!(err, OpenArchiveError::Parser(_)));
    assert!(object_store.puts.lock().unwrap().is_empty());
    assert!(object_store.deletes.lock().unwrap().is_empty());
}

#[test]
fn import_chatgpt_payload_raw_json_still_uses_json_format() {
    let store = MockStore::new();
    let object_store = MockObjectStore::new();
    let json_bytes = single_conversation_export().as_bytes();

    let response = import_chatgpt_payload(&store, &object_store, json_bytes).unwrap();

    assert_eq!(response.artifacts.len(), 1);

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();

    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::ChatGptExportJson
    );
    assert_eq!(import_set.payload_object.mime_type, "application/json");

    let expected_sha256 = sha256_hex(json_bytes);
    assert_eq!(import_set.payload_object.sha256, expected_sha256);
}

#[test]
fn sniff_chatgpt_payload_detects_zip_by_magic() {
    let zip_bytes = chatgpt_export_zip();
    assert!(zip_bytes.len() >= 4);
    assert_eq!(&zip_bytes[0..4], &[0x50, 0x4B, 0x03, 0x04]);

    let shape = sniff_chatgpt_payload(&zip_bytes).unwrap();
    assert!(
        matches!(shape, ChatGptPayloadShape::Zip { .. }),
        "should detect zip by magic bytes"
    );
}

#[test]
fn sniff_chatgpt_payload_detects_raw_json() {
    let json_bytes = single_conversation_export().as_bytes();

    let shape = sniff_chatgpt_payload(json_bytes).unwrap();
    assert!(
        matches!(shape, ChatGptPayloadShape::RawJson { bytes } if bytes == json_bytes),
        "should detect raw JSON"
    );
}

#[test]
fn sniff_chatgpt_payload_falls_back_to_raw_json_for_short_input() {
    let truncated = vec![0x50, 0x4B];

    let shape = sniff_chatgpt_payload(&truncated).unwrap();
    assert!(
        matches!(shape, ChatGptPayloadShape::RawJson { .. }),
        "short input should fall back to raw JSON"
    );
}

#[test]
fn sniff_chatgpt_payload_handles_empty_bytes() {
    let empty: &[u8] = b"";

    let shape = sniff_chatgpt_payload(empty).unwrap();
    assert!(
        matches!(shape, ChatGptPayloadShape::RawJson { bytes } if bytes.is_empty()),
        "empty bytes should fall back to raw JSON"
    );
}

#[test]
fn sniff_chatgpt_payload_rejects_corrupt_zip_after_valid_magic() {
    let corrupt = b"PK\x03\x04garbage that is not a valid zip structure";

    let err = sniff_chatgpt_payload(corrupt).unwrap_err();
    assert!(
        matches!(
            &err,
            OpenArchiveError::Parser(ParserError::UnsupportedPayload { detail })
            if detail.contains("corrupt")
        ),
        "expected corrupt zip error, got: {err:?}"
    );
}

#[test]
fn looks_like_chatgpt_zip_detects_root_level_conversations() {
    let zip_bytes = chatgpt_export_zip();
    assert!(looks_like_chatgpt_zip(&zip_bytes));
}

#[test]
fn looks_like_chatgpt_zip_detects_nested_conversations() {
    let json_content = single_conversation_export();
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file(
            "chatgpt-export-2024-01-01/conversations.json",
            SimpleFileOptions::default(),
        )
        .expect("file should start");
    writer
        .write_all(json_content.as_bytes())
        .expect("content should write");
    let zip_bytes = writer.finish().expect("zip should finish").into_inner();

    assert!(looks_like_chatgpt_zip(&zip_bytes));
}

#[test]
fn looks_like_chatgpt_zip_rejects_zip_without_conversations() {
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file("README.txt", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(b"No conversations here")
        .expect("content should write");
    let zip_bytes = writer.finish().expect("zip should finish").into_inner();

    assert!(!looks_like_chatgpt_zip(&zip_bytes));
}

#[test]
fn looks_like_chatgpt_zip_rejects_raw_json() {
    let json_bytes = single_conversation_export().as_bytes();
    assert!(!looks_like_chatgpt_zip(json_bytes));
}

#[test]
fn looks_like_chatgpt_zip_rejects_short_input() {
    let truncated = vec![0x50, 0x4B];
    assert!(!looks_like_chatgpt_zip(&truncated));
}

#[test]
fn looks_like_chatgpt_zip_rejects_corrupt_zip() {
    let corrupt = b"PK\x03\x04garbage that is not a valid zip structure";
    assert!(!looks_like_chatgpt_zip(corrupt));
}

#[test]
fn import_chatgpt_payload_accepts_nested_conversations_json() {
    let json_content = single_conversation_export();
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file(
            "chatgpt-export-2024-01-01/conversations.json",
            SimpleFileOptions::default(),
        )
        .expect("file should start");
    writer
        .write_all(json_content.as_bytes())
        .expect("content should write");
    let zip_bytes = writer.finish().expect("zip should finish").into_inner();

    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

    assert_eq!(response.artifacts.len(), 1);

    let captured = store.captured.lock().unwrap();
    let import_set = captured.first().unwrap();

    assert_eq!(
        import_set.payload_object.payload_format,
        PayloadFormat::ChatGptExportZip
    );
    assert_eq!(import_set.payload_object.mime_type, "application/zip");

    let artifact = &import_set.artifact_sets[0].artifact;
    assert_eq!(artifact.source_conversation_key.as_deref(), Some("conv-1"));
}

#[test]
fn import_chatgpt_payload_prefers_root_level_over_nested() {
    let root_json = single_conversation_export();
    let nested_json = multi_conversation_export();
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);

    writer
        .start_file(
            "nested/folder/conversations.json",
            SimpleFileOptions::default(),
        )
        .expect("file should start");
    writer
        .write_all(nested_json.as_bytes())
        .expect("content should write");

    writer
        .start_file("conversations.json", SimpleFileOptions::default())
        .expect("file should start");
    writer
        .write_all(root_json.as_bytes())
        .expect("content should write");

    let zip_bytes = writer.finish().expect("zip should finish").into_inner();

    let store = MockStore::new();
    let object_store = MockObjectStore::new();

    let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

    assert_eq!(response.artifacts.len(), 1);
}

#[test]
fn import_chatgpt_payload_zip_produces_same_artifacts_as_raw_json() {
    let zip_store = MockStore::new();
    let zip_object_store = MockObjectStore::new();
    let zip_bytes = chatgpt_export_zip();
    import_chatgpt_payload(&zip_store, &zip_object_store, &zip_bytes).unwrap();

    let json_store = MockStore::new();
    let json_object_store = MockObjectStore::new();
    let json_bytes = single_conversation_export().as_bytes();
    import_chatgpt_payload(&json_store, &json_object_store, json_bytes).unwrap();

    let zip_captured = zip_store.captured.lock().unwrap();
    let json_captured = json_store.captured.lock().unwrap();

    assert_eq!(
        zip_captured[0].artifact_sets[0]
            .artifact
            .source_conversation_key,
        json_captured[0].artifact_sets[0]
            .artifact
            .source_conversation_key,
        "source_conversation_key should match between zip and JSON imports"
    );

    assert_eq!(
        zip_captured[0].artifact_sets[0].segments.len(),
        json_captured[0].artifact_sets[0].segments.len(),
        "segment count should match between zip and JSON imports"
    );

    assert_ne!(
        zip_captured[0].payload_object.sha256, json_captured[0].payload_object.sha256,
        "zip and JSON should have different blob hashes"
    );
}
