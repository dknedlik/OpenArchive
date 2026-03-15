use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct EvalFixture {
    artifact_id: String,
    source_type: String,
    title: Option<String>,
    segments: Vec<EvalSegment>,
    expectations: EvalExpectations,
}

#[derive(Debug, Deserialize)]
struct EvalSegment {
    segment_id: String,
    participant_role: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct EvalExpectations {
    memory_count: EvalRange,
    classification_count: EvalRange,
    importance_score: EvalRange,
    escalate_to_frontier: bool,
    required_phrases: Vec<String>,
    forbidden_classification_values: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EvalRange {
    min: usize,
    max: usize,
}

#[derive(Debug, Deserialize)]
struct EvalOutput {
    summary: EvalSummary,
    classifications: Vec<EvalClassification>,
    memories: Vec<EvalMemory>,
    importance_score: u8,
    escalate_to_frontier: bool,
    escalation_reason: String,
}

#[derive(Debug, Deserialize)]
struct EvalSummary {
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EvalClassification {
    classification_type: String,
    classification_value: String,
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EvalMemory {
    memory_type: String,
    memory_scope: String,
    memory_scope_value: String,
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[test]
fn evaluation_fixtures_good_outputs_meet_expectations() {
    for case_name in [
        "architecture_direction",
        "worker_hardening",
        "low_signal_chat",
    ] {
        let fixture = load_fixture(case_name);
        let output = load_output(case_name);
        evaluate_fixture(case_name, &fixture, &output);
    }
}

fn evaluate_fixture(case_name: &str, fixture: &EvalFixture, output: &EvalOutput) {
    let valid_segment_ids: HashSet<&str> = fixture
        .segments
        .iter()
        .map(|segment| segment.segment_id.as_str())
        .collect();

    assert!(
        fixture.source_type == "chatgpt_export",
        "{case_name}: fixture source_type should currently be chatgpt_export"
    );
    assert!(
        !fixture.artifact_id.trim().is_empty(),
        "{case_name}: artifact_id must not be empty"
    );
    assert!(
        !fixture.segments.is_empty(),
        "{case_name}: fixture must contain at least one segment"
    );
    for segment in &fixture.segments {
        assert!(
            !segment.participant_role.trim().is_empty(),
            "{case_name}: participant_role must not be empty for segment {}",
            segment.segment_id
        );
        assert!(
            !segment.text.trim().is_empty(),
            "{case_name}: text must not be empty for segment {}",
            segment.segment_id
        );
    }

    assert!(
        !output.summary.title.trim().is_empty(),
        "{case_name}: summary title empty"
    );
    assert!(
        !output.summary.body_text.trim().is_empty(),
        "{case_name}: summary body empty"
    );
    assert_valid_evidence(
        case_name,
        "summary",
        &output.summary.evidence_segment_ids,
        &valid_segment_ids,
    );

    assert_in_range(
        case_name,
        "classification_count",
        output.classifications.len(),
        &fixture.expectations.classification_count,
    );
    assert_in_range(
        case_name,
        "memory_count",
        output.memories.len(),
        &fixture.expectations.memory_count,
    );
    assert_in_range(
        case_name,
        "importance_score",
        output.importance_score as usize,
        &fixture.expectations.importance_score,
    );
    assert_eq!(
        output.escalate_to_frontier, fixture.expectations.escalate_to_frontier,
        "{case_name}: escalate_to_frontier mismatch"
    );

    if output.escalate_to_frontier {
        assert!(
            !output.escalation_reason.trim().is_empty(),
            "{case_name}: escalation_reason must be present when escalating"
        );
    } else {
        assert!(
            output.escalation_reason.trim().is_empty(),
            "{case_name}: escalation_reason must be empty when not escalating"
        );
    }

    let mut seen_memory_titles = HashSet::new();
    for (index, classification) in output.classifications.iter().enumerate() {
        assert!(
            matches!(
                classification.classification_type.as_str(),
                "topic" | "intent"
            ),
            "{case_name}: classifications[{index}] has invalid type {}",
            classification.classification_type
        );
        assert!(
            !classification.classification_value.trim().is_empty(),
            "{case_name}: classifications[{index}] has empty value"
        );
        assert!(
            !classification.title.trim().is_empty(),
            "{case_name}: classifications[{index}] has empty title"
        );
        assert_valid_evidence(
            case_name,
            &format!("classifications[{index}]"),
            &classification.evidence_segment_ids,
            &valid_segment_ids,
        );
        let classification_value_lower = classification.classification_value.to_ascii_lowercase();
        assert!(
            !fixture
                .expectations
                .forbidden_classification_values
                .iter()
                .any(|value| value.eq_ignore_ascii_case(&classification_value_lower)),
            "{case_name}: classifications[{index}] used forbidden generic value {}",
            classification.classification_value
        );
    }

    for (index, memory) in output.memories.iter().enumerate() {
        assert!(
            matches!(
                memory.memory_type.as_str(),
                "preference" | "project_fact" | "identity_fact" | "ongoing_task" | "reference"
            ),
            "{case_name}: memories[{index}] has invalid type {}",
            memory.memory_type
        );
        assert_eq!(
            memory.memory_scope, "artifact",
            "{case_name}: memories[{index}] should use artifact scope in this fixture set"
        );
        assert_eq!(
            memory.memory_scope_value, fixture.artifact_id,
            "{case_name}: memories[{index}] should use fixture artifact_id as scope value"
        );
        assert!(
            !memory.title.trim().is_empty(),
            "{case_name}: memories[{index}] has empty title"
        );
        assert!(
            !memory.body_text.trim().is_empty(),
            "{case_name}: memories[{index}] has empty body"
        );
        let lowered_title = memory.title.to_ascii_lowercase();
        assert!(
            seen_memory_titles.insert(lowered_title),
            "{case_name}: duplicate memory title {}",
            memory.title
        );
        assert_valid_evidence(
            case_name,
            &format!("memories[{index}]"),
            &memory.evidence_segment_ids,
            &valid_segment_ids,
        );
    }

    let searchable_text = build_searchable_text(fixture, output);
    for phrase in &fixture.expectations.required_phrases {
        assert!(
            searchable_text.contains(&phrase.to_ascii_lowercase()),
            "{case_name}: required phrase {:?} missing from summary/memories/classifications",
            phrase
        );
    }
}

fn assert_in_range(case_name: &str, field: &str, value: usize, range: &EvalRange) {
    assert!(
        value >= range.min && value <= range.max,
        "{case_name}: {field} {value} not in expected range {}..={}",
        range.min,
        range.max
    );
}

fn assert_valid_evidence(
    case_name: &str,
    field: &str,
    evidence_segment_ids: &[String],
    valid_segment_ids: &HashSet<&str>,
) {
    assert!(
        !evidence_segment_ids.is_empty(),
        "{case_name}: {field} must contain at least one evidence_segment_id"
    );
    for segment_id in evidence_segment_ids {
        assert!(
            valid_segment_ids.contains(segment_id.as_str()),
            "{case_name}: {field} references unknown segment_id {segment_id}"
        );
    }
}

fn build_searchable_text(fixture: &EvalFixture, output: &EvalOutput) -> String {
    let mut parts = Vec::new();
    if let Some(title) = &fixture.title {
        parts.push(title.to_ascii_lowercase());
    }
    parts.push(output.summary.title.to_ascii_lowercase());
    parts.push(output.summary.body_text.to_ascii_lowercase());
    for classification in &output.classifications {
        parts.push(classification.classification_value.to_ascii_lowercase());
        parts.push(classification.title.to_ascii_lowercase());
        parts.push(classification.body_text.to_ascii_lowercase());
    }
    for memory in &output.memories {
        parts.push(memory.title.to_ascii_lowercase());
        parts.push(memory.body_text.to_ascii_lowercase());
    }
    parts.join("\n")
}

fn load_fixture(case_name: &str) -> EvalFixture {
    let path = fixture_dir().join(format!("{case_name}.fixture.json"));
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read fixture {}: {err}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|err| panic!("failed to parse fixture {}: {err}", path.display()))
}

fn load_output(case_name: &str) -> EvalOutput {
    let path = fixture_dir().join(format!("{case_name}.output.json"));
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read output {}: {err}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|err| panic!("failed to parse output {}: {err}", path.display()))
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("enrichment")
}
