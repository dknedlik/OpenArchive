use std::collections::HashSet;

use crate::config::ExtractionChunkingConfig;
use crate::processor::ArtifactProcessorInput;
use crate::storage::{ConversationWindowRef, LoadedSegment, TopicThreadRef};

pub(crate) fn build_preprocess_coverage_windows(
    artifact_id: &str,
    segments: &[LoadedSegment],
    topic_threads: &[TopicThreadRef],
    chunking: &ExtractionChunkingConfig,
) -> Vec<ConversationWindowRef> {
    if segments.is_empty() {
        return Vec::new();
    }

    let covered: HashSet<i32> = topic_threads
        .iter()
        .flat_map(|thread| thread.spans.iter())
        .flat_map(|span| span.start_sequence_no..=span.end_sequence_no)
        .collect();
    let uncovered: Vec<_> = segments
        .iter()
        .filter(|segment| !covered.contains(&segment.sequence_no))
        .cloned()
        .collect();

    build_contiguous_windows(artifact_id, &uncovered, chunking)
}

pub(crate) fn build_chunk_inputs(
    input: &ArtifactProcessorInput,
    windows: &[ConversationWindowRef],
    chunking: &ExtractionChunkingConfig,
) -> Vec<ArtifactProcessorInput> {
    windows
        .iter()
        .flat_map(|window| {
            let chunk = ArtifactProcessorInput {
                artifact_id: input.artifact_id.clone(),
                import_id: input.import_id.clone(),
                source_type: input.source_type,
                title: input.title.clone(),
                participants: input.participants.clone(),
                segments: input
                    .segments
                    .iter()
                    .filter(|segment| {
                        segment.sequence_no >= window.start_sequence_no
                            && segment.sequence_no <= window.end_sequence_no
                    })
                    .cloned()
                    .collect(),
            };
            split_chunk_input(chunk, chunking, "window")
        })
        .collect()
}

pub(crate) fn build_topic_thread_inputs(
    input: &ArtifactProcessorInput,
    topic_threads: &[TopicThreadRef],
    coverage_windows: &[ConversationWindowRef],
    chunking: &ExtractionChunkingConfig,
) -> Vec<ArtifactProcessorInput> {
    let mut inputs = Vec::new();
    let mut covered = HashSet::new();

    for thread in topic_threads {
        let segments: Vec<_> = input
            .segments
            .iter()
            .filter(|segment| {
                thread.spans.iter().any(|span| {
                    segment.sequence_no >= span.start_sequence_no
                        && segment.sequence_no <= span.end_sequence_no
                })
            })
            .cloned()
            .collect();
        if segments.is_empty() {
            continue;
        }
        for segment in &segments {
            covered.insert(segment.sequence_no);
        }
        let chunk = ArtifactProcessorInput {
            artifact_id: input.artifact_id.clone(),
            import_id: input.import_id.clone(),
            source_type: input.source_type,
            title: Some(match &input.title {
                Some(title) => format!("{title} [{}]", thread.label),
                None => thread.label.clone(),
            }),
            participants: input.participants.clone(),
            segments,
        };
        inputs.extend(split_chunk_input(chunk, chunking, "thread"));
    }

    for window in coverage_windows {
        let segments: Vec<_> = input
            .segments
            .iter()
            .filter(|segment| {
                !covered.contains(&segment.sequence_no)
                    && segment.sequence_no >= window.start_sequence_no
                    && segment.sequence_no <= window.end_sequence_no
            })
            .cloned()
            .collect();
        if segments.is_empty() {
            continue;
        }
        let chunk = ArtifactProcessorInput {
            artifact_id: input.artifact_id.clone(),
            import_id: input.import_id.clone(),
            source_type: input.source_type,
            title: input.title.clone(),
            participants: input.participants.clone(),
            segments,
        };
        inputs.extend(split_chunk_input(chunk, chunking, "coverage"));
    }

    if inputs.is_empty() {
        vec![input.clone()]
    } else {
        inputs
    }
}

fn build_contiguous_windows(
    artifact_id: &str,
    segments: &[LoadedSegment],
    chunking: &ExtractionChunkingConfig,
) -> Vec<ConversationWindowRef> {
    if segments.is_empty() {
        return Vec::new();
    }
    if segments.len() <= chunking.max_segments_per_chunk {
        return vec![ConversationWindowRef {
            window_id: format!("{artifact_id}:window:0"),
            label: "coverage fallback".to_string(),
            start_sequence_no: segments
                .first()
                .map(|segment| segment.sequence_no)
                .unwrap_or(0),
            end_sequence_no: segments
                .last()
                .map(|segment| segment.sequence_no)
                .unwrap_or(0),
        }];
    }

    let mut windows = Vec::new();
    let mut start = 0usize;
    let step = chunking
        .max_segments_per_chunk
        .saturating_sub(chunking.chunk_overlap_segments)
        .max(1);
    while start < segments.len() {
        let end = (start + chunking.max_segments_per_chunk).min(segments.len());
        let first = &segments[start];
        let last = &segments[end - 1];
        windows.push(ConversationWindowRef {
            window_id: format!("{artifact_id}:window:{}", windows.len()),
            label: format!("messages {}-{}", first.sequence_no, last.sequence_no),
            start_sequence_no: first.sequence_no,
            end_sequence_no: last.sequence_no,
        });
        if end == segments.len() {
            break;
        }
        start += step;
    }
    windows
}

fn split_chunk_input(
    input: ArtifactProcessorInput,
    chunking: &ExtractionChunkingConfig,
    split_label: &str,
) -> Vec<ArtifactProcessorInput> {
    if input.segments.is_empty() {
        return Vec::new();
    }
    if input.segments.len() <= chunking.max_segments_per_chunk
        && total_chars(&input.segments) <= chunking.max_chars_per_chunk
    {
        return vec![input];
    }

    let mut outputs = Vec::new();
    let mut start = 0usize;
    let max_segments = chunking.max_segments_per_chunk.max(1);
    let overlap = chunking
        .chunk_overlap_segments
        .min(max_segments.saturating_sub(1));

    while start < input.segments.len() {
        let mut end = start;
        let mut char_count = 0usize;
        while end < input.segments.len() && (end - start) < max_segments {
            let next_chars = input.segments[end].text_content.len();
            if end > start && char_count + next_chars > chunking.max_chars_per_chunk {
                break;
            }
            char_count += next_chars;
            end += 1;
        }
        if end == start {
            end += 1;
        }

        let first_sequence_no = input.segments[start].sequence_no;
        let last_sequence_no = input.segments[end - 1].sequence_no;
        outputs.push(ArtifactProcessorInput {
            artifact_id: input.artifact_id.clone(),
            import_id: input.import_id.clone(),
            source_type: input.source_type,
            title: Some(match &input.title {
                Some(title) => {
                    format!("{title} [{split_label} {first_sequence_no}-{last_sequence_no}]")
                }
                None => format!("{split_label} {first_sequence_no}-{last_sequence_no}"),
            }),
            participants: input.participants.clone(),
            segments: input.segments[start..end].to_vec(),
        });

        if end == input.segments.len() {
            break;
        }
        let next_start = end.saturating_sub(overlap);
        start = if next_start <= start {
            start + 1
        } else {
            next_start
        };
    }

    outputs
}

fn total_chars(segments: &[LoadedSegment]) -> usize {
    segments
        .iter()
        .map(|segment| segment.text_content.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ParticipantRole, VisibilityStatus};
    use crate::storage::{LoadedParticipant, SourceType};

    fn chunking() -> ExtractionChunkingConfig {
        ExtractionChunkingConfig {
            max_segments_per_chunk: 2,
            chunk_overlap_segments: 1,
            max_chars_per_chunk: 10,
        }
    }

    fn input() -> ArtifactProcessorInput {
        ArtifactProcessorInput {
            artifact_id: "artifact-1".to_string(),
            import_id: "import-1".to_string(),
            source_type: SourceType::ClaudeExport,
            title: Some("Title".to_string()),
            participants: vec![LoadedParticipant {
                participant_id: "p1".to_string(),
                display_name: Some("User".to_string()),
                participant_role: ParticipantRole::User,
                external_id: None,
            }],
            segments: vec![
                LoadedSegment {
                    segment_id: "s1".to_string(),
                    participant_id: Some("p1".to_string()),
                    participant_role: Some(ParticipantRole::User),
                    sequence_no: 0,
                    text_content: "12345".to_string(),
                    created_at_source: None,
                    visibility_status: VisibilityStatus::Visible,
                },
                LoadedSegment {
                    segment_id: "s2".to_string(),
                    participant_id: Some("p1".to_string()),
                    participant_role: Some(ParticipantRole::User),
                    sequence_no: 1,
                    text_content: "67890".to_string(),
                    created_at_source: None,
                    visibility_status: VisibilityStatus::Visible,
                },
                LoadedSegment {
                    segment_id: "s3".to_string(),
                    participant_id: Some("p1".to_string()),
                    participant_role: Some(ParticipantRole::User),
                    sequence_no: 2,
                    text_content: "abcde".to_string(),
                    created_at_source: None,
                    visibility_status: VisibilityStatus::Visible,
                },
            ],
        }
    }

    #[test]
    fn split_chunk_input_respects_segment_and_char_caps() {
        let chunks = split_chunk_input(input(), &chunking(), "thread");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].segments.len(), 2);
        assert_eq!(chunks[1].segments.len(), 2);
        assert_eq!(chunks[1].segments[0].sequence_no, 1);
    }
}
