use std::sync::Arc;

use crate::error::{OpenArchiveError, Result};
use crate::storage::{
    ArtifactDetailReadStore, DerivedObjectType, EnrichmentStatus, ParticipantRole, SourceType,
};

pub const DEFAULT_SEGMENT_LIMIT: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ArtifactDetailRequest {
    pub artifact_id: String,
    pub include_segments: bool,
    pub segment_offset: usize,
    pub segment_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ArtifactDetailSegmentView {
    pub segment_id: String,
    pub participant_id: Option<String>,
    pub participant_role: Option<ParticipantRole>,
    pub sequence_no: i32,
    pub text_content: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ArtifactDetailDerivedObjectView {
    pub derived_object_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub confidence_score: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ArtifactDetailResponse {
    pub artifact_id: String,
    pub title: Option<String>,
    pub source_type: SourceType,
    pub enrichment_status: EnrichmentStatus,
    pub segment_count: usize,
    pub segment_offset: usize,
    pub segment_limit: usize,
    pub returned_segment_count: usize,
    pub segments: Vec<ArtifactDetailSegmentView>,
    pub derived_objects: Vec<ArtifactDetailDerivedObjectView>,
}

pub struct ArtifactDetailService {
    read_store: Arc<dyn ArtifactDetailReadStore + Send + Sync>,
}

impl ArtifactDetailService {
    pub fn new(read_store: Arc<dyn ArtifactDetailReadStore + Send + Sync>) -> Self {
        Self { read_store }
    }

    pub fn get(&self, request: ArtifactDetailRequest) -> Result<Option<ArtifactDetailResponse>> {
        let Some(detail) = self
            .read_store
            .load_artifact_detail(&request.artifact_id)
            .map_err(OpenArchiveError::from)?
        else {
            return Ok(None);
        };

        let total_segments = detail.segments.len();
        let (segments, segment_offset, segment_limit) = if request.include_segments {
            let segment_offset = request.segment_offset.min(total_segments);
            let segment_limit = request.segment_limit.max(1);
            let segment_end = segment_offset
                .saturating_add(segment_limit)
                .min(total_segments);
            (
                detail.segments[segment_offset..segment_end].to_vec(),
                segment_offset,
                segment_limit,
            )
        } else {
            (Vec::new(), 0, 0)
        };
        let returned_segment_count = segments.len();

        Ok(Some(ArtifactDetailResponse {
            artifact_id: detail.artifact.artifact_id,
            title: detail.artifact.title,
            source_type: detail.artifact.source_type,
            enrichment_status: detail.artifact.enrichment_status,
            segment_count: total_segments,
            segment_offset,
            segment_limit,
            returned_segment_count,
            segments: segments
                .into_iter()
                .map(|segment| ArtifactDetailSegmentView {
                    segment_id: segment.segment_id,
                    participant_id: segment.participant_id,
                    participant_role: segment.participant_role,
                    sequence_no: segment.sequence_no,
                    text_content: segment.text_content,
                })
                .collect(),
            derived_objects: detail
                .derived_objects
                .into_iter()
                .map(|obj| ArtifactDetailDerivedObjectView {
                    derived_object_id: obj.derived_object_id,
                    derived_object_type: obj.derived_object_type,
                    title: obj.title,
                    body_text: obj.body_text,
                    confidence_score: obj.confidence_score,
                })
                .collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageResult;
    use crate::storage::{
        ArtifactDetailReadStore, ArtifactDetailRecord, ArtifactDetailSegment, ArtifactDetailView,
        DerivedObjectType,
    };

    struct MockArtifactDetailReadStore {
        detail: Option<ArtifactDetailView>,
    }

    impl ArtifactDetailReadStore for MockArtifactDetailReadStore {
        fn load_artifact_detail(
            &self,
            _artifact_id: &str,
        ) -> StorageResult<Option<ArtifactDetailView>> {
            Ok(self.detail.clone())
        }
    }

    fn service_with(detail: Option<ArtifactDetailView>) -> ArtifactDetailService {
        ArtifactDetailService::new(Arc::new(MockArtifactDetailReadStore { detail }))
    }

    fn request() -> ArtifactDetailRequest {
        ArtifactDetailRequest {
            artifact_id: "artifact-1".to_string(),
            include_segments: false,
            segment_offset: 0,
            segment_limit: DEFAULT_SEGMENT_LIMIT,
        }
    }

    #[test]
    fn returns_none_when_artifact_is_missing() {
        let service = service_with(None);
        assert!(service.get(request()).unwrap().is_none());
    }

    #[test]
    fn returns_enriched_artifact_detail() {
        let service = service_with(Some(ArtifactDetailView {
            artifact: ArtifactDetailRecord {
                artifact_id: "artifact-1".to_string(),
                title: Some("Artifact".to_string()),
                source_type: SourceType::ChatGptExport,
                enrichment_status: EnrichmentStatus::Completed,
            },
            segments: vec![
                ArtifactDetailSegment {
                    segment_id: "seg-1".to_string(),
                    participant_id: Some("participant-1".to_string()),
                    participant_role: Some(ParticipantRole::User),
                    sequence_no: 1,
                    text_content: "hello".to_string(),
                },
                ArtifactDetailSegment {
                    segment_id: "seg-2".to_string(),
                    participant_id: Some("participant-2".to_string()),
                    participant_role: Some(ParticipantRole::Assistant),
                    sequence_no: 2,
                    text_content: "world".to_string(),
                },
            ],
            derived_objects: vec![crate::storage::ArtifactDetailDerivedObject {
                derived_object_id: "derived-1".to_string(),
                derived_object_type: DerivedObjectType::Summary,
                title: Some("Summary".to_string()),
                body_text: Some("summary text".to_string()),
                confidence_score: Some(0.9),
            }],
        }));

        let response = service.get(request()).unwrap().unwrap();
        assert_eq!(response.artifact_id, "artifact-1");
        assert!(response.segments.is_empty());
        assert_eq!(response.segment_count, 2);
        assert_eq!(response.returned_segment_count, 0);
        assert_eq!(response.derived_objects.len(), 1);
        assert_eq!(
            response.derived_objects[0].derived_object_type,
            DerivedObjectType::Summary
        );
    }

    #[test]
    fn returns_not_yet_enriched_artifact_detail() {
        let service = service_with(Some(ArtifactDetailView {
            artifact: ArtifactDetailRecord {
                artifact_id: "artifact-1".to_string(),
                title: None,
                source_type: SourceType::ChatGptExport,
                enrichment_status: EnrichmentStatus::Pending,
            },
            segments: vec![ArtifactDetailSegment {
                segment_id: "seg-1".to_string(),
                participant_id: None,
                participant_role: None,
                sequence_no: 1,
                text_content: "raw text".to_string(),
            }],
            derived_objects: Vec::new(),
        }));

        let response = service.get(request()).unwrap().unwrap();
        assert_eq!(response.enrichment_status, EnrichmentStatus::Pending);
        assert_eq!(response.segment_count, 1);
        assert!(response.segments.is_empty());
        assert!(response.derived_objects.is_empty());
    }

    #[test]
    fn returns_segment_window_when_requested() {
        let service = service_with(Some(ArtifactDetailView {
            artifact: ArtifactDetailRecord {
                artifact_id: "artifact-1".to_string(),
                title: Some("Artifact".to_string()),
                source_type: SourceType::ChatGptExport,
                enrichment_status: EnrichmentStatus::Completed,
            },
            segments: vec![
                ArtifactDetailSegment {
                    segment_id: "seg-1".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 1,
                    text_content: "one".to_string(),
                },
                ArtifactDetailSegment {
                    segment_id: "seg-2".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 2,
                    text_content: "two".to_string(),
                },
                ArtifactDetailSegment {
                    segment_id: "seg-3".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 3,
                    text_content: "three".to_string(),
                },
            ],
            derived_objects: vec![],
        }));

        let response = service
            .get(ArtifactDetailRequest {
                artifact_id: "artifact-1".to_string(),
                include_segments: true,
                segment_offset: 1,
                segment_limit: 1,
            })
            .unwrap()
            .unwrap();

        assert_eq!(response.segment_count, 3);
        assert_eq!(response.segment_offset, 1);
        assert_eq!(response.segment_limit, 1);
        assert_eq!(response.returned_segment_count, 1);
        assert_eq!(response.segments[0].segment_id, "seg-2");
    }
}
