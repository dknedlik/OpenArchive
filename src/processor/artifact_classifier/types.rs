use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactArchetype {
    Conversation,
    ProceduralNote,
    ReferenceNote,
    DefinitionNote,
    WorkingNote,
    JournalLog,
    DashboardTemplate,
    Unknown,
}

impl ArtifactArchetype {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conversation => "conversation",
            Self::ProceduralNote => "procedural_note",
            Self::ReferenceNote => "reference_note",
            Self::DefinitionNote => "definition_note",
            Self::WorkingNote => "working_note",
            Self::JournalLog => "journal_log",
            Self::DashboardTemplate => "dashboard_template",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactFacet {
    Technical,
    Setup,
    Project,
    Planning,
    Personal,
    Reference,
    Scientific,
    Automation,
}

impl ArtifactFacet {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Technical => "technical",
            Self::Setup => "setup",
            Self::Project => "project",
            Self::Planning => "planning",
            Self::Personal => "personal",
            Self::Reference => "reference",
            Self::Scientific => "scientific",
            Self::Automation => "automation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactClassificationProfile {
    pub primary_archetype: ArtifactArchetype,
    pub facets: Vec<ArtifactFacet>,
    pub confidence: f32,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchetypeScore {
    pub archetype: ArtifactArchetype,
    pub score: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetScore {
    pub facet: ArtifactFacet,
    pub score: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactClassificationDebug {
    pub profile: ArtifactClassificationProfile,
    pub archetype_scores: Vec<ArchetypeScore>,
    pub facet_scores: Vec<FacetScore>,
}
