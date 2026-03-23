-- Add 'entity' to allowed derived_object_type values
ALTER TABLE oa_derived_object DROP CONSTRAINT ck_oa_derived_object_type;
ALTER TABLE oa_derived_object ADD CONSTRAINT ck_oa_derived_object_type
    CHECK (derived_object_type IN ('summary', 'classification', 'memory', 'relationship', 'entity'));

-- Add 'rejected' to allowed object_status values
ALTER TABLE oa_derived_object DROP CONSTRAINT oa_derived_object_object_status_check;
ALTER TABLE oa_derived_object ADD CONSTRAINT oa_derived_object_object_status_check
    CHECK (object_status IN ('active', 'superseded', 'failed', 'rejected'));
