ALTER TABLE oa_derived_object_embedding
    DROP CONSTRAINT IF EXISTS oa_derived_object_embedding_derived_object_type_check;

ALTER TABLE oa_derived_object_embedding
    ADD CONSTRAINT oa_derived_object_embedding_derived_object_type_check
    CHECK (
        derived_object_type IN (
            'summary',
            'classification',
            'memory',
            'relationship',
            'entity'
        )
    );
