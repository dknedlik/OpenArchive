DELETE FROM oa_derived_object_embedding;

ALTER TABLE oa_derived_object_embedding
    ALTER COLUMN embedding TYPE VECTOR(3072);
