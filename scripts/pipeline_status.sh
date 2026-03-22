#!/usr/bin/env bash
# Quick pipeline status dashboard — run from host while docker is up.
# Usage: ./scripts/pipeline_status.sh

set -euo pipefail

DB_URL="${OA_TEST_POSTGRES_URL:-postgres://openarchive:openarchive@127.0.0.1:5432/openarchive}"

psql "$DB_URL" --no-psqlrc -P pager=off <<'SQL'
\echo ''
\echo '=== IMPORTS ==='
SELECT import_status, count(*) AS n
  FROM oa_import
 GROUP BY import_status
 ORDER BY import_status;

\echo ''
\echo '=== ARTIFACTS ==='
SELECT enrichment_status, count(*) AS n
  FROM oa_artifact
 GROUP BY enrichment_status
 ORDER BY enrichment_status;

\echo ''
\echo '=== ENRICHMENT PIPELINE ==='
SELECT job_type,
       job_status,
       count(*) AS n
  FROM oa_enrichment_job
 GROUP BY job_type, job_status
 ORDER BY job_type, job_status;

\echo ''
\echo '=== EMBEDDINGS ==='
SELECT derived_object_type,
       embedding_model,
       count(*) AS n
  FROM oa_derived_object_embedding
 GROUP BY derived_object_type, embedding_model
 ORDER BY derived_object_type;

\echo ''
\echo '=== RECENT FAILURES (last 10) ==='
SELECT job_id,
       job_type,
       substring(last_error_message FROM 1 FOR 120) AS error_preview,
       created_at
  FROM oa_enrichment_job
 WHERE job_status = 'failed'
 ORDER BY created_at DESC
 LIMIT 10;
SQL
