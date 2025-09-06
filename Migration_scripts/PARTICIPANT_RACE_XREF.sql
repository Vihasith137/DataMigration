SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO PDS_DATA.PARTICIPANT_RACE_XREF tgt
  USING (
    SELECT DISTINCT
           p.participant_id,
           rx.race_id,                    -- <-- pulled from PDS_DATA.RACE
           p.is_active,
           p.created_date,
           p.updated_date,
           p.created_by,
           p.updated_by
    FROM STG_ODA.RACE r
    JOIN PDS_DATA.PARTICIPANT p
      ON p.pims_client_number = r.client_number
    /* Map source text to a canonical race name, then join to PDS_DATA.RACE */
    JOIN PDS_DATA.RACE rx
      ON UPPER(rx.race_name) =
         UPPER(
           CASE
             WHEN TRIM(r.race) IS NULL OR r.race IS NULL THEN 'Other'
             WHEN REGEXP_LIKE(r.race, '^\s*Asian\s*$', 'i') THEN 'Asian'
             WHEN REGEXP_LIKE(r.race, '^\s*Black(\s+or\s+African\s+American)?\s*$', 'i')
               THEN 'Black or African American'
             WHEN REGEXP_LIKE(r.race, '(American\s+Indian|Alaskan\s+Native|Native\s+American)', 'i')
               THEN 'American Indian or Alaskan Native'
             WHEN REGEXP_LIKE(r.race, '(Native\s+Hawaiian|Pacific)', 'i')
               THEN 'Native Hawaiian or Other Pacific Islander'
             WHEN REGEXP_LIKE(r.race, '^\s*White\s*$', 'i') THEN 'White'
             ELSE 'Other'
           END
         )
    /* Optional: test a subset
       WHERE r.client_number = :one_client */
  ) src
  ON (tgt.participant_id = src.participant_id
      AND tgt.race_id      = src.race_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (participant_id, race_id, is_active, created_date, updated_date, created_by, updated_by)
    VALUES (src.participant_id, src.race_id, src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by);

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('PARTICIPANT_RACE_XREF rows merged: ' || v_rows);
END;
/
