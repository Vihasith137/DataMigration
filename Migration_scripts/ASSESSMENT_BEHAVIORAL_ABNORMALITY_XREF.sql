SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_BEHAVIORAL_ABNORMALITY_XREF tgt
  USING (
    WITH base AS (
      SELECT
        a.assessment_id,
        mc.schizophrenic_disorder_ind,
        mc.delusional_disorder_ind,
        mc.secondary_dementia_ind,
        mc.walk_get_around_ind,
        mc.severe_anxiety_disorder_ind,
        mc.primary_dementia_ind,
        mc.mood_disorder_ind,
        a.is_active,
        a.created_date,
        a.updated_date,
        a.created_by,
        a.updated_by
      FROM STG_ODA.MENTAL_CONDITION mc
      JOIN STG_ODA.ASSESSMENT_BASE ab
        ON ab.assessment_number = mc.assessment_number
      JOIN LOC_DATA.ASSESSMENT a
        ON a.pims_assessment_number = ab.assessment_number

    ),
    flagged AS (
      SELECT assessment_id, 1 AS abnormality_id,
             is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(schizophrenic_disorder_ind,'N'))) = 'Y'
      UNION ALL
      SELECT assessment_id, 2, is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(delusional_disorder_ind,'N'))) = 'Y'
      UNION ALL
      SELECT assessment_id, 3, is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(secondary_dementia_ind,'N'))) = 'Y'
      UNION ALL
      SELECT assessment_id, 4, is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(walk_get_around_ind,'N'))) = 'Y'
      UNION ALL
      SELECT assessment_id, 5, is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(severe_anxiety_disorder_ind,'N'))) = 'Y'
      UNION ALL
      SELECT assessment_id, 6, is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(primary_dementia_ind,'N'))) = 'Y'
      UNION ALL
      SELECT assessment_id, 7, is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(mood_disorder_ind,'N'))) = 'Y'
    )
    SELECT DISTINCT * FROM flagged
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.abnormality_id = src.abnormality_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      abnormality_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.abnormality_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('Rows merged into BEHAVIORAL_ABNORMALITY_XREF: '|| v_rows);
END;
/
