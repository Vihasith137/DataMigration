SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows_denture   PLS_INTEGER := 0;
  v_rows_oral      PLS_INTEGER := 0;
  v_rows_eating    PLS_INTEGER := 0;
BEGIN

  MERGE INTO LOC_DATA.ASSESSMENT_DENTURE_TYPE_XREF tgt
  USING (
    SELECT DISTINCT
           a.assessment_id,
           CASE TO_NUMBER(REGEXP_SUBSTR(TRIM(det.condition_name), '^\d+'))
             WHEN 49 THEN 2  -- Upper Full
             WHEN 50 THEN 3  -- Lower Full
           END AS denture_id,
           a.is_active,
           a.created_date,
           a.updated_date,
           a.created_by,
           a.updated_by
    FROM STG_ODA.ASSESSMENT_COND_DET det
    JOIN STG_ODA.ASSESSMENT_COND_SYS sys
      ON sys.assessment_cond_sys_id = det.assessment_cond_sys_id
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = sys.assessment_number
    WHERE TO_NUMBER(REGEXP_SUBSTR(TRIM(det.condition_name), '^\d+')) IN (49,50)

  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.denture_id = src.denture_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (assessment_id, denture_id, is_active, created_date, updated_date, created_by, updated_by)
    VALUES (src.assessment_id, src.denture_id, src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by);

  v_rows_denture := SQL%ROWCOUNT;


  MERGE INTO LOC_DATA.ASSESSMENT_ORAL_HEALTH_CONDITION_XREF tgt
  USING (
    SELECT DISTINCT
           a.assessment_id,
           CASE TO_NUMBER(REGEXP_SUBSTR(TRIM(det.condition_name), '^\d+'))
             WHEN 55 THEN 1
             WHEN 56 THEN 2
             WHEN 58 THEN 3
           END AS condition_id,
           a.is_active,
           a.created_date,
           a.updated_date,
           a.created_by,
           a.updated_by
    FROM STG_ODA.ASSESSMENT_COND_DET det
    JOIN STG_ODA.ASSESSMENT_COND_SYS sys
      ON sys.assessment_cond_sys_id = det.assessment_cond_sys_id
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = sys.assessment_number
    WHERE TO_NUMBER(REGEXP_SUBSTR(TRIM(det.condition_name), '^\d+')) IN (55,56,58)

  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.condition_id = src.condition_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (assessment_id, condition_id, is_active, created_date, updated_date, created_by, updated_by)
    VALUES (src.assessment_id, src.condition_id, src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by);

  v_rows_oral := SQL%ROWCOUNT;


  MERGE INTO LOC_DATA.ASSESSMENT_EATING_DIFFICULTY_ISSUE_XREF tgt
  USING (
    SELECT DISTINCT
           a.assessment_id,
           CASE TO_NUMBER(REGEXP_SUBSTR(TRIM(det.condition_name), '^\d+'))
             WHEN 53 THEN 2  -- Chewing
             WHEN 54 THEN 3  -- Swallowing
           END AS eating_difficulty_id,
           a.is_active,
           a.created_date,
           a.updated_date,
           a.created_by,
           a.updated_by
    FROM STG_ODA.ASSESSMENT_COND_DET det
    JOIN STG_ODA.ASSESSMENT_COND_SYS sys
      ON sys.assessment_cond_sys_id = det.assessment_cond_sys_id
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = sys.assessment_number
    WHERE TO_NUMBER(REGEXP_SUBSTR(TRIM(det.condition_name), '^\d+')) IN (53,54)

  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.eating_difficulty_id = src.eating_difficulty_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (assessment_id, eating_difficulty_id, is_active, created_date, updated_date, created_by, updated_by)
    VALUES (src.assessment_id, src.eating_difficulty_id, src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by);

  v_rows_eating := SQL%ROWCOUNT;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_DENTURE_TYPE_XREF rows merged: '||v_rows_denture);
  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_ORAL_HEALTH_CONDITION_XREF rows merged: '||v_rows_oral);
  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_EATING_DIFFICULTY_ISSUE_XREF rows merged: '||v_rows_eating);
END;
/
