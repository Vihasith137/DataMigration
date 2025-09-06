SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_CAREGIVER_SUPPORT_NEED_XREF tgt
  USING (
    WITH mapped AS (
      SELECT
          a.assessment_id,
          cb.care_giver_id                                           AS caregiver_id,
          CASE
            WHEN UPPER(TRIM(cs.survey_question)) LIKE '%KNOWLEDGE%'   THEN 13
            WHEN UPPER(TRIM(cs.survey_question)) LIKE '%MY HEALTH%'   THEN 1
            WHEN UPPER(TRIM(cs.survey_question)) LIKE '%NO REST%'     THEN 1
            WHEN UPPER(TRIM(cs.survey_question)) LIKE '%TIME OTHER%'  THEN 1
            WHEN UPPER(TRIM(cs.survey_question)) LIKE '%TIME SELF%'   THEN 1
            ELSE NULL
          END                                                         AS support_need_id,
          a.is_active,
          a.created_date,
          a.updated_date,
          a.created_by,
          a.updated_by
      FROM STG_ODA.CAREGIVER_SURVEY cs
      JOIN STG_ODA.CARE_GIVER_BASE cb
        ON cb.care_giver_id = cs.care_giver_id
      JOIN STG_ODA.ASSESSMENT_BASE ab
        ON ab.assessment_number = cb.assessment_number
      JOIN LOC_DATA.ASSESSMENT a
        ON a.pims_assessment_number = ab.assessment_number
    )
    SELECT *
    FROM mapped
    WHERE support_need_id IS NOT NULL
  ) src
  ON (    tgt.assessment_id   = src.assessment_id
      AND tgt.caregiver_id    = src.caregiver_id
      AND tgt.support_need_id = src.support_need_id)
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
      caregiver_id,
      support_need_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.caregiver_id,
      src.support_need_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_CAREGIVER_SUPPORT_NEED_XREF rows merged: ' || v_rows);
END;
/
