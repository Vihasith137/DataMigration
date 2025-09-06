SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows_diag  PLS_INTEGER := 0;

BEGIN
  /* ---------- DIAGNOSE ---------- */
  MERGE INTO LOC_DATA.DIAGNOSE tgt
  USING (
    SELECT
      a.assessment_id                                   AS assessment_id,
      ab.diagnosis_comment                              AS comments,
      CASE WHEN ab.diagnosis_comment IS NOT NULL
           THEN 'Y' END                                 AS is_individual_have_any_diagnosis,
      a.is_active                                       AS is_active,
      a.created_date                                    AS created_date,
      a.updated_date                                    AS updated_date,
      a.created_by                                      AS created_by,
      a.updated_by                                      AS updated_by
    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
  ) src
  ON (tgt.assessment_id = src.assessment_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.comments                          = src.comments,
      tgt.is_individual_have_any_diagnosis  = src.is_individual_have_any_diagnosis,
      tgt.is_active                         = src.is_active,
      tgt.created_date                      = src.created_date,
      tgt.updated_date                      = src.updated_date,
      tgt.created_by                        = src.created_by,
      tgt.updated_by                        = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      comments,
      is_individual_have_any_diagnosis,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.comments,
      src.is_individual_have_any_diagnosis,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows_diag := SQL%ROWCOUNT;

 

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('DIAGNOSE merged: '||v_rows_diag);

END;
/
