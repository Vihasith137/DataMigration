SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.BEHAVIORAL_ABNORMALITIES_SCREENING tgt
  USING (
    SELECT
      a.assessment_id AS assessment_id,
      CASE
        WHEN UPPER(TRIM(ab.behavior_abnormality_ind)) IN ('Y','YES','1','TRUE','T') THEN 'Y'
        ELSE 'N'
      END                        AS has_behavioral_abnormalities,

      /* Audit from assessment */
      a.is_active               AS is_active,
      a.created_date            AS created_date,
      a.updated_date            AS updated_date,
      a.created_by              AS created_by,
      a.updated_by              AS updated_by
    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
   -- WHERE ab.assessment_number = 36943224   -- <<< change to your test assessment_number
  ) src
  ON (tgt.assessment_id = src.assessment_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.has_behavioral_abnormalities = src.has_behavioral_abnormalities,
      tgt.is_active                    = src.is_active,
      tgt.created_date                 = src.created_date,
      tgt.updated_date                 = src.updated_date,
      tgt.created_by                   = src.created_by,
      tgt.updated_by                   = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      has_behavioral_abnormalities,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.has_behavioral_abnormalities,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('BEHAVIORAL_ABNORMALITIES_SCREENING rows merged: '||v_rows);
END;
/
