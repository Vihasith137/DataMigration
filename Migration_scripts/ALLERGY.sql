SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ALLERGY tgt
  USING (
    SELECT
      a.assessment_id                       AS assessment_id,
      NULLIF(TRIM(ab.allergies), '')        AS allergy_name,
      a.is_active                           AS is_active,
      a.created_date                        AS created_date,
      a.updated_date                        AS updated_date,
      a.created_by                          AS created_by,
      a.updated_by                          AS updated_by
    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE TRIM(ab.allergies) IS NOT NULL
  
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.allergy_name = src.allergy_name)

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
      allergy_name,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.allergy_name,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ALLERGY rows merged: '||v_rows);
END;
/
