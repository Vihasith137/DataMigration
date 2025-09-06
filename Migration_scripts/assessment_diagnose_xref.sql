SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_DIAGNOSE_XREF tgt
  USING (
    WITH dl_dedup AS (      
      SELECT diagnosis_code,
             MIN(diagnosis_list_id) AS diagnosis_list_id
      FROM   LOC_DATA.DIAGNOSIS_LIST
      WHERE  diagnosis_code IS NOT NULL
      GROUP  BY diagnosis_code
    ),
    base AS (                
      SELECT a.assessment_id,
             cd.diagnosis_code,
             a.is_active,
             a.created_date,
             a.updated_date,
             a.created_by,
             a.updated_by
      FROM   STG_ODA.CLIENT_DIAGNOSIS cd
      JOIN   STG_ODA.ASSESSMENT_BASE ab
             ON ab.assessment_number = cd.assessment_number
      JOIN   LOC_DATA.ASSESSMENT a
             ON a.pims_assessment_number = ab.assessment_number
      WHERE  cd.diagnosis_code IS NOT NULL
    ),
    pairs AS (               
      SELECT DISTINCT
             b.assessment_id,
             d.diagnosis_list_id,
             b.is_active,
             b.created_date,
             b.updated_date,
             b.created_by,
             b.updated_by
      FROM   base b
      JOIN   dl_dedup d
             ON d.diagnosis_code = b.diagnosis_code
    )
    SELECT *
    FROM   pairs
  ) src
  ON (tgt.assessment_id     = src.assessment_id
      AND tgt.diagnosis_list_id = src.diagnosis_list_id)

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
      diagnosis_list_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.diagnosis_list_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_DIAGNOSE_XREF rows merged: '|| v_rows);
END;
/
