SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_VISION_SUPPORT_XREF tgt
  USING (
    SELECT
      a.assessment_id,
      CASE 
        WHEN d.condition_name = 34 THEN 
             (SELECT vision_support_id 
                FROM LOC_DATA.VISION_SUPPORT 
               WHERE UPPER(TRIM(vision_support)) = 'HAS CORRECTIVE LENSES')
        WHEN d.condition_name = 33 THEN 
             (SELECT vision_support_id 
                FROM LOC_DATA.VISION_SUPPORT 
               WHERE UPPER(TRIM(vision_support)) = 'USES VISUAL AID(S)')
        ELSE NULL
      END AS vision_support_id,
      a.is_active,
      a.created_date,
      a.updated_date,
      a.created_by,
      a.updated_by
    FROM STG_ODA.ASSESSMENT_COND_DET d
    JOIN STG_ODA.ASSESSMENT_COND_SYS s
      ON d.assessment_cond_sys_id = s.assessment_cond_sys_id
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON s.assessment_number = ab.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON ab.assessment_number = a.pims_assessment_number
    WHERE d.condition_name IN (33, 34)
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.vision_support_id = src.vision_support_id)
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
      vision_support_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.vision_support_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_VISION_SUPPORT_XREF rows merged: ' || v_rows);
END;
/
