SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_DIETARY_DISORDER_XREF tgt
  USING (
    SELECT DISTINCT
      a.assessment_id,
      CASE
        WHEN n.nutrition_question_number IN (1,2,3,4) 
             AND UPPER(TRIM(n.nutrition_question_answer)) = 'Y'
        THEN (SELECT dietary_disorder_id 
                FROM LOC_DATA.DIETARY_DISORDER 
               WHERE UPPER(dietary_disorder_name) = 'DIAGNOSED EATING DISORDER')
        WHEN n.nutrition_question_number IN (5,6,7,8,9,10) 
             AND UPPER(TRIM(n.nutrition_question_answer)) = 'Y'
        THEN (SELECT dietary_disorder_id 
                FROM LOC_DATA.DIETARY_DISORDER 
               WHERE UPPER(dietary_disorder_name) = 'N/A')
        ELSE NULL
      END AS dietary_disorder_id,
      a.is_active,
      a.created_date,
      a.updated_date,
      a.created_by,
      a.updated_by
    FROM STG_ODA.NUTRITION_STATUS n
    JOIN LOC_DATA.ASSESSMENT a
      ON n.assessment_number = a.pims_assessment_number
    WHERE UPPER(TRIM(n.nutrition_question_answer)) = 'Y'
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.dietary_disorder_id = src.dietary_disorder_id)
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
      dietary_disorder_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.dietary_disorder_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_DIETARY_DISORDER_XREF rows merged: ' || v_rows);
END;
/
