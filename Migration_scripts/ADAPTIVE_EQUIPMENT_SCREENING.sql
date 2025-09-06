SET SERVEROUTPUT ON;

DECLARE

  v_filter_pims NUMBER := 11205589;
  v_rows        NUMBER;
BEGIN
  MERGE INTO LOC_DATA.ADAPTIVE_EQUIPMENT_SCREENING tgt
  USING (
    SELECT
        a.assessment_id,
        a.pims_assessment_number,
        aae.item_comments                                            AS comments,
    
        FROM_TZ(CAST(ab.create_date      AS TIMESTAMP), SESSIONTIMEZONE) AS created_date,
        FROM_TZ(CAST(aae.last_update_time AS TIMESTAMP), SESSIONTIMEZONE) AS updated_date,
      
        SUBSTR(NVL(TO_CHAR(ab.created_by), aae.check_out_worker), 1, 100)      AS created_by,
        SUBSTR(NVL(aae.last_update_worker, TO_CHAR(ab.created_by)), 1, 100)    AS updated_by
    FROM STG_ODA.DME_AAE         aae
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = aae.assessment_number
    JOIN LOC_DATA.ASSESSMENT     a
      ON a.pims_assessment_number = aae.assessment_number
    WHERE (v_filter_pims IS NULL OR ab.assessment_number = v_filter_pims)
  ) src
  ON (tgt.assessment_id = src.assessment_id)   
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      pims_assessment_number,
      comments,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.pims_assessment_number,
      src.comments,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Rows inserted (MERGE): '||v_rows);
END;
/
