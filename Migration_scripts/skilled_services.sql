SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_SKILLED_SERVICE tgt
  USING (
    SELECT
      a.assessment_id                               AS assessment_id,

      /* Flags */
      CASE
        WHEN MAX(CASE WHEN UPPER(TRIM(ss.skilled_service_type)) = 'SNS' THEN 1 ELSE 0 END) = 1
        THEN 'Y' ELSE 'N'
      END                                           AS skilled_nursing_needs,
      CASE
        WHEN MAX(CASE WHEN UPPER(TRIM(ss.skilled_service_type)) = 'SRS' THEN 1 ELSE 0 END) = 1
        THEN 'Y' ELSE 'N'
      END                                           AS skilled_therapy_needs,

      /* Comments */
      MAX(CASE WHEN UPPER(TRIM(ss.skilled_service_type)) = 'SNS'
               THEN ss.service_freq END)            AS comments_skilled_nursing_needs,
      MAX(CASE WHEN UPPER(TRIM(ss.skilled_service_type)) = 'SRS'
               THEN ss.service_freq END)            AS comments_skilled_therapy_needs,

      /* Audit from assessment */
      a.is_active                                   AS is_active,
      a.created_date                                AS created_date,
      a.created_by                                  AS created_by,
      a.updated_by                                  AS updated_by
    FROM STG_ODA.SKILLED_SERVICE ss
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = ss.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE ss.assessment_number = 77374014
    GROUP BY a.assessment_id,
           
             a.is_active,
             a.created_date,
             a.created_by,
             a.updated_by
  ) src
  ON (
    tgt.assessment_id     = src.assessment_id
    
  )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.skilled_nursing_needs            = src.skilled_nursing_needs,
      tgt.skilled_therapy_needs            = src.skilled_therapy_needs,
      tgt.comments_skilled_nursing_needs   = src.comments_skilled_nursing_needs,
      tgt.comments_skilled_therapy_needs   = src.comments_skilled_therapy_needs,
      tgt.is_active                        = src.is_active,
      tgt.created_date                     = src.created_date,
      tgt.created_by                       = src.created_by,
      tgt.updated_by                       = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      skilled_nursing_needs,
      skilled_therapy_needs,
      comments_skilled_nursing_needs,
      comments_skilled_therapy_needs,
      is_active,
      created_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.skilled_nursing_needs,
      src.skilled_therapy_needs,
      src.comments_skilled_nursing_needs,
      src.comments_skilled_therapy_needs,
      src.is_active,
      src.created_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_SKILLED_SERVICE rows merged: '||v_rows);
END;
/
