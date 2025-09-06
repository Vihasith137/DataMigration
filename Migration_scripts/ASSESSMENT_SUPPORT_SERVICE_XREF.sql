SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_SUPPORT_SERVICE_XREF tgt
  USING (
    WITH src0 AS (
      SELECT
      
        a.assessment_id,
      
        s.support_id,
       
        COALESCE(
          s.support_type_id,
          (SELECT st.support_type_id
             FROM LOC_DATA.SUPPORT_TYPE st
            WHERE UPPER(st.support_type_ID) = UPPER(s.support_type_ID)
            FETCH FIRST 1 ROWS ONLY)
        ) AS support_type_id,

        CASE
          WHEN UPPER(TRIM(csd.caregiver_service)) LIKE 'LAUNDRY%' THEN
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'LAUNDRY SERVICES'
              FETCH FIRST 1 ROWS ONLY)
          WHEN UPPER(TRIM(csd.caregiver_service)) LIKE 'MEAL PREP%' THEN
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'MEAL PREPARATION SERVICES'
              FETCH FIRST 1 ROWS ONLY)
          WHEN UPPER(TRIM(csd.caregiver_service)) LIKE 'MONEY MANA%' THEN
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'MANAGING FINANCES'
              FETCH FIRST 1 ROWS ONLY)
          WHEN UPPER(TRIM(csd.caregiver_service)) LIKE 'PERSONAL C%' THEN
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'PERSONAL CARE SERVICES'
              FETCH FIRST 1 ROWS ONLY)
          WHEN UPPER(TRIM(csd.caregiver_service)) LIKE 'RESPITE%' THEN
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'RESPITE'
              FETCH FIRST 1 ROWS ONLY)
          WHEN UPPER(TRIM(csd.caregiver_service)) LIKE 'SHOPPING%' THEN
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'SHOPPING SERVICES'
              FETCH FIRST 1 ROWS ONLY)
          WHEN UPPER(TRIM(csd.caregiver_service)) LIKE 'TRANSPORTA%' THEN
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'TRANSPORTATION SERVICES'
              FETCH FIRST 1 ROWS ONLY)
          ELSE
            (SELECT support_service_id
               FROM LOC_DATA.SUPPORT_SERVICE
              WHERE UPPER(support_service_name) = 'OTHER'
              FETCH FIRST 1 ROWS ONLY)
        END AS support_service_id,

        CASE
          WHEN NOT (
            UPPER(TRIM(csd.caregiver_service)) LIKE 'LAUNDRY%' OR
            UPPER(TRIM(csd.caregiver_service)) LIKE 'MEAL PREP%' OR
            UPPER(TRIM(csd.caregiver_service)) LIKE 'MONEY MANA%' OR
            UPPER(TRIM(csd.caregiver_service)) LIKE 'PERSONAL C%' OR
            UPPER(TRIM(csd.caregiver_service)) LIKE 'RESPITE%' OR
            UPPER(TRIM(csd.caregiver_service)) LIKE 'SHOPPING%' OR
            UPPER(TRIM(csd.caregiver_service)) LIKE 'TRANSPORTA%'
          )
          THEN csd.caregiver_service
          ELSE NULL
        END AS other_support_service,

        /* Audit values from assessment */
        a.is_active,
        a.created_date,
        a.updated_date,
        a.created_by,
        a.updated_by
      FROM STG_ODA.CAREGIVER_SERVICE_DET csd
      JOIN STG_ODA.CARE_GIVER_BASE cb
        ON cb.care_giver_id = csd.care_giver_id
      JOIN LOC_DATA.ASSESSMENT a
        ON a.pims_assessment_number = cb.assessment_number
      LEFT JOIN LOC_DATA.SUPPORT s
        ON s.assessment_id = a.assessment_id
    ),
    src1 AS (
      SELECT *
      FROM src0
      WHERE assessment_id      IS NOT NULL
        AND support_id         IS NOT NULL
        AND support_type_id    IS NOT NULL
        AND support_service_id IS NOT NULL
    )
    SELECT DISTINCT
           assessment_id,
           support_id,
           support_type_id,
           support_service_id,
           other_support_service,
           is_active,
           created_date,
           updated_date,
           created_by,
           updated_by
    FROM src1
  ) src
  ON (    tgt.support_id         = src.support_id
      AND tgt.support_service_id = src.support_service_id
      AND tgt.assessment_id      = src.assessment_id
      AND tgt.support_type_id    = src.support_type_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.other_support_service= src.other_support_service,
      tgt.is_active            = src.is_active,
      tgt.created_date         = src.created_date,
      tgt.updated_date         = src.updated_date,
      tgt.created_by           = src.created_by,
      tgt.updated_by           = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      support_id,
      assessment_id,
      support_type_id,
      support_service_id,
      other_support_service,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.support_id,
      src.assessment_id,
      src.support_type_id,
      src.support_service_id,
      src.other_support_service,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_SUPPORT_SERVICE_XREF rows merged: '||v_rows);
END;
/
