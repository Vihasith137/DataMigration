SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows_nutrition   PLS_INTEGER := 0;
  v_rows_resp        PLS_INTEGER := 0;
  v_rows_comm        PLS_INTEGER := 0;
BEGIN

  MERGE INTO LOC_DATA.NUTRITION_MASTER tgt
  USING (
    SELECT
      a.assessment_id,
      CASE
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%FEEDINGPUMP%'    THEN 1
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%GLUCOSEMETER%'  THEN 2
        ELSE 3
      END AS nutrition_id,
      'Y' AS has_nutrition_equipment,
      'Y' AS needs_nutrition_equipment,
      MAX(NVL(a.is_active,1))                              AS is_active,
      MIN(NVL(a.created_date, SYSTIMESTAMP))               AS created_date,
      MAX(NVL(a.updated_date, SYSTIMESTAMP))               AS updated_date,
      MIN(NVL(a.created_by, USER))                         AS created_by,
      MAX(NVL(a.updated_by, USER))                         AS updated_by
    FROM STG_ODA.DME_AAE d
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = d.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    GROUP BY
      a.assessment_id,
      CASE
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%FEEDINGPUMP%'    THEN 1
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%GLUCOSEMETER%'  THEN 2
        ELSE 3
      END
  ) src
  ON (   tgt.assessment_id   = src.assessment_id
     AND tgt.has_nutrition_id = src.nutrition_id )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.has_nutrition_equipment  = src.has_nutrition_equipment,
      tgt.needs_nutrition_equipment= src.needs_nutrition_equipment,
      tgt.is_active                = src.is_active,
      tgt.updated_date             = src.updated_date,
      tgt.updated_by               = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      needs_nutrition_equipment,
      has_nutrition_equipment,
      needs_nutrition_id,
      has_nutrition_id,
      ownership_id, condition_id, funding_id,
      equipment_company, equipment_age, has_other_nutrition, needs_other_nutrition, other_funding_source,
      is_active, created_date, updated_date, created_by, updated_by
    )
    VALUES (
      src.assessment_id,
      src.needs_nutrition_equipment,
      src.has_nutrition_equipment,
      src.nutrition_id,
      src.nutrition_id,
      NULL, NULL, NULL,
      NULL, NULL, NULL, NULL, NULL,
      src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by
    );

  v_rows_nutrition := SQL%ROWCOUNT;


  MERGE INTO LOC_DATA.RESPIRATORY_EQUIPMENT_MASTER tgt
  USING (
    SELECT
      a.assessment_id,
      CASE
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%SUCTIONMACHINE%'  THEN 3
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%PULSEOXI%'        THEN 4
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%APNEAMONITOR%'    THEN 5
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%CPAP%'            THEN 6
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%COOLMIST%'        THEN 7
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%NEBULIZER%'       THEN 8
        ELSE 9 /* Other */
      END AS respiratory_equipment_id,
      MAX(NVL(a.is_active,1))                              AS is_active,
      MIN(NVL(a.created_date, SYSTIMESTAMP))               AS created_date,
      MAX(NVL(a.updated_date, SYSTIMESTAMP))               AS updated_date,
      MIN(NVL(a.created_by, USER))                         AS created_by,
      MAX(NVL(a.updated_by, USER))                         AS updated_by
    FROM STG_ODA.DME_AAE d
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = d.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    GROUP BY
      a.assessment_id,
      CASE
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%SUCTIONMACHINE%'  THEN 3
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%PULSEOXI%'        THEN 4
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%APNEAMONITOR%'    THEN 5
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%CPAP%'            THEN 6
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%COOLMIST%'        THEN 7
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%NEBULIZER%'       THEN 8
        ELSE 9
      END
  ) src
  ON (   tgt.assessment_id                 = src.assessment_id
     AND tgt.has_respiratory_equipment_id = src.respiratory_equipment_id )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active     = src.is_active,
      tgt.updated_date  = src.updated_date,
      tgt.updated_by    = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      needs_respiratory_equipment_id,
      has_respiratory_equipment_id,
      ownership_id, condition_id, funding_id,  
      equipment_company, equipment_age,
      has_other_respiratory_equipment, needs_other_respiratory_equipment, other_funding_source,
      is_active, created_date, updated_date, created_by, updated_by
    )
    VALUES (
      src.assessment_id,
      src.respiratory_equipment_id,
      src.respiratory_equipment_id,
      NULL, NULL, NULL,
      NULL, NULL,
      NULL, NULL, NULL,
      src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by
    );

  v_rows_resp := SQL%ROWCOUNT;


  MERGE INTO LOC_DATA.COMMUNICATION_DEVICE_MASTER tgt
  USING (
    SELECT
      a.assessment_id,
      CASE
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%SPEECHGENERATING%'   THEN 1
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%COCHLEAR%'           THEN 2
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%ELECTRONICTABLET%'   THEN 3
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%COMMUNICATIONBOOK%'  THEN 4
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%ASSISTIVELISTENING%' THEN 5
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%HEARINGAID%'         THEN 6
        ELSE 7 /* Other */
      END AS communication_device_id,
      MAX(NVL(a.is_active,1))                              AS is_active,
      MIN(NVL(a.created_date, SYSTIMESTAMP))               AS created_date,
      MAX(NVL(a.updated_date, SYSTIMESTAMP))               AS updated_date,
      MIN(NVL(a.created_by, USER))                         AS created_by,
      MAX(NVL(a.updated_by, USER))                         AS updated_by
    FROM STG_ODA.DME_AAE d
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = d.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    GROUP BY
      a.assessment_id,
      CASE
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%SPEECHGENERATING%'   THEN 1
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%COCHLEAR%'           THEN 2
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%ELECTRONICTABLET%'   THEN 3
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%COMMUNICATIONBOOK%'  THEN 4
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%ASSISTIVELISTENING%' THEN 5
        WHEN REGEXP_REPLACE(UPPER(d.item_name), '\s+', '') LIKE '%HEARINGAID%'         THEN 6
        ELSE 7
      END
  ) src
  ON (   tgt.assessment_id                    = src.assessment_id
     AND tgt.has_communication_device_id     = src.communication_device_id )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active     = src.is_active,
      tgt.updated_date  = src.updated_date,
      tgt.updated_by    = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      needs_communication_device_id,
      has_communication_device_id,
      ownership_id, condition_id, funding_id,  
      has_other_communication_device, needs_other_communication_device,
      equipment_company, equipment_age, other_funding_source,
      is_active, created_date, updated_date, created_by, updated_by
    )
    VALUES (
      src.assessment_id,
      src.communication_device_id,
      src.communication_device_id,
      NULL, NULL, NULL,
      NULL, NULL,
      NULL, NULL, NULL,
      src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by
    );

  v_rows_comm := SQL%ROWCOUNT;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('NUTRITION_MASTER rows merged:   '||v_rows_nutrition);
  DBMS_OUTPUT.PUT_LINE('RESP_EQUIPMENT_MASTER merged:   '||v_rows_resp);
  DBMS_OUTPUT.PUT_LINE('COMM_DEVICE_MASTER merged:      '||v_rows_comm);
END;
/
