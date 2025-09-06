SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_NEEDS_ADAPTIVE_EQUIPMENT_DETAILS_XREF tgt
  USING (
    SELECT
      a.assessment_id                                         AS assessment_id,
      sub.equipment_subtype_id                                AS needs_equipment_subtype_id,
      CASE
        WHEN UPPER(TRIM(ab.rss_stairs_ramp_needed_ind)) IN ('Y','YES','1','TRUE','T')
          THEN 'Y'
        ELSE 'N'
      END                                                     AS needs_additional_equipment,
      /* audit from assessment */
      a.is_active                                             AS is_active,
      a.created_date                                          AS created_date,
      a.updated_date                                          AS updated_date,
      a.created_by                                            AS created_by,
      a.updated_by                                            AS updated_by
    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    JOIN LOC_DATA.ADAPTIVE_EQUIPMENT_SUBTYPE sub
      ON UPPER(sub.equipment_subtype_name) = 'STAIR'
    WHERE ab.rss_stairs_ramp_needed_ind IS NOT NULL
    /* Optional: test a single assessment
       AND ab.assessment_number = 36943224
    */
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.needs_equipment_subtype_id = src.needs_equipment_subtype_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.needs_additional_equipment = src.needs_additional_equipment,
      tgt.is_active                  = src.is_active,
      tgt.created_date               = src.created_date,
      tgt.updated_date               = src.updated_date,
      tgt.created_by                 = src.created_by,
      tgt.updated_by                 = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      needs_equipment_subtype_id,
      needs_additional_equipment,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.needs_equipment_subtype_id,
      src.needs_additional_equipment,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_NEEDS_ADAPTIVE_EQUIPMENT_DETAILS_XREF rows merged: '||v_rows);
END;
/
