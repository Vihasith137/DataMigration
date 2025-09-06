SET SERVEROUTPUT ON;

DECLARE
  v_rows_adl  PLS_INTEGER := 0;

BEGIN
  MERGE INTO LOC_DATA.ADL_ASSESSMENT tgt
  USING (
    SELECT
      a.assessment_id                                                AS assessment_id,  
      sa.last_update_time                                            AS updated_date,
      sa.last_update_worker                                          AS updated_by,
      SUBSTR(sa.check_out_worker, 1, 100)                            AS created_by,
      CASE
        WHEN UPPER(TRIM(sa.help_needed)) IN ('Y','YES','1','TRUE','T') THEN 'Y'
        WHEN UPPER(TRIM(sa.help_needed)) IN ('N','NO','0','FALSE','F') THEN 'N'
        ELSE 'N'
      END                                                            AS needs_additional_assistance,
      CASE
        WHEN UPPER(sa.scored_activity_description) LIKE '%BATH%'  THEN 1
        WHEN UPPER(sa.scored_activity_description) LIKE '%DRESS%' THEN 2
        WHEN UPPER(sa.scored_activity_description) LIKE '%GROOM%' THEN 3
        WHEN UPPER(sa.scored_activity_description) LIKE '%MOBIL%' THEN 4
        WHEN UPPER(sa.scored_activity_description) LIKE '%EAT%'   THEN 5
        ELSE 6
      END                                                            AS adl_type_id,
      sa.scored_activity_score                                       AS adl_assistance_level_id
    FROM STG_ODA.SCORED_ACTIVITY sa
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = sa.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE UPPER(TRIM(sa.scored_activity_type)) = 'ADL'
      AND sa.assessment_number = 36943224
  ) src
    ON (tgt.assessment_id = src.assessment_id AND tgt.adl_type_id = src.adl_type_id)
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      updated_date,
      updated_by,
      created_by,
     needs_additional_assistance,
      adl_type_id,
      adl_assistance_level_id
    )
    VALUES (
      src.assessment_id,
      src.updated_date,
      src.updated_by,
      src.created_by,
   src.needs_additional_assistance,
      src.adl_type_id,
      src.adl_assistance_level_id
    );

  v_rows_adl := SQL%ROWCOUNT;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ADL inserted:  '||v_rows_adl);
 
END;
/
