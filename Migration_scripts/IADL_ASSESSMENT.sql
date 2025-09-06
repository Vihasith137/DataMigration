SET SERVEROUTPUT ON;

DECLARE
  v_rows_iadl PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.IADL_ASSESSMENT tgt
  USING (
    SELECT
      assessment_id,
      iadl_type_id,
      MAX(updated_date)                AS updated_date,
      MAX(updated_by)                  AS updated_by,
      MAX(created_by)                  AS created_by,
      /* If any row says help-needed, keep 'Y' else 'N' */
      CASE WHEN MAX(CASE WHEN needs_additional_assistance = 'Y' THEN 1 ELSE 0 END) = 1
           THEN 'Y' ELSE 'N' END       AS needs_additional_assistance,
      MAX(iadl_assistance_level_id)    AS iadl_assistance_level_id
    FROM (
      SELECT
        a.assessment_id,
        /* audit fields as-is from the activity rows */
        sa.last_update_time                                        AS updated_date,
        SUBSTR(NVL(sa.last_update_worker, sa.check_out_worker),1,100) AS updated_by,
        SUBSTR(sa.check_out_worker, 1, 100)                        AS created_by,
        CASE
          WHEN UPPER(TRIM(sa.help_needed)) IN ('Y','YES','1','TRUE','T') THEN 'Y'
          ELSE 'N'
        END                                                         AS needs_additional_assistance,
        /* map description -> IADL type */
        CASE
          WHEN UPPER(sa.scored_activity_description) LIKE '%LAUNDRY%'                                         THEN 1
          WHEN UPPER(sa.scored_activity_description) LIKE '%SHOP%'                                             THEN 2
          WHEN UPPER(sa.scored_activity_description) LIKE '%MEAL%'                                             THEN 3
          WHEN UPPER(sa.scored_activity_description) LIKE '%MONEY%'
            OR UPPER(sa.scored_activity_description) LIKE '%FINAN%'
            OR UPPER(sa.scored_activity_description) LIKE '%TRANSPORT%'
            OR UPPER(sa.scored_activity_description) LIKE '%TRAVEL%'                                           THEN 4
          WHEN UPPER(sa.scored_activity_description) LIKE '%CHORES%'
            OR UPPER(sa.scored_activity_description) LIKE '%CLEAN%'                                            THEN 5
          ELSE 6
        END                                                             AS iadl_type_id,
        sa.scored_activity_score                                        AS iadl_assistance_level_id
      FROM STG_ODA.SCORED_ACTIVITY sa
      JOIN STG_ODA.ASSESSMENT_BASE ab
        ON ab.assessment_number = sa.assessment_number
      JOIN LOC_DATA.ASSESSMENT a
        ON a.pims_assessment_number = ab.assessment_number
      WHERE UPPER(TRIM(sa.scored_activity_type)) = 'IADL'
        AND sa.assessment_number = 36943224
    )
    GROUP BY assessment_id, iadl_type_id
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.iadl_type_id = src.iadl_type_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.updated_date                = src.updated_date,
      tgt.updated_by                  = src.updated_by,
      tgt.created_by                  = src.created_by,
      tgt.needs_additional_assistance = src.needs_additional_assistance,
      tgt.iadl_assistance_level_id    = src.iadl_assistance_level_id

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      updated_date,
      updated_by,
      created_by,
      needs_additional_assistance,
      iadl_type_id,
      iadl_assistance_level_id
    )
    VALUES (
      src.assessment_id,
      src.updated_date,
      src.updated_by,
      src.created_by,
      src.needs_additional_assistance,
      src.iadl_type_id,
      src.iadl_assistance_level_id
    );

  v_rows_iadl := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('IADL merged: '||v_rows_iadl);
END;
/
