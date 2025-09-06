SET SERVEROUTPUT ON;

DECLARE
  v_filter_caregiver_id NUMBER := 11606391;  
  v_filter_pims         NUMBER := NULL;      

  v_rows NUMBER;
BEGIN
  MERGE INTO LOC_DATA.CAREGIVER_ASSESSMENT tgt
  USING (
    WITH csd1 AS (
      SELECT care_giver_id, MAX(service_frequency) AS service_frequency
      FROM STG_ODA.CAREGIVER_SERVICE_DET
      GROUP BY care_giver_id
    )
    SELECT
        a.assessment_id,
        a.pims_assessment_number,
        NVL(csd1.care_giver_id, cb.care_giver_id)              AS caregiver_id,

        TRIM(NVL(cb.first_name,'')) ||
        CASE WHEN TRIM(NVL(cb.first_name,'')) IS NOT NULL
               AND TRIM(NVL(cb.last_name,''))  IS NOT NULL THEN ' ' ELSE '' END ||
        TRIM(NVL(cb.last_name,''))                               AS primary_caregiver_name,

        CASE WHEN UPPER(TRIM(cb.primary_caregiver_ind)) IN ('Y','YES','1','TRUE','T') THEN 'Y'
             WHEN UPPER(TRIM(cb.primary_caregiver_ind)) IN ('N','NO','0','FALSE','F')  THEN 'N'
             ELSE 'N'
        END                                                      AS have_primary_caregiver,

        CASE WHEN UPPER(TRIM(cb.education_training_ind)) IN ('Y','YES','1','TRUE','T') THEN 'Y'
             WHEN UPPER(TRIM(cb.education_training_ind)) IN ('N','NO','0','FALSE','F')  THEN 'N'
             ELSE 'N'
        END                                                      AS caregiver_trained,

        CASE
          WHEN UPPER(TRIM(cb.employment)) LIKE '%FULL%TIME%'                      THEN 1  
          WHEN UPPER(TRIM(cb.employment)) LIKE '%PART%TIME%'                      THEN 2  
          WHEN UPPER(TRIM(cb.employment)) LIKE '%UNEMPLOY%' 
            OR  UPPER(TRIM(cb.employment)) LIKE '%NOT EMPLOYED%'                  THEN 3  
          ELSE NULL
        END                                                      AS caregiver_employment_id,

        CASE
          WHEN UPPER(TRIM(csd1.service_frequency)) LIKE '%DAILY%'                  THEN 1
          WHEN UPPER(TRIM(csd1.service_frequency)) LIKE '%WEEKLY%'                 THEN 2
          WHEN UPPER(TRIM(csd1.service_frequency)) LIKE '%MONTHLY%'                THEN 3
          WHEN UPPER(TRIM(csd1.service_frequency)) LIKE '%LESS%' 
            OR  UPPER(TRIM(csd1.service_frequency)) LIKE '%ONCE PER MONTH%'        THEN 4
          ELSE NULL
        END                                                      AS caregiving_frequency_id,

        cb.time_as_caregiver                                      AS caregiving_duration_month,
        cb.caregiver_comment                                      AS comments,
        cb.cg_comments                                            AS other_details
    FROM STG_ODA.CARE_GIVER_BASE cb
    LEFT JOIN csd1
           ON csd1.care_giver_id = cb.care_giver_id
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = cb.assessment_number
    WHERE (v_filter_caregiver_id IS NULL OR cb.care_giver_id      = v_filter_caregiver_id)
      AND (v_filter_pims         IS NULL OR cb.assessment_number  = v_filter_pims)
  ) src
  ON (tgt.assessment_id = src.assessment_id AND tgt.caregiver_id = src.caregiver_id)
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      pims_assessment_number,
      primary_caregiver_name,
      have_primary_caregiver,
      caregiver_trained,
      caregiver_employment_id,
      caregiving_frequency_id,
      caregiving_duration_month,
      comments,
      other_details
    )
    VALUES (
      src.assessment_id,
      src.pims_assessment_number,
      src.primary_caregiver_name,
      src.have_primary_caregiver,
      src.caregiver_trained,
      src.caregiver_employment_id,
      src.caregiving_frequency_id,
      src.caregiving_duration_month,
      src.comments,
      src.other_details
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('CAREGIVER rows inserted (MERGE count): '||v_rows);
END;
/

