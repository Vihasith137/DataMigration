SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.MEDICAL_VISIT tgt
  USING (
    SELECT
      a.assessment_id                                                                              AS assessment_id,


      CASE
        WHEN ab.hosp_admin_date IS NOT NULL THEN 3
        WHEN ab.nf_admin_date   IS NOT NULL THEN 5
        ELSE 2
      END                                                                                          AS visit_type_id,


      CASE
        WHEN ab.hosp_admin_date IS NOT NULL THEN ab.recent_hospitalization_comment
        ELSE NULL
      END                                                                                          AS visit_reason,
      CASE
        WHEN ab.nf_admin_date IS NOT NULL THEN ab.recent_nf_comment
        ELSE NULL
      END                                                                                          AS facility_name_city,

      CASE
        WHEN ab.hosp_admin_date IS NOT NULL THEN ab.hosp_admin_date
        WHEN ab.nf_admin_date   IS NOT NULL THEN ab.nf_admin_date
        ELSE NULL
      END                                                                                          AS last_admission_date,
      CASE
        WHEN ab.hosp_admin_date IS NOT NULL THEN ab.hosp_disch_date
        WHEN ab.nf_admin_date   IS NOT NULL THEN ab.nf_disch_date
        ELSE NULL
      END                                                                                          AS last_discharge_date,
      CASE
        WHEN ab.hosp_admin_date IS NOT NULL THEN ab.past_year_hosp_admins
        WHEN ab.nf_admin_date   IS NOT NULL THEN ab.past_year_nf_admins
        ELSE NULL
      END                                                                                          AS number_of_visits_last_12_months,

      a.is_active                                                                                 AS is_active,
      a.created_date                                                                              AS created_date,
      a.updated_date                                                                              AS updated_date,
      a.created_by                                                                                AS created_by,
      a.updated_by                                                                                AS updated_by

    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number   -- same “assessment_id” derivation as before
    WHERE ab.assessment_number = 11937993
    
  ) src
  ON (
    tgt.assessment_id = src.assessment_id
    AND tgt.visit_type_id = src.visit_type_id   
  )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.visit_reason                       = src.visit_reason,
      tgt.facility_name_city                 = src.facility_name_city,
      tgt.last_admission_date                = src.last_admission_date,
      tgt.last_discharge_date                = src.last_discharge_date,
      tgt.number_of_visits_last_12_months    = src.number_of_visits_last_12_months,
      tgt.is_active                          = src.is_active,
      tgt.created_date                       = src.created_date,
      tgt.updated_date                       = src.updated_date,
      tgt.created_by                         = src.created_by,
      tgt.updated_by                         = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      visit_type_id,
      visit_reason,
      facility_name_city,
      last_admission_date,
      last_discharge_date,
      number_of_visits_last_12_months,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.visit_type_id,
      src.visit_reason,
      src.facility_name_city,
      src.last_admission_date,
      src.last_discharge_date,
      src.number_of_visits_last_12_months,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('MEDICAL_VISIT rows merged: '||v_rows);
END;
/
