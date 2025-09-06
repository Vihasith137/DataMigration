SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.DEVELOPMENTAL_DISABILITY_SCREENING tgt
  USING (
    SELECT
      a.assessment_id                                        AS assessment_id,
      case when upper(trim(mc.closely_related_mr_cond_ind)) in ('Y') then 'Y'
      else 'N'
     END                      AS has_physical_mental_disability,
     case when upper(trim(mc.currently_served_by_mrdd_ind)) in ('Y') then 'Y'
      else 'N'
     END      AS received_dd_services,
     case when upper(trim(mc.diagnosis_of_mr_ind)) in ('Y') then 'Y'
      else 'N'
     END     AS indications_of_developmental_disability,
     case when upper(trim(mc.disability_beforE22_ind)) in ('Y') then 'Y'
      else 'N'
     END       AS manifested_before_age_22,
     case when upper(trim(mc.disability_cont_indefinite_ind)) in ('Y') then 'Y'
      else 'N'
     END     AS condition_continues_indefinitely,
     case when upper(trim(mc.functional_limitations_ind)) in ('Y') then 'Y'
      else 'N'
     END      AS has_functional_impairments,
      a.is_active                                            AS is_active,
      a.created_date                                         AS created_date,
      a.updated_date                                         AS updated_date,
      a.created_by                                           AS created_by,
      a.updated_by                                           AS updated_by
    FROM STG_ODA.MENTAL_CONDITION mc
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = mc.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE mc.assessment_number IS NOT NULL
  ) src
  ON (tgt.assessment_id = src.assessment_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.has_physical_mental_disability     = src.has_physical_mental_disability,
      tgt.received_dd_services               = src.received_dd_services,
      tgt.indications_of_developmental_disability = src.indications_of_developmental_disability,
      tgt.manifested_before_age_22           = src.manifested_before_age_22,
      tgt.condition_continues_indefinitely   = src.condition_continues_indefinitely,
      tgt.has_functional_impairments         = src.has_functional_impairments,
      tgt.is_active                          = src.is_active,
      tgt.created_date                       = src.created_date,
      tgt.updated_date                       = src.updated_date,
      tgt.created_by                         = src.created_by,
      tgt.updated_by                         = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      has_physical_mental_disability,
      received_dd_services,
      indications_of_developmental_disability,
      manifested_before_age_22,
      condition_continues_indefinitely,
      has_functional_impairments,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.has_physical_mental_disability,
      src.received_dd_services,
      src.indications_of_developmental_disability,
      src.manifested_before_age_22,
      src.condition_continues_indefinitely,
      src.has_functional_impairments,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('DEVELOPMENTAL_DISABILITY_SCREENING rows merged: ' || v_rows);
END;
/
