SET SERVEROUTPUT ON SIZE 1000000;

BEGIN
  MERGE INTO LOC_DATA.FALLS_SCREENING tgt
  USING (
    SELECT
      a.assessment_id AS assessment_id,

      MAX(CASE WHEN frs.falls_question_number = '1'  THEN frs.falls_question_answer END) AS age_greater_than_65,
      MAX(CASE WHEN frs.falls_question_number = '2'  THEN frs.falls_question_answer END) AS falling_history_last_year,
      MAX(CASE WHEN frs.falls_question_number = '3'  THEN frs.falls_question_answer END) AS multiple_chronic_conditions,
      MAX(CASE WHEN frs.falls_question_number = '4'  THEN frs.falls_question_answer END) AS incontinence,
      MAX(CASE WHEN frs.falls_question_number = '5'  THEN frs.falls_question_answer END) AS vision_impairment,
      MAX(CASE WHEN frs.falls_question_number = '6'  THEN frs.falls_question_answer END) AS impaired_mobility,
      MAX(CASE WHEN frs.falls_question_number = '7'  THEN frs.falls_question_answer END) AS environmental_hazards,
      MAX(CASE WHEN frs.falls_question_number = '8'  THEN frs.falls_question_answer END) AS medication_risk,
      MAX(CASE WHEN frs.falls_question_number = '9'  THEN frs.falls_question_answer END) AS pain_level_of_function,
      MAX(CASE WHEN frs.falls_question_number = '10' THEN frs.falls_question_answer END) AS cognitive_impairment,
      MAX(CASE WHEN frs.falls_question_number = '11' THEN frs.falls_question_answer END) AS postural_hypotension,
      MAX(CASE WHEN frs.falls_question_number = '12' THEN frs.falls_question_answer END) AS fear_of_falling,

      MAX(frs.last_update_time)   AS updated_date,
      MAX(frs.check_out_worker)   AS created_by,
      MAX(frs.last_update_worker) AS updated_by,
      max(a.created_date) as created_date
    FROM stg_ODA.FALLS_RISK_STATUS frs
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = frs.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE frs.assessment_number = 403697726
    GROUP BY a.assessment_id
  ) src
  ON (tgt.assessment_id = src.assessment_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.age_greater_than_65         = src.age_greater_than_65,
      tgt.falling_history_last_year   = src.falling_history_last_year,
      tgt.multiple_chronic_conditions = src.multiple_chronic_conditions,
      tgt.incontinence                = src.incontinence,
      tgt.vision_impairment           = src.vision_impairment,
      tgt.impaired_mobility           = src.impaired_mobility,
      tgt.environmental_hazards       = src.environmental_hazards,
      tgt.medication_risk             = src.medication_risk,
      tgt.pain_level_of_function      = src.pain_level_of_function,
      tgt.cognitive_impairment        = src.cognitive_impairment,
      tgt.postural_hypotension        = src.postural_hypotension,
      tgt.fear_of_falling             = src.fear_of_falling,
      tgt.updated_date                = src.updated_date,
      tgt.created_by                  = src.created_by,
      tgt.updated_by                  = src.updated_by,
      tgt.created_date                 = src.created_date
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      age_greater_than_65,
      falling_history_last_year,
      multiple_chronic_conditions,
      incontinence,
      vision_impairment,
      impaired_mobility,
      environmental_hazards,
      medication_risk,
      pain_level_of_function,
      cognitive_impairment,
      postural_hypotension,
      fear_of_falling,
      updated_date,
      created_by,
      updated_by,
      created_date
    )
    VALUES (
      src.assessment_id,
      src.age_greater_than_65,
      src.falling_history_last_year,
      src.multiple_chronic_conditions,
      src.incontinence,
      src.vision_impairment,
      src.impaired_mobility,
      src.environmental_hazards,
      src.medication_risk,
      src.pain_level_of_function,
      src.cognitive_impairment,
      src.postural_hypotension,
      src.fear_of_falling,
      src.updated_date,
      src.created_by,
      src.updated_by,
      src.created_date
    );

  DBMS_OUTPUT.PUT_LINE('Rows merged: '||SQL%ROWCOUNT);
END;
/
