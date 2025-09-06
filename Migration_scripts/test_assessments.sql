SET SERVEROUTPUT ON SIZE 1000000;
DECLARE
  CURSOR c_base IS
    SELECT * FROM STG_ODA.ASSESSMENT_BASE WHERE ROWNUM <= 100;

  -- Assessment master
  v_pims_assessment_number   LOC_DATA.ASSESSMENT.PIMS_ASSESSMENT_NUMBER%TYPE;
  v_participant_id           LOC_DATA.ASSESSMENT.PARTICIPANT_ID%TYPE;
  v_assessor_first_name      LOC_DATA.ASSESSMENT.ASSESSOR_FIRST_NAME%TYPE;
  v_assessor_last_name       LOC_DATA.ASSESSMENT.ASSESSOR_LAST_NAME%TYPE;
  v_assess_begin_date        LOC_DATA.ASSESSMENT.ASSESSMENT_BEGIN_DATE%TYPE;
  v_place_of_assessment_id   LOC_DATA.ASSESSMENT.PLACE_OF_ASSESSMENT_ID%TYPE;
  v_assessment_finalized_date LOC_DATA.ASSESSMENT.ASSESSMENT_FINALIZED_DATE%TYPE;
  v_assessment_agency        LOC_DATA.ASSESSMENT.ASSESSMENT_AGENCY%TYPE;
  v_created_date             LOC_DATA.ASSESSMENT.CREATED_DATE%TYPE;  -- TSTZ
  v_created_by               LOC_DATA.ASSESSMENT.CREATED_BY%TYPE;
  v_updated_date             LOC_DATA.ASSESSMENT.UPDATED_DATE%TYPE;  -- TSTZ
  v_updated_by               LOC_DATA.ASSESSMENT.UPDATED_BY%TYPE;
  v_assessment_id            LOC_DATA.ASSESSMENT.ASSESSMENT_ID%TYPE;

  -- FK parents to validate/derive
  v_form_type_id       LOC_DATA.FORM_TYPE.FORM_TYPE_ID%TYPE;
  v_assessment_type_id LOC_DATA.ASSESSMENT_TYPE.ASSESSMENT_TYPE_ID%TYPE;
  v_assess_type_name   LOC_DATA.ASSESSMENT_TYPE.ASSESSMENT_TYPE_NAME%TYPE;

  -- Child-table vars
  v_has_animals_in_home      LOC_DATA.HOME_ASSESSMENT.HAS_ANIMALS_IN_HOME%TYPE;
  v_home_type_id             LOC_DATA.HOME_ASSESSMENT.HOME_TYPE_ID%TYPE;
  v_home_level_id            LOC_DATA.HOME_ASSESSMENT.HOME_LEVEL_ID%TYPE;
  v_ownership_status_id      LOC_DATA.HOME_ASSESSMENT.OWNERSHIP_STATUS_ID%TYPE;
  v_property_owner           LOC_DATA.HOME_ASSESSMENT.PROPERTY_OWNER%TYPE;

  v_finalized                LOC_DATA.FINALIZATION.FINALIZED%TYPE;
  v_assessor_name            LOC_DATA.FINALIZATION.ASSESSOR_NAME%TYPE;
  v_finalized_date           LOC_DATA.FINALIZATION.FINALIZED_DATE%TYPE;

  v_support_level_id         LOC_DATA.COGNITIVE_IMPAIRMENT.SUPPORT_LEVEL_ID%TYPE;

  v_medical_condition_status LOC_DATA.MEDICAL_STABILITY.MEDICAL_CONDITION_STATUS%TYPE;

  v_med_admin_comments       LOC_DATA.MEDICATION_ADMINISTRATION.COMMENTS%TYPE;

  v_requires_help            LOC_DATA.MEDICATION_SAFETY.REQUIRES_HELP_WITH_MEDICATION%TYPE;
  v_help_level_id            LOC_DATA.MEDICATION_SAFETY.HELP_LEVEL_ID%TYPE;

  v_current_height           LOC_DATA.NUTRITIONAL_SCREENING.CURRENT_HEIGHT%TYPE;
  v_current_weight           LOC_DATA.NUTRITIONAL_SCREENING.CURRENT_WEIGHT%TYPE;
  v_diet                     LOC_DATA.NUTRITIONAL_SCREENING.DIET%TYPE;
  v_nutrition_comments       LOC_DATA.NUTRITIONAL_SCREENING.COMMENTS%TYPE;

  v_recommendation_level_id  LOC_DATA.RECOMMENDATION.RECOMMENDATION_LEVEL_ID%TYPE;
  v_recommended_loc          LOC_DATA.RECOMMENDATION.RECOMMENDED_LOC%TYPE;

  v_where_employed           LOC_DATA.SCHOOL_WORK_ASSESSMENT.WHERE_EMPLOYED%TYPE;
  v_employment_status_id     LOC_DATA.SCHOOL_WORK_ASSESSMENT.EMPLOYMENT_STATUS_ID%TYPE;

  v_tmp                      VARCHAR2(4000);
  v_row_err                  VARCHAR2(4000);
  v_dummy                    NUMBER;

  -- Counters
  v_count_assessment   NUMBER := 0;
  v_count_home         NUMBER := 0;
  v_count_finalization NUMBER := 0;
  v_count_cognitive    NUMBER := 0;
  v_count_medical      NUMBER := 0;
  v_count_med_admin    NUMBER := 0;
  v_count_med_safety   NUMBER := 0;
  v_count_nutrition    NUMBER := 0;
  v_count_recommend    NUMBER := 0;
  v_count_recovery     NUMBER := 0;
  v_count_schoolwork   NUMBER := 0;
  v_skipped_no_parent  NUMBER := 0;

  FUNCTION to_tstz(p_date DATE) RETURN TIMESTAMP WITH TIME ZONE IS
  BEGIN
    IF p_date IS NULL THEN RETURN NULL; END IF;
    RETURN FROM_TZ(CAST(p_date AS TIMESTAMP), SESSIONTIMEZONE);
  END;

  -- Resolve PARTICIPANT_ID from PDS_DATA.PARTICIPANT using CLIENT_NUMBER (with/without leading zeros)
  FUNCTION resolve_participant_id(p_client_no VARCHAR2) RETURN NUMBER IS
    v_digits VARCHAR2(50);
    v_pad7   VARCHAR2(50);
    v_pid    NUMBER;
  BEGIN
    v_digits := REGEXP_REPLACE(NVL(p_client_no,''), '[^0-9]', '');
    v_pad7   := LPAD(v_digits, 7, '0');

    SELECT MAX(p.participant_id)
      INTO v_pid
      FROM PDS_DATA.PARTICIPANT p
     WHERE TO_CHAR(p.pims_client_number) IN (v_digits, v_pad7);

    RETURN v_pid; -- NULL if not found
  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END;

  -- Derive FORM_TYPE_ID and ASSESSMENT_TYPE_ID from ASSESS_TYPE via DOMAIN_VALUE → ASSESSMENT_TYPE
  PROCEDURE resolve_assessment_type(
    p_assess_type     IN  VARCHAR2,
    o_form_type_id    OUT NUMBER,
    o_assess_type_id  OUT NUMBER
  ) IS
    v_expanded   VARCHAR2(4000);
    v_form_txt   VARCHAR2(10);
    v_name_guess VARCHAR2(200);
    v_u          VARCHAR2(4000);
  BEGIN
    o_form_type_id   := NULL;
    o_assess_type_id := NULL;

    IF p_assess_type IS NULL THEN
      -- default to form 1 / Initial
      o_form_type_id := 1;
      SELECT assessment_type_id
        INTO o_assess_type_id
        FROM LOC_DATA.assessment_type
       WHERE form_type_id = 1
         AND assessment_type_name = 'Initial Assessment';
      RETURN;
    END IF;

    BEGIN
      SELECT dv.expanded_value
        INTO v_expanded
        FROM STG_ODA.DOMAIN_VALUE dv
       WHERE dv.name = 'Assess Types'
         AND TO_CHAR(dv.value) = TO_CHAR(p_assess_type)
         AND ROWNUM = 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- if not found in domain, default to form 1 / Initial
      o_form_type_id := 1;
      SELECT assessment_type_id
        INTO o_assess_type_id
        FROM LOC_DATA.assessment_type
       WHERE form_type_id = 1
         AND assessment_type_name = 'Initial Assessment';
      RETURN;
    END;

    -- Extract form type from the leading number in EXPANDED_VALUE (e.g., '1 Reassessment - Routine')
    v_form_txt := REGEXP_SUBSTR(v_expanded, '^\s*(\d+)', 1, 1, NULL, 1);
    o_form_type_id := NVL(TO_NUMBER(v_form_txt), 1);

    -- Decide canonical assessment type by keywords
    v_u := UPPER(v_expanded);

    IF v_u LIKE '%REASSESS%' THEN
      v_name_guess := CASE o_form_type_id
                        WHEN 2 THEN 'LOC Reassessment'
                        ELSE 'Reassessment'
                      END;
    ELSIF v_u LIKE '%SIGNIFICANT%' THEN
      v_name_guess := 'Significant Change';
    ELSIF v_u LIKE '%UPDATE%' OR v_u LIKE '%VALIDATION%' OR v_u LIKE '%DELAYED%' OR v_u LIKE '%ADVERSE%' THEN
      v_name_guess := CASE o_form_type_id
                        WHEN 2 THEN 'LOC Validation and/or Update'
                        ELSE 'Assessment Validation and/or Update'
                      END;
    ELSE
      v_name_guess := CASE o_form_type_id
                        WHEN 2 THEN 'Initial LOC Assessment'
                        ELSE 'Initial Assessment'
                      END;
    END IF;

    BEGIN
      SELECT assessment_type_id
        INTO o_assess_type_id
        FROM LOC_DATA.assessment_type
       WHERE form_type_id = o_form_type_id
         AND assessment_type_name = v_name_guess;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Fallback: pick the lowest id for this form type
      SELECT MIN(assessment_type_id)
        INTO o_assess_type_id
        FROM LOC_DATA.assessment_type
       WHERE form_type_id = o_form_type_id;
    END;
  END;
BEGIN
  FOR rec IN c_base LOOP
    BEGIN
      --------------------------------------------------------------------
      -- PARTICIPANT_ID (FK -> PDS_DATA.PARTICIPANT)
      --------------------------------------------------------------------
      v_participant_id := resolve_participant_id(rec.CLIENT_NUMBER);

      IF v_participant_id IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('Skip '||rec.ASSESSMENT_NUMBER||
                             ' : no parent in PDS_DATA.PARTICIPANT for CLIENT_NUMBER='||rec.CLIENT_NUMBER);
        v_skipped_no_parent := v_skipped_no_parent + 1;
        CONTINUE;
      END IF;

      --------------------------------------------------------------------
      -- FORM_TYPE_ID and ASSESSMENT_TYPE_ID via DOMAIN_VALUE
      --------------------------------------------------------------------
      resolve_assessment_type(rec.ASSESS_TYPE, v_form_type_id, v_assessment_type_id);

      -- Validate PLACE_OF_ASSESSMENT_ID (fallback to min existing)
      v_place_of_assessment_id :=
        CASE
          WHEN UPPER(TRIM(rec.ASSESS_PLACE)) = 'HOME' THEN 1
          WHEN UPPER(TRIM(rec.ASSESS_PLACE)) = 'HOSPITAL' THEN 2
          WHEN UPPER(TRIM(rec.ASSESS_PLACE)) LIKE 'RESIDENTIAL%RTF%' THEN 3
          WHEN UPPER(TRIM(rec.ASSESS_PLACE)) = 'VIRTUAL' THEN 4
          WHEN UPPER(TRIM(rec.ASSESS_PLACE)) = 'NF' THEN 5
          WHEN UPPER(TRIM(rec.ASSESS_PLACE)) = 'ICF/IID' THEN 6
          WHEN UPPER(TRIM(rec.ASSESS_PLACE)) = 'TELEPHONIC' THEN 7
          ELSE 8
        END;

      BEGIN
        SELECT 1 INTO v_dummy FROM LOC_DATA.PLACE_OF_ASSESSMENT WHERE PLACE_OF_ASSESSMENT_ID = v_place_of_assessment_id;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        SELECT MIN(PLACE_OF_ASSESSMENT_ID) INTO v_place_of_assessment_id FROM LOC_DATA.PLACE_OF_ASSESSMENT;
      END;

-- assessment
      v_pims_assessment_number := rec.ASSESSMENT_NUMBER;

      -- Assessor / agency
      BEGIN
        SELECT SUBSTR(w.FIRST_NAME,1,100),
               SUBSTR(w.LAST_NAME,1,100),
               SUBSTR(TO_CHAR(w.ODA_AGENCY_NUMBER),1,255)
          INTO v_assessor_first_name, v_assessor_last_name, v_assessment_agency
          FROM STG_ODA.WORKER w
         WHERE w.WORKER_NUMBER = rec.WORKER_NUMBER;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        v_assessor_first_name := 'Unknown';
        v_assessor_last_name  := 'Unknown';
        v_assessment_agency   := 'Unknown';
      END;

      v_assess_begin_date         := rec.ASSESS_DATE;
      v_assessment_finalized_date := rec.ORIGINAL_FINALIZE_DATE;

      v_created_date := to_tstz(rec.CREATE_DATE);
      v_updated_date := to_tstz(rec.LAST_UPDATE_TIME);
      v_created_by   := SUBSTR(NVL(TO_CHAR(rec.CREATED_BY), rec.CHECK_OUT_WORKER),1,100);
      v_updated_by   := SUBSTR(NVL(rec.LAST_UPDATE_WORKER, TO_CHAR(rec.CREATED_BY)),1,100);

      INSERT INTO LOC_DATA.ASSESSMENT (
        PIMS_ASSESSMENT_NUMBER,
        PARTICIPANT_ID,
        FORM_TYPE_ID,
        ASSESSMENT_AGENCY,
        ASSESSOR_FIRST_NAME,
        ASSESSOR_LAST_NAME,
        ASSESSMENT_BEGIN_DATE,
        ASSESSMENT_FINALIZED_DATE,
        ASSESSMENT_TYPE_ID,
        PLACE_OF_ASSESSMENT_ID,
        CREATED_DATE,
        CREATED_BY,
        UPDATED_DATE,
        UPDATED_BY
      ) VALUES (
        v_pims_assessment_number,
        v_participant_id,
        v_form_type_id,
        v_assessment_agency,
        v_assessor_first_name,
        v_assessor_last_name,
        v_assess_begin_date,
        v_assessment_finalized_date,
        v_assessment_type_id,
        v_place_of_assessment_id,
        v_created_date,
        v_created_by,
        v_updated_date,
        v_updated_by
      )
      RETURNING ASSESSMENT_ID INTO v_assessment_id;

      v_count_assessment := v_count_assessment + 1;

-- home assessment
      IF rec.PETS_IND IS NOT NULL OR rec.HOUSING_TYPE IS NOT NULL THEN
        v_has_animals_in_home := CASE WHEN UPPER(TRIM(rec.PETS_IND)) IN ('Y','YES','1') THEN 'Y' ELSE 'N' END;

        BEGIN
          SELECT ht.HOME_TYPE_ID INTO v_home_type_id
            FROM LOC_DATA.HOME_TYPE ht
           WHERE UPPER(TRIM(ht.HOME_TYPE_NAME)) = UPPER(TRIM(rec.HOUSING_TYPE)) AND ROWNUM=1;
        EXCEPTION WHEN NO_DATA_FOUND THEN v_home_type_id := 7; END;

        BEGIN
          SELECT hl.HOME_LEVEL_ID INTO v_home_level_id
            FROM LOC_DATA.HOME_LEVEL_TYPE hl
           WHERE UPPER(TRIM(hl.HOME_LEVEL_NAME)) = UPPER(TRIM(rec.HOUSING_STORIES)) AND ROWNUM=1;
        EXCEPTION WHEN NO_DATA_FOUND THEN v_home_level_id := 4; END;

        BEGIN
          SELECT os.OWNERSHIP_STATUS_ID INTO v_ownership_status_id
            FROM LOC_DATA.HOME_OWNERSHIP_STATUS os
           WHERE UPPER(TRIM(os.OWNERSHIP_STATUS)) = UPPER(TRIM(rec.HOUSING_OWNERSHIP)) AND ROWNUM=1;
        EXCEPTION WHEN NO_DATA_FOUND THEN v_ownership_status_id := 3; END;

        v_property_owner := SUBSTR(NVL(rec.RENTAL_PROPERTY_OWNER, rec.RENTAL_OWNER_PHONE), 1, 255);

        INSERT INTO LOC_DATA.HOME_ASSESSMENT (
          ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, HAS_ANIMALS_IN_HOME, HOME_TYPE_ID, HOME_LEVEL_ID,
          OWNERSHIP_STATUS_ID, PROPERTY_OWNER, RESIDENTIAL_STABILITY_ID, SAFETY_ID,
          CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
        ) VALUES (
          v_assessment_id, v_pims_assessment_number, v_has_animals_in_home, v_home_type_id, v_home_level_id,
          v_ownership_status_id, v_property_owner, 1, 1,
          v_created_date, v_created_by, v_updated_date, v_updated_by
        );
        v_count_home := v_count_home + 1;
      END IF;

      v_finalized      := CASE WHEN UPPER(TRIM(rec.ASSESSMENT_COMPLETE_IND)) IN ('Y','YES','1') THEN 'Y' ELSE 'N' END;
      v_assessor_name  := SUBSTR(NVL(rec.ORIGINAL_FINALIZE_WORKER, v_assessor_first_name||' '||v_assessor_last_name),1,100);
      v_finalized_date := rec.ORIGINAL_FINALIZE_DATE;

      INSERT INTO LOC_DATA.FINALIZATION (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, FINALIZED, ASSESSOR_NAME, FINALIZED_DATE,
        FORM_TYPE_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, v_finalized, v_assessor_name, v_finalized_date,
        v_form_type_id, v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_finalization := v_count_finalization + 1;

      IF rec.SUPERVISION_NEED_TYPE IS NOT NULL THEN
        v_support_level_id :=
          CASE
            WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%24%HOUR%' THEN 1
            WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%PARTIAL%'  THEN 2
            ELSE 3
          END;

        INSERT INTO LOC_DATA.COGNITIVE_IMPAIRMENT (
          ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, SUPPORT_LEVEL_ID,
          CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
        ) VALUES (
          v_assessment_id, v_pims_assessment_number, v_support_level_id,
          v_created_date, v_created_by, v_updated_date, v_updated_by
        );
        v_count_cognitive := v_count_cognitive + 1;
      END IF;

      v_medical_condition_status := SUBSTR(rec.CONDITION,1,10);
      INSERT INTO LOC_DATA.MEDICAL_STABILITY (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, MEDICAL_CONDITION_STATUS,
        CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, v_medical_condition_status,
        v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_medical := v_count_medical + 1;

      v_med_admin_comments := rec.CAREPLAN_IMPL_MEDICATION;
      INSERT INTO LOC_DATA.MEDICATION_ADMINISTRATION (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, COMMENTS,
        CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, v_med_admin_comments,
        v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_med_admin := v_count_med_admin + 1;

      v_requires_help := CASE WHEN UPPER(TRIM(rec.MED_ADMIN_SUPERVISION_IND)) IN ('Y','YES','1') THEN 'Y' ELSE 'N' END;
      v_help_level_id := CASE WHEN v_requires_help = 'Y' THEN 2 ELSE 1 END;

      INSERT INTO LOC_DATA.MEDICATION_SAFETY (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, REQUIRES_HELP_WITH_MEDICATION, HELP_LEVEL_ID,
        CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, v_requires_help, v_help_level_id,
        v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_med_safety := v_count_med_safety + 1;

      v_tmp := REGEXP_REPLACE(NVL(rec.HEIGHT,''), '[^0-9\.]', '');
      v_current_height := CASE WHEN v_tmp IS NOT NULL AND REGEXP_LIKE(v_tmp, '^\d+(\.\d+)?$') THEN TO_NUMBER(v_tmp) ELSE NULL END;

      v_tmp := REGEXP_REPLACE(NVL(rec.WEIGHT,''), '[^0-9\.]', '');
      v_current_weight := CASE WHEN v_tmp IS NOT NULL AND REGEXP_LIKE(v_tmp, '^\d+(\.\d+)?$') THEN TO_NUMBER(v_tmp) ELSE NULL END;

      v_diet := SUBSTR(rec.DIET_TYPE,1,255);
      v_nutrition_comments := COALESCE(rec.NUTRITION_COMMENT, rec.CAREPLAN_IMPL_NUTRITION);

      INSERT INTO LOC_DATA.NUTRITIONAL_SCREENING (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, CURRENT_HEIGHT, CURRENT_WEIGHT, DIET, COMMENTS,
        CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, v_current_height, v_current_weight, v_diet, v_nutrition_comments,
        v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_nutrition := v_count_nutrition + 1;

      v_recommendation_level_id :=
        CASE UPPER(TRIM(rec.LOC_DETERMINATION))
          WHEN 'NONE'    THEN 1
          WHEN 'PRO'     THEN 2
          WHEN 'INT'     THEN 3
          WHEN 'SKILLED' THEN 4
          WHEN 'APPEARS TO HAVE INDICATIONS OF DEVELOPMENTAL DISABILITIES/REQUIRES DD REVIEW' THEN 5
          ELSE NULL
        END;

      v_recommended_loc := SUBSTR(rec.LOC_DETERMINATION,1,255);

      INSERT INTO LOC_DATA.RECOMMENDATION (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, RECOMMENDATION_LEVEL_ID, RECOMMENDED_LOC,
        FORM_TYPE_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, v_recommendation_level_id, v_recommended_loc,
        v_form_type_id, v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_recommend := v_count_recommend + 1;

      INSERT INTO LOC_DATA.RECOVERY_PROGRAM (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, PROGRAM_DETAILS,
        CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, rec.CAREPLAN_IMPL_CHEMICAL,
        v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_recovery := v_count_recovery + 1;

      v_where_employed := SUBSTR(rec.CLIENT_OCCUPATION,1,255);
      v_employment_status_id :=
        CASE UPPER(TRIM(rec.CLIENT_OCCUPATION_STATUS))
          WHEN 'UNEMPLOYED' THEN 1
          WHEN 'FULL-TIME'  THEN 2
          WHEN 'PART-TIME'  THEN 3
          WHEN 'SEEKING EMPLOYMENT' THEN 4
          WHEN 'RETIRED'    THEN 5
          WHEN 'UNKNOWN'    THEN 6
          WHEN 'PARTICIPATING IN PRE-EMPLOYMENT ACTIVITIES/SUPPORTS' THEN 7
          ELSE 6
        END;

      INSERT INTO LOC_DATA.SCHOOL_WORK_ASSESSMENT (
        ASSESSMENT_ID, PIMS_ASSESSMENT_NUMBER, WHERE_EMPLOYED, EMPLOYMENT_STATUS_ID,
        VOLUNTEER_STATUS_ID, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
      ) VALUES (
        v_assessment_id, v_pims_assessment_number, v_where_employed, v_employment_status_id,
        1, v_created_date, v_created_by, v_updated_date, v_updated_by
      );
      v_count_schoolwork := v_count_schoolwork + 1;

    EXCEPTION
      WHEN OTHERS THEN
        v_row_err := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        DBMS_OUTPUT.PUT_LINE('Row error for PIMS '||rec.ASSESSMENT_NUMBER||' -> '||SQLERRM||' @ '||v_row_err);
    END;
  END LOOP;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('Inserted into ASSESSMENT:              '|| v_count_assessment);
  DBMS_OUTPUT.PUT_LINE('Inserted into HOME_ASSESSMENT:         '|| v_count_home);
  DBMS_OUTPUT.PUT_LINE('Inserted into FINALIZATION:            '|| v_count_finalization);
  DBMS_OUTPUT.PUT_LINE('Inserted into COGNITIVE_IMPAIRMENT:    '|| v_count_cognitive);
  DBMS_OUTPUT.PUT_LINE('Inserted into MEDICAL_STABILITY:       '|| v_count_medical);
  DBMS_OUTPUT.PUT_LINE('Inserted into MEDICATION_ADMINISTRATION:'|| v_count_med_admin);
  DBMS_OUTPUT.PUT_LINE('Inserted into MEDICATION_SAFETY:       '|| v_count_med_safety);
  DBMS_OUTPUT.PUT_LINE('Inserted into NUTRITIONAL_SCREENING:   '|| v_count_nutrition);
  DBMS_OUTPUT.PUT_LINE('Inserted into RECOMMENDATION:          '|| v_count_recommend);
  DBMS_OUTPUT.PUT_LINE('Inserted into RECOVERY_PROGRAM:        '|| v_count_recovery);
  DBMS_OUTPUT.PUT_LINE('Inserted into SCHOOL_WORK_ASSESSMENT:  '|| v_count_schoolwork);
END;
/
