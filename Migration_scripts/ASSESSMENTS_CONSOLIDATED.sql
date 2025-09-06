SET SERVEROUTPUT ON SIZE 1_000_000;

DECLARE
  
  -- vursors selecting all rows from the source tables
  
  CURSOR c_assess IS
    SELECT * FROM ODA.ASSESSMENT_BASE WHERE ASSESSMENT_NUMBER IS NOT NULL;

  CURSOR c_skilled IS
    SELECT ss.*, ab.CREATED_DATE AS BASE_CREATED_DATE
      FROM ODA.SKILLED_SERVICE ss
      JOIN ODA.ASSESSMENT_BASE ab
        ON ss.ASSESSMENT_NUMBER = ab.ASSESSMENT_NUMBER
     WHERE ss.ASSESSMENT_NUMBER IS NOT NULL;

  CURSOR c_env_rev IS
    SELECT * FROM ODA.ASSESSMENT_BASE
     WHERE CAREPLAN_IMPL_ENVIRONMENT IS NOT NULL;

  CURSOR c_home IS
    SELECT * FROM ODA.ASSESSMENT_BASE;

  CURSOR c_allergy IS
    SELECT * FROM ODA.ALLERGY WHERE ASSESSMENT_NUMBER IS NOT NULL;

  CURSOR c_final IS
    SELECT * FROM ODA.ASSESSMENT_BASE WHERE ASSESSMENT_COMPLETE_IND IS NOT NULL;

  CURSOR c_cog IS
    SELECT * FROM ODA.ASSESSMENT_BASE;

  
  
  v_count NUMBER := 0;
 
BEGIN
  
  -- Migrate ASSESSMENT
  
  FOR rec IN c_assess LOOP
    INSERT INTO LOC_DATA.ASSESSMENT (
      ASSESSMENT_ID,
      PARTICIPANT_ID,
      ASSESSOR_FIRST_NAME,
      ASSESSOR_LAST_NAME,
      ASSESSMENT_BEGIN_DATE,
      ASSESSMENT_FINALIZED_DATE,
      ASSESSMENT_TYPE_ID,
      PLACE_OF_ASSESSMENT_ID,
      IS_ACTIVE,
      CREATED_DATE,
      UPDATED_DATE,
      CREATED_BY,
      UPDATED_BY
    ) VALUES (
      rec.ASSESSMENT_NUMBER,
      rec.CLIENT_NUMBER,
      rec.CHECK_OUT_WORKER,
      rec.LAST_UPDATE_WORKER,
      rec.ASSESS_DATE,
      rec.ORIGINAL_FINALIZE_DATE,
      (SELECT dv.ASSESS_TYPE_ID
         FROM DOMAIN_VALUE dv
        WHERE dv.NAME = rec.ASSESS_TYPE
          AND ROWNUM = 1),
      CASE UPPER(TRIM(rec.ASSESS_PLACE))
        WHEN like '%HOME%'                                 THEN 1
        WHEN like '%HOSPITAL%'                             THEN 2
        WHEN LIKE '%RTF%'  THEN 3
        WHEN like '%VIRTUAL%'                              THEN 4
        WHEN like '%NF%'                                   THEN 5
        WHEN like '%ICF%%IID%'                              THEN 6
        WHEN like '%TELEPHONIC%'                           THEN 7
        WHEN like '%OTHER%'                                THEN 8
        ELSE NULL
      END,
      rec.IS_ACTIVE,
      rec.CREATED_DATE,
      rec.LAST_UPDATE_TIME,
      rec.CHECK_OUT_WORKER,
      rec.LAST_UPDATE_WORKER
    );
    v_count := v_count + 1;
  END LOOP;

  
  -- Migrate ASSESSMENT_SKILLED_SERVICE
  
  FOR rec IN c_skilled LOOP
    INSERT INTO LOC_DATA.ASSESSMENT_SKILLED_SERVICE (
      ASSESSMENT_ID,
      SKILLED_SERVICE_ID,
      SKILLED_NURSING_NEEDS,
      SKILLED_THERAPY_NEEDS,
      COMMENTS_SKILLED_NURSING_NEEDS,
      COMMENTS_SKILLED_THERAPY_NEEDS,
      IS_ACTIVE,
      CREATED_BY,
      CREATED_DATE,
      UPDATED_BY,
      UPDATED_DATE
    ) VALUES (
      rec.ASSESSMENT_NUMBER,
      rec.SKILLED_SERVICE_ID,
      CASE WHEN rec.SKILLED_SERVICE_TYPE = 'SNS' THEN 'Y' ELSE NULL END,
      CASE WHEN rec.SKILLED_SERVICE_TYPE = 'SRS' THEN 'Y' ELSE NULL END,
      CASE WHEN rec.SKILLED_SERVICE_TYPE = 'SNS' THEN rec.SERVICE_FREQ ELSE NULL END,
      CASE WHEN rec.SKILLED_SERVICE_TYPE = 'SRS' THEN rec.SERVICE_FREQ ELSE NULL END,
      rec.IS_ACTIVE,
      rec.CHECK_OUT_WORKER,
      rec.BASE_CREATED_DATE,
      rec.LAST_UPDATE_WORKER,
      rec.LAST_UPDATE_TIME
    );
    v_count := v_count + 1;
  END LOOP;

  
  -- Migrate ENVIRONMENTAL REVIEW XREF
  
  FOR rec IN c_env_rev LOOP
    INSERT INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF (
      ASSESSMENT_ID,
      COMMENTS,
      IS_ACTIVE,
      CREATED_DATE,
      UPDATED_DATE,
      CREATED_BY,
      UPDATED_BY
    ) VALUES (
      rec.ASSESSMENT_NUMBER,
      rec.CAREPLAN_IMPL_ENVIRONMENT,
      rec.IS_ACTIVE,
      rec.CREATED_DATE,
      rec.LAST_UPDATE_TIME,
      rec.CHECK_OUT_WORKER,
      rec.LAST_UPDATE_WORKER
    );
    v_count := v_count + 1;
  END LOOP;

  
  -- Migrate HOME_ASSESSMENT
  
  FOR rec IN c_home LOOP
    INSERT INTO LOC_DATA.HOME_ASSESSMENT (
      ASSESSMENT_ID,
      HAS_ANIMALS_IN_HOME,
      PROPERTY_OWNER,
      IS_ACTIVE,
      CREATED_DATE,
      UPDATED_DATE,
      CREATED_BY,
      UPDATED_BY
    ) VALUES (
      rec.ASSESSMENT_NUMBER,
      rec.PETS_IND,
      NVL(rec.RENTAL_PROPERTY_OWNER, rec.RENTAL_OWNER_PHONE),
      rec.IS_ACTIVE,
      rec.CREATED_DATE,
      rec.LAST_UPDATE_TIME,
      rec.CHECK_OUT_WORKER,
      rec.LAST_UPDATE_WORKER
    );
    v_count := v_count + 1;
  END LOOP;

  
  -- Migrate ALLERGY
  
  FOR rec IN c_allergy LOOP
    INSERT INTO LOC_DATA.ALLERGY (
      ASSESSMENT_ID,
      ALLERGY_ID,
      SUBSTANCE,
      REACTION,
      SEVERITY,
      COMMENTS,
      IS_ACTIVE,
      CREATED_DATE,
      UPDATED_DATE,
      CREATED_BY,
      UPDATED_BY
    ) VALUES (
      rec.ASSESSMENT_NUMBER,
      rec.ALLERGY_ID,
      rec.SUBSTANCE,
      rec.REACTION,
      rec.SEVERITY,
      rec.ALLERGY_COMMENT,
      rec.IS_ACTIVE,
      rec.CREATED_DATE,
      rec.LAST_UPDATE_TIME,
      rec.CHECK_OUT_WORKER,
      rec.LAST_UPDATE_WORKER
    );
    v_count := v_count + 1;
  END LOOP;

  
  -- Migrate FINALIZATION
  
  FOR rec IN c_final LOOP
    INSERT INTO LOC_DATA.FINALIZATION (
      ASSESSMENT_ID,
      FINALIZED,
      ASSESSOR_NAME,
      IS_ACTIVE,
      CREATED_DATE,
      UPDATED_DATE,
      CREATED_BY,
      UPDATED_BY
    ) VALUES (
      rec.ASSESSMENT_NUMBER,
      rec.ASSESSMENT_COMPLETE_IND,
      rec.ORIGINAL_FINALIZE_WORKER,
      rec.IS_ACTIVE,
      rec.CREATED_DATE,
      rec.LAST_UPDATE_TIME,
      rec.CHECK_OUT_WORKER,
      rec.LAST_UPDATE_WORKER
    );
    v_count := v_count + 1;
  END LOOP;

  
  -- Migrate COGNITIVE_IMPAIRMENT
  
  FOR rec IN c_cog LOOP
    INSERT INTO LOC_DATA.COGNITIVE_IMPAIRMENT (
      ASSESSMENT_ID,
      SUPPORT_LEVEL_ID,
      IS_ACTIVE,
      CREATED_DATE,
      UPDATED_DATE,
      CREATED_BY,
      UPDATED_BY
    ) VALUES (
      rec.ASSESSMENT_NUMBER,
      CASE
        WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%24%HOUR%' THEN 1
        WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%PARTIAL%'  THEN 2
        WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%NONE%'     THEN 3
        ELSE NULL
      END,
      rec.IS_ACTIVE,
      rec.CREATED_DATE,
      rec.LAST_UPDATE_TIME,
      rec.CHECK_OUT_WORKER,
      rec.LAST_UPDATE_WORKER
    );
    v_count := v_count + 1;
  END LOOP;

  
  -- Done
  
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('Total records migrated in all the tables ' || v_count);
END;
/
