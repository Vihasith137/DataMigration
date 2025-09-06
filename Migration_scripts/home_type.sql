SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
    CURSOR c_home IS
        SELECT *
          FROM STG_ODA.ASSESSMENT_BASE
         WHERE ASSESSMENT_NUMBER = 24314362;

    v_assessment_id            LOC_DATA.HOME_ASSESSMENT_TEST.ASSESSMENT_ID%TYPE;
    v_has_animals_in_home      LOC_DATA.HOME_ASSESSMENT_TEST.HAS_ANIMALS_IN_HOME%TYPE;
    v_home_type_id             LOC_DATA.HOME_ASSESSMENT_TEST.HOME_TYPE_ID%TYPE;
    v_home_level_id            LOC_DATA.HOME_ASSESSMENT_TEST.HOME_LEVEL_ID%TYPE;
    v_ownership_status_id      LOC_DATA.HOME_ASSESSMENT_TEST.OWNERSHIP_STATUS_ID%TYPE;
    v_property_owner           LOC_DATA.HOME_ASSESSMENT_TEST.PROPERTY_OWNER%TYPE;

 

    v_count NUMBER := 0;
BEGIN
    <<home_loop>>
    FOR rec IN c_home LOOP
        -- map source columns
        v_assessment_id       := rec.ASSESSMENT_NUMBER;
        v_has_animals_in_home := rec.PETS_IND;
        v_property_owner      := NVL(rec.RENTAL_PROPERTY_OWNER, rec.RENTAL_OWNER_PHONE);
     
       

        -- HOME_TYPE_ID lookup
        BEGIN
            SELECT HOME_TYPE_ID
              INTO v_home_type_id
              FROM HOME_TYPE
             WHERE UPPER(TRIM(HOME_TYPE_NAME)) = UPPER(TRIM(rec.HOUSING_TYPE))
               AND ROWNUM = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_home_type_id := 7;
       
        END;

        -- HOME_LEVEL_ID lookup
        BEGIN
            SELECT HOME_LEVEL_ID
              INTO v_home_level_id
              FROM HOME_LEVEL_TYPE
             WHERE UPPER(TRIM(HOME_LEVEL_NAME)) = UPPER(TRIM(rec.HOUSING_STORIES))
               AND ROWNUM = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_home_level_id := 4;
          
        END;

        -- OWNERSHIP_STATUS_ID lookup
        BEGIN
            SELECT OWNERSHIP_STATUS_ID
              INTO v_ownership_status_id
              FROM HOME_OWNERSHIP_STATUS
             WHERE UPPER(TRIM(OWNERSHIP_STATUS)) = UPPER(TRIM(rec.HOUSING_OWNERSHIP))
               AND ROWNUM = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_ownership_status_id := 3;
        
        END;

        -- Insert into HOME_ASSESSMENT
        BEGIN
            INSERT INTO LOC_DATA.HOME_ASSESSMENT_TEST (
            HOME_ASSESSMENT_ID,
                ASSESSMENT_ID,
                HAS_ANIMALS_IN_HOME,
                HOME_TYPE_ID,
                HOME_LEVEL_ID,
                OWNERSHIP_STATUS_ID,
                PROPERTY_OWNER,
                RESIDENTIAL_STABILITY_ID,
                SAFETY_ID
            ) VALUES (1,
                v_assessment_id,
                v_has_animals_in_home,
                v_home_type_id,
                v_home_level_id,
                v_ownership_status_id,
                v_property_owner,
                1,
                1
            );
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                  'Insert error (HOME_ASSESSMENT) for ASSESSMENT_ID=' || v_assessment_id || ': ' || SQLERRM
                );
        END;
    END LOOP home_loop;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE(
      'Total HOME_ASSESSMENT records inserted: ' || v_count
    );
END;
/
