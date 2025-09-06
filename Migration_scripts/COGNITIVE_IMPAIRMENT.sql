SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
    CURSOR c_cognitive IS
        SELECT *
          FROM STG_ODA.ASSESSMENT_BASE
         WHERE ASSESSMENT_NUMBER = 11624808 AND SUPERVISION_NEED_TYPE IS NOT NULL;

    -- Variables for LOC_DATA.COGNITIVE_IMPAIRMENT
    v_assessment_id     LOC_DATA.COGNITIVE_IMPAIRMENT_TEST.ASSESSMENT_ID%TYPE;
    v_SUPPORT_LEVEL_ID  LOC_DATA.COGNITIVE_IMPAIRMENT_TEST.SUPPORT_LEVEL_ID%TYPE;


    v_count NUMBER := 0;
BEGIN
    <<cognitive_loop>>
    FOR rec IN c_cognitive LOOP
        -- Map source columns to variables
        v_assessment_id    := rec.ASSESSMENT_NUMBER;
        v_SUPPORT_LEVEL_ID := CASE
                                 WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%24%HOUR%' THEN 1
                                 WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%PARTIAL%'  THEN 2
                                 --WHEN UPPER(TRIM(rec.SUPERVISION_NEED_TYPE)) LIKE '%NONE%'   OR NULL  THEN 3
                                 ELSE 3
                              END;

        -- Insert into COGNITIVE_IMPAIRMENT_TEST
        BEGIN
            INSERT INTO LOC_DATA.COGNITIVE_IMPAIRMENT_TEST (
                ASSESSMENT_ID,
                SUPPORT_LEVEL_ID,
                COGNITIVE_IMPAIRMENT_ID
            
            ) VALUES (
                v_assessment_id,
                v_SUPPORT_LEVEL_ID ,
                1);

            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                  'Insert Error into COGNITIVE_IMPAIRMENT_TEST for ASSESSMENT_ID='
                  || v_assessment_id
                  || ': '
                  || SQLERRM
                );
        END;
    END LOOP cognitive_loop;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE(
      'Total ASSESSMENT_BASE records processed: '
      || v_count
    );
END;
/
