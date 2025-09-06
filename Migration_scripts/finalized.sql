SET SERVEROUTPUT ON SIZE 1_000_000;

DECLARE
    CURSOR c_final IS
        SELECT *
          FROM ODA.ASSESSMENT_BASE
         WHERE ASSESSMENT_NUMBER IS NOT NULL;

    v_assessment_id   LOC_DATA.FINALIZATION.ASSESSMENT_ID%TYPE;
    v_finalized       LOC_DATA.FINALIZATION.FINALIZED%TYPE;
    v_assessor_name   LOC_DATA.FINALIZATION.ASSESSOR_NAME%TYPE;
    v_is_active       LOC_DATA.FINALIZATION.IS_ACTIVE%TYPE;
    v_created_date    LOC_DATA.FINALIZATION.CREATED_DATE%TYPE;
    v_updated_date    LOC_DATA.FINALIZATION.UPDATED_DATE%TYPE;
    v_created_by      LOC_DATA.FINALIZATION.CREATED_BY%TYPE;
    v_updated_by      LOC_DATA.FINALIZATION.UPDATED_BY%TYPE;

    v_count NUMBER := 0;
BEGIN
    <<final_loop>>
    FOR rec IN c_final LOOP
        -- map source columns
        v_assessment_id  := rec.ASSESSMENT_NUMBER;
        v_finalized      := rec.ASSESSMENT_COMPLETE_IND;
        v_assessor_name  := rec.ORIGINAL_FINALIZE_WORKER;
        v_is_active      := rec.IS_ACTIVE;
        v_created_date   := rec.CREATED_DATE;
        v_updated_date   := rec.LAST_UPDATE_TIME;
        v_created_by     := rec.CHECK_OUT_WORKER;
        v_updated_by     := rec.LAST_UPDATE_WORKER;

        BEGIN
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
                v_assessment_id,
                v_finalized,
                v_assessor_name,
                v_is_active,
                v_created_date,
                v_updated_date,
                v_created_by,
                v_updated_by
            );

            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                  'Insert Error (FINALIZATION) for ASSESSMENT_ID='
                  || v_assessment_id
                  || ': '
                  || SQLERRM
                );
        END;
    END LOOP final_loop;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Total records inserted into FINALIZATION: ' || v_count);
END;
/
