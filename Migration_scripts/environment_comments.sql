SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
    CURSOR c_home IS
        SELECT *
          FROM ODA.ASSESSMENT_BASE
         WHERE ASSESSMENT_NUMBER IS NOT NULL;

    v_assessment_id   LOC_DATA.HOME_ASSESSMENT.ASSESSMENT_ID%TYPE;
    v_comments        LOC_DATA.HOME_ASSESSMENT.COMMENTS%TYPE;
    v_count           NUMBER := 0;
BEGIN
    <<home_loop>>
    FOR rec IN c_home LOOP
        -- Mapping
        v_assessment_id := rec.ASSESSMENT_NUMBER;
        v_comments      := rec.ENVIRONMENT_COMMENT;

        -- INSERT INTO HOME_ASSESSMENT
        BEGIN
            INSERT INTO LOC_DATA.HOME_ASSESSMENT (
                ASSESSMENT_ID,
                COMMENTS
            ) VALUES (
                v_assessment_id,
                v_comments
            );
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                  'Insert Error (HOME_ASSESSMENT) for ASSESSMENT_ID='
                  || v_assessment_id
                  || ': ' || SQLERRM
                );
        END;

        -- INSERT INTO ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF
        BEGIN
            INSERT INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF (
                ASSESSMENT_ID,
                COMMENTS
            ) VALUES (
                v_assessment_id,
                v_comments
            );
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                  'Insert Error (ENVIRONMENTAL_REVIEW_XREF) for ASSESSMENT_ID='
                  || v_assessment_id
                  || ': ' || SQLERRM
                );
        END;

        -- INSERT INTO ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF
        BEGIN
            INSERT INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF (
                ASSESSMENT_ID,
                COMMENTS
            ) VALUES (
                v_assessment_id,
                v_comments
            );
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                  'Insert Error (ENVIRONMENTAL_ISSUE_XREF) for ASSESSMENT_ID='
                  || v_assessment_id
                  || ': ' || SQLERRM
                );
        END;
    END LOOP home_loop;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Total records inserted: ' || v_count);
END;
/
