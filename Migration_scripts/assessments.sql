
SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
    CURSOR c_assessment IS
        SELECT *
          FROM STG_ODA.ASSESSMENT_BASE
         WHERE ASSESSMENT_NUMBER = '11648318';

    -- Variables for LOC_DATA.ASSESSMENT
    v_assessment_id              LOC_DATA.ASSESSMENT_TEST.ASSESSMENT_ID%TYPE;
    v_participant_id             LOC_DATA.ASSESSMENT_TEST.PARTICIPANT_ID%TYPE;
    v_assessor_first_name        LOC_DATA.ASSESSMENT_TEST.ASSESSOR_FIRST_NAME%TYPE;
    v_assessor_last_name         LOC_DATA.ASSESSMENT_TEST.ASSESSOR_LAST_NAME%TYPE;
    v_assess_begin_date          LOC_DATA.ASSESSMENT_TEST.ASSESSMENT_BEGIN_DATE%TYPE;
    v_assess_type_id             LOC_DATA.ASSESSMENT_TEST.ASSESSMENT_TYPE_ID%TYPE;
    v_place_of_assessment_id     LOC_DATA.ASSESSMENT_TEST.PLACE_OF_ASSESSMENT_ID%TYPE;
    v_assessment_finalized_date  LOC_DATA.ASSESSMENT_TEST.ASSESSMENT_FINALIZED_DATE%TYPE;
    v_updated_by                 LOC_DATA.ASSESSMENT_TEST.UPDATED_BY%TYPE;
    v_updated_date               LOC_DATA.ASSESSMENT_TEST.UPDATED_DATE%TYPE;
    v_created_by                 LOC_DATA.ASSESSMENT_TEST.CREATED_BY%TYPE;
   v_created_date               LOC_DATA.ASSESSMENT_TEST.CREATED_DATE%TYPE;

    v_count NUMBER := 0;
BEGIN
    <<assessment_loop>>
    FOR rec IN c_assessment LOOP
        -- map source columns
        v_assessment_id := rec.ASSESSMENT_NUMBER;
        v_participant_id := rec.CLIENT_NUMBER;

        -- fetch assessor name
        BEGIN
            SELECT first_name,
                   last_name
              INTO v_assessor_first_name,
                   v_assessor_last_name
              FROM STG_ODA.WORKER
             WHERE WORKER_NUMBER = rec.WORKER_NUMBER;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_assessor_first_name := 'Unknown';
                v_assessor_last_name  := 'Unknown';

        END;

        -- map place_of_assessment
        v_place_of_assessment_id := CASE UPPER(TRIM(rec.ASSESS_PLACE))
                                       WHEN 'HOME'                                  THEN 1
                                       WHEN 'HOSPITAL'                              THEN 2
                                       WHEN 'RESIDENTIAL TREATMENT FACILITY(RTF)'  THEN 3
                                       WHEN 'VIRTUAL'                               THEN 4
                                       WHEN 'NF'                                    THEN 5
                                       WHEN 'ICF/IID'                               THEN 6
                                       WHEN 'TELEPHONIC'                            THEN 7
                                       WHEN 'OTHER'                                 THEN 8
                                       ELSE NULL
                                   END;

        v_assess_begin_date         := rec.ASSESS_DATE;
        --assessment_agency
        --MAP ASSESS TYPE
        /* begin
        select assess_type_id into v_assess_type_id from STG_oda.domain_value
        where name = 'assess_type' and rownum = 1;
        exception when no_data_found then v_assess_type_id := null;
        end; */
        v_assessment_finalized_date := rec.ORIGINAL_FINALIZE_DATE;
        v_updated_by                := rec.LAST_UPDATE_WORKER;
        v_updated_date              := rec.LAST_UPDATE_TIME;
        v_created_by                := rec.CHECK_OUT_WORKER;
      v_created_date              := rec.CREATE_DATE;

        -- insert into target
        BEGIN
            INSERT INTO LOC_DATA.ASSESSMENT_TEST (
               assessment_id,
                PARTICIPANT_ID,
                form_type_id,
                assessment_agency,
                ASSESSOR_FIRST_NAME,
                ASSESSOR_LAST_NAME,
                ASSESSMENT_BEGIN_DATE,
                ASSESSMENT_FINALIZED_DATE,
               ASSESSMENT_TYPE_ID,
                PLACE_OF_ASSESSMENT_ID,
                UPDATED_BY,
                UPDATED_DATE,
                CREATED_BY,
                CREATED_DATE
            ) VALUES (
              v_assessment_id,
                v_participant_id,
                2,
                000,
                v_assessor_first_name,
                v_assessor_last_name,
                v_assess_begin_date,
                v_assessment_finalized_date,
                1,
                --v_assess_type_id,
                v_place_of_assessment_id,
                v_updated_by,
                v_updated_date,
                v_created_by,
               v_created_date
            );
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                  'Insert Error into ASSESSMENT for ASSESSMENT_ID='
                  || v_assessment_id
                  || ': '
                  || SQLERRM
                );
        END;
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE(
      'Total ASSESSMENT_BASE records processed: '
      || v_count
    );
END;
/
