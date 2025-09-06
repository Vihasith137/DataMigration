SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
    CURSOR c_client IS
        SELECT * FROM CLIENT_BASE_LEGACY WHERE CLIENT_NUMBER IS NOT NULL;

    v_participant_id        LOC_DATA.PARTICIPANT_TMP.PARTICIPANT_ID%TYPE;
    v_legacy_client_number  LOC_DATA.PARTICIPANT_TMP.LEGACY_CLIENT_NUMBER%TYPE;
    v_first_name            LOC_DATA.PARTICIPANT_TMP.FIRST_NAME%TYPE;
    v_last_name             LOC_DATA.PARTICIPANT_TMP.LAST_NAME%TYPE;
    v_age                   LOC_DATA.PARTICIPANT_TMP.AGE%TYPE;
    v_middle_initial        LOC_DATA.PARTICIPANT_TMP.MIDDLE_INITIAL%TYPE;
    v_address               LOC_DATA.PARTICIPANT_TMP.PERMANENT_ADDRESS%TYPE;
    v_city                  LOC_DATA.PARTICIPANT_TMP.CITY%TYPE;
    v_state                 LOC_DATA.PARTICIPANT_TMP.STATE%TYPE;
    v_zip                   LOC_DATA.PARTICIPANT_TMP.ZIP_CODE%TYPE;
    v_phone_home            LOC_DATA.PARTICIPANT_TMP.PHONE_HOME%TYPE;
    v_ssn                   LOC_DATA.PARTICIPANT_TMP.SSN%TYPE;
    v_dob                   LOC_DATA.PARTICIPANT_TMP.DATE_OF_BIRTH%TYPE;
    v_sex                   LOC_DATA.PARTICIPANT_TMP.SEX%TYPE;
    v_interpreter_required  LOC_DATA.PARTICIPANT_TMP.INTERPRETER_REQUIRED%TYPE;
    v_marital_status_id     LOC_DATA.PARTICIPANT_TMP.MARITAL_STATUS_ID%TYPE;
    v_email                 LOC_DATA.PARTICIPANT_TMP.EMAIL%TYPE;
    v_phone_cell            LOC_DATA.PARTICIPANT_TMP.PHONE_CELL%TYPE;
    v_veteran_status_id     LOC_DATA.PARTICIPANT_TMP.VETERAN_STATUS_ID%TYPE;
    v_nickname              LOC_DATA.PARTICIPANT_TMP.NICKNAME%TYPE;
    v_updated_date          LOC_DATA.PARTICIPANT_TMP.UPDATED_DATE%TYPE;
    v_comments              LOC_DATA.PARTICIPANT_TMP.VETERAN_COMMENTS%TYPE;
    v_preferred_language_id LOC_DATA.PARTICIPANT_TMP.PREFERRED_LANGUAGE_ID%TYPE;
    v_county                LOC_DATA.PARTICIPANT_TMP.COUNTY%TYPE;
    v_created_by            LOC_DATA.PARTICIPANT_TMP.CREATED_BY%TYPE;
    v_updated_by            LOC_DATA.PARTICIPANT_TMP.UPDATED_BY%TYPE;
    v_created_date          LOC_DATA.PARTICIPANT_TMP.CREATED_DATE%TYPE;
    v_count   NUMBER := 0;
    v_exists  NUMBER;
BEGIN
    <<c_client_loop>>
    FOR rec IN c_client LOOP
        -- basic fields & NVL for purely address/city/state/zip only
        v_legacy_client_number := rec.CLIENT_NUMBER;
        v_first_name           := rec.FIRST_NAME;
        v_last_name            := rec.LAST_NAME;
        v_middle_initial       := rec.MIDDLE_NAME;
        v_address              := NVL(rec.ADDRESS, 'UNKNOWN');
        v_city                 := NVL(rec.CITY,    'UNKNOWN');
        v_state                := NVL(rec.STATE,   'UNKNOWN');
        v_zip                  := NVL(rec.ZIP,     'UNKNOWN');
        v_phone_home           := rec.PHONE;
        v_ssn                  := rec.SSN;

        -- DOB now nullable
        v_dob := rec.DATE_OF_BIRTH;

        -- AGE null if no DOB
        BEGIN
            IF rec.DATE_OF_BIRTH IS NOT NULL THEN
                v_age := FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12);
            ELSE
                v_age := NULL;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Age Calc Error: ' || SQLERRM);
                v_age := NULL;
        END;

        -- SEX now null when unrecognized
        v_sex := CASE UPPER(TRIM(rec.SEX))
                    WHEN 'F' THEN 'FEMALE'
                    WHEN 'M' THEN 'MALE'
                    ELSE NULL
                 END;

        v_interpreter_required := CASE WHEN UPPER(TRIM(rec.LANGUAGE_BARRIER_IND)) IN ('Y','YES')
                                       THEN 'Y' ELSE 'N' END;
        v_marital_status_id    := CASE UPPER(TRIM(rec.MARITAL_STATUS))
                                    WHEN 'SINGLE'   THEN 1
                                    WHEN 'MARRIED'  THEN 2
                                    WHEN 'DIVORCED' THEN 3
                                    WHEN 'WIDOWED'  THEN 4
                                    ELSE 1
                                  END;

        -- VETERAN_STATUS_ID now nullable
        v_veteran_status_id := CASE TRIM(UPPER(rec.VETERAN_STATUS))
                                  WHEN '1' THEN 1
                                  WHEN '2' THEN 2
                                  WHEN '3' THEN 3
                                  WHEN '4' THEN 4
                                  ELSE NULL
                               END;

        v_comments    := rec.CLIENT_COMMENTS;
        v_county      := rec.COUNTY_CODE;
        v_created_by  := rec.CHECK_OUT_WORKER;
        v_updated_by  := rec.LAST_UPDATE_WORKER;
        v_created_date:= rec.CREATION_DATE;
        v_updated_date:= rec.LAST_UPDATE_TIME;
        v_email       := rec.EMAIL_ADDRESS;
        v_phone_cell  := rec.CELL_PHONE;
        v_nickname    := rec.NICKNAME;

        -- PREFERRED_LANGUAGE_ID now nullable if unmapped
        IF rec.ORIGIN_LANGUAGE IS NULL THEN
            v_preferred_language_id := NULL;
        ELSE
            v_preferred_language_id := CASE UPPER(TRIM(rec.ORIGIN_LANGUAGE))
                WHEN 'ENGLISH'   THEN 1
                WHEN 'HINDI'     THEN 2
                WHEN 'SPANISH'   THEN 3
                WHEN 'MALAYALAM' THEN 4
                -- add more known codes here…
                ELSE
                    -- try lookup, else leave NULL
                    (SELECT LANGUAGE_ID
                       FROM LOC_DATA.LANGUAGE
                      WHERE TRIM(UPPER(LANGUAGE_NAME)) = TRIM(UPPER(rec.ORIGIN_LANGUAGE))
                        AND ROWNUM = 1)
            END CASE;
        END IF;

        -- skip duplicates
        BEGIN
            SELECT 1 INTO v_exists
              FROM LOC_DATA.PARTICIPANT_TMP
             WHERE LEGACY_CLIENT_NUMBER = rec.CLIENT_NUMBER
               AND ROWNUM = 1;
            CONTINUE c_client_loop;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;

        -- insert
        BEGIN
            INSERT INTO LOC_DATA.PARTICIPANT_TMP (
                LEGACY_CLIENT_NUMBER, FIRST_NAME, LAST_NAME, MIDDLE_INITIAL,
                PERMANENT_ADDRESS, CITY, STATE, ZIP_CODE, PHONE_HOME, PHONE_CELL,
                SSN, SEX, DATE_OF_BIRTH, AGE, INTERPRETER_REQUIRED, EMAIL,
                NICKNAME, VETERAN_STATUS_ID, MARITAL_STATUS_ID, CREATED_BY,
                CREATED_DATE, UPDATED_BY, UPDATED_DATE, VETERAN_COMMENTS,
                PREFERRED_LANGUAGE_ID, COUNTY
            ) VALUES (
                v_legacy_client_number, v_first_name, v_last_name, v_middle_initial,
                v_address, v_city, v_state, v_zip, v_phone_home, v_phone_cell,
                v_ssn, v_sex, v_dob, v_age, v_interpreter_required, v_email,
                v_nickname, v_veteran_status_id, v_marital_status_id, v_created_by,
                v_created_date, v_updated_by, v_updated_date, v_comments,
                v_preferred_language_id, v_county
            )
            RETURNING PARTICIPANT_ID INTO v_participant_id;
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Insert Error for '
                                     || rec.CLIENT_NUMBER || ': ' || SQLERRM);
        END;
    END LOOP c_client_loop;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Total records inserted: ' || v_count);
END;
/
