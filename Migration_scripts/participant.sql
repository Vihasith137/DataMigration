DECLARE
  CURSOR c_client IS
    SELECT *
    FROM STG_ODA.CLIENT_BASE
    ORDER BY CLIENT_NUMBER;

  v_participant_id          PDS_DATA.PARTICIPANT.PARTICIPANT_ID%TYPE;
  v_pims_client_number      PDS_DATA.PARTICIPANT.PIMS_CLIENT_NUMBER%TYPE;
  v_first_name              PDS_DATA.PARTICIPANT.FIRST_NAME%TYPE;
  v_last_name               PDS_DATA.PARTICIPANT.LAST_NAME%TYPE;
  v_age_id                  PDS_DATA.PARTICIPANT.AGE_ID%TYPE;
  v_middle_initial          PDS_DATA.PARTICIPANT.MIDDLE_INITIAL%TYPE;
  v_address                 PDS_DATA.PARTICIPANT.ADDRESS%TYPE;
  v_home_phone              PDS_DATA.PARTICIPANT.HOME_PHONE%TYPE;
  v_cell_phone              PDS_DATA.PARTICIPANT.CELL_PHONE%TYPE;
  v_ssn                     PDS_DATA.PARTICIPANT.SSN%TYPE;
  v_dob                     PDS_DATA.PARTICIPANT.DATE_OF_BIRTH%TYPE;
  v_gender                  PDS_DATA.PARTICIPANT.GENDER%TYPE;
  v_interpreter_required    PDS_DATA.PARTICIPANT.INTERPRETER_REQUIRED%TYPE;
  v_marital_status_id       PDS_DATA.PARTICIPANT.MARITAL_STATUS_ID%TYPE;
  v_email                   PDS_DATA.PARTICIPANT.EMAIL%TYPE;
  v_veteran_status_id       PDS_DATA.PARTICIPANT.VETERAN_STATUS_ID%TYPE;
  v_nickname                PDS_DATA.PARTICIPANT.NICKNAME%TYPE;
  v_updated_date            PDS_DATA.PARTICIPANT.UPDATED_DATE%TYPE;
  v_comments                PDS_DATA.PARTICIPANT.VETERAN_COMMENTS%TYPE;
  v_preferred_language_id   PDS_DATA.PARTICIPANT.PREFERRED_LANGUAGE_ID%TYPE;
  v_zip_city_county_id      PDS_DATA.PARTICIPANT.ZIP_CITY_COUNTY_ID%TYPE;
  v_created_by              PDS_DATA.PARTICIPANT.CREATED_BY%TYPE;
  v_updated_by              PDS_DATA.PARTICIPANT.UPDATED_BY%TYPE;
  v_created_date            PDS_DATA.PARTICIPANT.CREATED_DATE%TYPE;
  v_language_known_id       PDS_DATA.LANGUAGE.LANGUAGE_ID%TYPE;

  v_rec_count               NUMBER := 0;
  v_commit_count            NUMBER := 10000;
  v_error_bt                VARCHAR2(4000);
  v_tmp                     VARCHAR2(64);

BEGIN
  FOR rec IN c_client LOOP
    -- PIMS_CLIENT_NUMBER: numeric-only
    IF rec.CLIENT_NUMBER IS NOT NULL AND REGEXP_LIKE(rec.CLIENT_NUMBER, '^\d+$') THEN
      v_pims_client_number := TO_NUMBER(rec.CLIENT_NUMBER);
    ELSE
      v_pims_client_number := NULL;
    END IF;

    v_first_name      := NVL(rec.FIRST_NAME, 'UNKNOWN');
    v_last_name       := NVL(rec.LAST_NAME,  'UNKNOWN');
    v_middle_initial  := rec.MIDDLE_NAME;
    v_address         := rec.ADDRESS;

    -- Phones: strip non-digits, truncate to 15
    v_home_phone := SUBSTR(REGEXP_REPLACE(NVL(rec.PHONE, ''),      '[^0-9]', ''), 1, 15);
    v_cell_phone := SUBSTR(REGEXP_REPLACE(NVL(rec.CELL_PHONE, ''), '[^0-9]', ''), 1, 15);

    -- SSN: keep only digits; must be exactly 9 to keep, else NULL
    v_tmp := REGEXP_REPLACE(NVL(rec.SSN, ''), '[^0-9]', '');
    IF LENGTH(v_tmp) = 9 THEN
      v_ssn := TO_NUMBER(v_tmp);
    ELSE
      v_ssn := NULL;
    END IF;

    v_dob          := rec.DATE_OF_BIRTH;
    v_created_date := CAST(rec.CREATION_DATE    AS TIMESTAMP WITH TIME ZONE);
    v_updated_date := CAST(rec.LAST_UPDATE_TIME AS TIMESTAMP WITH TIME ZONE);
    v_created_by   := rec.CHECK_OUT_WORKER;
    v_updated_by   := rec.LAST_UPDATE_WORKER;
    v_email        := rec.EMAIL_ADDRESS;
    v_nickname     := rec.NICKNAME;
    v_comments     := rec.CLIENT_COMMENTS;

    v_gender := CASE UPPER(TRIM(rec.SEX))
                  WHEN 'F' THEN 'FEMALE'
                  WHEN 'M' THEN 'MALE'
                  ELSE NULL
                END;

    IF rec.DATE_OF_BIRTH IS NOT NULL THEN
      v_age_id := CASE
                    WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12) < 14 THEN 1
                    WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12) BETWEEN 14 AND 20 THEN 2
                    WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12) BETWEEN 21 AND 59 THEN 3
                    WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12) BETWEEN 60 AND 64 THEN 4
                    WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12) BETWEEN 65 AND 74 THEN 5
                    WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12) BETWEEN 75 AND 84 THEN 6
                    WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, rec.DATE_OF_BIRTH)/12) > 84 THEN 7
                    ELSE NULL
                  END;
    ELSE
      v_age_id := NULL;
    END IF;

    v_interpreter_required := CASE
                                WHEN UPPER(TRIM(rec.LANGUAGE_BARRIER_IND)) IN ('Y', 'YES') THEN 1
                                ELSE 2
                              END;

    v_marital_status_id := CASE UPPER(TRIM(rec.MARITAL_STATUS))
                             WHEN 'SINGLE'  THEN 1
                             WHEN 'MARRIED' THEN 2
                             WHEN 'DIVORCED' THEN 3
                             WHEN 'WIDOWED' THEN 4
                             ELSE 1
                           END;

    v_veteran_status_id := CASE TRIM(UPPER(rec.VETERAN_STATUS))
                             WHEN '1' THEN 1 WHEN '2' THEN 2 WHEN '3' THEN 3 WHEN '4' THEN 4
                             ELSE NULL
                           END;

    IF rec.ORIGIN_LANGUAGE IS NOT NULL THEN
      v_preferred_language_id := CASE UPPER(TRIM(rec.ORIGIN_LANGUAGE))
        WHEN 'ENGLISH' THEN 1 WHEN 'SPANISH' THEN 2 WHEN 'RUSSIAN' THEN 3
        WHEN 'SOMALI' THEN 4 WHEN 'LAOTIAN' THEN 5 WHEN 'SERBIAN' THEN 6
        WHEN 'NEPALESE' THEN 7 WHEN 'ARABIC' THEN 8 WHEN 'CANTONESE' THEN 9
        WHEN 'MANDARIN' THEN 10 WHEN 'ESTONIAN' THEN 11 ELSE 12
      END;
    ELSE
      v_preferred_language_id := NULL;
    END IF;

    IF rec.LANGUAGE_KNOWN IS NOT NULL THEN
      v_language_known_id := CASE UPPER(TRIM(rec.LANGUAGE_KNOWN))
        WHEN 'ENGLISH' THEN 1 WHEN 'SPANISH' THEN 2 WHEN 'RUSSIAN' THEN 3
        WHEN 'SOMALI' THEN 4 WHEN 'LAOTIAN' THEN 5 WHEN 'SERBIAN' THEN 6
        WHEN 'NEPALESE' THEN 7 WHEN 'ARABIC' THEN 8 WHEN 'CANTONESE' THEN 9
        WHEN 'MANDARIN' THEN 10 WHEN 'ESTONIAN' THEN 11 ELSE 12
      END;
    ELSE
      v_language_known_id := NULL;
    END IF;

    -- ZIP/CITY/COUNTY lookup by county code (fixes self-comparison)
    IF rec.COUNTY_CODE IS NOT NULL THEN
      BEGIN
        SELECT z.ZIP_CITY_COUNTY_ID
          INTO v_zip_city_county_id
          FROM PDS_DATA.ZIP_CITY_COUNTY z
         WHERE UPPER(TRIM(z.COUNTY_CODE)) = UPPER(TRIM(rec.COUNTY_CODE))
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          v_zip_city_county_id := NULL;
      END;
    ELSE
      v_zip_city_county_id := NULL;
    END IF;

    INSERT INTO PDS_DATA.PARTICIPANT (
      PIMS_CLIENT_NUMBER, FIRST_NAME, LAST_NAME, MIDDLE_INITIAL, ADDRESS,
      HOME_PHONE, CELL_PHONE, SSN, GENDER, DATE_OF_BIRTH, AGE_ID,
      INTERPRETER_REQUIRED, EMAIL, NICKNAME, VETERAN_STATUS_ID,
      MARITAL_STATUS_ID, CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE,
      VETERAN_COMMENTS, PREFERRED_LANGUAGE_ID, ZIP_CITY_COUNTY_ID
    ) VALUES (
      v_pims_client_number, v_first_name, v_last_name, v_middle_initial, v_address,
      v_home_phone, v_cell_phone, v_ssn, v_gender, v_dob, v_age_id,
      v_interpreter_required, v_email, v_nickname, v_veteran_status_id,
      v_marital_status_id, v_created_by, v_created_date, v_updated_by, v_updated_date,
      v_comments, v_preferred_language_id, v_zip_city_county_id
    );

    v_rec_count := v_rec_count + 1;
    IF MOD(v_rec_count, v_commit_count) = 0 THEN
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Processed participants: ' || v_rec_count);
    END IF;
  END LOOP;

  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    v_error_bt := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    DBMS_OUTPUT.PUT_LINE('ERROR LINE : ' || v_error_bt);
    DBMS_OUTPUT.PUT_LINE('ERROR      : ' || SQLCODE || ' ' || SQLERRM);
    DBMS_OUTPUT.PUT_LINE('Total records processed: ' || v_rec_count);
    ROLLBACK;
END;
/
