SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO PDS_DATA.PARTICIPANT_LANGUAGE_XREF tgt
  USING (
    WITH src0 AS (
      SELECT
        p.participant_id,
        CASE
          WHEN REGEXP_LIKE(cb.language_known, '^\s*ENGLISH\s*$',  'i') THEN 1
          WHEN REGEXP_LIKE(cb.language_known, '^\s*SPAN(ISH)?\s*$', 'i') THEN 2
          WHEN REGEXP_LIKE(cb.language_known, '^\s*RUSSIAN\s*$',  'i') THEN 3
          WHEN REGEXP_LIKE(cb.language_known, '^\s*SOMALI\s*$',   'i') THEN 4
          WHEN REGEXP_LIKE(cb.language_known, '^\s*LAOTI?AN\s*$', 'i') THEN 5
          WHEN REGEXP_LIKE(cb.language_known, '^\s*SERBIAN\s*$',  'i') THEN 6
          WHEN REGEXP_LIKE(cb.language_known, '^\s*NEPALESE\s*$', 'i') THEN 7
          WHEN REGEXP_LIKE(cb.language_known, '^\s*ARABIC\s*$',   'i') THEN 8
          WHEN REGEXP_LIKE(cb.language_known, '^\s*CANTONESE\s*$', 'i') THEN 9
          WHEN REGEXP_LIKE(cb.language_known, '^\s*MANDARIN\s*$',  'i') THEN 10
          WHEN REGEXP_LIKE(cb.language_known, '^\s*ESTONIAN\s*$',  'i') THEN 11
          WHEN TRIM(cb.language_known) IS NOT NULL THEN 12 
          ELSE NULL
        END AS language_id,
        /* audit from participant */
        p.is_active,
        p.created_date,
        p.updated_date,
        p.created_by,
        p.updated_by
      FROM STG_ODA.CLIENT_BASE cb
      JOIN PDS_DATA.PARTICIPANT p
        ON p.pims_client_number = cb.client_number
 
    ),
    src AS ( 
      SELECT DISTINCT
             participant_id,
             language_id,
             is_active, created_date, updated_date, created_by, updated_by
      FROM src0
      WHERE language_id IS NOT NULL
    )
    SELECT * FROM src
  ) src
  ON (tgt.participant_id = src.participant_id
      AND tgt.language_id = src.language_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      participant_id,
      language_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.participant_id,
      src.language_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('PARTICIPANT_LANGUAGE_XREF rows merged: ' || v_rows);
END;
/
