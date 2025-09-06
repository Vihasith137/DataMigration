SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_SOURCE_OF_ASSESSMENT_XREF tgt
  USING (
    SELECT
      a.assessment_id                                                        AS assessment_id,

      ab.source_id                                                          as SOURCE_OF_ASSESSMENT_ID,
      a.is_active                                                            AS is_active,
      a.created_date                                                         AS created_date,
      a.updated_date                                                         AS updated_date,
      a.created_by                                                           AS created_by,
      a.updated_by                                                           AS updated_by
    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number         
  --  WHERE ab.assessment_number = 783681494                     -- <<< change to your test value
  ) src
  ON (
       tgt.assessment_id = src.assessment_id
  )
  WHEN MATCHED THEN
    UPDATE SET

      tgt.SOURCE_OF_ASSESSMENT_ID  = src.SOURCE_OF_ASSESSMENT_ID,
      tgt.is_active                = src.is_active,
      tgt.created_date             = src.created_date,
      tgt.updated_date             = src.updated_date,
      tgt.created_by               = src.created_by,
      tgt.updated_by               = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      SOURCE_OF_ASSESSMENT_ID,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
      -- provider_id omitted (auto-generated)
    )
    VALUES (
      src.assessment_id,
      src.SOURCE_OF_ASSESSMENT_ID,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('PROVIDER rows merged: '||v_rows);
END;
/
