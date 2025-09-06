SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ADL_OBSERVATION tgt
  USING (
    SELECT
      a.assessment_id                                 AS assessment_id,
      ab.adl_iadl_comment                             AS observation_notes,
      a.is_active                                     AS is_active,
      a.created_date                                  AS created_date,
      a.updated_date                                  AS updated_date,
      a.created_by                                    AS created_by,
      a.updated_by                                    AS updated_by
    FROM STG_ODA.SCORED_ACTIVITY sa
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = sa.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE UPPER(TRIM(sa.scored_activity_type)) = 'ADL'
      AND TRIM(ab.adl_iadl_comment) IS NOT NULL
   
    GROUP BY
      a.assessment_id,
      ab.adl_iadl_comment,
      a.is_active, a.created_date, a.updated_date, a.created_by, a.updated_by
  ) src
  ON (tgt.assessment_id = src.assessment_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.observation_notes = src.observation_notes,
      tgt.is_active         = src.is_active,
      tgt.created_date      = src.created_date,
      tgt.updated_date      = src.updated_date,
      tgt.created_by        = src.created_by,
      tgt.updated_by        = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      observation_notes,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.observation_notes,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ADL_OBSERVATION rows merged: '||v_rows);
END;
/
