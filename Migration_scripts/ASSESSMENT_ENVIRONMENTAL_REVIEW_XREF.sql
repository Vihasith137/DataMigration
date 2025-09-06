SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN

  MERGE INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF tgt
  USING (
    WITH src AS (
      SELECT
        a.assessment_id,
        ab.environment_comment,
        a.is_active,
        a.created_date,
        a.updated_date,
        a.created_by,
        a.updated_by,
        UPPER(NVL(ab.environment_comment, '')) AS c    
      FROM STG_ODA.ASSESSMENT_BASE ab
      JOIN LOC_DATA.ASSESSMENT a
        ON a.pims_assessment_number = ab.assessment_number
      WHERE ab.environment_comment IS NOT NULL
     
       --  AND ab.assessment_number = 10482404
     
    ),
    matches AS (
      /* 1 Neighborhood Safety */
      SELECT s.assessment_id, 1 AS environmental_review_id, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%NEIGHBORHOOD%' OR s.c LIKE '%SAFETY%'

      UNION ALL
      /* 2 Fire plan */
      SELECT s.assessment_id, 2, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%FIRE PLAN%' OR (s.c LIKE '%FIRE%' AND s.c NOT LIKE '%SMOKE%')

      UNION ALL
      /* 3 Carbon monoxide detector */
      SELECT s.assessment_id, 3, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%CARBON MONOXIDE%' OR s.c LIKE '%CO DETECTOR%'

      UNION ALL
      /* 4 Smoke alarm  */
      SELECT s.assessment_id, 4, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%SMOKE ALARM%' OR s.c LIKE '%SMOKE DETECTOR%'

      UNION ALL
      /* 5 Knowledge of infection control techniques */
      SELECT s.assessment_id, 5, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%INFECTION CONTROL%'

      UNION ALL
      /* 6 Individuals who smoke  */
      SELECT s.assessment_id, 6, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE (s.c LIKE '%SMOKE%' OR s.c LIKE '%SMOKER%' OR s.c LIKE '%SMOKING%')
        AND s.c NOT LIKE '%SMOKE ALARM%'
        AND s.c NOT LIKE '%SMOKE DETECTOR%'

      UNION ALL
      /* 7 First aid kit */
      SELECT s.assessment_id, 7, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%FIRST AID%'

      UNION ALL
      /* 8 Weapons secured in home */
      SELECT s.assessment_id, 8, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%WEAPON%' OR s.c LIKE '%GUN%'

      UNION ALL
      /* 9 Oxygen storage */
      SELECT s.assessment_id, 9, s.environment_comment,
             s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
      FROM src s
      WHERE s.c LIKE '%OXYGEN%'
    ),
    -- De-duplicate in case multiple clauses matched the same (assessment, id)
    src_final AS (
      SELECT DISTINCT
        m.assessment_id,
        m.environmental_review_id,
        m.environment_comment,
        m.is_active, m.created_date, m.updated_date, m.created_by, m.updated_by
      FROM matches m
    )
    SELECT
      assessment_id,
      environmental_review_id,
      environment_comment AS comments,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    FROM src_final
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.environmental_review_id = src.environmental_review_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.comments     = src.comments,
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      environmental_review_id,
      comments,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.environmental_review_id,
      src.comments,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF rows merged: '||v_rows);
END;
/
