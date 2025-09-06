SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF tgt
  USING (
    SELECT
      a.assessment_id AS assessment_id,
      era.area_id     AS area_id,
      CASE
        WHEN UPPER(TRIM(ab.rss_stairs_using_ability_ind)) IN ('Y','YES','1','TRUE','T')
          THEN 'Y'
        ELSE 'N'
      END          AS is_accessibility_issue_identified,
      /* audit values from assessment */
      a.is_active    AS is_active,
      a.created_date AS created_date,
      a.updated_date AS updated_date,
      a.created_by   AS created_by,
      a.updated_by   AS updated_by
    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    JOIN LOC_DATA.ENVIRONMENTAL_REVIEW_AREA era
      ON UPPER(era.area_name) = 'STAIRS'
    -- WHERE ab.assessment_number = 36943224   
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.area_id    = src.area_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_accessibility_issue_identified = src.is_accessibility_issue_identified,
      tgt.is_active                         = src.is_active,
      tgt.created_date                      = src.created_date,
      tgt.updated_date                      = src.updated_date,
      tgt.created_by                        = src.created_by,
      tgt.updated_by                        = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      area_id,
      is_accessibility_issue_identified,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.area_id,
      src.is_accessibility_issue_identified,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('Rows merged into ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF: '||v_rows);
END;
/
