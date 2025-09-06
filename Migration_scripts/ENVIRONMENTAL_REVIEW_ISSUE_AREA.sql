SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows_review  PLS_INTEGER := 0;
  v_rows_issue   PLS_INTEGER := 0;
BEGIN

  MERGE INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF tgt
  USING (

    SELECT *
    FROM (
      SELECT
        a.assessment_id,
        er.environmental_review_id,

      
        CASE
          WHEN UPPER(TRIM(d.env_rev_observ)) = 'ADEQUATE' THEN 'Y'
          ELSE NULL
        END AS is_acceptable,
        CASE
          WHEN UPPER(TRIM(d.env_rev_observ)) <> 'ADEQUATE' THEN 'N'
          ELSE NULL
        END AS is_repair_identified,

        a.is_active,
        a.created_date,
        a.updated_date,
        a.created_by,
        a.updated_by,

        ROW_NUMBER() OVER (
          PARTITION BY a.assessment_id, er.environmental_review_id
          ORDER BY NVL(a.updated_date, a.created_date) DESC
        ) AS rn
      FROM STG_ODA.ENVIRONMENTAL_REV_DET d
      JOIN STG_ODA.ASSESSMENT_BASE ab
        ON ab.assessment_number = d.assessment_number
      JOIN LOC_DATA.ASSESSMENT a
        ON a.pims_assessment_number = ab.assessment_number

      JOIN LOC_DATA.ENVIRONMENTAL_REVIEW er
        ON UPPER(er.environmental_review_type) = UPPER(
             CASE
               WHEN d.env_rev_area LIKE '%CarbonMono%' THEN 'Carbon monoxide detector'
               WHEN d.env_rev_area LIKE '%SmokeAlarm%' THEN 'Smoke alarm'
               ELSE NULL
             END
           )
      WHERE
           (CASE
              WHEN d.env_rev_area LIKE '%CarbonMono%' THEN 'Carbon monoxide detector'
              WHEN d.env_rev_area LIKE '%SmokeAlarm%' THEN 'Smoke alarm'
              ELSE NULL
            END) IS NOT NULL
    ) s
    WHERE s.rn = 1
  ) src
  ON (
       tgt.assessment_id            = src.assessment_id
   AND tgt.environmental_review_id  = src.environmental_review_id
  )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_acceptable        = src.is_acceptable,
      tgt.is_repair_identified = src.is_repair_identified,
      tgt.is_active            = src.is_active,
      tgt.created_date         = src.created_date,
      tgt.updated_date         = src.updated_date,
      tgt.created_by           = src.created_by,
      tgt.updated_by           = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      environmental_review_id,
      is_acceptable,
      is_repair_identified,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.environmental_review_id,
      src.is_acceptable,
      src.is_repair_identified,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows_review := SQL%ROWCOUNT;

 
  MERGE INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF tgt
  USING (
    SELECT *
    FROM (
      SELECT
        a.assessment_id,
        era.area_id,

        CASE
          WHEN UPPER(TRIM(d.env_rev_observ)) = 'ADEQUATE' THEN 'Y'
          ELSE NULL
        END AS is_acceptable,
        CASE
          WHEN UPPER(TRIM(d.env_rev_observ)) <> 'ADEQUATE' THEN 'N'
          ELSE NULL
        END AS is_repair_identified,

        a.is_active,
        a.created_date,
        a.updated_date,
        a.created_by,
        a.updated_by,

        ROW_NUMBER() OVER (
          PARTITION BY a.assessment_id, era.area_id
          ORDER BY NVL(a.updated_date, a.created_date) DESC
        ) AS rn
      FROM STG_ODA.ENVIRONMENTAL_REV_DET d
      JOIN STG_ODA.ASSESSMENT_BASE ab
        ON ab.assessment_number = d.assessment_number
      JOIN LOC_DATA.ASSESSMENT a
        ON a.pims_assessment_number = ab.assessment_number

      JOIN LOC_DATA.ENVIRONMENTAL_REVIEW_AREA era
        ON UPPER(era.area_name) = UPPER(
             CASE
               WHEN d.env_rev_area = 'Floors'                         THEN 'Floors'
               WHEN d.env_rev_area LIKE '%Heating/Co%'                THEN 'Heating/Cooling'
               WHEN d.env_rev_area LIKE '%Stair%'                      THEN 'Stairs'
               WHEN d.env_rev_area = 'Windows'                         THEN 'Windows'
               WHEN d.env_rev_area LIKE '%Electrical%'                 THEN 'Electrical'
               WHEN d.env_rev_area = 'Toilet'                          THEN 'Bathroom(s) Safety'
               WHEN d.env_rev_area LIKE '%Neighborho%'                 THEN 'Neighborhood Safety'
               WHEN d.env_rev_area LIKE '%Chemical%'                   THEN 'Chemical Hazards'
               ELSE NULL
             END
           )
      WHERE
           (CASE
              WHEN d.env_rev_area = 'Floors'                         THEN 'Floors'
              WHEN d.env_rev_area LIKE '%Heating/Co%'                THEN 'Heating/Cooling'
              WHEN d.env_rev_area LIKE '%Stair%'                      THEN 'Stairs'
              WHEN d.env_rev_area = 'Windows'                         THEN 'Windows'
              WHEN d.env_rev_area LIKE '%Electrical%'                 THEN 'Electrical'
              WHEN d.env_rev_area = 'Toilet'                          THEN 'Bathroom(s) Safety'
              WHEN d.env_rev_area LIKE '%Neighborho%'                 THEN 'Neighborhood Safety'
              WHEN d.env_rev_area LIKE '%Chemical%'                   THEN 'Chemical Hazards'
              ELSE NULL
            END) IS NOT NULL
    ) s
    WHERE s.rn = 1
  ) src
  ON (
       tgt.assessment_id = src.assessment_id
   AND tgt.area_id       = src.area_id
  )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_acceptable        = src.is_acceptable,
      tgt.is_repair_identified = src.is_repair_identified,
      tgt.is_active            = src.is_active,
      tgt.created_date         = src.created_date,
      tgt.updated_date         = src.updated_date,
      tgt.created_by           = src.created_by,
      tgt.updated_by           = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      area_id,
      is_acceptable,
      is_repair_identified,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.area_id,
      src.is_acceptable,
      src.is_repair_identified,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows_issue := SQL%ROWCOUNT;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF rows merged: '||v_rows_review);
  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF  rows merged: '||v_rows_issue);
END;
/
