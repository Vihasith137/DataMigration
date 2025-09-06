SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.DENTAL_SCREENING tgt
  USING (
    SELECT
      agg.assessment_id,
      tt.teeth_type_id,
      agg.has_broken_teeth,
      agg.has_missing_teeth,
      agg.is_active,
      agg.created_date,
      agg.updated_date,
      agg.created_by,
      agg.updated_by
    FROM (
      SELECT
        a.assessment_id,

        /* ---- derive teeth type name from condition_name codes ----
           49/50 => Artificial, 64 => Natural, both => Both, else => None */
        CASE
          WHEN MAX(CASE WHEN TRIM(d.condition_name) IN ('49','50') THEN 1 END) = 1
           AND MAX(CASE WHEN TRIM(d.condition_name) = '64' THEN 1 END) = 1
            THEN 'Both'
          WHEN MAX(CASE WHEN TRIM(d.condition_name) IN ('49','50') THEN 1 END) = 1
            THEN 'Artificial'
          WHEN MAX(CASE WHEN TRIM(d.condition_name) = '64' THEN 1 END) = 1
            THEN 'Natural'
          ELSE 'None'
        END AS teeth_type_name,

        CASE WHEN MAX(CASE WHEN TRIM(d.condition_name) = '52' THEN 1 END) = 1 THEN 'Y' END AS has_broken_teeth,
        CASE WHEN MAX(CASE WHEN TRIM(d.condition_name) = '51' THEN 1 END) = 1 THEN 'Y' END AS has_missing_teeth,

        a.is_active,
        a.created_date,
        a.updated_date,
        a.created_by,
        a.updated_by
      FROM STG_ODA.assessment_cond_det d
      JOIN STG_ODA.assessment_cond_sys s
        ON s.assessment_cond_sys_id = d.assessment_cond_sys_id
      JOIN LOC_DATA.assessment a
        ON a.pims_assessment_number = s.assessment_number
 
      GROUP BY
        a.assessment_id,
        a.is_active, a.created_date, a.updated_date, a.created_by, a.updated_by
    ) agg
    JOIN LOC_DATA.teeth_type tt
      ON tt.teeth_type_name = agg.teeth_type_name
  ) src
  ON (tgt.assessment_id = src.assessment_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.teeth_type_id     = src.teeth_type_id,
      tgt.has_broken_teeth  = src.has_broken_teeth,
      tgt.has_missing_teeth = src.has_missing_teeth,
      tgt.is_active         = src.is_active,
      tgt.created_date      = src.created_date,
      tgt.updated_date      = src.updated_date,
      tgt.created_by        = src.created_by,
      tgt.updated_by        = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      teeth_type_id,
      has_broken_teeth,
      has_missing_teeth,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.teeth_type_id,
      src.has_broken_teeth,
      src.has_missing_teeth,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('DENTAL_SCREENING rows merged: ' || v_rows);
END;
/
