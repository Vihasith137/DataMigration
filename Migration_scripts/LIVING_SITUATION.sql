SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.LIVING_SITUATION tgt
  USING (
    SELECT
      a.assessment_id AS assessment_id,

      /* Map LIV_ARRGMT_CURRENT -> DOMAIN_VALUE (expanded_value) -> RESIDENCE_NAME(residence_type_id).
         If no match, default to 17.
      */
      NVL(rn.residence_type_id, 17) AS residence_type_id,

      /* audit from assessment */
      a.is_active    AS is_active,
      a.created_date AS created_date,
      a.updated_date AS updated_date,
      a.created_by   AS created_by,
      a.updated_by   AS updated_by
    FROM STG_ODA.ASSESSMENT_BASE ab
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    LEFT JOIN (
      SELECT UPPER(TRIM(value)) AS value_key,
             UPPER(TRIM(expanded_value)) AS expanded_key
      FROM STG_ODA.DOMAIN_VALUE
      WHERE UPPER(name) LIKE 'LIVING ARRANGEMENTS%'
    ) dv
      ON dv.value_key = UPPER(TRIM(ab.liv_arrgmt_current))
    LEFT JOIN LOC_DATA.RESIDENCE_TYPE rn
      ON UPPER(TRIM(rn.residence_name)) = dv.expanded_key
    WHERE ab.liv_arrgmt_current IS NOT NULL
    /* -- quick test for one assessment:
       AND ab.assessment_number = 36943224
    */
  ) src
  ON (tgt.assessment_id = src.assessment_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.residence_type_id = src.residence_type_id,
      tgt.is_active         = src.is_active,
      tgt.created_date      = src.created_date,
      tgt.updated_date      = src.updated_date,
      tgt.created_by        = src.created_by,
      tgt.updated_by        = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      residence_type_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.residence_type_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('LIVING_SITUATION rows merged: '||v_rows);
END;
/
