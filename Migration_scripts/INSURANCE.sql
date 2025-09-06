SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.INSURANCE tgt
  USING (
    SELECT
      a.assessment_id                                        AS assessment_id,

      /* Direct mappings */
      cb.medicaid_number                                     AS medicaid_number,
      cb.ob_case_number                                      AS ohio_benefits_number,

      /* Medicare parts: only set when the number contains the letter */
      CASE WHEN cb.medicare_number IS NOT NULL
             AND INSTR(UPPER(cb.medicare_number), 'A') > 0
           THEN 'Y' ELSE 'N'
      END                                                    AS medicare_part_a,
      CASE WHEN cb.medicare_number IS NOT NULL
             AND INSTR(UPPER(cb.medicare_number), 'B') > 0
           THEN 'Y' ELSE 'N'
      END                                                    AS medicare_part_b,
      CASE WHEN cb.medicare_number IS NOT NULL
             AND INSTR(UPPER(cb.medicare_number), 'D') > 0
           THEN 'Y' ELSE 'N'
      END                                                    AS medicare_part_d,

      /* audit from assessment */
      a.is_active                                            AS is_active,
      a.created_date                                         AS created_date,
      a.updated_date                                         AS updated_date,
      a.created_by                                           AS created_by,
      a.updated_by                                           AS updated_by
    FROM STG_ODA.CLIENT_BASE cb
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.client_number = cb.client_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    /* --- For testing one record, uncomment and put a number:
       WHERE ab.assessment_number = 36943224
    */
  ) src
  ON (tgt.assessment_id = src.assessment_id)

  WHEN MATCHED THEN
    UPDATE SET
      tgt.medicaid_number        = src.medicaid_number,
      tgt.ohio_benefits_number   = src.ohio_benefits_number,
      tgt.medicare_part_a        = src.medicare_part_a,
      tgt.medicare_part_b        = src.medicare_part_b,
      tgt.medicare_part_d        = src.medicare_part_d,
      tgt.is_active              = src.is_active,
      tgt.created_date           = src.created_date,
      tgt.updated_date           = src.updated_date,
      tgt.created_by             = src.created_by,
      tgt.updated_by             = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      medicaid_number,
      medicare_part_a,
      ohio_benefits_number,
      medicare_part_b,
      medicare_part_d,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.medicaid_number,
      src.medicare_part_a,
      src.ohio_benefits_number,
      src.medicare_part_b,
      src.medicare_part_d,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('INSURANCE rows merged: '||v_rows);
END;
/
