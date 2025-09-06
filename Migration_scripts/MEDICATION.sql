SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.MEDICATION tgt
  USING (
    SELECT
      a.assessment_id                                       AS assessment_id,
      NULLIF(TRIM(m.medication_name), '')                   AS medication_name,
      CASE
        WHEN UPPER(TRIM(m.rx_otc_ind)) IN ('RX','PRESCRIPTION','PRESC','SCRIPT') THEN 'RX'
        WHEN UPPER(TRIM(m.rx_otc_ind)) IN ('OTC','OVER THE COUNTER','OVER-THE-COUNTER','NON PRESCRIPTION','NON-PRESCRIPTION') THEN 'OTC'
        WHEN UPPER(TRIM(m.rx_otc_ind)) LIKE '%RX%'  THEN 'RX'
        WHEN UPPER(TRIM(m.rx_otc_ind)) LIKE '%OTC%' THEN 'OTC'
        ELSE NULL
      END                                                    AS drug_type,
      TRIM(m.dose_freq)                                     AS dose,
      a.is_active                                           AS is_active,
      a.created_date                                        AS created_date,
      a.updated_date                                        AS updated_date,
      a.created_by                                          AS created_by,
      a.updated_by                                          AS updated_by
    FROM STG_ODA.MEDICATION m
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = m.assessment_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE TRIM(m.medication_name) IS NOT NULL
  ) src
  ON (tgt.assessment_id   = src.assessment_id
      AND tgt.medication_name = src.medication_name
      AND NVL(tgt.dose,'#')    = NVL(src.dose,'#'))
  WHEN MATCHED THEN
    UPDATE SET
      tgt.drug_type     = src.drug_type,
      tgt.is_active     = src.is_active,
      tgt.created_date  = src.created_date,
      tgt.updated_date  = src.updated_date,
      tgt.created_by    = src.created_by,
      tgt.updated_by    = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      medication_name,
      drug_type,
      dose,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.medication_name,
      src.drug_type,
      src.dose,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('MEDICATION rows merged: '||v_rows);
END;
/
