SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.DIAGNOSIS_LIST tgt
  USING (
    SELECT
      cd.diagnosis_code,
      SUBSTR(
        MAX(TRIM(cd.diagnosis_details)) KEEP (
          DENSE_RANK LAST ORDER BY
            CASE WHEN TRIM(cd.diagnosis_details) IS NOT NULL THEN 1 ELSE 0 END,
            LENGTH(TRIM(cd.diagnosis_details))
        ),
        1, 255
      ) AS expanded_value
    FROM STG_ODA.CLIENT_DIAGNOSIS cd
    WHERE cd.diagnosis_code IS NOT NULL
    GROUP BY cd.diagnosis_code
  ) src
  ON (tgt.diagnosis_code = src.diagnosis_code)

  WHEN MATCHED THEN
    UPDATE SET tgt.expanded_value = src.expanded_value

  WHEN NOT MATCHED THEN
    INSERT (expanded_value, diagnosis_code)
    VALUES (src.expanded_value, src.diagnosis_code);

  v_rows := SQL%ROWCOUNT;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('DIAGNOSIS_LIST rows merged: '||v_rows);
END;
/
