SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.PHARMACY tgt
  USING (
    SELECT
      p.PHARMACY_ID                          AS pharmacy_id,
      a.ASSESSMENT_ID                        AS assessment_id,
      p.PHARMACY_NAME                        AS pharmacy_name,
      p.PHONE                                AS phone_number,
--         p.FAX                               AS fax_number, */
      p.ADDRESS                              AS address,
      p.CITY                                 AS city,
      p.STATE                                AS state,
      p.ZIP                                  AS zip_code,
      a.IS_ACTIVE                           AS is_active,
      a.CREATED_DATE                        AS created_date,
      a.UPDATED_DATE                        AS updated_date,
      a.CREATED_BY                          AS created_by,
      a.UPDATED_BY                          AS updated_by
    FROM STG_ODA.PHARMACY p
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.ASSESSMENT_NUMBER = p.ASSESSMENT_NUMBER
    JOIN LOC_DATA.ASSESSMENT a
      ON a.PIMS_ASSESSMENT_NUMBER = ab.ASSESSMENT_NUMBER
    WHERE p.ASSESSMENT_NUMBER = 23713203          
  ) src
  ON (
    tgt.ASSESSMENT_ID = src.assessment_id
    
  )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.PHARMACY_NAME = src.pharmacy_name,
      tgt.PHONE_NUMBER  = src.phone_number,
      /* tgt.FAX_NUMBER = src.fax_number, */
      tgt.ADDRESS       = src.address,
      tgt.CITY          = src.city,
      tgt.STATE         = src.state,
      tgt.ZIP_CODE      = src.zip_code,
      tgt.IS_ACTIVE     = src.is_active,
      tgt.CREATED_DATE  = src.created_date,
      tgt.UPDATED_DATE  = src.updated_date,
      tgt.CREATED_BY    = src.created_by,
      tgt.UPDATED_BY    = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      
      ASSESSMENT_ID,
      PHARMACY_NAME,
      PHONE_NUMBER,
      /* FAX_NUMBER, */
      ADDRESS,
      CITY,
      STATE,
      ZIP_CODE,
      IS_ACTIVE,
      CREATED_DATE,
      UPDATED_DATE,
      CREATED_BY,
      UPDATED_BY
    )
    VALUES (
      
      src.assessment_id,
      src.pharmacy_name,
      src.phone_number,
      /* src.fax_number, */
      src.address,
      src.city,
      src.state,
      src.zip_code,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('PHARMACY rows merged: '||v_rows);
END;
/
