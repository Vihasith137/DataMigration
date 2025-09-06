SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows PLS_INTEGER := 0;
BEGIN
  MERGE INTO LOC_DATA.PROVIDER tgt
  USING (
    SELECT
      a.assessment_id                                                        AS assessment_id,

      NVL(NULLIF(TRIM(hp.first_name), ''), 'UNKNOWN')                        AS first_name,
      NVL(NULLIF(TRIM(hp.last_name),  ''), 'UNKNOWN')                        AS last_name,
      NVL(NULLIF(TRIM(hp.SPECIALTY),       ''), 'UNKNOWN')                        AS type,
      hp.address                                                             AS address,
      hp.city                                                                AS city,
      hp.state                                                               AS state,
      hp.zip                                                            AS zip_code,
      hp.phone                                                           AS phone_primary,
      hp.fax                                                                 AS fax,
      hp.email                                                               AS email,
      hp.EFFECTIVE_DATE                                                      AS last_seen_date,
      CASE
        WHEN UPPER(NVL(hp.PRIM_PHYS_FOR_CLIENT_IND,'Y')) IN ('Y','YES','1','TRUE','T')
          THEN 'Y'
        ELSE 'N'
      END                                                                    AS is_primary_care_provider,

      /* audit from assessment */
      a.is_active                                                            AS is_active,
      a.created_date                                                         AS created_date,
      a.updated_date                                                         AS updated_date,
      a.created_by                                                           AS created_by,
      a.updated_by                                                           AS updated_by
    FROM STG_ODA.HEALTH_CARE_PROF hp
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.assessment_number = hp.assessment_number        
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number     
    WHERE ab.assessment_number = 12829897
  ) src
  ON (
       tgt.assessment_id = src.assessment_id
   AND UPPER(tgt.first_name) = UPPER(src.first_name)
   AND UPPER(tgt.last_name)  = UPPER(src.last_name)
   AND UPPER(NVL(tgt.type,'UNKNOWN')) = UPPER(NVL(src.type,'UNKNOWN'))
  )
  WHEN MATCHED THEN
    UPDATE SET

      tgt.address                  = src.address,
      tgt.city                     = src.city,
      tgt.state                    = src.state,
      tgt.zip_code                 = src.zip_code,
      tgt.phone_primary            = src.phone_primary,
      tgt.fax                      = src.fax,
      tgt.email                    = src.email,
      tgt.last_seen_date           = src.last_seen_date,
      tgt.is_primary_care_provider = src.is_primary_care_provider,
      tgt.is_active                = src.is_active,
      tgt.created_date             = src.created_date,
      tgt.updated_date             = src.updated_date,
      tgt.created_by               = src.created_by,
      tgt.updated_by               = src.updated_by

  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      first_name,
      last_name,
      type,
      address,
      city,
      state,
      zip_code,
      phone_primary,
      fax,
      email,
      last_seen_date,
      is_primary_care_provider,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.assessment_id,
      src.first_name,
      src.last_name,
      src.type,
      src.address,
      src.city,
      src.state,
      src.zip_code,
      src.phone_primary,
      src.fax,
      src.email,
      src.last_seen_date,
      src.is_primary_care_provider,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('PROVIDER rows merged: '||v_rows);
END;
/
