SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
  v_rows_contact          PLS_INTEGER := 0;
  v_rows_rel_xref         PLS_INTEGER := 0;
  v_rows_legal_rel_xref   PLS_INTEGER := 0;
BEGIN
  /* --------------------- CONTACT --------------------- */
  MERGE INTO LOC_DATA.CONTACT tgt
  USING (
    SELECT
      a.assessment_id,
      NVL(NULLIF(TRIM(c.last_name),  ''), 'UNKNOWN')   AS last_name,
      NVL(NULLIF(TRIM(c.first_name), ''), 'UNKNOWN')   AS first_name,
      c.address                                        AS address,
      c.city                                           AS city,
      c.state                                          AS state,
      c.zip                                            AS zip_code,
      cb.email_address                                 AS email,
      cb.building_name                                 AS apartment_unit,
      cb.county_code                                   AS county,
      cb.phone                                         AS phone_mobile,
      CASE
        WHEN UPPER(NVL(c.active_ind, 'Y')) IN ('Y','YES','1','TRUE','T') THEN 1
        ELSE 0
      END                                              AS contact_status_id,
      a.is_active,
      a.created_date,
      a.updated_date,
      a.created_by,
      a.updated_by
    FROM STG_ODA.CONTACT c
    JOIN STG_ODA.CLIENT_BASE cb
      ON cb.client_number = c.client_number
    JOIN STG_ODA.ASSESSMENT_BASE ab
      ON ab.client_number = cb.client_number
    JOIN LOC_DATA.ASSESSMENT a
      ON a.pims_assessment_number = ab.assessment_number
    WHERE ab.assessment_number = 433158553
  ) src
  ON (
       tgt.assessment_id = src.assessment_id
   AND tgt.last_name     = src.last_name
   AND tgt.first_name    = src.first_name
  )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.address            = src.address,
      tgt.city               = src.city,
      tgt.state              = src.state,
      tgt.zip_code           = src.zip_code,
      tgt.contact_status_id  = src.contact_status_id,
      tgt.is_active          = src.is_active,
      tgt.created_date       = src.created_date,
      tgt.updated_date       = src.updated_date,
      tgt.created_by         = src.created_by,
      tgt.updated_by         = src.updated_by,
      tgt.email              = src.email,
      tgt.apartment_unit     = src.apartment_unit,
      tgt.county             = src.county,
      tgt.phone_mobile       = src.phone_mobile
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id,
      last_name,
      first_name,
      address,
      city,
      state,
      zip_code,
      contact_status_id,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by,
      email,
      apartment_unit,
      county,
      phone_mobile
    )
    VALUES (
      src.assessment_id,
      src.last_name,
      src.first_name,
      src.address,
      src.city,
      src.state,
      src.zip_code,
      src.contact_status_id,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by,
      src.email,
      src.apartment_unit,
      src.county,
      src.phone_mobile
    );

  v_rows_contact := SQL%ROWCOUNT;

  /* ------- ASSESSMENT_CONTACT_RELATIONSHIP_XREF ------- */
  MERGE INTO LOC_DATA.ASSESSMENT_CONTACT_RELATIONSHIP_XREF tgt
  USING (
    WITH base AS (
      SELECT
        a.assessment_id,
        NVL(NULLIF(TRIM(c.last_name),  ''), 'UNKNOWN') AS last_name,
        NVL(NULLIF(TRIM(c.first_name), ''), 'UNKNOWN') AS first_name,
        c.client_relationship,
        a.is_active, a.created_date, a.updated_date, a.created_by, a.updated_by
      FROM STG_ODA.CONTACT c
      JOIN STG_ODA.CLIENT_BASE cb  ON cb.client_number = c.client_number
      JOIN STG_ODA.ASSESSMENT_BASE ab ON ab.client_number = cb.client_number
      JOIN LOC_DATA.ASSESSMENT a ON a.pims_assessment_number = ab.assessment_number
      WHERE ab.assessment_number = 433158553
        AND c.client_relationship IS NOT NULL
    ),
    rel_map AS (
      SELECT b.assessment_id, b.last_name, b.first_name,
             CASE
               WHEN UPPER(b.client_relationship) LIKE '%MOTHER%' OR UPPER(b.client_relationship) LIKE '%FATHER%' THEN
                 (SELECT relationship_id FROM LOC_DATA.RELATIONSHIP_TYPE WHERE UPPER(relationship_name)='PARENT')
               WHEN UPPER(b.client_relationship) LIKE '%HUSBAND%' OR UPPER(b.client_relationship) LIKE '%WIFE%' THEN
                 (SELECT relationship_id FROM LOC_DATA.RELATIONSHIP_TYPE WHERE UPPER(relationship_name)='SPOUSE')
               WHEN UPPER(b.client_relationship) LIKE '%BROTHER%' OR UPPER(b.client_relationship) LIKE '%SISTER%' THEN
                 (SELECT relationship_id FROM LOC_DATA.RELATIONSHIP_TYPE WHERE UPPER(relationship_name)='SIBLING')
               ELSE
                 (SELECT relationship_id FROM LOC_DATA.RELATIONSHIP_TYPE WHERE UPPER(relationship_name)='OTHER')
             END AS relationship_id,
             b.is_active, b.created_date, b.updated_date, b.created_by, b.updated_by
      FROM base b
    ),
    join_contact AS (
      SELECT r.assessment_id, c.contact_id, r.relationship_id,
             r.is_active, r.created_date, r.updated_date, r.created_by, r.updated_by
      FROM rel_map r
      JOIN LOC_DATA.CONTACT c
        ON c.assessment_id = r.assessment_id
       AND c.last_name     = r.last_name
       AND c.first_name    = r.first_name
    )
    SELECT * FROM join_contact
  ) src
  ON (tgt.assessment_id   = src.assessment_id
      AND tgt.contact_id  = src.contact_id
      AND tgt.relationship_id = src.relationship_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      assessment_id, contact_id, relationship_id,
      is_active, created_date, updated_date, created_by, updated_by
    )
    VALUES (
      src.assessment_id, src.contact_id, src.relationship_id,
      src.is_active, src.created_date, src.updated_date, src.created_by, src.updated_by
    );

  v_rows_rel_xref := SQL%ROWCOUNT;

  /* --- ASSESSMENT_CONTACT_LEGAL_RELATIONSHIP_XREF --- */
  MERGE INTO LOC_DATA.ASSESSMENT_CONTACT_LEGAL_RELATIONSHIP_XREF tgt
  USING (
    /* Build one row per 'legal relationship' flag that is truthy */
    WITH base AS (
      SELECT
        a.assessment_id,
        NVL(NULLIF(TRIM(c.last_name),  ''), 'UNKNOWN') AS last_name,
        NVL(NULLIF(TRIM(c.first_name), ''), 'UNKNOWN') AS first_name,
        c.direct_care_ind,
        c.poa_ind,
        c.guardian_ind,
        c.financial_resp_ind,
        c.auth_rep_ind,
        a.is_active, a.created_date, a.updated_date, a.created_by, a.updated_by
      FROM STG_ODA.CONTACT c
      JOIN STG_ODA.CLIENT_BASE cb  ON cb.client_number  = c.client_number
      JOIN STG_ODA.ASSESSMENT_BASE ab ON ab.client_number = cb.client_number
      JOIN LOC_DATA.ASSESSMENT a ON a.pims_assessment_number = ab.assessment_number
      WHERE ab.assessment_number = 433158553
    ),
    flags AS (
      /* map each YES flag to its LEGAL_RELATIONSHIP_ID */
      SELECT assessment_id, last_name, first_name,
             1 AS legal_relationship_id,  /* Primary Caregiver */
             is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(direct_care_ind,'N'))) IN ('Y','YES','1','TRUE','T')
      UNION ALL
      SELECT assessment_id, last_name, first_name,
             6 AS legal_relationship_id,  /* Power of Attorney – Healthcare */
             is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(poa_ind,'N'))) IN ('Y','YES','1','TRUE','T')
      UNION ALL
      SELECT assessment_id, last_name, first_name,
             8 AS legal_relationship_id,  /* Legal Guardian */
             is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(guardian_ind,'N'))) IN ('Y','YES','1','TRUE','T')
      UNION ALL
      SELECT assessment_id, last_name, first_name,
             5 AS legal_relationship_id,  /* Representative Payee (Financial responsibility) */
             is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(financial_resp_ind,'N'))) IN ('Y','YES','1','TRUE','T')
      UNION ALL
      SELECT assessment_id, last_name, first_name,
             3 AS legal_relationship_id,  /* Authorized Representative */
             is_active, created_date, updated_date, created_by, updated_by
      FROM base
      WHERE UPPER(TRIM(NVL(auth_rep_ind,'N'))) IN ('Y','YES','1','TRUE','T')
      /* If you want a fallback "Other = 9" when none are Y, we can add it—left out per request */
    ),
    join_contact AS (
      SELECT
        f.assessment_id,
        c.contact_id,
        f.legal_relationship_id,
        CAST(NULL AS VARCHAR2(255)) AS other_legal_relationship_type,
        f.is_active, f.created_date, f.updated_date, f.created_by, f.updated_by
      FROM flags f
      JOIN LOC_DATA.CONTACT c
        ON c.assessment_id = f.assessment_id
       AND c.last_name     = f.last_name
       AND c.first_name    = f.first_name
    ),
    dedup AS (  -- prevent PK/UK collisions
      SELECT DISTINCT
             assessment_id, contact_id, legal_relationship_id,
             other_legal_relationship_type,
             is_active, created_date, updated_date, created_by, updated_by
      FROM join_contact
    )
    SELECT * FROM dedup
  ) src
  ON (tgt.assessment_id = src.assessment_id
      AND tgt.contact_id = src.contact_id
      AND tgt.legal_relationship_id = src.legal_relationship_id)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.other_legal_relationship_type = src.other_legal_relationship_type,
      tgt.is_active    = src.is_active,
      tgt.created_date = src.created_date,
      tgt.updated_date = src.updated_date,
      tgt.created_by   = src.created_by,
      tgt.updated_by   = src.updated_by
  WHEN NOT MATCHED THEN
    INSERT (
      contact_id,
      legal_relationship_id,
      assessment_id,
      other_legal_relationship_type,
      is_active,
      created_date,
      updated_date,
      created_by,
      updated_by
    )
    VALUES (
      src.contact_id,
      src.legal_relationship_id,
      src.assessment_id,
      src.other_legal_relationship_type,
      src.is_active,
      src.created_date,
      src.updated_date,
      src.created_by,
      src.updated_by
    );

  v_rows_legal_rel_xref := SQL%ROWCOUNT;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('CONTACT rows merged: '|| v_rows_contact);
  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_CONTACT_RELATIONSHIP_XREF rows merged: '|| v_rows_rel_xref);
  DBMS_OUTPUT.PUT_LINE('ASSESSMENT_CONTACT_LEGAL_RELATIONSHIP_XREF rows merged: '|| v_rows_legal_rel_xref);
END;
/
