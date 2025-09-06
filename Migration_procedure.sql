CREATE OR REPLACE PROCEDURE MIGRATE_ASSESSMENTS (p_assessment_number IN NUMBER DEFAULT NULL) IS
   v_rows   PLS_INTEGER := 0;
BEGIN
   ---------------------------------------------------------------------------
   -- STEP 0: SAFETY – ensure we can find at least one source row (fast fail)
   ---------------------------------------------------------------------------
   DECLARE
      v_exist NUMBER;
   BEGIN
      SELECT COUNT(*)
        INTO v_exist
        FROM STG_ODA.ASSESSMENT_BASE ab
       WHERE p_assessment_number IS NULL OR ab.assessment_number = p_assessment_number;

      IF v_exist = 0 THEN
         DBMS_OUTPUT.PUT_LINE('No STG_ODA.ASSESSMENT_BASE rows match the filter; nothing to do.');
         RETURN;
      END IF;
   END;

   ---------------------------------------------------------------------------
   -- STEP 1: PARENT TABLE  — LOC_DATA.ASSESSMENT (idempotent MERGE)
   -- Adapted from your ASSESSMENT_TEST loader (now targeting ASSESSMENT):contentReference[oaicite:0]{index=0}
   ---------------------------------------------------------------------------
   MERGE INTO LOC_DATA.ASSESSMENT tgt
   USING (
     SELECT
         ab.assessment_number                            AS assessment_id,          -- using assessment_number as id
         ab.client_number                                AS participant_id,
         2                                               AS form_type_id,
         000                                             AS assessment_agency,
         -- assessor name from WORKER (fallback to 'Unknown' if missing)
         COALESCE( (SELECT w.first_name FROM STG_ODA.WORKER w
                     WHERE w.worker_number = ab.worker_number AND ROWNUM = 1),
                   'Unknown')                             AS assessor_first_name,
         COALESCE( (SELECT w.last_name FROM STG_ODA.WORKER w
                     WHERE w.worker_number = ab.worker_number AND ROWNUM = 1),
                   'Unknown')                             AS assessor_last_name,
         ab.assess_date                                  AS assessment_begin_date,
         ab.original_finalize_date                        AS assessment_finalized_date,
         1                                               AS assessment_type_id,     -- (placeholder per your script)
         CASE UPPER(TRIM(ab.assess_place))
            WHEN 'HOME' THEN 1
            WHEN 'HOSPITAL' THEN 2
            WHEN 'RESIDENTIAL TREATMENT FACILITY(RTF)' THEN 3
            WHEN 'VIRTUAL' THEN 4
            WHEN 'NF' THEN 5
            WHEN 'ICF/IID' THEN 6
            WHEN 'TELEPHONIC' THEN 7
            WHEN 'OTHER' THEN 8
            ELSE NULL
         END                                             AS place_of_assessment_id,
         ab.last_update_worker                           AS updated_by,
         ab.last_update_time                             AS updated_date,
         ab.check_out_worker                             AS created_by,
         ab.create_date                                  AS created_date,
         -- IMPORTANT: children join on this column in your scripts
         ab.assessment_number                            AS pims_assessment_number
     FROM STG_ODA.ASSESSMENT_BASE ab
     WHERE p_assessment_number IS NULL OR ab.assessment_number = p_assessment_number
   ) src
   ON (tgt.assessment_id = src.assessment_id)
   WHEN MATCHED THEN
      UPDATE SET
         tgt.participant_id            = src.participant_id,
         tgt.form_type_id              = src.form_type_id,
         tgt.assessment_agency         = src.assessment_agency,
         tgt.assessor_first_name       = src.assessor_first_name,
         tgt.assessor_last_name        = src.assessor_last_name,
         tgt.assessment_begin_date     = src.assessment_begin_date,
         tgt.assessment_finalized_date = src.assessment_finalized_date,
         tgt.assessment_type_id        = src.assessment_type_id,
         tgt.place_of_assessment_id    = src.place_of_assessment_id,
         tgt.updated_by                = src.updated_by,
         tgt.updated_date              = src.updated_date,
         tgt.created_by                = src.created_by,
         tgt.created_date              = src.created_date,
         tgt.pims_assessment_number    = src.pims_assessment_number
   WHEN NOT MATCHED THEN
      INSERT (
         assessment_id, participant_id, form_type_id, assessment_agency,
         assessor_first_name, assessor_last_name,
         assessment_begin_date, assessment_finalized_date,
         assessment_type_id, place_of_assessment_id,
         updated_by, updated_date, created_by, created_date,
         pims_assessment_number
      )
      VALUES (
         src.assessment_id, src.participant_id, src.form_type_id, src.assessment_agency,
         src.assessor_first_name, src.assessor_last_name,
         src.assessment_begin_date, src.assessment_finalized_date,
         src.assessment_type_id, src.place_of_assessment_id,
         src.updated_by, src.updated_date, src.created_by, src.created_date,
         src.pims_assessment_number
      );

   v_rows := SQL%ROWCOUNT;
   DBMS_OUTPUT.PUT_LINE('ASSESSMENT parent rows merged: '||v_rows);
   COMMIT;

   ---------------------------------------------------------------------------
   -- STEP 2: SOURCE OF ASSESSMENT XREF  — LOC_DATA.ASSESSMENT_SOURCE_OF_ASSESSMENT_XREF:contentReference[oaicite:1]{index=1}
   ---------------------------------------------------------------------------
   MERGE INTO LOC_DATA.ASSESSMENT_SOURCE_OF_ASSESSMENT_XREF tgt
   USING (
     SELECT
       a.assessment_id                                          AS assessment_id,
       ab.source_id                                             AS source_of_assessment_id,
       a.is_active                                              AS is_active,
       a.created_date                                           AS created_date,
       a.updated_date                                           AS updated_date,
       a.created_by                                             AS created_by,
       a.updated_by                                             AS updated_by
     FROM STG_ODA.ASSESSMENT_BASE ab
     JOIN LOC_DATA.ASSESSMENT a
       ON a.pims_assessment_number = ab.assessment_number
     WHERE p_assessment_number IS NULL OR ab.assessment_number = p_assessment_number
   ) src
   ON (tgt.assessment_id = src.assessment_id)
   WHEN MATCHED THEN
     UPDATE SET
       tgt.source_of_assessment_id  = src.source_of_assessment_id,
       tgt.is_active                = src.is_active,
       tgt.created_date             = src.created_date,
       tgt.updated_date             = src.updated_date,
       tgt.created_by               = src.created_by,
       tgt.updated_by               = src.updated_by
   WHEN NOT MATCHED THEN
     INSERT (
       assessment_id,
       source_of_assessment_id,
       is_active,
       created_date,
       updated_date,
       created_by,
       updated_by
     )
     VALUES (
       src.assessment_id,
       src.source_of_assessment_id,
       src.is_active,
       src.created_date,
       src.updated_date,
       src.created_by,
       src.updated_by
     );

   v_rows := SQL%ROWCOUNT;
   DBMS_OUTPUT.PUT_LINE('Rows merged: ASSESSMENT_SOURCE_OF_ASSESSMENT_XREF = '||v_rows);
   COMMIT;

   ---------------------------------------------------------------------------
   -- STEP 3: BEHAVIORAL ABNORMALITY XREF — LOC_DATA.ASSESSMENT_BEHAVIORAL_ABNORMALITY_XREF:contentReference[oaicite:2]{index=2}
   ---------------------------------------------------------------------------
   MERGE INTO LOC_DATA.ASSESSMENT_BEHAVIORAL_ABNORMALITY_XREF tgt
   USING (
     WITH base AS (
       SELECT
         a.assessment_id,
         mc.schizophrenic_disorder_ind,
         mc.delusional_disorder_ind,
         mc.secondary_dementia_ind,
         mc.walk_get_around_ind,
         mc.severe_anxiety_disorder_ind,
         mc.primary_dementia_ind,
         mc.mood_disorder_ind,
         a.is_active,
         a.created_date,
         a.updated_date,
         a.created_by,
         a.updated_by
       FROM STG_ODA.MENTAL_CONDITION mc
       JOIN STG_ODA.ASSESSMENT_BASE ab
         ON ab.assessment_number = mc.assessment_number
       JOIN LOC_DATA.ASSESSMENT a
         ON a.pims_assessment_number = ab.assessment_number
       WHERE p_assessment_number IS NULL OR ab.assessment_number = p_assessment_number
     ),
     flagged AS (
       SELECT assessment_id, 1 AS abnormality_id, is_active, created_date, updated_date, created_by, updated_by
         FROM base WHERE UPPER(TRIM(NVL(schizophrenic_disorder_ind,'N'))) = 'Y'
       UNION ALL
       SELECT assessment_id, 2, is_active, created_date, updated_date, created_by, updated_by
         FROM base WHERE UPPER(TRIM(NVL(delusional_disorder_ind,'N'))) = 'Y'
       UNION ALL
       SELECT assessment_id, 3, is_active, created_date, updated_date, created_by, updated_by
         FROM base WHERE UPPER(TRIM(NVL(secondary_dementia_ind,'N'))) = 'Y'
       UNION ALL
       SELECT assessment_id, 4, is_active, created_date, updated_date, created_by, updated_by
         FROM base WHERE UPPER(TRIM(NVL(walk_get_around_ind,'N'))) = 'Y'
       UNION ALL
       SELECT assessment_id, 5, is_active, created_date, updated_date, created_by, updated_by
         FROM base WHERE UPPER(TRIM(NVL(severe_anxiety_disorder_ind,'N'))) = 'Y'
       UNION ALL
       SELECT assessment_id, 6, is_active, created_date, updated_date, created_by, updated_by
         FROM base WHERE UPPER(TRIM(NVL(primary_dementia_ind,'N'))) = 'Y'
       UNION ALL
       SELECT assessment_id, 7, is_active, created_date, updated_date, created_by, updated_by
         FROM base WHERE UPPER(TRIM(NVL(mood_disorder_ind,'N'))) = 'Y'
     )
     SELECT DISTINCT * FROM flagged
   ) src
   ON (tgt.assessment_id = src.assessment_id
       AND tgt.abnormality_id = src.abnormality_id)
   WHEN MATCHED THEN
     UPDATE SET
       tgt.is_active    = src.is_active,
       tgt.created_date = src.created_date,
       tgt.updated_date = src.updated_date,
       tgt.created_by   = src.created_by,
       tgt.updated_by   = src.updated_by
   WHEN NOT MATCHED THEN
     INSERT (
       assessment_id,
       abnormality_id,
       is_active,
       created_date,
       updated_date,
       created_by,
       updated_by
     )
     VALUES (
       src.assessment_id,
       src.abnormality_id,
       src.is_active,
       src.created_date,
       src.updated_date,
       src.created_by,
       src.updated_by
     );

   v_rows := SQL%ROWCOUNT;
   DBMS_OUTPUT.PUT_LINE('Rows merged: ASSESSMENT_BEHAVIORAL_ABNORMALITY_XREF = '||v_rows);
   COMMIT;

   ---------------------------------------------------------------------------
   -- STEP 4: NEEDS ADAPTIVE EQUIPMENT DETAILS XREF — LOC_DATA.ASSESSMENT_NEEDS_ADAPTIVE_EQUIPMENT_DETAILS_XREF:contentReference[oaicite:3]{index=3}
   ---------------------------------------------------------------------------
   MERGE INTO LOC_DATA.ASSESSMENT_NEEDS_ADAPTIVE_EQUIPMENT_DETAILS_XREF tgt
   USING (
     SELECT
       a.assessment_id                                         AS assessment_id,
       sub.equipment_subtype_id                                AS needs_equipment_subtype_id,
       CASE
         WHEN UPPER(TRIM(ab.rss_stairs_ramp_needed_ind)) IN ('Y','YES','1','TRUE','T')
           THEN 'Y'
         ELSE 'N'
       END                                                     AS needs_additional_equipment,
       a.is_active                                             AS is_active,
       a.created_date                                          AS created_date,
       a.updated_date                                          AS updated_date,
       a.created_by                                            AS created_by,
       a.updated_by                                            AS updated_by
     FROM STG_ODA.ASSESSMENT_BASE ab
     JOIN LOC_DATA.ASSESSMENT a
       ON a.pims_assessment_number = ab.assessment_number
     JOIN LOC_DATA.ADAPTIVE_EQUIPMENT_SUBTYPE sub
       ON UPPER(sub.equipment_subtype_name) = 'STAIR'
     WHERE ab.rss_stairs_ramp_needed_ind IS NOT NULL
       AND (p_assessment_number IS NULL OR ab.assessment_number = p_assessment_number)
   ) src
   ON (tgt.assessment_id = src.assessment_id
       AND tgt.needs_equipment_subtype_id = src.needs_equipment_subtype_id)
   WHEN MATCHED THEN
     UPDATE SET
       tgt.needs_additional_equipment = src.needs_additional_equipment,
       tgt.is_active                  = src.is_active,
       tgt.created_date               = src.created_date,
       tgt.updated_date               = src.updated_date,
       tgt.created_by                 = src.created_by,
       tgt.updated_by                 = src.updated_by
   WHEN NOT MATCHED THEN
     INSERT (
       assessment_id,
       needs_equipment_subtype_id,
       needs_additional_equipment,
       is_active,
       created_date,
       updated_date,
       created_by,
       updated_by
     )
     VALUES (
       src.assessment_id,
       src.needs_equipment_subtype_id,
       src.needs_additional_equipment,
       src.is_active,
       src.created_date,
       src.updated_date,
       src.created_by,
       src.updated_by
     );

   v_rows := SQL%ROWCOUNT;
   DBMS_OUTPUT.PUT_LINE('Rows merged: ASSESSMENT_NEEDS_ADAPTIVE_EQUIPMENT_DETAILS_XREF = '||v_rows);
   COMMIT;

   ---------------------------------------------------------------------------
   -- STEP 5: ENVIRONMENTAL REVIEW XREF — LOC_DATA.ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF:contentReference[oaicite:4]{index=4}
   ---------------------------------------------------------------------------
   MERGE INTO LOC_DATA.ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF tgt
   USING (
     WITH src AS (
       SELECT
         a.assessment_id,
         ab.environment_comment,
         a.is_active,
         a.created_date,
         a.updated_date,
         a.created_by,
         a.updated_by,
         UPPER(NVL(ab.environment_comment, '')) AS c    
       FROM STG_ODA.ASSESSMENT_BASE ab
       JOIN LOC_DATA.ASSESSMENT a
         ON a.pims_assessment_number = ab.assessment_number
       WHERE ab.environment_comment IS NOT NULL
         AND (p_assessment_number IS NULL OR ab.assessment_number = p_assessment_number)
     ),
     matches AS (
       SELECT s.assessment_id, 1 AS environmental_review_id, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%NEIGHBORHOOD%' OR s.c LIKE '%SAFETY%'
       UNION ALL
       SELECT s.assessment_id, 2, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%FIRE PLAN%' OR (s.c LIKE '%FIRE%' AND s.c NOT LIKE '%SMOKE%')
       UNION ALL
       SELECT s.assessment_id, 3, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%CARBON MONOXIDE%' OR s.c LIKE '%CO DETECTOR%'
       UNION ALL
       SELECT s.assessment_id, 4, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%SMOKE ALARM%' OR s.c LIKE '%SMOKE DETECTOR%'
       UNION ALL
       SELECT s.assessment_id, 5, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%INFECTION CONTROL%'
       UNION ALL
       SELECT s.assessment_id, 6, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE (s.c LIKE '%SMOKE%' OR s.c LIKE '%SMOKER%' OR s.c LIKE '%SMOKING%')
         AND s.c NOT LIKE '%SMOKE ALARM%'
         AND s.c NOT LIKE '%SMOKE DETECTOR%'
       UNION ALL
       SELECT s.assessment_id, 7, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%FIRST AID%'
       UNION ALL
       SELECT s.assessment_id, 8, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%WEAPON%' OR s.c LIKE '%GUN%'
       UNION ALL
       SELECT s.assessment_id, 9, s.environment_comment,
              s.is_active, s.created_date, s.updated_date, s.created_by, s.updated_by
       FROM src s
       WHERE s.c LIKE '%OXYGEN%'
     ),
     src_final AS (
       SELECT DISTINCT
         m.assessment_id,
         m.environmental_review_id,
         m.environment_comment,
         m.is_active, m.created_date, m.updated_date, m.created_by, m.updated_by
       FROM matches m
     )
     SELECT
       assessment_id,
       environmental_review_id,
       environment_comment AS comments,
       is_active,
       created_date,
       updated_date,
       created_by,
       updated_by
     FROM src_final
   ) src
   ON (tgt.assessment_id = src.assessment_id
       AND tgt.environmental_review_id = src.environmental_review_id)
   WHEN MATCHED THEN
     UPDATE SET
       tgt.comments     = src.comments,
       tgt.is_active    = src.is_active,
       tgt.created_date = src.created_date,
       tgt.updated_date = src.updated_date,
       tgt.created_by   = src.created_by,
       tgt.updated_by   = src.updated_by
   WHEN NOT MATCHED THEN
     INSERT (
       assessment_id,
       environmental_review_id,
       comments,
       is_active,
       created_date,
       updated_date,
       created_by,
       updated_by
     )
     VALUES (
       src.assessment_id,
       src.environmental_review_id,
       src.comments,
       src.is_active,
       src.created_date,
       src.updated_date,
       src.created_by,
       src.updated_by
     );

   v_rows := SQL%ROWCOUNT;
   DBMS_OUTPUT.PUT_LINE('Rows merged: ASSESSMENT_ENVIRONMENTAL_REVIEW_XREF = '||v_rows);
   COMMIT;

   ---------------------------------------------------------------------------
   -- STEP 6: ENVIRONMENTAL ISSUE XREF — LOC_DATA.ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF:contentReference[oaicite:5]{index=5}
   ---------------------------------------------------------------------------
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
     WHERE p_assessment_number IS NULL OR ab.assessment_number = p_assessment_number
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
   DBMS_OUTPUT.PUT_LINE('Rows merged: ASSESSMENT_ENVIRONMENTAL_ISSUE_XREF = '||v_rows);
   COMMIT;

   DBMS_OUTPUT.PUT_LINE(
     'Migration complete for '
     || CASE WHEN p_assessment_number IS NULL THEN 'ALL eligible assessments' ELSE 'assessment '||p_assessment_number END
   );

EXCEPTION
   WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Unexpected error in MIGRATE_ASSESSMENTS: '||SQLERRM);
      ROLLBACK;
END MIGRATE_ASSESSMENTS;
/
