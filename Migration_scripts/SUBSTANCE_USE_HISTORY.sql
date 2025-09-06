SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
    v_rows pls_integer := 0;
begin merge into loc_data.SUBSTANCE_USE_HISTORY tgt
using (
select 
   a.assessment_id          as assessment_id,
    c.CHEMICAL_ID    as substance_use_id,
      c.CHEMICAL_AMOUNT  as amount,           
      c.CHEMICAL_FREQ  as frequency,        
        a.is_active        as IS_ACTIVE,
        a.created_date     as CREATED_DATE,
        a.updated_by       as updated_by,
        1               as use_status_id,
 CASE
  WHEN UPPER(c.chemical_type) LIKE '%ALCOHOL%'  THEN 1
  WHEN UPPER(c.chemical_type) LIKE '%Caffeine%' THEN 2
  WHEN UPPER(c.chemical_type) LIKE '%NICOTINE%' THEN 3
  --WHEN UPPER(c.chemical_type) LIKE '%OTHER%' OR UPPER(c.chemical_type) IS NULL THEN 4
  else 4
END                                                            AS SUBSTANCE_ID,
        a.created_by       as created_by,
        a.updated_date     as updated_date

        from stg_oda.chemical c
        join stg_oda.assessment_base ab
        on ab.assessment_number = c.assessment_number
        join loc_data.assessment a
            ON a.PIMS_ASSESSMENT_NUMBER = ab.ASSESSMENT_NUMBER
  WHERE c.ASSESSMENT_NUMBER = 24314362         
) src
on (tgt.assessment_id = src.assessment_id)
when matched then update set
tgt.amount = src.amount,
tgt.frequency = src.frequency,
TGT.SUBSTANCE_ID = SRC.SUBSTANCE_ID,
tgt.CREATED_DATE = src.CREATED_DATE,
tgt.updated_by   = src.updated_by,
tgt.created_by   = src.created_by,
tgt.IS_ACTIVE    = src.IS_ACTIVE,
tgt.updated_date = src.updated_date,
tgt.use_status_id = src.use_status_id

when not matched then insert(
assessment_id,
amount,
SUBSTANCE_ID,
frequency,
is_active,
created_date,
updated_by,
created_by,
updated_date,
USE_STATUS_ID)
values (
src.assessment_id,
src.amount,
SRC.SUBSTANCE_ID,
src.frequency,
src.is_active,
src.created_date,
src.updated_by,
src.created_by,
src.updated_date ,
src.USE_STATUS_ID);
  v_rows := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE('SUBSTANCE_USE_HISTORY rows merged: '||v_rows);
END;
    