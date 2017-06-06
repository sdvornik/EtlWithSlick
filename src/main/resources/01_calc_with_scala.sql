create table RCPT_2 AS (
    SELECT * FROM create_toh_input_table_scala(
             #PRODUCT#,
             #V_PLANCURRENT#,
             #V_PLANEND#
         )
);
create table DC_2 AS (
SELECT DISTINCT
    "idx" AS indx
    ,"sbkt" AS dc_sbkt
    ,"dc_raw"
    ,"outbound"
    ,"dc_poh"
    ,"deficit"
    ,"dc_rcpt"
    ,"dc_oh_rsv"
    ,"dc_ata"
    ,"a_out"
FROM RCPT_2
);

ALTER TABLE public.RCPT_2 DROP COLUMN "dc_raw", "outbound", "dc_poh", "deficit", "dc_rcpt", "dc_oh_rsv", "dc_ata", "a_out";
