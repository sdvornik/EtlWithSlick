create table RCPT AS (
    SELECT * FROM FINAL_UNCONS_MOD()
);

create table DC AS (
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
FROM RCPT
);

ALTER TABLE public.RCPT DROP COLUMN "dc_raw", "outbound", "dc_poh", "deficit", "dc_rcpt", "dc_oh_rsv", "dc_ata", "a_out";

