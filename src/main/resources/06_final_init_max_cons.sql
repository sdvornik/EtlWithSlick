--[2017-04-08 15:27:51] Summary: 2 of 2 statements executed in 96ms (726 symbols in file)
--[2017-04-08 15:27:19] completed in 86ms
create table MAX_CONS AS (
  SELECT
    location,
    CASE
    WHEN COALESCE(c.Ttl_Str_Unc_Need, 0) = 0
      THEN 0
    ELSE /*half_round*/((COALESCE(a.Ttl_DC_Rcpt, 0) + COALESCE(beg_eoh, 0)) *
                    b.Str_Unc_Need / CAST(c.Ttl_Str_Unc_Need AS NUMERIC))
    END AS value
  FROM
    (
      SELECT
        SUM(dc_raw) AS Ttl_DC_Rcpt
      FROM DC
    ) AS a
    ,(
      select
        coalesce(eoh,0) AS beg_eoh
      from EOH
      WHERE location = 'DC'
     ) AS d
    ,(
      SELECT
        location,
        SUM(unc_need_lt) AS Str_Unc_Need
      FROM RCPT
      GROUP BY location
     ) AS b
    ,(
      SELECT
        SUM(unc_need_lt) AS Ttl_Str_Unc_Need
      FROM RCPT
     ) AS c
);
CREATE HASH INDEX MAX_CONS_location_indx_idx ON MAX_CONS(location);
--drop table MAX_CONS;