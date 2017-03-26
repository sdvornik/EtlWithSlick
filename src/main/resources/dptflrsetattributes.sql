select
  ATTR_TABLE.product as department
  ,ATTR_TABLE.time   as flrset
  ,TIME_T.indx   as week_indx
from public.dptflrsetattributes AS ATTR_TABLE
JOIN public.time AS TIME_T ON
  TIME_T.levelid = 'Week' AND
  TIME_T.id >= ATTR_TABLE.rcptstart and
  TIME_T.id <= ATTR_TABLE.slsend;