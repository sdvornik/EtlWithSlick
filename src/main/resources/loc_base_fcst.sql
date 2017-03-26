SELECT
  location
  ,TIME_T.indx AS week_indx
  ,fcst
FROM public.loc_base_fcst AS LOC_BASE_FCST
JOIN public.time AS TIME_T ON LOC_BASE_FCST.week = TIME_T.id
WHERE LOC_BASE_FCST.product = ?;


