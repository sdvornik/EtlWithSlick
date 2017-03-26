SELECT
  TIME_T.indx AS id_indx
  ,ancestor3 AS time
FROM public.timestd AS TIME_STD_T
JOIN public.time AS TIME_T USING(id);