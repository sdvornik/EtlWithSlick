select
  product
  ,location
  ,time AS flrset
  ,unnest(replace(grade,'''','')::text[]) as grade
  ,unnest(replace(strClimate,'''','')::text[]) as strClimate
  ,TIME_1.indx AS initrcptwk_indx
  ,TIME_2.indx AS exitdate_indx
  ,TIME_3.indx AS dbtwk_indx
  ,TIME_4.indx AS erlstmkdnwk_indx
  ,validsizes
  ,TIME_5.indx AS lastdcrcpt_indx
    from  public.frontline
JOIN public.time AS TIME_1 ON initrcptwk = TIME_1.id
JOIN public.time AS TIME_2 ON exitdate = TIME_2.id
JOIN public.time AS TIME_3 ON dbtwk = TIME_3.id
JOIN public.time AS TIME_4 ON erlstmkdnwk = TIME_4.id
JOIN public.time AS TIME_5 ON lastdcrcpt = TIME_5.id;