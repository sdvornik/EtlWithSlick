select
  product AS department
  ,time
  ,unnest(replace(stores,'''','')::text[]) as location
  ,grade
from public.storelookup;