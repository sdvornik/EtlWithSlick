select
  product
  ,sizes
  ,too
  ,aps_lower
  ,aps
  ,CAST(woc AS int)
from public.invmodellkp_mod AS INV_MODEL;