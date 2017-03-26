select
  product
  ,TIME_T.indx AS time_indx
  ,user_vrp
  ,locked_qty
  ,user_adj_qty
  ,on_order_qty
  ,oo_revision_qty
  ,CAST(adj_cost AS DOUBLE PRECISION)
from public.temp_dc_adj AS DC_ADJ
JOIN public.time AS TIME_T ON
  DC_ADJ.time=TIME_T.id;