SELECT ro.id FROM M_TTK.RSC_ORDER ro
                      JOIN M_TTK.RSC_ORDER_SERVICE_ENTRY rose
                           ON ro.ID = rose.ORDER_ID
WHERE TYPE_ID = 108 AND CLOSE_REASON_ID = 5 AND NOTICE LIKE '%id_esm: {}_%' and REG_USER_ID = 2400
