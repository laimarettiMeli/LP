
SELECT 
shp.shp_shipment_id
,sco.ord_order_id
,shp.SHP_DATE_CREATED_ID
,chk0.max_date as date_chk0
--,chk1.max_date as date_chk1
,chk2.max_date as date_chk2
,sco.bpp_cashout
-- ,shp.*
,sco.DOM_DOMAIN_ID
,shp.SHP_ORDER_COST_USD
,shp.SHP_ORDER_COST
FROM WHOWNER.BT_SHP_SHIPMENTS as shp
  --INNER JOIN WHOWNER.BT_SHP_ELASTICSEARCH_CHECKPOINTS chk
  --ON chk.shp_id = shp.shp_shipment_id AND chk.shp_sub_status='cancelled'
    LEFT JOIN 
    (SEL  SHP_ID,MAX(shp_checkpoint_date) max_date
      FROM WHOWNER.BT_SHP_ELASTICSEARCH_CHECKPOINTS
      WHERE shp_sub_status IN ('ready_for_pickup')
      GROUP BY 1
    ) chk0
      ON chk0.shp_id = shp.shp_shipment_id
  /*LEFT JOIN 
    (SEL  SHP_ID,MAX(shp_checkpoint_date) max_date
      FROM WHOWNER.BT_SHP_ELASTICSEARCH_CHECKPOINTS
      WHERE shp_sub_status IN ('creating_route','invoice_pending','waiting_for_carrier_authorization','ready_to_print','printed','dropped_off')
      GROUP BY 1
    ) chk1
      ON chk1.shp_id = shp.shp_shipment_id*/
  LEFT JOIN 
    (SEL SHP_ID,MAX(shp_checkpoint_date) max_date
      FROM WHOWNER.BT_SHP_ELASTICSEARCH_CHECKPOINTS
      WHERE shp_sub_status IN ('picked_up','in_hub','authorized_by_carrier','in_packing_list','ready_to_ship','shipped')
      GROUP BY 1
    ) chk2
      ON chk2.shp_id = shp.shp_shipment_id
  LEFT JOIN
    SCORING.ORDERS_MAIN_ME sco 
    ON sco.shp_shipment_id=shp.shp_shipment_id 
    AND SCO.SIT_SITE_ID='MLA' AND sco.SHP_DATE_SHIPPED_ID IS NULL AND shp_picking_type_id='cross_docking'
    
WHERE shp.SIT_SITE_ID IN ('MLA')-- AND chk.shp_sub_status='cancelled'
AND shp.SHP_DATE_CREATED_ID > current_date-200
  AND shp.SHP_STATUS_ID='cancelled'
  AND shp.SHP_DATE_CANCELLED_ID IS NOT NULL
  AND shp.SHP_DATE_SHIPPED_ID IS NULL
  AND shp.SHP_DATE_DELIVERED_ID IS NULL
--chk.shp_checkpoint_date BETWEEN date '2021-08-10' and date '2021-08-23'
  AND shp.shp_picking_type_id not in ('self_service', 'default')
  AND shp.SHP_SOURCE_ID = 'MELI'
  AND shp.shp_shipping_mode_id = 'me2'
  AND shp.shp_type = 'forward'
  AND shp.shp_picking_type_id in ('cross_docking')
  AND date_chk2 is null
  -- Si no queres ningun sender puntual comentar la linea de abajo 
  AND shp.SHP_SENDER_ID	IN (134232558)
  -- AND sco.bpp_cashout = 0 OR sco.bpp_cashout IS NULL

  --AND shp_ID IN (40768081538)
