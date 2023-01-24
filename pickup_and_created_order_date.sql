-- pickup 39 / created 0
WITH created_table AS 
(
    SELECT
        shipment_id
        ,status AS status_name
        ,from_unixtime(ctime-3600) AS created_timestamp
        ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    WHERE status = 0
)
,pickup_done_table AS 
(
    SELECT
        shipment_id
        ,status
        ,from_unixtime(ctime-3600) AS pickup_done_timestamp
        ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
    WHERE status = 39
)
,order_type_id AS
(
    SELECT
        shipment_id
        ,CASE 
        WHEN order_type_name = 'CB' THEN 'CB'
        WHEN order_type_name = 'Shopee Xpress' THEN 'WH'
        WHEN order_type_name = 'NON_SHOPEE_MARKETPLACE_STANDARD' THEN 'OSV'
        WHEN order_type_name = 'BULKY_MARKETPLACE' THEN 'BKY'
        ELSE 'MKP' END AS order_type
        from thopsbi_spx.dwd_pub_shipment_info_df_th 
--     WHERE shipment_id IN ('SPXTH02516235159C',
-- 'SPXTH02563620891B') 
)

SELECT
    created_table.shipment_id
    ,order_type
    ,created_timestamp
    ,DATE(pickup_done_timestamp) AS pickup_done_date
    FROM created_table
    LEFT JOIN pickup_done_table ON created_table.shipment_id = pickup_done_table.shipment_id
    LEFT JOIN order_type_id ON created_table.shipment_id = order_type_id.shipment_id
    WHERE created_table.shipment_id IN 
()