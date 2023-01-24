WITH order_detail AS
(
    SELECT 
        shipment_id
        ,order_type
        ,current_status
        ,station_xx.station_name
        ,latest_timestamp
        ,cogs_amount
        ,buyer_name
        ,buyer_address
        ,buyer_phone
        ,seller_shop_id
        ,seller_shop_name
    FROM
        (
            SELECT
                row_number() OVER(PARTITION BY main_universe.shipment_id ORDER BY main_universe.ctime DESC) AS row_number
                ,main_universe.shipment_id
                ,status_map.status_name AS current_status
                ,from_unixtime(main_universe.ctime-3600) AS latest_timestamp
                ,buyer_name
                ,buyer_address
                ,seller_shop_id
                ,seller_shop_name
                ,CAST(TRY(JSON_EXTRACT(JSON_PARSE(CONTENT), '$.station_id')) AS INT) AS latest_station
                ,cogs_amount
                ,buyer_phone
                ,CASE
                    WHEN is_warehouse = TRUE THEN 'WH'
                    WHEN is_cross_border = TRUE THEN 'CB'
                    WHEN is_open_service = TRUE THEN 'OSV'
                    WHEN is_bulky = TRUE THEN 'BKY'
                    ELSE 'MKP' END AS order_type
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS main_universe
            LEFT JOIN spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live AS fleet_order
            ON main_universe.shipment_id = fleet_order.shipment_id
            LEFT JOIN thopsbi_spx.dwd_pub_shipment_info_df_th AS seller_detail
            ON main_universe.shipment_id = seller_detail.shipment_id
            LEFT JOIN
                (
                    SELECT 
                        status
                        ,status_name
                    FROM thopsbi_lof.thspx_fact_order_tracking_di_th 
                    GROUP BY 
                        status
                        ,status_name   
                ) AS status_map
            ON main_universe.status = status_map.status
        ) AS base_detail
    LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS station_xx
    ON base_detail.latest_station = station_xx.station_id
    WHERE row_number = 1
)   

SELECT *
FROM order_detail
WHERE 
shipment_id IN
()