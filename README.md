SELECT  
    order_track2.shipment_id AS shipment_id
    ,status_map.status_name AS status_name
    ,station_name.station_name AS current_station
    ,order_track2.operator AS operator
    ,order_track2.order_type_id AS order_type
    ,order_track2.status_time AS latest_timestamp
    ,CAST(order_detail.cogs AS INT) AS cogs
    ,CASE
    WHEN order_track2.operator = 'Flash' THEN 'Flash'
    ELSE order_detail.lm_station END AS lm_destination
    ,order_detail.ret_station AS return_station_name
    ,lh_to_detail.latest_lh_task_id AS lh_trip_number
    ,lh_to_detail.latest_to_number AS to_number
FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as fleet_order

----- destination & return station & cogs
LEFT JOIN 
    (
        SELECT 
            shipment_id
            ,destination.station_name AS lm_station
            ,return_station.station_name AS ret_station
            ,shipment_detail.cogs
        FROM spx_mart.dwd_spx_fleet_order_tab_ri_th_ro AS shipment_detail

        LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as destination
        ON shipment_detail.station_id = destination.station_id 
    
        LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as return_station
        ON shipment_detail.return_station_id = return_station.station_id 

    ) AS order_detail
ON fleet_order.shipment_id = order_detail.shipment_id

-- status & current station & timesatmp & operator
LEFT JOIN 
    (
        SELECT
            shipment_id
            ,status
            ,station_id
            ,operator
            ,order_type_id
            ,status_time
            ,row_number
        FROM
            (
                SELECT
                    fleet_order.shipment_id
                    ,fleet_order.status
                    ,fleet_order.station_id    
                    ,fleet_order.operator      
                    ,dwd.order_type_id                    
                    -- ,dwd.latest_operator_name AS operator                                                        
                    ,from_unixtime(fleet_order.ctime-3600) AS status_time
                    ,row_number() OVER (PARTITION BY fleet_order.shipment_id ORDER BY ctime DESC) AS row_number
                FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS fleet_order
                LEFT JOIN thopsbi_spx.dwd_pub_shipment_info_df_th AS dwd
                ON fleet_order.shipment_id = dwd.shipment_id
            )   AS order_track1
        WHERE row_number = 1
    ) AS order_track2
ON fleet_order.shipment_id = order_track2.shipment_id

LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS station_name
ON order_track2.station_id = station_name.station_id

-- status name 
LEFT JOIN 
    (
        SELECT 
            status
            ,status_name
        FROM thopsbi_lof.thspx_fact_order_tracking_di_th
        GROUP BY status,status_name
    )   AS status_map
ON order_track2.status = status_map.status 

-- LT & TO number
LEFT JOIN 
    (
        SELECT  
            shipment_id
            ,CAST(lh_task_id AS VARCHAR) AS latest_lh_task_id
            ,CAST(to_number AS VARCHAR) AS latest_to_number
        FROM  
            (
                SELECT  
                    shipment_id
                    ,TRY(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id')) AS lh_task_id
                    ,TRY(JSON_EXTRACT(JSON_PARSE(content), '$.to_number')) AS to_number
                    ,row_number() OVER (PARTITION BY shipment_id ORDER BY ctime DESC) AS rank_no_lt
                FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
                WHERE status IN (46, 47, 35, 15, 211, 233, 55, 56, 62, 64)
            )
        WHERE rank_no_lt = 1
    ) AS lh_to_detail
ON  fleet_order.shipment_id = lh_to_detail.shipment_id

WHERE order_track2.shipment_id IN 
()
GROUP BY 
     order_track2.shipment_id 
    ,status_map.status_name 
    ,station_name.station_name 
    ,order_track2.operator 
    ,order_track2.order_type_id 
    ,order_track2.status_time 
    ,CAST(order_detail.cogs AS INT) 
    ,order_detail.lm_station 
    ,order_detail.ret_station 
    ,lh_to_detail.latest_lh_task_id 
    ,lh_to_detail.latest_to_number 
