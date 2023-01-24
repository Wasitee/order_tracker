WITH main_universe AS
(
    SELECT
        spx_track.shipment_id
        ,status_name
        ,dl_timestamp
        ,DATE(dl_timestamp) AS end_status_timestamp
        ,seller_detail.seller_name	
        ,dwd.cod_amount
        ,is_dropoff
        ,cogs
        ,_4pl_name
        ,cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as VARCHAR) AS origin_station
        ,cast(trim(replace(split_part((origin_order_path),',',1),'[','')) AS VARCHAR) AS direct_shuttle_type
        ,updated_order_path_lm_hub_station_name AS update_hub_destination
        ,CASE
            WHEN drop_off_sp = 0 THEN 'PICKUP' ELSE 'SHUTTLE' END AS original_fm_type 
    FROM 
        (   
            SELECT
                shipment_id
                ,status_map.status_name
                ,from_unixtime(ctime-3600) AS dl_timestamp
                ,row_number() OVER(PARTITION BY shipment_id ORDER BY ctime ASC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS spx_track
            LEFT JOIN 
                (
                    SELECT 
                        status
                        ,status_name
                    FROM thopsbi_lof.thspx_fact_order_tracking_di_th
                    GROUP BY status,status_name
                )   AS status_map
            ON spx_track.status = status_map.status 
            WHERE spx_track.status IN (11,12)
        ) AS spx_track
    LEFT JOIN spx_mart.dwd_spx_fleet_order_tab_ri_th_ro AS order_detail
    ON spx_track.shipment_id = order_detail.shipment_id
    LEFT JOIN thopsbi_spx.dwd_pub_shipment_info_df_th AS dwd
    ON spx_track.shipment_id = dwd.shipment_id
    LEFT JOIN spx_mart.dwd_spx_seller_order_tab_ri_th_ro AS seller_detail
    ON spx_track.shipment_id = seller_detail.shipment_id
    WHERE row_number = 1 
    AND DATE(dl_timestamp) = CURRENT_DATE - INTERVAL '1' DAY
    -- AND DATE(dl_timestamp) BETWEEN DATE(DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '' DAY) AND DATE(DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '1' DAY) 
)
,craeted_detail AS
(
    SELECT 
        shipment_id
        ,created_timestamp
    FROM 
        (
            SELECT 
                shipment_id
                ,from_unixtime(ctime-3600) AS created_timestamp
                ,row_number() OVER(PARTITION BY shipment_id ORDER BY ctime ASC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            WHERE status = 0
        )
    WHERE row_number = 1
)
,pickup_detail AS
(
    SELECT 
        shipment_id
        ,pickup_timestamp
    FROM 
        (
            SELECT 
                shipment_id
                ,from_unixtime(ctime-3600) AS pickup_timestamp
                ,row_number() OVER(PARTITION BY shipment_id ORDER BY ctime ASC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            WHERE status IN (39,13)
        )
    WHERE row_number = 1
)
,unsuccessful_log AS
(   
    SELECT
        shipment_id
        ,error_msg
        ,unsucessful_log_hub
        ,error_timestamp
    FROM
        (
            SELECT 
                shipment_id
                ,operator_name
                ,split_part(station_name,' -',1) AS unsucessful_log_hub
                ,context_info
                ,error_msg
                ,holder_id
                ,from_unixtime(ctime-3600) AS error_timestamp
                ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) DESC) AS row_number 
            FROM spx_mart.shopee_fms_log_th_db__order_unsuccessful_operation_log_tab__reg_continuous_s0_live                     
        )
    WHERE row_number = 1
)    
,detail_before_dl AS
(
    SELECT 
        shipment_id
        ,status_map.status_name
        ,xx_timestamp
        ,status_mapp.status_name AS status_before_dl
        ,lag_station.station_name
        ,from_unixtime(lag_timestamp-3600) AS timesatmp_before_dl
        ,lag_operator
    FROM
        (
            SELECT 
                shipment_id
                ,status
                ,station_id
                ,from_unixtime(ctime-3600) AS xx_timestamp
                ,lag(status) OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS lag_status
                ,lag(station_id) OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS lag_station
                ,lag(ctime) OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS lag_timestamp
                ,lag(operator) OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS lag_operator
                ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        ) AS ssc_table
    LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as destination
    ON ssc_table.station_id = destination.station_id 
    LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as lag_station
    ON ssc_table.lag_station = lag_station.station_id
    LEFT JOIN 
    (
        SELECT 
            status
            ,status_name
        FROM thopsbi_lof.thspx_fact_order_tracking_di_th
        GROUP BY status,status_name
    )   AS status_map
    ON ssc_table.status = status_map.status 
    LEFT JOIN 
    (
        SELECT 
            status
            ,status_name
        FROM thopsbi_lof.thspx_fact_order_tracking_di_th
        GROUP BY status,status_name
    )   AS status_mapp
    ON ssc_table.lag_status = status_mapp.status 
    WHERE ssc_table.status IN (11,12,96) 
)
,to_lt_detail AS
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
)
,dl_remark AS
(
    SELECT
        order_id
        ,remark
    FROM
        (
            SELECT 
                order_id
                ,from_unixtime(ctime-3600) AS xx_timestamo
                ,remark
                ,row_number() OVER(PARTITION BY order_id ORDER BY ctime ASC) AS row_number
            FROM spx_mart.shopee_fms_th_db__abnormal_order_record_tab__reg_continuous_s0_live
            WHERE new_status IN (11,12)
        )
    WHERE row_number = 1
) 
SELECT 
    main_universe.shipment_id
    ,main_universe.status_name AS latest_status_name
    ,cod_amount
    ,CAST(cogs AS DOUBLE) AS cogs_amount
    ,seller_name
    ,original_fm_type
    ,CASE
        WHEN direct_shuttle_type = '3' THEN 'DIRECT' ELSE 'SHUTTLE' END AS direct_shuttle_type
    ,CASE
        WHEN _4pl_name = 'SPX' THEN NULL ELSE _4pl_name END AS _4pl_provider
    ,latest_lh_task_id
    ,latest_to_number
    ,origin_hub.station_name AS origin_hub_destination
    ,update_hub_destination
    ,detail_before_dl.lag_operator AS latest_operator
    ,status_before_dl AS latest_status_before_dl
    ,detail_before_dl.station_name AS latest_station_before_dl
    ,timesatmp_before_dl
    ,created_timestamp
    ,pickup_timestamp
    ,end_status_timestamp
    ,error_timestamp
    ,unsucessful_log_hub
    ,error_msg
    ,CASE
        WHEN status_before_dl LIKE '3PL%' THEN '4PL' 
        WHEN status_before_dl LIKE 'SP%' OR status_before_dl LIKE 'DOP%' OR status_before_dl = 'Auto_to_Pending_Drop_off' THEN 'SP'
        WHEN status_before_dl LIKE 'FM%' OR status_before_dl LIKE 'Return_FM%' OR status_before_dl LIKE 'LM%' OR status_before_dl LIKE 'Return_LM%'OR status_before_dl LIKE 'Exc%' OR status_before_dl = 'Delivering' OR status_before_dl = 'Onhold' THEN 'CTO'
        WHEN status_before_dl = 'SOC_Pickup_Done' OR status_before_dl = 'FMHub_Pickup_Done' THEN 'CTO'
        WHEN (status_before_dl LIKE 'SOC%' OR status_before_dl LIKE 'Return_SOC%') AND (detail_before_dl.station_name = 'SOCE' OR detail_before_dl.station_name = 'Sorting center' OR detail_before_dl.station_name = 'SOCW') THEN 'MM'
        WHEN status_before_dl = 'Reallocate' AND (detail_before_dl.station_name = 'SOCE' OR detail_before_dl.station_name = 'Sorting center' OR detail_before_dl.station_name = 'SOCW') THEN 'MM'
        WHEN status_before_dl = 'Created' THEN 'MM'
        WHEN status_before_dl = 'FMHub_LHTransporting' THEN 'CTO/MM'
        WHEN status_before_dl = 'FMHub_LHTransported' THEN 'CTO/MM'
        WHEN status_before_dl = 'Exception_LHTransporting' THEN 'CTO/MM'
        WHEN status_before_dl = 'Exception_LHTransported' THEN 'CTO/MM'
        WHEN status_before_dl = 'Return_LMHub_LHTransporting' THEN 'CTO/MM'
        WHEN status_before_dl = 'Return_LMHub_LHTransported' THEN 'CTO/MM'
        WHEN status_before_dl = 'Return_SOC_LHTransporting' THEN 'CTO/MM'
        WHEN status_before_dl = 'Return_SOC_LHTransported' THEN 'CTO/MM'
        WHEN status_before_dl = 'LMHub_LHTransporting' THEN 'CTO/MM'
        WHEN status_before_dl = 'LMHub_LHTransported' THEN 'CTO/MM'
        WHEN status_before_dl = 'Return_FMHub_LHTransporting' THEN 'CTO/MM'
        WHEN status_before_dl = 'Return_FMHub_LHTransported' THEN 'CTO/MM'
        WHEN status_before_dl = 'SOC_Pickup_Onhold' THEN 'SP/CTO'
        WHEN status_before_dl = 'SOC_Pickup_Failed' THEN 'SP/CTO'
        WHEN status_before_dl = 'FMHub_Pickup_Onhold' THEN 'SP/CTO'
        WHEN status_before_dl = 'FMHub_Pickup_Failed' THEN 'SP/CTO'
        WHEN status_before_dl = 'FMHub_PendingReceive' THEN 'SP/CTO'
        WHEN status_before_dl = 'Transporting_to_sp' THEN 'SP/CTO'
        WHEN status_before_dl = 'Transported_to_sp' THEN 'SP/CTO'
        WHEN status_before_dl = 'SOC_Received' THEN 'CTO'
        WHEN status_before_dl = 'SOC_Packing' THEN 'CTO'
        WHEN status_before_dl = 'SOC_Packed' THEN 'CTO'
        WHEN status_before_dl = 'SOC_Handover' THEN 'CTO'
        WHEN status_before_dl = 'Reallocate' THEN 'CTO' 
        WHEN status_before_dl = 'SOC_Pickup_Handedover' AND is_dropoff = TRUE THEN 'SP/CTO'
        WHEN status_before_dl = 'FMHub_Pickup_Handedover'AND is_dropoff = TRUE THEN 'SP/CTO'
        WHEN status_before_dl = 'SOC_Pickup_Handedover' AND is_dropoff = FALSE THEN 'CTO/MM'
        WHEN status_before_dl = 'FMHub_Pickup_Handedover'AND is_dropoff = FALSE THEN 'CTO/MM' ELSE NULL END AS lost_pic
        ,dl_remark.remark 
FROM main_universe
LEFT JOIN craeted_detail
ON main_universe.shipment_id = craeted_detail.shipment_id
LEFT JOIN pickup_detail
ON main_universe.shipment_id = pickup_detail.shipment_id
LEFT JOIN unsuccessful_log
ON main_universe.shipment_id = unsuccessful_log.shipment_id
LEFT JOIN detail_before_dl
ON main_universe.shipment_id = detail_before_dl.shipment_id
LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as origin_hub
ON main_universe.origin_station = CAST(origin_hub.station_id AS VARCHAR) 
LEFT JOIN to_lt_detail
ON main_universe.shipment_id = to_lt_detail.shipment_id
LEFT JOIN dl_remark
ON main_universe.shipment_id = dl_remark.order_id
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
ORDER BY end_status_timestamp ASC


