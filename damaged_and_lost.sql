WITH dl_detail AS
(
    SELECT 
        order_track.shipment_id
        ,order_track.status
        ,destination.station_name
        ,xx_timestamp AS xx_tiemstamp
        ,lag_status
        ,lag_station.station_name AS station_before
        ,lag_operator AS operator_before_dl
        ,from_unixtime(lag_timestamp-3600) AS before_tiemstamp
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
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS order_track 
            -- WHERE shipment_id = 'SPXTH032830804421'
        ) AS order_track
    LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as destination
    ON order_track.station_id = destination.station_id 
    LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as lag_station
    ON order_track.lag_station = lag_station.station_id 
    WHERE order_track.status IN (11,12,96) 
)
,order_path AS
(
    SELECT 
        shipment_id
        ,lm_station
        ,ret_station
        ,cogs
        ,order_type
        ,drop_off_station
    FROM  
    (   
        SELECT 
            shipment_id
            ,destination.station_name AS lm_station
            ,return_station.station_name AS ret_station
            ,shipment_detail.cogs
            ,shipment_detail.order_type
            ,drop_station.station_name AS drop_off_station
        FROM spx_mart.dwd_spx_fleet_order_tab_ri_th_ro AS shipment_detail

        LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as destination
        ON shipment_detail.station_id = destination.station_id 
    
        LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as return_station
        ON shipment_detail.return_station_id = return_station.station_id 

        LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro as drop_station
        ON shipment_detail.drop_off_sp = drop_station.station_id 
    ) AS shipment_detail
)

SELECT 
    dl_detail.shipment_id
    ,status_map1.status_name AS dl_status
    ,dl_detail.station_name AS dl_station
    ,dl_detail.xx_tiemstamp AS dl_timestamp
    ,status_map2.status_name AS status_before_dl
    ,dl_detail.station_before AS station_before_dl
    ,operator_before_dl
    ,before_tiemstamp 
    ,CASE 
        WHEN status_map2.status_name LIKE 'Return%' THEN ret_station 
        ELSE lm_station END AS destination_hub
    ,CAST(cogs AS DOUBLE)
    ,order_type
    ,drop_off_station    
FROM dl_detail     
LEFT JOIN order_path
ON dl_detail.shipment_id = order_path.shipment_id
LEFT JOIN 
    (
        SELECT 
            status
            ,status_name
        FROM thopsbi_lof.thspx_fact_order_tracking_di_th
        GROUP BY status,status_name
    )   AS status_map1
ON dl_detail.status = status_map1.status  
LEFT JOIN 
    (
        SELECT 
            status
            ,status_name
        FROM thopsbi_lof.thspx_fact_order_tracking_di_th
        GROUP BY status,status_name
    )   AS status_map2
ON dl_detail.lag_status = status_map2.status  

WHERE dl_detail.shipment_id IN
()
-- 11/96 = Lost
-- 12 = Damaged
-- 26 = Disposed