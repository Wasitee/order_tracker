WITH main_universe AS
(
SELECT  
    shipment_id
    ,date_received
    ,receive_time
    ,operator
    ,order_type
    ,returned_time
    ,CASE
        WHEN operator LIKE 'IND%' THEN 'IND000001'
        WHEN operator LIKE 'CAB%' THEN 'CAB000001'
        WHEN operator LIKE 'DWS%' THEN 'DWS000001'
        WHEN operator LIKE 'MQB%' THEN 'MQB000001'
        ELSE 'return_agent' END AS operator_group
    ,IF(receive_time <= CAST(date_received AS TIMESTAMP) + INTERVAL '1' DAY +INTERVAL '2' HOUR + INTERVAL '0' MINUTE + INTERVAL '0' SECOND,'ontime',NULL) AS ontime_received_2am
    ,IF(receive_time > CAST(date_received AS TIMESTAMP) + INTERVAL '1' DAY +INTERVAL '2' HOUR + INTERVAL '0' MINUTE + INTERVAL '0' SECOND,'late',NULL) AS late_received
FROM    
    (
        SELECT  
            ssc.shipment_id
            ,from_unixtime(ctime-3600) AS receive_time
            ,CASE 
                 WHEN from_unixtime(ctime-3600) >= CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                AND from_unixtime(ctime-3600) < CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                THEN DATE(from_unixtime(ctime-3600)) ELSE DATE(from_unixtime(ctime-3600)) - INTERVAL '1' DAY END AS date_received
            ,operator 
            ,order_type
            ,returned_time
            ,status_name
            ,row_number() OVER (PARTITION BY ssc.shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS ssc
        LEFT JOIN 
            (
                -- order type
                SELECT 
                    shipment_id
                    ,CASE 
                        WHEN returned_time = 0 THEN NULL
                        ELSE from_unixtime(returned_time-3600) END AS returned_time
                    ,status_map.status_name
                    ,CASE
                        WHEN order_type = 0 THEN 'SCOM'
                        WHEN order_type = 1 THEN 'SCOM'
                        WHEN gbkk_upc_ops_region = 'GBKK' THEN 'GBKK' ELSE 'UPC' END AS order_type
                FROM spx_mart.dwd_spx_fleet_order_tab_ri_th_ro AS dwd
                LEFT JOIN 
                    (
                -- status
                        SELECT 
                            status
                            ,status_name
                        FROM thopsbi_lof.thspx_fact_order_tracking_di_th 
                        GROUP BY 
                            status
                            ,status_name
                    ) AS status_map
                ON dwd.status = status_map.status
                -- return statio
                LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS destination
                ON dwd.return_station_id = destination.station_id 
                -- destination zone
                LEFT JOIN thopsbi_lof.spx_index_region_temp AS hub_zone
                ON substring (split_part(destination.station_name,'-',1),2,4) = hub_zone.district_code
            ) AS xx
        ON ssc.shipment_id = xx.shipment_id
        WHERE ssc.status = 58 AND ssc.station_id = 3 AND ssc.operator <> 'gee.boonprat@shopee.com'
    )    
WHERE date_received BETWEEN DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) - INTERVAL '30' DAY) and DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) - INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE + INTERVAL '00' SECOND)
AND row_number = 1
)
,lhpack_detail AS
(   
    SELECT
        intable_lhpacked.shipment_id
        ,lhpack_time
        ,IF(lhpack_time <= CAST(date_received AS TIMESTAMP) + INTERVAL '1' DAY +INTERVAL '6' HOUR + INTERVAL '0' MINUTE + INTERVAL '0' SECOND ,'ontime',NULL) AS ontime_lhpacked_gbkk_d16am
        ,IF(lhpack_time <= CAST(date_received AS TIMESTAMP) + INTERVAL '2' DAY +INTERVAL '6' HOUR + INTERVAL '0' MINUTE + INTERVAL '0' SECOND,'ontime',NULL) AS ontime_lhpacked_upc_d26am
        ,IF(lhpack_time > CAST(date_received AS TIMESTAMP) + INTERVAL '1' DAY +INTERVAL '6' HOUR + INTERVAL '0' MINUTE + INTERVAL '0' SECOND,'late',NULL) AS late_lhpacked
    FROM
        (
            SELECT
                shipment_id
                ,CASE 
                    WHEN from_unixtime(ctime-3600) >= CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                    AND from_unixtime(ctime-3600) < CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                    THEN DATE(from_unixtime(ctime-3600)) ELSE DATE(from_unixtime(ctime-3600)) - INTERVAL '1' DAY END AS date_lhpacked
                ,from_unixtime(ctime-3600) AS lhpack_time
                ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            WHERE status = 62 AND station_id = 3
        ) AS intable_lhpacked
    LEFT JOIN main_universe
    ON main_universe.shipment_id = intable_lhpacked.shipment_id 
    WHERE row_number = 1
)     
,returning_detail AS
(   
    SELECT
        intable_returning.shipment_id
        ,returning_time
        ,IF(returning_time <= CAST(date_received AS TIMESTAMP) + INTERVAL '1' DAY +INTERVAL '11' HOUR + INTERVAL '0' MINUTE + INTERVAL '0' SECOND ,'ontime',NULL) AS ontime_returning_scom_11am
        ,IF(returning_time > CAST(date_received AS TIMESTAMP) + INTERVAL '1' DAY +INTERVAL '11' HOUR + INTERVAL '0' MINUTE + INTERVAL '0' SECOND,'late',NULL) AS late_returning
    FROM
        (
            SELECT
                shipment_id
                ,CASE 
                    WHEN from_unixtime(ctime-3600) >= CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                    AND from_unixtime(ctime-3600) < CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                    THEN DATE(from_unixtime(ctime-3600)) ELSE DATE(from_unixtime(ctime-3600)) - INTERVAL '1' DAY END AS date_returning
                ,from_unixtime(ctime-3600) AS returning_time
                ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            WHERE status IN (63,117) AND station_id = 3
        ) intable_returning
    LEFT JOIN main_universe
    ON main_universe.shipment_id = intable_returning.shipment_id
    WHERE row_number = 1
)
,lhtransporting_detail AS
(   
    SELECT
        intable_lhtransporting.shipment_id
        ,IF(lhtransporting_time <= CAST(date_received AS TIMESTAMP) + INTERVAL '1' Day + INTERVAL '12' HOUR + INTERVAL '00' MINUTE + INTERVAL '00' SECOND,'ontime',NULL) AS handover_D112pm
        ,IF(lhtransporting_time > CAST(date_received AS TIMESTAMP) + INTERVAL '1' Day + INTERVAL '12' HOUR + INTERVAL '00' MINUTE + INTERVAL '00' SECOND,'ontime',NULL) AS "handover_>D112pm"
    FROM
        (
            SELECT
                shipment_id
                ,CASE 
                    WHEN from_unixtime(ctime-3600) >= CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                    AND from_unixtime(ctime-3600) < CAST(DATE(from_unixtime(ctime-3600)) AS timestamp) + INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                    THEN DATE(from_unixtime(ctime-3600)) ELSE DATE(from_unixtime(ctime-3600)) - INTERVAL '1' DAY END AS date_lhtransporting
                ,from_unixtime(ctime-3600) AS lhtransporting_time
                ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            WHERE status = 64 AND station_id = 3
        ) AS intable_lhtransporting
    LEFT JOIN main_universe
    ON main_universe.shipment_id = intable_lhtransporting.shipment_id 
    WHERE row_number = 1
) 
,current_detail AS
(   
    SELECT
        *
    FROM
        (
            SELECT
                shipment_id
                ,status
                ,station_id
                ,operator
                ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) DESC) AS row_number
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        ) 
    WHERE row_number = 1 AND status IN (11,12)
)



SELECT 
    date_received
    ,order_type
    ,count(main_universe.shipment_id) AS overall_received
    ,COUNT(ontime_received_2am) AS "#ontime_received_2am"
    ,CASE
        WHEN order_type = 'SCOM' THEN CAST(COUNT(ontime_received_2am) FILTER (WHERE order_type = 'SCOM') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'SCOM') AS DOUBLE)
        WHEN order_type = 'GBKK' THEN CAST(COUNT(ontime_received_2am) FILTER (WHERE order_type = 'GBKK') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'GBKK') AS DOUBLE)
        WHEN order_type = 'UPC' THEN CAST(COUNT(ontime_received_2am) FILTER (WHERE order_type = 'UPC') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'UPC') AS DOUBLE)
        ELSE NULL END AS "%ontime_received_2am"
    ,CASE
        WHEN order_type = 'SCOM' THEN CAST(COUNT(late_received) FILTER (WHERE order_type = 'SCOM') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'SCOM') AS DOUBLE)
        WHEN order_type = 'GBKK' THEN CAST(COUNT(late_received) FILTER (WHERE order_type = 'GBKK') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'GBKK') AS DOUBLE)
        WHEN order_type = 'UPC' THEN CAST(COUNT(late_received) FILTER (WHERE order_type = 'UPC') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'UPC') AS DOUBLE)
        ELSE NULL END AS "%late_received"
     ,CASE
        WHEN order_type = 'SCOM' THEN CAST(COUNT(returning_time) FILTER (WHERE order_type = 'SCOM') AS DOUBLE)
        WHEN order_type = 'GBKK' THEN CAST(COUNT(lhpack_time) FILTER (WHERE order_type = 'GBKK') AS DOUBLE)
        WHEN order_type = 'UPC' THEN CAST(COUNT(lhpack_time) FILTER (WHERE order_type = 'UPC') AS DOUBLE)
        ELSE NULL END AS "infull_outbound"
    ,CASE
        WHEN order_type = 'SCOM' THEN CAST(COUNT(returning_time) FILTER (WHERE order_type = 'SCOM') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'SCOM') AS DOUBLE)
        WHEN order_type = 'GBKK' THEN CAST(COUNT(lhpack_time) FILTER (WHERE order_type = 'GBKK') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'GBKK') AS DOUBLE)
        WHEN order_type = 'UPC' THEN CAST(COUNT(lhpack_time) FILTER (WHERE order_type = 'UPC') AS DOUBLE) / CAST(count(main_universe.shipment_id) FILTER (WHERE order_type = 'UPC') AS DOUBLE)
        ELSE NULL END AS "%infull_outbound"
    ,CASE
        WHEN order_type = 'SCOM' THEN CAST(COUNT(ontime_returning_scom_11am) FILTER (WHERE order_type = 'SCOM') AS DOUBLE) / CAST(COUNT(returning_time) FILTER (WHERE order_type = 'SCOM') AS DOUBLE)
        WHEN order_type = 'GBKK' THEN CAST(COUNT(ontime_lhpacked_gbkk_d16am) FILTER (WHERE order_type = 'GBKK') AS DOUBLE) / CAST(COUNT(lhpack_time) FILTER (WHERE order_type = 'GBKK') AS DOUBLE)
        WHEN order_type = 'UPC' THEN CAST(COUNT(ontime_lhpacked_upc_d26am) FILTER (WHERE order_type = 'UPC') AS DOUBLE) / CAST(COUNT(lhpack_time) FILTER (WHERE order_type = 'UPC') AS DOUBLE)
        ELSE NULL END AS "%ontime_outbound"
    ,CASE
        WHEN order_type = 'SCOM' THEN CAST(COUNT(ontime_returning_scom_11am) FILTER (WHERE order_type = 'SCOM') AS DOUBLE) 
        WHEN order_type = 'GBKK' THEN CAST(COUNT(ontime_lhpacked_gbkk_d16am) FILTER (WHERE order_type = 'GBKK') AS DOUBLE) 
        WHEN order_type = 'UPC' THEN CAST(COUNT(ontime_lhpacked_upc_d26am) FILTER (WHERE order_type = 'UPC') AS DOUBLE) 
        ELSE NULL END AS "#ontime_outbound"
    ,CASE
        WHEN order_type = 'GBKK' THEN CAST(COUNT(handover_D112pm) FILTER (WHERE order_type = 'GBKK') AS DOUBLE) + CAST(COUNT("handover_>D112pm") FILTER (WHERE order_type = 'GBKK') AS DOUBLE)  
        WHEN order_type = 'UPC' THEN CAST(COUNT(handover_D112pm) FILTER (WHERE order_type = 'UPC') AS DOUBLE) + CAST(COUNT("handover_>D112pm") FILTER (WHERE order_type = 'UPC') AS DOUBLE)
        ELSE NULL END AS "#infull_handover"
    ,CASE
        WHEN order_type = 'SCOM' THEN CAST(COUNT(current_detail.shipment_id) FILTER (WHERE order_type = 'SCOM') AS DOUBLE) 
        WHEN order_type = 'GBKK' THEN CAST(COUNT(current_detail.shipment_id) FILTER (WHERE order_type = 'GBKK') AS DOUBLE) 
        WHEN order_type = 'UPC' THEN CAST(COUNT(current_detail.shipment_id) FILTER (WHERE order_type = 'UPC') AS DOUBLE) 
        ELSE NULL END AS "#damaged_lost"
    
FROM main_universe
LEFT JOIN lhpack_detail 
ON main_universe.shipment_id = lhpack_detail.shipment_id 
LEFT JOIN returning_detail 
ON main_universe.shipment_id = returning_detail.shipment_id 
LEFT JOIN lhtransporting_detail 
ON main_universe.shipment_id = lhtransporting_detail.shipment_id 
LEFT JOIN current_detail 
ON main_universe.shipment_id = current_detail.shipment_id 
GROUP BY
    date_received
    ,order_type
ORDER BY 
    date_received DESC



