WITH main_universe AS
(
WITH cte AS
(
    SELECT 
        time_stamp AS _3pl_received_timestamp
        ,_3pl_received.shipment_id
        ,_3pl_received.station_name AS _3pl_station
        ,date_3pl_received
    FROM
        (
            SELECT
                order_track.shipment_id
                ,from_unixtime(order_track.ctime-3600) AS time_stamp
                ,CASE 
                    WHEN from_unixtime(order_track.ctime-3600) >= CAST(DATE(from_unixtime(order_track.ctime-3600)) AS timestamp) + INTERVAL '2' HOUR + INTERVAL '00' MINUTE
                    AND from_unixtime(order_track.ctime-3600) < CAST(DATE(from_unixtime(order_track.ctime-3600)) AS timestamp) + INTERVAL '1' DAY + INTERVAL '2' HOUR + INTERVAL '00' MINUTE
                    THEN DATE(from_unixtime(order_track.ctime-3600)) ELSE DATE(from_unixtime(order_track.ctime-3600)) - INTERVAL '1' DAY END AS date_3pl_received
                ,order_track.status
                ,order_track.operator
                ,staion_table_name.station_name 
                ,try(CAST(json_extract(json_parse(content),'$.station_id') AS INT)) as station_id
                ,row_number() OVER (PARTITION BY order_track.shipment_id ORDER BY from_unixtime(order_track.ctime-3600) ASC) AS rank_num
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
            LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS staion_table_name
            ON try(CAST(json_extract(json_parse(content),'$.station_id') AS INT)) = staion_table_name.station_id
            WHERE
                order_track.status = 89
        ) AS _3pl_received
    WHERE 
        rank_num = 1    
        AND date_3pl_received BETWEEN DATE_TRUNC('day', current_timestamp) - interval '120' day and DATE_TRUNC('day', current_timestamp) - interval '1' day
)    
,handover_detail AS
(    
    SELECT 
        time_stamp AS min_soc_handover_timestamp
        ,min_soc_handover.shipment_id
        ,min_soc_handover.station_name AS soc_handover_station
    FROM
        (
            SELECT
                order_track.shipment_id
                ,from_unixtime(order_track.ctime-3600) AS time_stamp
                ,order_track.status
                ,order_track.operator
                ,staion_table_name.station_name 
                ,try(CAST(json_extract(json_parse(content),'$.station_id') AS INT)) as station_id
                ,row_number() OVER (PARTITION BY order_track.shipment_id ORDER BY from_unixtime(order_track.ctime-3600) ASC) AS rank_num
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
            LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS staion_table_name
            ON try(CAST(json_extract(json_parse(content),'$.station_id') AS INT)) = staion_table_name.station_id
            WHERE
                order_track.status = 18
        ) AS min_soc_handover
    WHERE 
        rank_num = 1    
)
,delivered_detail AS
(    
    SELECT 
        time_stamp AS delivered_timestamp
        ,delivered.shipment_id
    FROM
        (
            SELECT
                order_track.shipment_id
                ,from_unixtime(order_track.ctime-3600) AS time_stamp
                ,order_track.status
                ,order_track.operator
                ,staion_table_name.station_name 
                ,try(CAST(json_extract(json_parse(content),'$.station_id') AS INT)) as station_id
                ,row_number() OVER (PARTITION BY order_track.shipment_id ORDER BY from_unixtime(order_track.ctime-3600) ASC) AS rank_num
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
            LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS staion_table_name
            ON try(CAST(json_extract(json_parse(content),'$.station_id') AS INT)) = staion_table_name.station_id
            WHERE
                order_track.status = 81
        ) AS delivered
    WHERE 
        rank_num = 1    
)


SELECT 
    date_3pl_received
    ,cte.shipment_id
    ,soc_handover_station
    ,IF(_3pl_station IS NULL,'Flash',_3pl_station) AS _3pl_station
    ,_3pl_received_timestamp
    ,min_soc_handover_timestamp
    ,delivered_timestamp
    ,date_diff('MINUTE',_3pl_received_timestamp, delivered_timestamp) AS "leadtime_rec_to_del(mins)"
    ,date_diff('MINUTE',min_soc_handover_timestamp, delivered_timestamp) AS "leadtime_handover_to_del(mins)"
    ,COALESCE(1.0000*(delivered_timestamp-_3pl_received_timestamp),NULL) AS leadtime_rec_to_del
    ,COALESCE(1.0000*(delivered_timestamp-min_soc_handover_timestamp),NULL) AS leadtime_handover_to_del
FROM cte
LEFT JOIN handover_detail
ON cte.shipment_id = handover_detail.shipment_id
LEFT JOIN delivered_detail
ON cte.shipment_id = delivered_detail.shipment_id
WHERE soc_handover_station IN ('NERC-A','NERC-B','SORC-A','SORC-B','NORC-A','NORC-B') AND delivered_timestamp IS NOT NULL
GROUP BY 
    1,2,3,4,5,6,7,8,9,10,11
ORDER BY 
    date_3pl_received ASC
)

SELECT 
    date_3pl_received
    ,soc_handover_station
    ,count(_3pl_received_timestamp) AS _3pl_received_vol
    ,CAST(AVG("leadtime_rec_to_del(mins)") AS DECIMAL) AS avg_leadtime_rec_to_del
    ,CAST(AVG("leadtime_handover_to_del(mins)") AS DECIMAL) AS avg_leadtime_handover_to_del
    ,AVG(leadtime_rec_to_del) AS leadtime_rec_to_del
    ,AVG(leadtime_handover_to_del) AS leadtime_handover_to_del
FROM main_universe
GROUP BY 
    date_3pl_received
    ,soc_handover_station
ORDER BY 
    date_3pl_received ASC