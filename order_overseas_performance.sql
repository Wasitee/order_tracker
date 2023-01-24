SELECT
    shipment_id
    ,CASE
    WHEN shipment_id IS NULL THEN NULL ELSE 'SPX' END AS channel
    ,received_time
    ,lhtransported_time
    ,received_date
    ,CASE
        WHEN lhtransported_time >= CAST(DATE(lhtransported_time) AS timestamp) + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
        AND lhtransported_time < CAST(DATE(lhtransported_time) AS timestamp) + INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
        THEN DATE(lhtransported_time) ELSE DATE(lhtransported_time) - INTERVAL '1' DAY END AS lhtransported_date
    -- ,CASE
    --     WHEN day_of_week(received_date) <> 7 THEN date_trunc('week', received_date) - INTERVAL '1' DAY
    --     ELSE date_add('day', 0, received_date) END AS week_num
    ,WEEK(received_time) + 1 AS week_num
FROM
    (
        SELECT  
            destination_SIP_LMHub.shipment_id
            ,soc_received.status_time AS received_time
            ,from_unixtime(destination_SIP_LMHub.ctime-3600) AS lhtransported_time
            ,received_date
            -- ,try(cast(json_extract(json_parse(content),'$.pickup_station_name') as varchar)) as pickup_station
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS destination_SIP_LMHub
        LEFT JOIN 
            (
                SELECT 
                    shipment_id
                    ,status_time 
                    ,CASE
                        WHEN status_time >= CAST(DATE(status_time) AS timestamp) + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                        AND status_time < CAST(DATE(status_time) AS timestamp) + INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                        THEN DATE(status_time) ELSE DATE(status_time) - INTERVAL '1' DAY END AS received_date
                FROM 
                    (
                        SELECT 
                            shipment_id
                            ,from_unixtime(ctime-3600) AS status_time
                            ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
                        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
                        WHERE 
                            status = 8 AND try(CAST(json_extract(json_parse(content),'$.station_id') AS VARCHAR)) = '3' 
                    )
                WHERE row_number = 1       
            ) AS soc_received
        ON destination_SIP_LMHub.shipment_id = soc_received.shipment_id 
        WHERE destination_SIP_LMHub.status = 36 AND try(CAST(json_extract(json_parse(content),'$.dest_station_name') AS VARCHAR)) = 'SIP-LMHub'
    )
WHERE received_date BETWEEN DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '18' DAY AND DATE_TRUNC('day', CURRENT_TIMESTAMP)
ORDER BY received_date