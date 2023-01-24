SELECT 
    shipment_id
    ,status_name
    ,operator
    ,x_timestamp
    ,received_date
    ,station_name
FROM
    (
        SELECT 
            shipment_id
            ,status_map.status_name
            ,operator
            ,from_unixtime(main_table.ctime-3600) AS x_timestamp
            ,CASE
                WHEN from_unixtime(main_table.ctime-3600) >= CAST(DATE(from_unixtime(main_table.ctime-3600)) AS timestamp) + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                AND from_unixtime(main_table.ctime-3600) < CAST(DATE(from_unixtime(main_table.ctime-3600)) AS timestamp) + INTERVAL '1' DAY + INTERVAL '6' HOUR + INTERVAL '00' MINUTE
                THEN DATE(from_unixtime(main_table.ctime-3600)) ELSE DATE(from_unixtime(main_table.ctime-3600)) - INTERVAL '1' DAY END AS received_date
            ,station.station_name
            ,row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(main_table.ctime-3600) ASC) AS row_number
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS main_table
        LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS station
        ON main_table.station_id = station.station_id
        LEFT JOIN 
            (
                SELECT 
                    status
                    ,status_name
                FROM thopsbi_lof.thspx_fact_order_tracking_di_th
                GROUP BY status,status_name
            )   AS status_map
        ON main_table.status = status_map.status 
        WHERE station.station_name = 'Sorting center' 
    )
WHERE row_number = 1 AND received_date = CURRENT_DATE - INTERVAL '1' DAY
AND operator IN ()