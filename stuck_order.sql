WITH overall_detail AS
    (
        WITH order_detail AS 
            (   
                SELECT
                    order_tracking.shipment_id
                    ,fleet_order.status AS status_id
                    -- ,latest_status_name
                    ,order_tracking.status
                    ,dwd.latest_operator_name AS operator
                    ,current_station
                    ,lh_task_id
                    ,to_number
                    ,last_timestamp
                    ,dwd.closed_timestamp
                    ,date_diff('day',date(last_timestamp),current_date) + 1 AS aging_last_timestamp
                FROM
                    (
                        SELECT 
                            row_number() OVER (PARTITION BY order_track.shipment_id ORDER BY order_track.ctime DESC) AS rank_no
                            ,shipment_id
                            ,status 
                            ,CAST(TRY(JSON_EXTRACT(JSON_PARSE(content), '$.station_id')) AS INT) AS current_station
                            ,CAST(TRY(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id')) AS VARCHAR) AS lh_task_id
                            ,CAST(TRY(JSON_EXTRACT(JSON_PARSE(content), '$.to_number')) AS VARCHAR) AS to_number
                            ,from_unixtime(order_track.ctime-3600) AS last_timestamp
                        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS order_track 
                    )   AS order_tracking
                LEFT JOIN thopsbi_spx.dwd_pub_shipment_info_df_th AS dwd
                ON order_tracking.shipment_id = dwd.shipment_id
                LEFT JOIN spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live AS fleet_order
                ON order_tracking.shipment_id = fleet_order.shipment_id
                WHERE 
                order_tracking.status IN (8, 9, 15, 18, 32, 33, 34, 35, 36, 40, 47, 48, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 86, 87, 114, 117, 118, 233, 234) 
                AND dwd.closed_timestamp IS NULL
                AND date(last_timestamp) BETWEEN DATE(DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '30' DAY) AND DATE(DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '2' DAY) 
                -- OR (date(last_timestamp) = DATE(DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '1' DAY) AND HOUR(last_timestamp) >= HOUR(CURRENT_TIMESTAMP))
                AND rank_no = 1 
            ) 
			,unsuccessful_log AS
            (   
                SELECT
                    shipment_id
                    ,error_msg
                    ,holder_id
                    ,context_info
                    ,operator_name
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
                -- AND shipment_id IN
                -- ()
            )
        ,forward_aging AS
            (
                SELECT
                    shipment_id
                    ,pickup_done_timestamp
                    ,date_diff('day',date(pickup_done_timestamp),current_date) + 1 AS aging_pickup
                FROM
                    (
                        SELECT
                            row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
                            ,shipment_id
                            ,status
                            ,from_unixtime(ctime-3600) AS pickup_done_timestamp
                        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live AS forward_sla
                        WHERE status in (39,13,0) 
                        AND date(from_unixtime(ctime-3600)) BETWEEN DATE(DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '30' DAY) AND DATE(DATE_TRUNC('DAY', CURRENT_DATE) - INTERVAL '2' DAY)
                    )
                WHERE row_number = 1
            )
        ,reverse_aging AS
            (
                SELECT
                    shipment_id
                    ,first_attempt_return_timestamp
                    ,date_diff('day',date(first_attempt_return_timestamp),current_date) + 1 AS aging_return
                FROM
                (
                    SELECT
                        row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
                        ,shipment_id
                        ,status
                        ,from_unixtime(ctime-3600) AS first_attempt_return_timestamp
                    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as reverse_sla
                    WHERE status in (10,58,95)
                )
            WHERE row_number = 1
            )
        ,handover_aging AS
            (
                SELECT
                    shipment_id
                    ,handover_timestamp
                FROM
                (
                    SELECT
                        row_number() OVER (PARTITION BY shipment_id ORDER BY from_unixtime(ctime-3600) ASC) AS row_number
                        ,shipment_id
                        ,status
                        ,from_unixtime(ctime-3600) AS handover_timestamp
                    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as handover_sla
                    WHERE status in (18)
                )
                WHERE row_number = 1
            )
        ,lh_driver AS
            (
                SELECT
                    shipment_id
                    ,operator AS lh_operator
                FROM
                    (
                        SELECT
                            shipment_id
                            ,operator
                            ,row_number() OVER (PARTITION BY shipment_id ORDER BY ctime DESC) AS rank_operator
                        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
                        -- WHERE status IN (47,48,15,36,233,234,56,57,64,65)
                        WHERE status IN (47,15,233,56,64)
                    )
                WHERE rank_operator = 1
            )

        SELECT
            order_detail.shipment_id
            ,CASE WHEN split_part(status_map.status_name,'_',1) = 'Return' THEN 'Return' ELSE 'Forward' END AS status_flow 
            ,CASE
                WHEN fleet_order.is_warehouse = TRUE THEN 'WH'
                WHEN fleet_order.is_cross_border = TRUE THEN 'CB'
                WHEN fleet_order.is_marketplace = TRUE THEN 'MKP'
                WHEN fleet_order.is_bulky = TRUE THEN 'BKY'
                WHEN fleet_order.is_open_service = TRUE THEN 'NS'
                ELSE 'MKP' END AS order_type 
            ,fleet_order.cogs_amount
            ,CASE
                WHEN order_station.station_name IN ('SOCE','Sorting center','NERC-A','NERC-B','NORC-A','NORC-B','SORC-A','SORC-B','CERC') THEN 'MM'
                ELSE 'CTO/MM' END AS stuck_function 
            ,CASE
                WHEN split_part(order_station.station_name,' -',1) IS NULL AND order_detail.operator LIKE '%thspxagt%' THEN 'SOCE'
                WHEN split_part(order_station.station_name,' -',1) IS NULL AND (order_detail.operator = 'peem.pornpras@shopee.com' OR order_detail.operator = 'aof.wichaidi@shopee.com') THEN 'SORC-A'
                WHEN split_part(order_station.station_name,' -',1) IS NULL AND (order_detail.operator = 'hot.seeharat@shopee.com' OR order_detail.operator = 'jame.kulmorn@shopee.com') THEN 'NERC-B'
                WHEN split_part(order_station.station_name,' -',1) IS NULL AND (order_detail.operator = 'note.kongklay@shopee.com' OR order_detail.operator = 'tong.daon@shopee.com') THEN 'NORC-A'
                ELSE split_part(order_station.station_name,' -',1) END AS current_station
            ,CASE    
                -- check path return order
                WHEN split_part(status_map.status_name,'_',1) = 'Return' AND split_part(order_station.station_name,' -',1) IN ('HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') THEN 'NORC-A'
                WHEN split_part(status_map.status_name,'_',1) = 'Return' AND split_part(order_station.station_name,' -',1) IN ('HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') THEN 'NORC-B'
                WHEN split_part(status_map.status_name,'_',1) = 'Return' AND split_part(order_station.station_name,' -',1) IN ('HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') THEN 'NERC-A'
                WHEN split_part(status_map.status_name,'_',1) = 'Return' AND split_part(order_station.station_name,' -',1) IN ('HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') THEN 'NERC-B'
                WHEN split_part(status_map.status_name,'_',1) = 'Return' AND split_part(order_station.station_name,' -',1) IN ('HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') THEN 'SORC-A'
                WHEN split_part(status_map.status_name,'_',1) = 'Return' AND split_part(order_station.station_name,' -',1) IN ('HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') THEN 'SORC-B'
                WHEN split_part(status_map.status_name,'_',1) = 'Return' AND split_part(order_station.station_name,' -',1) IN ('HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') THEN 'CERC'
            
                -- check path by current station
                WHEN split_part(order_station.station_name,' -',1) = 'Sorting center' THEN 'SOCE'
                WHEN split_part(order_station.station_name,' -',1) IS NULL AND order_detail.operator LIKE '%thspxagt%' THEN 'SOCE'
                WHEN split_part(order_station.station_name,' -',1) IN ('CERC','NERC-A','NERC-B','SORC-A','SORC-B','NORC-A','NORC-B','SOCE') THEN split_part(order_station.station_name,' -',1)
                WHEN split_part(order_station.station_name,' ',1) IN ('FPHIT-A','FPHIT-B','FWTNG','FKPET','FTAKK','FMSOD','FNANN','FPJIT','FPBUN','FLOMS','FPRAE','FLOEI','FTHAI','FSWKL','FUTTA') THEN 'NORC-A'
                WHEN split_part(order_station.station_name,' ',1) IN ( 'FSRPI','FCMAI-A','FCMAI-B','FSSAI','FMRIM','FDSKT','FCDAO','FFRNG','FSTNG','FDONG','FSANK','FPAAN','FCRAI','FMSAI','FMJUN','FPYAO','FLPNG','FLPUN') THEN 'NORC-B'
                WHEN split_part(order_station.station_name,' ',1) IN ('FKRAT-A','FKRAT-B','FKRAT-C','FNSUG','FCOCH','FPAKC','FPIMY','FBUAY','FSKIU','FDKTD','FPTCI','FKNBR','FSNEN','FPHUK','FCYPM','FBRAM','FLPMT','FSTUK','FNRNG','FPKCI','FYASO','FSSKT','FSRIN','FSKPM','FPSAT','FUBON-A','FUBON-B','FWRIN','FDUDM') THEN 'NERC-A'
                WHEN split_part(order_station.station_name,' ',1) IN ('FKKAN-B','FKKAN-A','FBPAI','FCPAE','FKLSN','FYTAD','FNKPN','FTPNM','FMKAM','FKSPS','FKTWC','FMDHN','FROET','FSKON','FNKAI','FPSAI','FUDON-A','FUDON-B') THEN 'NERC-B' 
                WHEN split_part(order_station.station_name,' ',1) IN ( 'FPPIN','FSMUI','FSRAT','FKDIT','FBNSN','FKRBI','FCPON','FPTIL','FSAWE','FTSNG','FCOUD','FNKSI','FTYAI','FTSLA','FSICN','FTLNG','FPHKT-A','FPHKT-B','FRNNG') THEN 'SORC-A' 
                WHEN split_part(order_station.station_name,' ',1) IN ('FHYAI-B','FHYAI-A','FSKLA','FSDAO','FTANG','FNARA','FPTNI','FKGPO','FMYOR','FYLNG','FPATL','FKUKN','FYALA','FRMAN','FSTUN') THEN 'SORC-B' 
                WHEN split_part(order_station.station_name,' ',1) IN ('FSWAN','FTAKI','FBPIN','FAYUT','FSENA','FAUTH','FWNOI','FLOPB','FKSRG','FCBDN','FPTNK','FKKOI','FSRBR','FBAMO','FPTBT','FPTBT','FNKAE','FSING','FTONG') THEN 'CERC'
                WHEN split_part(order_station.station_name,' -',1) IN ('FSWAN', 'FTAKI', 'FWNOI', 'FBPIN', 'FAYUT', 'FAUTH', 'FKKOI', 'FSRBR', 'FLOPB') THEN 'CERC'
                WHEN split_part(order_station.station_name,' -',1) IN ('HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') THEN 'NORC-A'
                WHEN split_part(order_station.station_name,' -',1) IN ('HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') THEN 'NORC-B'
                WHEN split_part(order_station.station_name,' -',1) IN ('HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') THEN 'NERC-A'
                WHEN split_part(order_station.station_name,' -',1) IN ('HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') THEN 'NERC-B'
                WHEN split_part(order_station.station_name,' -',1) IN ('HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') THEN 'SORC-A'
                WHEN split_part(order_station.station_name,' -',1) IN ('HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') THEN 'SORC-B'
                WHEN split_part(order_station.station_name,' -',1) IN ('HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') THEN 'CERC'
            
                -- check path by destination hub
                WHEN split_part(order_station.station_name,' -',1) = 'SOCE' AND split_part(destination_hub.lm_station,' -',1) IN ('HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') THEN 'NORC-A'
                WHEN split_part(order_station.station_name,' -',1) = 'SOCE' AND split_part(destination_hub.lm_station,' -',1) IN ('HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') THEN 'NORC-B'
                WHEN split_part(order_station.station_name,' -',1) = 'SOCE' AND split_part(destination_hub.lm_station,' -',1) IN ('HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') THEN 'NERC-A'
                WHEN split_part(order_station.station_name,' -',1) = 'SOCE' AND split_part(destination_hub.lm_station,' -',1) IN ('HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') THEN 'NERC-B'
                WHEN split_part(order_station.station_name,' -',1) = 'SOCE' AND split_part(destination_hub.lm_station,' -',1) IN ('HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') THEN 'SORC-A'
                WHEN split_part(order_station.station_name,' -',1) = 'SOCE' AND split_part(destination_hub.lm_station,' -',1) IN ('HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') THEN 'SORC-B'
                WHEN split_part(order_station.station_name,' -',1) = 'SOCE' AND split_part(destination_hub.lm_station,' -',1) IN ('HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') THEN 'CERC'
                WHEN order_station.station_name LIKE 'F%' OR order_station.station_name LIKE 'D%' OR order_station.station_name LIKE 'H%' OR order_station.station_name = 'Sorting Center' THEN 'SOCE'
                ELSE split_part(order_station.station_name,' -',1) END AS pic_name
            ,order_detail.last_timestamp
            ,status_map.status_name AS latest_status_name
            -- ,order_detail.latest_status_name
            ,CASE
                WHEN status_map.status IN (47,48,15,36,233,234,56,57,64,65) THEN lh_driver.lh_operator
                ELSE order_detail.operator END AS operator
            ,forward_aging.pickup_done_timestamp
            ,aging_pickup
            ,aging_last_timestamp
            ,aging_return
            ,date_diff('day',date(handover_aging.handover_timestamp),current_date) + 1 AS aging_handover
            ,order_detail.lh_task_id
            ,order_detail.to_number
            ,CASE
                WHEN split_part(status_map.status_name,'_',1) = 'Return' THEN split_part(destination_hub.ret_station,' -',1)
                WHEN operator = 'Flash' or destination_hub.lm_station = 'SOCE' THEN 'Flash'
                WHEN split_part(destination_hub.lm_station,' -',1) LIKE '%Kerry%'THEN 'Kerry'
                WHEN split_part(destination_hub.lm_station,' -',1) LIKE '%Ninja%'THEN 'Ninja van'
                ELSE split_part(destination_hub.lm_station,' -',1) END AS hub_destination
            ,CASE
                WHEN split_part(destination_hub.lm_station,' -',1) = ANY (VALUES 'HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') THEN 'NORC-A'
                WHEN split_part(destination_hub.lm_station,' -',1) = ANY (VALUES 'HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') THEN 'NORC-B'
                WHEN split_part(destination_hub.lm_station,' -',1) = ANY (VALUES 'HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') THEN 'NERC-A'
                WHEN split_part(destination_hub.lm_station,' -',1) = ANY (VALUES 'HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') THEN 'NERC-B' 
                WHEN split_part(destination_hub.lm_station,' -',1) = ANY (VALUES 'HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') THEN 'SORC-A' 
                WHEN split_part(destination_hub.lm_station,' -',1) = ANY (VALUES 'HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') THEN 'SORC-B' 
                WHEN split_part(destination_hub.lm_station,' -',1) = ANY (VALUES 'HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') THEN 'CERC'
                ELSE NULL END AS rc_pass
            ,CASE
                WHEN error_timestamp > order_detail.last_timestamp THEN unsucessful_log_hub ELSE NULL END AS unsucessful_log_hub
            ,CASE
                WHEN (error_timestamp > order_detail.last_timestamp AND unsucessful_log_hub LIKE 'H%') 
                OR (error_timestamp > order_detail.last_timestamp AND unsucessful_log_hub LIKE 'F%') 
                OR (error_timestamp > order_detail.last_timestamp AND unsucessful_log_hub LIKE 'D%') 
                OR (error_timestamp > order_detail.last_timestamp AND unsucessful_log_hub LIKE '4%') 
                THEN 'others' ELSE 'mm' END AS unsuccessful_log_check
            ,CASE
            WHEN error_timestamp > order_detail.last_timestamp THEN error_timestamp ELSE NULL END AS error_timestamp
        FROM order_detail
        LEFT JOIN thopsbi_spx.dwd_pub_shipment_info_df_th AS fleet_order
        ON order_detail.shipment_id = fleet_order.shipment_id
        LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS order_station
        ON order_detail.current_station = order_station.station_id
        LEFT JOIN unsuccessful_log
        ON order_detail.shipment_id = unsuccessful_log.shipment_id
        LEFT JOIN forward_aging
        ON order_detail.shipment_id = forward_aging.shipment_id
        LEFT JOIN reverse_aging
        ON order_detail.shipment_id = reverse_aging.shipment_id
        LEFT JOIN handover_aging
        ON order_detail.shipment_id = handover_aging.shipment_id
        LEFT JOIN lh_driver
        ON order_detail.shipment_id = lh_driver.shipment_id
        LEFT JOIN 
            (
                SELECT 
                    shipment_id 
                    ,destination.station_name AS lm_station
                    ,return_station.station_name AS ret_station
                FROM spx_mart.dwd_spx_fleet_order_tab_ri_th_ro AS shipment_detail

                LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS destination
                ON shipment_detail.station_id = destination.station_id 
    
                LEFT JOIN spx_mart.dim_spx_station_tab_ri_th_ro AS return_station
                ON shipment_detail.return_station_id = return_station.station_id 
            )   AS destination_hub
        ON order_detail.shipment_id = destination_hub.shipment_id
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
        ON order_detail.status = status_map.status
        WHERE order_detail.shipment_id LIKE 'SPXTH%' 
        AND aging_pickup < 30
        -- ('SPXTH02082209899B',
        -- 'SPXTH02082354968B',
        -- 'SPXTH02082518375C')
    )

    SELECT
        CASE
        WHEN current_station IN ('NERC-A','NERC-B','NORC-A','NORC-B','SORC-A','SORC-B','CERC') THEN current_station
        ELSE pic_name END AS pic_name 
        ,CASE
            WHEN latest_status_name  = ANY (VALUES 'LMHub_LHTransporting','FMHub_LHTransporting','Exception_LHTransporting','Return_LMHub_LHTransporting','Return_SOC_LHTransporting','SOC_LHTransporting') THEN 'LH'
            WHEN pic_name = 'SOCE' THEN 'SOC'
            WHEN current_station IN ('NERC-A','NERC-B','NORC-A','NORC-B','SORC-A','SORC-B','CERC') THEN 'RC'
            WHEN pic_name IN ('NERC-A','NERC-B','NORC-A','NORC-B','SORC-A','SORC-B','CERC') THEN 'RC'
            ELSE NULL END AS station_pic
        ,shipment_id
        ,status_flow 
        ,order_type
        ,cogs_amount
        ,stuck_function 
        ,current_station
        ,last_timestamp
        ,latest_status_name
        ,operator
        ,aging_pickup
        ,aging_last_timestamp
        -- ,aging_handover
        ,aging_return
        ,unsucessful_log_hub
        ,to_number
        ,lh_task_id
        ,CASE
        WHEN status_flow = 'Return' AND (order_type = 'CB' OR order_type = 'WH') THEN 'SCOM'
        WHEN hub_destination IN ('NERC-A','NERC-B','NORC-A','NORC-B','SORC-A','SORC-B','CERC','SOCE') THEN 'Flash'
        ELSE hub_destination END AS hub_destination
        ,CASE
            WHEN status_flow = 'Return' AND (order_type = 'CB' OR order_type = 'WH') THEN 'GBKK'
            WHEN hub_destination IN ('NERC-A','NERC-B','NORC-A','NORC-B','SORC-A','SORC-B','CERC') THEN '4PL'
            WHEN hub_destination IN ('SIP-LMHub','HNKCS','HGUAI','HLDLK','HPEAW','HPMSK','LMBKY') THEN 'GBKK'
            WHEN hub_destination IN ('HAUTH','HTSLA','HCOCH') THEN 'UPC'
            WHEN gbkk_upc_ops_region is NULL THEN '4PL' 
            WHEN gbkk_upc_ops_region <> 'GBKK' AND gbkk_upc_ops_region <> '4PL' THEN 'UPC'
            ELSE gbkk_upc_ops_region END AS destination_zone
        -- ,rc_pass
        -- ,pickup_done_timestamp
    FROM overall_detail
    LEFT JOIN thopsbi_lof.spx_index_region_temp AS hub_zone
    ON substring (split_part(hub_destination,'-',1),2,4) = hub_zone.district_code
    WHERE hub_destination IS NOT NULL 
    AND current_station <> 'SOCW'
    AND unsuccessful_log_check = 'mm'
    ORDER BY aging_pickup DESC
