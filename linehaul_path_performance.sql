with sla_precale AS (
    SELECT 
        DATE(report_date) AS recieve_date,
        DATE(sla_d1) AS sla_d_1_date,
        DATE(sla_d2) AS sla_d_2_date,
        DATE(sla_d3) AS sla_d_3_date,
        CAST(FROM_ISO8601_TIMESTAMP(report_date) AS TIMESTAMP) rec_date_time,
        CAST(FROM_ISO8601_TIMESTAMP(sla_d1) AS TIMESTAMP) sla_d_1_time,
        CAST(FROM_ISO8601_TIMESTAMP(sla_d2) AS TIMESTAMP) sla_d_2_time
     FROM dev_thopsbi_lof.spx_analytics_sla_precal_date_v1
),
seller_address AS (
    SELECT 
        fleet_order.shipment_id,
        CASE WHEN pickup.seller_province IS NOT NULL THEN pickup.seller_province
            ELSE dropoff.seller_province END AS seller_province,
        CASE WHEN pickup.seller_district IS NOT NULL THEN pickup.seller_district
            ELSE dropoff.seller_district END AS seller_district
        
    FROM spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live AS fleet_order

    LEFT JOIN (
        SELECT 
            pickup1.pickup_order_id,
            seller_addr_state AS seller_province,
            seller_addr_district AS seller_district
        FROM spx_mart.shopee_fms_th_db__pickup_order_tab__th_daily_s0_live pickup1
        INNER JOIN (
            SELECT
                pickup_order_id,
                MAX(ctime) AS latest_ctime 
            FROM spx_mart.shopee_fms_th_db__pickup_order_tab__th_daily_s0_live
            GROUP BY 1
        ) AS pickup2
        ON pickup1.pickup_order_id = pickup2.pickup_order_id
        AND pickup1.ctime = pickup2.latest_ctime
        
    ) pickup
    ON pickup.pickup_order_id = fleet_order.shipment_id

    LEFT JOIN (
        SELECT 
            shipment_id,
            seller_state AS seller_province,
            seller_city AS seller_district
        FROM spx_mart.shopee_fms_th_db__dropoff_order_tab__th_daily_s0_live
    ) dropoff
    ON dropoff.shipment_id = fleet_order.shipment_id
),
raw_rc as(
select 

fleet_order.shipment_id
,case 
    when xrc_received_time < min_soc_received then pu_rc_rec_track.rc_station  
    when min_soc_received is null then pu_rc_rec_track.rc_station 
  else null  
end as pickup_rc_station 
,case 
        when buyer_region = seller_region and min_soc_received is null and max_RC_Received > min_RC_Received then del_rc_rec_track.rc_station 
        when max_RC_Received > min_soc_received then del_rc_rec_track.rc_station 
  else null 
end as delivery_rc_station  
,xrc_received_time
,min_soc_received
,min_soc_lh_transporting
,min_xrc_lh_transporting_time
,min_xrc_lh_transported_time
,cast("date_diff"('hour',min_xrc_lh_transporting_time,min_xrc_lh_transported_time) as int) as rc_to_soc_delivery_leadtime
,back_haul_line_haul_task_id.lh_task_id as backhaul_lh_task
,shuttle_line_haul_task_id.lh_task_id as shuttle_lh_task 


from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join 
   (
       select 
       shipment_id
       ,min(case when station_id in (3,242) and status = 8 then from_unixtime(ctime-3600) end) as min_soc_received 
       ,min(case when station_id in (3,242) and status = 15 then from_unixtime(ctime-3600) end) as min_soc_lh_transporting 
       ,min(case when station_id in (71,77,82,983,1479,1480)  and status = 8 then from_unixtime(ctime-3600) end) as xrc_received_time
       ,min(case when station_id in (71,77,82,983,1479,1480)  and status = 15 then from_unixtime(ctime-3600) end) as min_xrc_lh_transporting_time
       ,min(case when station_id in (71,77,82,983,1479,1480)  and status = 36 then from_unixtime(ctime-3600) end) as min_xrc_lh_transported_time

        ,max(case when station_id in (71,77,78,82,983,1350,1479,1480) and status = 8 then from_unixtime(ctime-3600) end) as max_RC_Received
        ,min(case when station_id in (71,77,78,82,983,1350,1479,1480) and status = 8 then from_unixtime(ctime-3600) end) as min_RC_Received
     
      
    
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
        where date(from_unixtime(ctime-3600)) >= current_date - interval '120' day 
        group by 1 
   ) as order_track 
on order_track.shipment_id = fleet_order.shipment_id
LEFT JOIN
    (
        select 
        shipment_id
        ,station.station_name as rc_station
        from 
            (
            select 
                shipment_id
                ,station_id
                ,row_number() over(partition by shipment_id order by ctime) as row_number

            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            where status = 8 and station_id in (71,77,82,983,1479,1480) and date(from_unixtime(ctime-3600)) >= current_date - interval '14' day 
            ) as rc_rec 
            left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station 
            on station.id = rc_rec.station_id
        where row_number = 1

    ) as pu_rc_rec_track 
on pu_rc_rec_track.shipment_id = fleet_order.shipment_id

LEFT JOIN
    (
        select 
        shipment_id
        ,station.station_name as rc_station
        from 
            (
            select 
                shipment_id
                ,station_id
                ,row_number() over(partition by shipment_id order by ctime desc ) as row_number

            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            where status = 8 and station_id in (71,77,82,983,1350,1479,1480) and date(from_unixtime(ctime-3600)) >= current_date - interval '14' day 
            ) as rc_rec 
            left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station 
            on station.id = rc_rec.station_id
        where row_number = 1

    ) as del_rc_rec_track 
on del_rc_rec_track.shipment_id = fleet_order.shipment_id
    
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live AS buyer_info
ON fleet_order.shipment_id = buyer_info.shipment_id

LEFT JOIN seller_address
ON fleet_order.shipment_id = seller_address.shipment_id

LEFT JOIN 
       (
        select 
        distinct
        province
        ,lh_region as buyer_region  
        from thopsbi_lof.spx_index_region_temp
        ) as buyer_region_mapping
ON buyer_info.buyer_addr_state = buyer_region_mapping.province

LEFT JOIN
       (
        select 
        distinct
        province
        ,lh_region  as seller_region 
        from thopsbi_lof.spx_index_region_temp
        ) as seller_region_mapping
ON seller_address.seller_province = seller_region_mapping.province

LEFT JOIN
    (
        select 
        shipment_id
        ,lh_task_id
        from 
            (
            select 
                shipment_id
                ,station_id
                ,try(cast(json_extract(json_parse(content),'$.linehaul_task_id') as varchar)) as lh_task_id
                ,row_number() over(partition by shipment_id order by ctime) as row_number

            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            where status = 35 and station_id in (71,77,82,983,1350,1479,1480) and date(from_unixtime(ctime-3600)) >= current_date - interval '60' day 
            ) as rc_rec 
        where row_number = 1

    ) as back_haul_line_haul_task_id   
on back_haul_line_haul_task_id.shipment_id = fleet_order.shipment_id

LEFT JOIN
    (
        select 
        shipment_id
        ,lh_task_id
        from 
            (
            select 
                shipment_id
                ,station_id
                ,try(cast(json_extract(json_parse(content),'$.linehaul_task_id') as varchar)) as lh_task_id
                ,row_number() over(partition by shipment_id order by ctime) as row_number

            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            where status = 35 and station_id in (3,242) and date(from_unixtime(ctime-3600)) >= current_date - interval '60' day 
            ) as rc_rec 
        where row_number = 1

    ) as shuttle_line_haul_task_id   
on shuttle_line_haul_task_id.shipment_id = fleet_order.shipment_id

where date(min_soc_received - interval '6' hour) between current_date - interval '30' day and current_date - interval '1' day  
),
route_map as(
select 
    *
    ,case when min_soc_received is not null then 
        case 
            when pickup_rc_station = 'CERC' and delivery_rc_station = 'NERC-A' then 'CERC-SOC-NERCA'
            when pickup_rc_station = 'CERC' and delivery_rc_station = 'NERC-B' then 'CERC-SOC-NERCB'
            when pickup_rc_station = 'CERC' and delivery_rc_station = 'NORC-A' then 'CERC-SOC-NORCA'
            when pickup_rc_station = 'CERC' and delivery_rc_station = 'NORC-B' then 'CERC-SOC-NORCB'
            when pickup_rc_station = 'CERC' and delivery_rc_station = 'SORC-A' then 'CERC-SOC-SORCA'
            when pickup_rc_station = 'CERC' and delivery_rc_station = 'SORC-B' then 'CERC-SOC-SORCB'

            when pickup_rc_station = 'NERC-A' and delivery_rc_station = 'NORC-A' then 'NERCA-SOC-NORCA'
            when pickup_rc_station = 'NERC-A' and delivery_rc_station = 'NORC-B' then 'NERCA-SOC-NORCB'
            when pickup_rc_station = 'NERC-A' and delivery_rc_station = 'SORC-A' then 'NERCA-SOC-SORCA'
            when pickup_rc_station = 'NERC-A' and delivery_rc_station = 'SORC-B' then 'NERCA-SOC-SORCB'
            when pickup_rc_station = 'NERC-A' and delivery_rc_station = 'CERC' then 'NERCA-SOC-CERC'

            when pickup_rc_station = 'NERC-B' and delivery_rc_station = 'NORC-A' then 'NERCB-SOC-NORCA'
            when pickup_rc_station = 'NERC-B' and delivery_rc_station = 'NORC-B' then 'NERCB-SOC-NORCB'
            when pickup_rc_station = 'NERC-B' and delivery_rc_station = 'SORC-A' then 'NERCB-SOC-SORCA'
            when pickup_rc_station = 'NERC-B' and delivery_rc_station = 'SORC-B' then 'NERCB-SOC-SORCB'
            when pickup_rc_station = 'NERC-B' and delivery_rc_station = 'CERC' then 'NERCB-SOC-CERC'

            when pickup_rc_station = 'NORC-A' and delivery_rc_station = 'NERC-A' then 'NORCA-SOC-NERCA'
            when pickup_rc_station = 'NORC-A' and delivery_rc_station = 'NERC-B' then 'NORCA-SOC-NERCB'
            when pickup_rc_station = 'NORC-A' and delivery_rc_station = 'SORC-A' then 'NORCA-SOC-SORCA'
            when pickup_rc_station = 'NORC-A' and delivery_rc_station = 'SORC-B' then 'NORCA-SOC-SORCB'
            when pickup_rc_station = 'NORC-A' and delivery_rc_station = 'CERC' then 'NORCA-SOC-CERC'

            when pickup_rc_station = 'NORC-B' and delivery_rc_station = 'NERC-A' then 'NORCB-SOC-NERCA'
            when pickup_rc_station = 'NORC-B' and delivery_rc_station = 'NERC-B' then 'NORCB-SOC-NERCB'
            when pickup_rc_station = 'NORC-B' and delivery_rc_station = 'SORC-A' then 'NORCB-SOC-SORCA'
            when pickup_rc_station = 'NORC-B' and delivery_rc_station = 'SORC-B' then 'NORCB-SOC-SORCB'
            when pickup_rc_station = 'NORC-B' and delivery_rc_station = 'CERC' then 'NORCB-SOC-CERC'

            when pickup_rc_station = 'SORC-B' and delivery_rc_station = 'NERC-A' then 'SORCB-SOC-NERCA'
            when pickup_rc_station = 'SORC-B' and delivery_rc_station = 'NERC-B' then 'SORCB-SOC-NERCB'
            when pickup_rc_station = 'SORC-B' and delivery_rc_station = 'NORC-A' then 'SORCB-SOC-NORCA'
            when pickup_rc_station = 'SORC-B' and delivery_rc_station = 'NORC-B' then 'SORCB-SOC-NORCB'
            when pickup_rc_station = 'SORC-B' and delivery_rc_station = 'CERC' then 'SORCB-SOC-CERC'
        END 
    end AS lh_route 
    ,back_haul_driver.lh_driver as back_haul_driver
    ,shuttle_driver.lh_driver as shuttle_driver
from raw_rc

left join
    (
    select
    lh_task.task_number
    ,lh_task.driver_id as lh_driver_id  
    ,driver_tab.driver_name as lh_driver
    from spx_mart.shopee_fms_th_db__line_haul_task_tab__th_continuous_s0_live as lh_task
    left join spx_mart.shopee_fms_th_db__driver_tab__th_continuous_s0_live as driver_tab
    on lh_task.driver_id = driver_tab.driver_id
    ) as back_haul_driver
on raw_rc.backhaul_lh_task = back_haul_driver.task_number

left join
    (
    select
    lh_task.task_number
    ,lh_task.driver_id as lh_driver_id  
    ,driver_tab.driver_name as lh_driver
    from spx_mart.shopee_fms_th_db__line_haul_task_tab__th_continuous_s0_live as lh_task
    left join spx_mart.shopee_fms_th_db__driver_tab__th_continuous_s0_live as driver_tab
    on lh_task.driver_id = driver_tab.driver_id
    ) as shuttle_driver
on raw_rc.shuttle_lh_task = shuttle_driver.task_number
),
ontime_cal as(
select 
    *
    ,case 
            when lh_route = 'CERC-SOC-NERCA' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'CERC-SOC-NERCB' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'CERC-SOC-NORCA' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'CERC-SOC-NORCB' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'CERC-SOC-SORCA' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1
            when lh_route = 'CERC-SOC-SORCB' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 

            when lh_route = 'NERCA-SOC-NORCA' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-NORCB' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-SORCA' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-SORCB' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-CERC'  and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 

            when lh_route  = 'NERCB-SOC-NORCA' and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCB-SOC-NORCB'  and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCB-SOC-SORCA'  and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1
            when lh_route = 'NERCB-SOC-SORCB'  and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCB-SOC-CERC'   and date(min_xrc_lh_transporting_time - interval '5' hour) <= date(xrc_received_time) then 1

            when lh_route = 'NORCA-SOC-NERCA' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1
            when lh_route = 'NORCA-SOC-NERCB' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1
            when lh_route = 'NORCA-SOC-SORCA' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NORCA-SOC-SORCB' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NORCA-SOC-CERC'  and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 

            when  lh_route = 'NORCB-SOC-NERCA' and date(min_xrc_lh_transporting_time + interval '2' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-NERCB' and date(min_xrc_lh_transporting_time + interval '2' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-SORCA' and date(min_xrc_lh_transporting_time + interval '2' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-SORCB' and date(min_xrc_lh_transporting_time + interval '2' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-CERC' and date(min_xrc_lh_transporting_time + interval '2' hour) <= date(xrc_received_time) then 1 

            when  lh_route = 'SORCA-SOC-NERCA' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-NERCB' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-NORCA' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-NORCB' and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-CERC'  and date(min_xrc_lh_transporting_time - interval '3' hour) <= date(xrc_received_time) then 1 

            when  lh_route = 'SORCB-SOC-NERCA' and date(min_xrc_lh_transporting_time + interval '5' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-NERCB' and date(min_xrc_lh_transporting_time + interval '5' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-NORCA' and date(min_xrc_lh_transporting_time + interval '5' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-NORCB' and date(min_xrc_lh_transporting_time + interval '5' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-CERC'  and date(min_xrc_lh_transporting_time + interval '5' hour) <= date(xrc_received_time) then 1 
            else 0
        end as is_rc_transporting_ontime 

        ,case 
            when lh_route = 'CERC-SOC-NERCA' and date(min_xrc_lh_transported_time - interval '9' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'CERC-SOC-NERCB' and date(min_xrc_lh_transported_time - interval '9' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'CERC-SOC-NORCA' and date(min_xrc_lh_transported_time - interval '9' hour) <= date(xrc_received_time)  then 1 
            when lh_route = 'CERC-SOC-NORCB' and date(min_xrc_lh_transported_time - interval '9' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'CERC-SOC-SORCA' and date(min_xrc_lh_transported_time - interval '9' hour) <= date(xrc_received_time) then 1
            when lh_route = 'CERC-SOC-SORCB' and date(min_xrc_lh_transported_time - interval '9' hour) <= date(xrc_received_time) then 1 

            when lh_route = 'NERCA-SOC-NORCA' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-NORCB' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-SORCA' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-SORCB' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCA-SOC-CERC'  and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 

            when lh_route  = 'NERCB-SOC-NORCA' and date(min_xrc_lh_transported_time - interval '11' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCB-SOC-NORCB'  and date(min_xrc_lh_transported_time - interval '11' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCB-SOC-SORCA'  and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1
            when lh_route = 'NERCB-SOC-SORCB'  and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NERCB-SOC-CERC'   and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1

            when lh_route = 'NORCA-SOC-NERCA' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1
            when lh_route = 'NORCA-SOC-NERCB' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1
            when lh_route = 'NORCA-SOC-SORCA' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NORCA-SOC-SORCB' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when lh_route = 'NORCA-SOC-CERC'  and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 

            when  lh_route = 'NORCB-SOC-NERCA' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-NERCB' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-SORCA' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-SORCB' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'NORCB-SOC-CERC' and date(min_xrc_lh_transported_time - interval '10' hour) <= date(xrc_received_time) then 1 

            when  lh_route = 'SORCA-SOC-NERCA' and date(min_xrc_lh_transported_time - interval '14' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-NERCB' and date(min_xrc_lh_transported_time - interval '14' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-NORCA' and date(min_xrc_lh_transported_time - interval '14' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-NORCB' and date(min_xrc_lh_transported_time - interval '14' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCA-SOC-CERC'  and date(min_xrc_lh_transported_time - interval '14' hour) <= date(xrc_received_time) then 1 

            when  lh_route = 'SORCB-SOC-NERCA' and date(min_xrc_lh_transported_time - interval '11' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-NERCB' and date(min_xrc_lh_transported_time - interval '11' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-NORCA' and date(min_xrc_lh_transported_time - interval '11' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-NORCB' and date(min_xrc_lh_transported_time - interval '11' hour) <= date(xrc_received_time) then 1 
            when  lh_route = 'SORCB-SOC-CERC'  and date(min_xrc_lh_transported_time - interval '11' hour) <= date(xrc_received_time) then 1 
            else 0
        end as is_rc_transported_ontime 

        ,case 
            when lh_route = 'CERC-SOC-NERCA' and rc_to_soc_delivery_leadtime <= 4  then 1 
            when lh_route = 'CERC-SOC-NERCB' and rc_to_soc_delivery_leadtime <= 4 then 1 
            when lh_route = 'CERC-SOC-NORCA' and rc_to_soc_delivery_leadtime <= 4   then 1 
            when lh_route = 'CERC-SOC-NORCB' and rc_to_soc_delivery_leadtime <= 4   then 1 
            when lh_route = 'CERC-SOC-SORCA' and rc_to_soc_delivery_leadtime <= 4   then 1
            when lh_route = 'CERC-SOC-SORCB' and rc_to_soc_delivery_leadtime <= 4   then 1 

            when lh_route = 'NERCA-SOC-NORCA' and rc_to_soc_delivery_leadtime <= 5  then 1 
            when lh_route = 'NERCA-SOC-NORCB' and rc_to_soc_delivery_leadtime <= 5  then 1 
            when lh_route = 'NERCA-SOC-SORCA' and rc_to_soc_delivery_leadtime <= 5 then 1 
            when lh_route = 'NERCA-SOC-SORCB' and rc_to_soc_delivery_leadtime <= 5  then 1 
            when lh_route = 'NERCA-SOC-CERC'  and rc_to_soc_delivery_leadtime <= 5  then 1 

            when lh_route  = 'NERCB-SOC-NORCA' and rc_to_soc_delivery_leadtime <= 8  then 1 
            when lh_route = 'NERCB-SOC-NORCB'  and rc_to_soc_delivery_leadtime <= 8  then 1 
            when lh_route = 'NERCB-SOC-SORCA'  and rc_to_soc_delivery_leadtime <= 5  then 1
            when lh_route = 'NERCB-SOC-SORCB'  and rc_to_soc_delivery_leadtime <= 5  then 1 
            when lh_route = 'NERCB-SOC-CERC'   and rc_to_soc_delivery_leadtime <= 5  then 1

            when lh_route = 'NORCA-SOC-NERCA' and rc_to_soc_delivery_leadtime <= 7  then 1
            when lh_route = 'NORCA-SOC-NERCB' and rc_to_soc_delivery_leadtime <= 7  then 1
            when lh_route = 'NORCA-SOC-SORCA' and rc_to_soc_delivery_leadtime <= 7  then 1 
            when lh_route = 'NORCA-SOC-SORCB' and rc_to_soc_delivery_leadtime <= 7  then 1 
            when lh_route = 'NORCA-SOC-CERC'  and rc_to_soc_delivery_leadtime <= 7  then 1 

            when  lh_route = 'NORCB-SOC-NERCA' and rc_to_soc_delivery_leadtime <= 12  then 1 
            when  lh_route = 'NORCB-SOC-NERCB' and rc_to_soc_delivery_leadtime <= 12  then 1 
            when  lh_route = 'NORCB-SOC-SORCA' and rc_to_soc_delivery_leadtime <= 12  then 1 
            when  lh_route = 'NORCB-SOC-SORCB' and rc_to_soc_delivery_leadtime <= 12  then 1 
            when  lh_route = 'NORCB-SOC-CERC'  and rc_to_soc_delivery_leadtime <= 12  then 1 

            when  lh_route = 'SORCA-SOC-NERCA' and rc_to_soc_delivery_leadtime <= 11  then 1 
            when  lh_route = 'SORCA-SOC-NERCB' and rc_to_soc_delivery_leadtime <= 11  then 1 
            when  lh_route = 'SORCA-SOC-NORCA' and rc_to_soc_delivery_leadtime <= 11  then 1 
            when  lh_route = 'SORCA-SOC-NORCB' and rc_to_soc_delivery_leadtime <= 11  then 1 
            when  lh_route = 'SORCA-SOC-CERC'  and rc_to_soc_delivery_leadtime <= 11  then 1 

            when  lh_route = 'SORCB-SOC-NERCA' and rc_to_soc_delivery_leadtime <= 16  then 1 
            when  lh_route = 'SORCB-SOC-NERCB' and rc_to_soc_delivery_leadtime <= 16  then 1 
            when  lh_route = 'SORCB-SOC-NORCA' and rc_to_soc_delivery_leadtime <= 16  then 1 
            when  lh_route = 'SORCB-SOC-NORCB' and rc_to_soc_delivery_leadtime <= 16  then 1 
            when  lh_route = 'SORCB-SOC-CERC'  and rc_to_soc_delivery_leadtime <= 16  then 1 
            else 0
        end as rc_traveling_soc_leadtime_check 

        ,case 
            when lh_route = 'CERC-SOC-NERCA' and date(min_soc_lh_transporting - interval '17' hour) <= date(min_soc_received-interval '6' hour)then 1 
            when lh_route = 'CERC-SOC-NERCB' and date(min_soc_lh_transporting - interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'CERC-SOC-NORCA' and date(min_soc_lh_transporting - interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'CERC-SOC-NORCB' and date(min_soc_lh_transporting - interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'CERC-SOC-SORCA' and date(min_soc_lh_transporting - interval '13' hour) <= date(min_soc_received-interval '6' hour) then 1
            when lh_route = 'CERC-SOC-SORCB' and date(min_soc_lh_transporting - interval '12' hour) <= date(min_soc_received-interval '6' hour) then 1 

            when lh_route = 'NERCA-SOC-NORCA' and date(min_soc_lh_transporting - interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NERCA-SOC-NORCB' and date(min_soc_lh_transporting - interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NERCA-SOC-SORCA' and date(min_soc_lh_transporting - interval '13' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NERCA-SOC-SORCB' and date(min_soc_lh_transporting - interval '12' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NERCA-SOC-CERC'  and date(min_soc_lh_transporting - interval '18' hour) <= date(min_soc_received-interval '6' hour) then 1 

            when lh_route  = 'NERCB-SOC-NORCA' and date(min_soc_lh_transporting - interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NERCB-SOC-NORCB'  and date(min_soc_lh_transporting - interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NERCB-SOC-SORCA'  and date(min_soc_lh_transporting - interval '13' hour) <= date(min_soc_received-interval '6' hour) then 1
            when lh_route = 'NERCB-SOC-SORCB'  and date(min_soc_lh_transporting - interval '12' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NERCB-SOC-CERC'   and date(min_soc_lh_transporting - interval '18' hour) <= date(min_soc_received-interval '6' hour) then 1

            when lh_route = 'NORCA-SOC-NERCA' and date(min_soc_lh_transporting - interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1
            when lh_route = 'NORCA-SOC-NERCB' and date(min_soc_lh_transporting - interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1
            when lh_route = 'NORCA-SOC-SORCA' and date(min_soc_lh_transporting - interval '13' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NORCA-SOC-SORCB' and date(min_soc_lh_transporting - interval '12' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when lh_route = 'NORCA-SOC-CERC'  and date(min_soc_lh_transporting - interval '18' hour) <= date(min_soc_received-interval '6' hour) then 1 

            when  lh_route = 'NORCB-SOC-NERCA' and date(min_soc_lh_transporting + interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'NORCB-SOC-NERCB' and date(min_soc_lh_transporting + interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'NORCB-SOC-SORCA' and date(min_soc_lh_transporting + interval '13' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'NORCB-SOC-SORCB' and date(min_soc_lh_transporting + interval '12' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'NORCB-SOC-CERC' and date(min_soc_lh_transporting + interval '18' hour) <= date(min_soc_received-interval '6' hour) then 1 

            when  lh_route = 'SORCA-SOC-NERCA' and date(min_soc_lh_transporting - interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCA-SOC-NERCB' and date(min_soc_lh_transporting - interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCA-SOC-NORCA' and date(min_soc_lh_transporting - interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCA-SOC-NORCB' and date(min_soc_lh_transporting - interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCA-SOC-CERC'  and date(min_soc_lh_transporting - interval '18' hour) <= date(min_soc_received-interval '6' hour) then 1 

            when  lh_route = 'SORCB-SOC-NERCA' and date(min_soc_lh_transporting + interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCB-SOC-NERCB' and date(min_soc_lh_transporting + interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCB-SOC-NORCA' and date(min_soc_lh_transporting + interval '17' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCB-SOC-NORCB' and date(min_soc_lh_transporting + interval '16' hour) <= date(min_soc_received-interval '6' hour) then 1 
            when  lh_route = 'SORCB-SOC-CERC'  and date(min_soc_lh_transporting + interval '18' hour) <= date(min_soc_received-interval '6' hour) then 1 
            else 0
        end as is_soc_transporting_ontime 

from route_map 
)
select 
    date(min_soc_received - interval '6' hour ) as report_date
    ,shipment_id
    ,lh_route
    ,pickup_rc_station
    ,backhaul_lh_task
    ,back_haul_driver
    ,'SOCE' as soc_station
    ,shuttle_lh_task
    ,shuttle_driver 
    ,delivery_rc_station
    ,is_rc_transporting_ontime
    ,is_rc_transported_ontime
    ,rc_traveling_soc_leadtime_check
    ,is_soc_transporting_ontime
    ,xrc_received_time as pu_rc_received_time 
    ,min_xrc_lh_transporting_time
    ,min_xrc_lh_transported_time
    ,min_soc_received
    ,min_soc_lh_transporting
    ,rc_to_soc_delivery_leadtime



from ontime_cal
where lh_route is not null 
order by 1 desc,3



