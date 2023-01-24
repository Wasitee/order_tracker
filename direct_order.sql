with fleet_order as(
select 
fleet_order.shipment_id 
,fleet_order.cogs  
,case when soc_rec_time is not null then 1 else 0 end as is_soc_rec
,case when fm_hub_rec_time is not null then 1 else 0 end as is_fm_hub_rec        
,pickup_track.pickup_time
,user_id as driver_id
,operator as driver_name
,pickup_task_id
,pickup_station_name
,seller_info.shop_id
,seller_info.seller_name
,pickup_point_id
FROM spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join   
  (
    select 
        shipment_id
        ,pickup_time
        ,user_id
        ,operator
        ,station.station_name as pickup_station_name

    from
            (
            select 
            shipment_id
            ,from_unixtime(ctime-3600) as pickup_time 
            ,user_id
            ,station_id
            ,operator
            ,row_number() over(partition by shipment_id order by ctime) as row_number 
            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as tracking_tab
            where status in (13,39)
            ) as status 
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station 
    on cast(status.station_id as int) = cast(station.id as int)
    where row_number = 1 
    ) as pickup_track
on fleet_order.shipment_id = pickup_track.shipment_id

left join 
    (
        select 
            shipment_id
            ,min(case when status = 8 and station_id in (3,242) then from_unixtime(ctime-3600) end ) as soc_rec_time 
            ,min(case when status = 42 then from_unixtime(ctime - 3600 ) end) as fm_hub_rec_time
            
            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as tracking_tab
            group by 1 
     ) as order_track
on order_track.shipment_id = fleet_order.shipment_id

left join 
    (
    select 
        cast(pickup_order_id as varchar) as  pickup_order_id  
        ,pickup_task_id
        ,pickup_point_id
        
        
        from 
            (select  
                pickup_order_id
                ,pickup_task_id
                ,pickup_point_id
                ,ROW_NUMBER() OVER(PARTITION BY pickup_order_id ORDER BY allocated_time DESC) as rank_no
                 
            FROM spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_continuous_s0_live 
        
        )
        WHERE rank_no = 1


    ) as pu_tab
on pu_tab.pickup_order_id = fleet_order.shipment_id

left join spx_mart.shopee_fleet_order_th_db__seller_info_tab__reg_daily_s0_live as seller_info 
on seller_info.shipment_id = fleet_order.shipment_id

where pickup_track.pickup_time is not null 
)
select 
    date(pickup_time) as report_date
    --,pickup_task_id
    --,pickup_station_name
    ,driver_id
    ,pickup_point_id
    --,driver_name
    --,seller_name
    ,count(*) as total_order_in_pu_task
    ,sum(is_soc_rec) as total_soc_rec 
    ,count(*) - sum(is_soc_rec) as diff
    ,sum(case when pickup_task_id = '' then 1 else 0 end  ) as total_no_pu_task



    from fleet_order 
    where is_fm_hub_rec = 0 and  date(pickup_time) between current_date - interval '30' day and current_date - interval '1' day
    and driver_id is not null 
    and driver_name is not null
    and pickup_station_name is not null 
    group by 1,2,3
    order by 1 desc,2,3
