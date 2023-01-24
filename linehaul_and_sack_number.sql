select  
    shipment_id
    ,status
    ,cast(lh_task_id as varchar) as latest_lh_task_id
    ,cast(to_number as varchar) as latest_to_number
from    
    (
        select  
            shipment_id
            ,status
            ,TRY(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id')) as lh_task_id
            ,TRY(JSON_EXTRACT(JSON_PARSE(content), '$.to_number')) as to_number
            ,row_number() OVER (PARTITION BY shipment_id ORDER BY ctime DESC) as rank_no
        from    spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where   status in (46, 47, 35, 15, 211, 233, 55, 56, 62, 64)
        and   shipment_id in () 
    ) as lh_task_tab
where   rank_no = 1




