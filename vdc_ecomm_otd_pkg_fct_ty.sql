WITH
    dates AS (
        SELECT
            --PARSE_DATE('%Y%m%d', '20230829') AS dt_begin,
            PARSE_DATE('%Y%m%d', '20230129') AS dt_begin,
            DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY) AS dt_end
    ),

  os_init_alloc AS
 (
        SELECT 
            txna.order_sku_key,
            txna.order_fulfill_key,
            txna.txn_date AS init_alloc_date,
            DATETIME_ADD(DATE_TRUNC(txna.estimated_ship_dttm, DAY), INTERVAL 86399 SECOND) AS estimated_ship_dttm,
            MIN(CASE WHEN (EXTRACT(DAYofWEEK FROM DATE_TRUNC(txna.estimated_ship_dttm, DAY))) = 6 THEN 
                DATETIME_ADD(DATE_TRUNC(txna.estimated_ship_dttm, DAY), INTERVAL 84 HOUR) END) AS estimated_ship_dttm_adjusted_grace,
        FROM dates, `entdata.ecm.order_sku_txn_allocations` txna
        JOIN `entdata.ecm.order_fulfill` oful
            ON txna.order_fulfill_key = oful.order_fulfill_key
        JOIN `entdata.ecm.order_sku` os
            ON txna.order_sku_key = os.order_sku_key
        WHERE os.order_date BETWEEN dates.dt_begin AND dates.dt_end
            AND (oful.fulfill_channel = 'VDC') 
        GROUP BY
            txna.order_sku_key,
            txna.order_fulfill_key,
            txna.txn_date,
            txna.estimated_ship_dttm
 )

    SELECT
        od.order_delivery_key,
        txnf.order_fulfill_key,
        os.web_ord_num,
        os.order_sku_key,
        os.product_id,
        os.product_number,
        MIN(txnf.order_fulfill_number) AS order_fulfill_number,
        MIN(os.cust_req_fulfillment_mode_key) AS cust_req_fulfillment_mode_key,
        MIN(oh.order_source_code) AS order_source_code,
        MIN(os.order_date) AS order_date,
        MIN(os.promise_date) AS promise_date,
        MIN(osia.estimated_ship_dttm) AS estimated_ship_dttm,
        MIN(osia.estimated_ship_dttm_adjusted_grace) AS estimated_ship_dttm_adjusted_grace,
        MIN(extract(DAYofWEEK FROM osia.estimated_ship_dttm)) AS day_of_week_estimated_ship_date, --for testing
        MIN(oh.order_placed_dttm) AS click_dttm,
        MIN(oful.distribution_create_dttm) AS distribution_create_dttm,
        MIN(osia.init_alloc_date) AS init_alloc_date,
        MIN(od.actual_shipped_dttm) AS actual_shipped_dttm,
        MIN(COALESCE(DATE(fdx.actual_pickup_date), od.local_origin_scan_date)) AS pickup_date,
        MIN(od.local_delivery_date) AS local_delivery_date,
        MIN(od.local_delivery_dttm) AS local_delivery_dttm,
        MIN(CASE
                WHEN os.po_box_cd = 'GOV' THEN 'GOV'
                WHEN os.po_box_cd = 'POB' THEN 'POB'
                WHEN os.po_box_cd = 'TER' THEN 'TER'
                WHEN oful.ship_state = 'AK' THEN 'Alaska'
                WHEN oful.ship_state = 'HI' THEN 'Hawaii'
                WHEN oful.ship_state NOT IN ('AK', 'HI') THEN 'Regular'
                ELSE 'N/A'
            END) AS address_type,
        MIN(txnf.webstore_key) AS webstore_key,    
        MAX(CASE WHEN os.web_special_order_flg = 'Y' THEN 1 ELSE 0 END) AS web_special_order_ind,
        MAX(CASE WHEN os.clearance_type_key IN (3, 4) THEN 1 ELSE 0 END) AS clearance_ind,
        MAX(os.gtgt_ind) AS gtgt_ind,
        MIN(os.gtgt_date) AS gtgt_date,
        MAX(oh.demand_ind) AS demand_ind,
        MIN(oh.order_status_desc) AS order_status_desc,
        MIN(od.delivery_status_cd) AS delivery_status_cd,
        MIN(od.delivery_status) AS delivery_status,
        MIN(od.tracking_number) AS tracking_number,
        MIN(od.sci_tc_lpn_id) AS sci_tc_lpn_id,
        MIN(oful.fulfill_organization_hierarchy_id) AS fulfill_organization_hierarchy_id,
        MIN(oful.fulfill_vendor_key) AS fulfill_vendor_key,
        MIN(oful.ship_state) AS ship_state,
        MIN(oful.ship_zip) AS ship_zip,
        MIN(oful.fulfill_channel) AS fulfill_channel,
        MIN(fml.shipping_definition) AS shipping_definition,
        MIN(fml.service_level_desc) AS service_level_desc,
        MIN(CAST(oful.fulfillment_location_cd AS INT64)) AS fulfillment_location_cd,
        MIN(oful.first_distribution_ind) AS first_distribution_ind,
        MIN(oful.distribution_fulfill_status) AS distribution_fulfill_status,
        MIN(od.carrier_billed_weight_lbs) AS carrier_billed_weight_lbs,
        MIN(od.carrier_code) AS carrier_code,
        MIN(od.carrier_desc) AS carrier_desc,
        MIN(od.parcel_zone) AS parcel_zone,
        COUNT(DISTINCT od.order_delivery_key) OVER (PARTITION BY os.web_ord_num) AS package_count,
        MIN(od.package_type_descr) AS package_type_descr,
        MIN(od.local_origin_scan_date) AS local_origin_scan_date,
        MIN(fdx.actual_pickup_date) AS actual_pickup_date,

        MIN(CASE WHEN os.promise_date IS NULL THEN 1 ELSE 0 END) AS missing_promise_date,
        MIN(CASE WHEN od.local_delivery_date IS NULL THEN 1 ELSE 0 END) AS missing_delivery_date,
        MIN(CASE WHEN osia.estimated_ship_dttm IS NULL THEN 1 ELSE 0 END) AS missing_estimated_shipped_date,
        
        MIN(CASE WHEN actual_shipped_dttm <= osia.estimated_ship_dttm THEN 1 ELSE 0 END) AS on_time_shipped,
        MIN(CASE WHEN actual_shipped_dttm <= osia.estimated_ship_dttm_adjusted_grace THEN 1 ELSE 0 END) AS on_time_shipped_adjusted_grace,

        MIN(CASE WHEN od.local_delivery_date <= os.promise_date THEN 1 ELSE 0 END) AS on_time_success,
        MIN(CASE WHEN od.local_delivery_date < os.promise_date THEN 1 ELSE 0 END) AS early_packages,
        MIN(CASE WHEN od.local_delivery_date < os.promise_date THEN ABS(DATE_DIFF(od.local_delivery_date, os.promise_date, DAY)) END) AS days_early,
        MIN(CASE WHEN od.local_delivery_date > os.promise_date THEN 1 ELSE 0 END) AS late_packages,
        MIN(CASE WHEN od.local_delivery_date > os.promise_date THEN ABS(DATE_DIFF(os.promise_date, od.local_delivery_date, DAY)) END) AS days_late,

        MIN(ABS(DATE_DIFF(os.order_date, os.promise_date, DAY))) AS order_date_to_promise_date_days,
        MIN(ABS(DATE_DIFF(os.order_date, od.local_delivery_date, DAY))) AS order_date_to_delivery_date_days,
        MIN(ABS(DATE_DIFF(os.promise_date, od.local_delivery_date, DAY))) AS promise_date_to_delivery_date_days,
        MIN(ABS(DATE_DIFF(od.actual_shipped_date, od.local_delivery_date, DAY))) AS in_transit_days,

        MIN(ABS(DATE_DIFF(DATE(oh.order_placed_dttm), osia.init_alloc_date, DAY))) AS click_to_alloc_days,
        MIN(ABS(`p-wardar-381480765.reporting_function.date_diff_business_days`(DATE(oh.order_placed_dttm), osia.init_alloc_date))) AS click_to_alloc_days_business, 

        MIN(ABS(DATE_DIFF(DATE(oh.order_placed_dttm), od.actual_shipped_date, DAY))) AS click_to_ship_days,
        MIN(ABS(`p-wardar-381480765.reporting_function.date_diff_business_days`(DATE(oh.order_placed_dttm), od.actual_shipped_date))) AS click_to_ship__days_business,

        MIN(ABS(DATE_DIFF(DATE(oh.order_placed_dttm), od.local_delivery_date, DAY))) AS click_to_deliver_days,
        MIN(ABS(`p-wardar-381480765.reporting_function.date_diff_business_days`(DATE(oh.order_placed_dttm), od.local_delivery_date))) AS click_to_deliver_days_business,  

        MIN(ABS(DATE_DIFF(osia.init_alloc_date, oful.distribution_create_date, DAY))) AS init_alloc_to_do_create_days,
        MIN(ABS(`p-wardar-381480765.reporting_function.date_diff_business_days`(osia.init_alloc_date, oful.distribution_create_date))) AS init_alloc_to_do_create_days_business, 
        
        MIN(ABS(DATE_DIFF(od.actual_shipped_date, oful.distribution_create_date, DAY))) AS do_create_to_ship_days,
        MIN(ABS(`p-wardar-381480765.reporting_function.date_diff_business_days`(oful.distribution_create_date, od.actual_shipped_date))) AS do_create_to_ship_days_business,  

        MIN(ABS(DATE_DIFF(od.local_delivery_date, oful.distribution_create_date, DAY))) AS do_create_to_delivery_days,

        MIN(ABS(DATETIME_DIFF(oh.order_placed_dttm, oful.distribution_create_date, MINUTE))) AS minutes_to_process,
        
        MIN(ABS(DATE_DIFF(os.order_date, COALESCE(DATE(fdx.actual_pickup_date), od.local_origin_scan_date), DAY))) AS click_to_pickup_days,

        MIN(ABS(DATE_DIFF(od.actual_shipped_date, COALESCE(DATE(fdx.actual_pickup_date), od.local_origin_scan_date), DAY))) AS ship_to_pickup_days,
        MIN(ABS(`p-wardar-381480765.reporting_function.date_diff_business_days`(od.actual_shipped_date, COALESCE(DATE(fdx.actual_pickup_date), od.local_origin_scan_date)))) AS ship_to_pickup_days_business, -- 4 Ship To Pickup

        MIN(ABS(DATE_DIFF(COALESCE(DATE(fdx.actual_pickup_date), od.local_origin_scan_date), od.local_delivery_date, DAY))) AS pickup_to_delivery_days,
        MIN(ABS(`p-wardar-381480765.reporting_function.date_diff_business_days`(COALESCE(DATE(fdx.actual_pickup_date), od.local_origin_scan_date), od.local_delivery_date))) AS pickup_to_delivery_days_business, --5 Pickup To Delivery

        MIN(txnf.units) AS units,
        MIN(txnf.extended_amt) AS revenue,
        MIN(txnf.average_cost * os.orig_tot_units) AS cost,
        MIN(txnf.extended_amt) - MIN(txnf.average_cost * os.orig_tot_units) AS margin

        FROM dates, `entdata.ecm.order_sku_txn_fulfill` txnf
            INNER JOIN `entdata.ecm.order_fulfill` oful
            ON txnf.order_fulfill_key = oful.order_fulfill_key
            INNER JOIN os_init_alloc osia
            ON txnf.order_fulfill_key = osia.order_fulfill_key AND txnf.order_sku_key = osia.order_sku_key
            INNER JOIN `entdata.ecm.order_sku` os
            ON os.order_sku_key = txnf.order_sku_key
            INNER JOIN `entdata.ecm.order_header` oh
            ON oh.order_header_key = txnf.order_header_key
            INNER JOIN `entdata.ecm.order_delivery` od
            ON od.order_delivery_key = txnf.order_delivery_key
            INNER JOIN `entdata.ecm.fulfillment_mode_lkup` fml
            ON od.fulfillment_mode_key = fml.fulfillment_mode_key
            LEFT OUTER JOIN (
                SELECT
                    tracking_id,
                    MIN(actual_pickup_date) AS actual_pickup_date
                FROM `p-wardar-381480765.lighthouse.fedex_event_tracking_data`
                WHERE status_description = 'Picked up'
                    AND customer_order_number IS NOT NULL 
                GROUP BY
                    tracking_id,
                    status_description,
                    customer_order_number
            ) fdx
            ON fdx.tracking_id = od.tracking_number
                AND od.carrier_code IN ('FEDEX', 'FDXGRND', 'FEDX', 'FDXEXPR')
        WHERE os.order_date BETWEEN dates.dt_begin AND dates.dt_end
            AND oful.fulfill_channel = 'VDC'
            AND od.order_delivery_key <> -1

    GROUP BY
        od.order_delivery_key,
        txnf.order_fulfill_key,
        os.order_date,
        os.web_ord_num,
        os.order_sku_key,
        os.product_id,
        os.product_number  
;
