# TYPE: View
# JOB: 
# TARGET: `@PROJECT@.reporting.vdc_ecomm_order_sku_fct_ty`
/* 20221013 LD  Initial VIEW creation.
   20230908 LD  Updated for VDC Special Dashboard DAR-1964
========================================================================================================================================================== */
  
WITH
  dates AS (
      SELECT
          -- PARSE_DATE('%Y%m%d', '@FISCAL_YEAR_BEGIN_DATE@') AS dt_begin,
           CAST('2023-01-29' AS DATE) AS dt_begin,
          --CAST('2022-01-30' AS DATE) AS dt_begin,
          --CAST('2021-03-12' AS DATE) AS dt_end
          DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY) AS dt_end
  ),

  os_txn_sku_alloc_esd AS
 (
              SELECT 
                    txna.order_sku_key,
                    txna.order_fulfill_key,
                    oful.fulfill_channel,
                    oful.fulfill_vendor_key,
                    DATE(txna.estimated_ship_date) AS estimated_ship_date  
                FROM dates, `entdata.ecm.order_sku_txn_allocations` txna
                JOIN `entdata.ecm.order_sku` os
                    ON txna.order_sku_key = os.order_sku_key
                JOIN `entdata.ecm.order_fulfill` oful
                    ON oful.order_fulfill_key = txna.order_fulfill_key
                WHERE os.order_date BETWEEN dates.dt_begin AND dates.dt_end
                    AND (oful.fulfill_channel = 'VDC') 
                GROUP BY
                    txna.order_sku_key,
                    txna.order_fulfill_key,
                    oful.fulfill_channel,
                    oful.fulfill_vendor_key,
                    txna.estimated_ship_date
 )

      SELECT
          os.order_date,
          os.webstore_key,
          os.order_sku_key,
          os.web_ord_num,
          os.product_number,
          os.product_id,
          os.designated_ff_channel,
          os.order_sku_status_desc,
          IFNULL(os.web_special_order_flg, 'N') AS web_special_order_flg,
          esd.order_fulfill_key,
          esd.fulfill_channel AS ff_channel,
          vdd.vendor_number AS ff_vendor_number,
          vdd.name AS ff_vendor_name,
          esd.estimated_ship_date,
          MIN(CASE WHEN esd.estimated_ship_date IS NULL THEN 1 ELSE 0 END) AS missing_estimated_ship_date,
          oh.demand_ind,
          oh.order_source_code,
          oh.order_status_desc,
          oh.order_placed_dttm AS click_dttm,
          # Demand
          MIN(os.orig_tot_units) AS ods_units,
          MIN(os.orig_tot_extended_amt) AS ods_revenue,
          MIN(IFNULL(os.sku_average_cost, os.sku_current_cost) * os.orig_tot_units) AS ods_cost,
          # Cancel
          MIN(c.cancel_reason_code) AS ods_cancel_reason_code,
          MIN(c.cancel_reason_desc) AS ods_cancel_reason_desc,
          MIN(c.cancel_units) AS ods_cancel_units,
          MIN(c.cancel_amt) AS ods_cancel_amt,
          MIN(c.cancel_cost) AS ods_cancel_cost,
          # Decline Reasons
          MIN(d.decline_reason_code) AS decline_reason_code,
          MIN(d.decline_reason_desc) AS decline_reason_desc,
          # Open Orders
          MIN(CASE WHEN oh.order_source_code <> 'RESHIP' THEN o.open_units END) AS ods_open_units,
          MIN(CASE WHEN oh.order_source_code <> 'RESHIP' THEN o.open_amt END) AS ods_open_amt,
          MIN(CASE WHEN oh.order_source_code <> 'RESHIP' THEN o.open_cost END) AS ods_open_cost,
          # Fulfillment
          MIN(f.fulfill_channel_fulfillment) AS ods_fulfill_channel_fulfillment,
          MIN(f.fulfill_units) AS ods_fulfill_units,
          MIN(f.fulfill_revenue) AS ods_fulfill_revenue,
          MIN(f.fulfill_cost) AS ods_fulfill_cost
      FROM dates, os_txn_sku_alloc_esd esd
      JOIN `entdata.ecm.order_sku` os
          ON esd.order_sku_key = os.order_sku_key
      JOIN `entdata.ecm.order_header` oh
          ON oh.order_header_key = os.order_header_key
      LEFT JOIN `entdata.ddw.vendor_dim` vdd
          ON esd.fulfill_vendor_key = vdd.vendor_id    
      # Cancel
      LEFT JOIN (
          SELECT
              txnc.order_sku_key,
              MAX(IFNULL(txnc.cancel_reason_desc, 'Unk')) AS cancel_reason_desc,
              MAX(IFNULL(txnc.cancel_reason_code, 'Unk')) AS cancel_reason_code,
              SUM(CASE
                  WHEN txnc.cancel_units > os.orig_tot_units THEN os.orig_tot_units
                  ELSE txnc.cancel_units
              END) AS cancel_units,
              SUM(CASE
                  WHEN txnc.cancel_amt > os.orig_tot_extended_amt THEN os.orig_tot_extended_amt
                  ELSE txnc.cancel_amt
              END) AS cancel_amt,
              SUM(CASE
                  WHEN txnc.cancel_units > os.orig_tot_units THEN os.orig_tot_units
                  ELSE txnc.cancel_units
              END * IFNULL(os.sku_average_cost, os.sku_current_cost)) AS cancel_cost
          FROM dates, `entdata.ecm.order_sku_txn_cancel` txnc
          JOIN `entdata.ecm.order_sku` os
              ON os.order_sku_key = txnc.order_sku_key
          WHERE txnc.txn_date BETWEEN dates.dt_begin AND dates.dt_end
              AND os.order_date BETWEEN dates.dt_begin AND dates.dt_end
          GROUP BY
              txnc.order_sku_key ) c
          ON c.order_sku_key = os.order_sku_key 
      # Decline Reasons
      LEFT JOIN (
          SELECT
              txna.order_sku_key,
              txna.order_fulfill_key,
              txna.decline_reason_desc,
              txna.decline_reason_code
          FROM `entdata.ecm.order_sku_txn_allocations` txna
          GROUP BY
              txna.order_sku_key,
              txna.order_fulfill_key,
              txna.decline_reason_desc,
              txna.decline_reason_code ) d
          ON d.order_sku_key = esd.order_sku_key AND d.order_fulfill_key = esd.order_fulfill_key
      # Open Orders
      LEFT JOIN (
          WITH
            order_sku_curr_alloc_not_cancelled AS (
                SELECT
                    os.order_sku_key,
                    SUM(txna.units - IFNULL(txna.decline_units, 0)) AS units,
                    SUM(CASE
                        WHEN (txna.units - IFNULL(txna.decline_units, 0)) = os.orig_tot_units THEN os.orig_tot_extended_amt
                        ELSE ROUND(os.orig_tot_extended_amt / NULLIF(os.orig_tot_units / NULLIF((txna.units - IFNULL(txna.decline_units, 0)), 0), 0), 2)
                    END) AS revenue,
                    SUM(CASE
                        WHEN (txna.units - IFNULL(txna.decline_units, 0)) = os.orig_tot_units THEN os.orig_tot_units
                        ELSE (txna.units - IFNULL(txna.decline_units, 0))
                    END * IFNULL(os.sku_average_cost, os.sku_current_cost)) AS cost
                FROM dates, `entdata.ecm.order_sku_txn_allocations` txna
                JOIN `entdata.ecm.order_sku` os
                    ON os.order_sku_key = txna.order_sku_key
                JOIN `entdata.ecm.order_fulfill` oful
                    ON oful.order_fulfill_key = txna.order_fulfill_key
                WHERE os.order_date BETWEEN dates.dt_begin AND dates.dt_end
                    AND oful.fulfill_channel = 'VDC'
                    AND oful.distribution_fulfill_status_cd IN ('A', 'M')
                    AND IFNULL(os.order_sku_status_desc, "") NOT IN (
                      'Cancelled',
                      'Fraud Analyst Cancel',
                      'Fraud Cancel',
                      'Partial Fulfill (Complete)',
                      'Ship Confirmed',
                      'Shipped and Posted')
                GROUP BY
                    os.order_sku_key
            ),
            order_sku_curr_alloc_allocated AS (
                SELECT
                    os.order_sku_key,
                    SUM(os.orig_tot_units) AS units,
                    SUM(os.orig_tot_extended_amt) AS revenue,
                    SUM(IFNULL(os.sku_average_cost, os.sku_current_cost) * os.orig_tot_units) AS cost,
                FROM dates, `entdata.ecm.order_sku` os
                WHERE os.order_date BETWEEN dates.dt_begin AND dates.dt_end
                    AND os.designated_ff_channel = 'VDC'
                    AND IFNULL(os.order_sku_status_desc, "") IN (
                      'Allocated',
                      'Alloc-Pending Street Release Date',
                      'Alloc-Pending Total Ship Group',
                      'Auth in Progress',
                      'Auth Waiting',
                      'B/O-Do Not Ship Before Order',
                      'B/O-Wait-Approval',
                      'BackOrder Companion Product',
                      'BackOrder Out of Stock',
                      'BackOrder PreSell SKU',
                      'BackOrder Street Release Date',
                      'BackOrdered',
                      'Backordered - Line dependency',
                      'Cancellation Pending',
                      'Capture in Progress',
                      'Inv B/O',
                      'Inventory Transferred',
                      'Ord Payment Authorized',
                      'Ord Totaled',
                      'Ord.Entered',
                      'Partial Fulfill (Pending)',
                      'Pick Approved',
                      'Ship Inv Created',
                      'Suspended',
                      'Totaled and Tendered',
                      'Unknown',
                      'Wait-Approval',
                      'Wait-CS Cancel',
                      'Wait-Finance',
                      'Wait-Order Review',
                      'Wait-Payment')
                    AND NOT EXISTS (
                        SELECT 1
                        FROM order_sku_curr_alloc_not_cancelled canc
                        WHERE os.order_sku_key = canc.order_sku_key
                    )
                GROUP BY
                    os.order_sku_key
            ),
            order_sku_curr_alloc AS (
                SELECT
                    order_sku_key,
                    units,
                    revenue,
                    cost
                FROM order_sku_curr_alloc_not_cancelled
                UNION ALL
                SELECT
                    order_sku_key,
                    units,
                    revenue,
                    cost
                FROM order_sku_curr_alloc_allocated
            )
            SELECT
                order_sku_key,
                SUM(units) AS open_units,
                SUM(revenue) AS open_amt,
                SUM(cost) AS open_cost
            FROM order_sku_curr_alloc
            GROUP BY
                order_sku_key ) o
            ON o.order_sku_key = os.order_sku_key
      # Fulfillment
      LEFT JOIN (
          SELECT
              txnf.order_sku_key,
              txnf.order_fulfill_key,
              oful.fulfill_channel AS fulfill_channel_fulfillment,
              SUM(txnf.units) AS fulfill_units,
              SUM(txnf.extended_amt) AS fulfill_revenue,
              SUM(txnf.units * txnf.average_cost) AS fulfill_cost
          FROM dates, `entdata.ecm.order_sku_txn_fulfill` txnf
          JOIN `entdata.ecm.order_sku` os
              ON os.order_sku_key = txnf.order_sku_key
          JOIN `entdata.ecm.order_fulfill` oful
              ON txnf.order_fulfill_key = oful.order_fulfill_key
          WHERE txnf.txn_date BETWEEN dates.dt_begin AND dates.dt_end
          GROUP BY
              txnf.order_sku_key,
              txnf.order_fulfill_key,
              oful.fulfill_channel ) f
          ON f.order_sku_key = os.order_sku_key AND f.order_fulfill_key = esd.order_fulfill_key
      WHERE os.order_date BETWEEN dates.dt_begin AND dates.dt_end
      GROUP BY
          os.order_date,
          os.webstore_key,
          os.order_sku_key,
          os.web_ord_num,
          os.product_number,
          os.product_id,
          os.designated_ff_channel,
          os.order_sku_status_desc,
          os.web_special_order_flg,
          esd.order_fulfill_key,
          esd.fulfill_channel,
          vdd.vendor_number,
          vdd.name,
          esd.estimated_ship_date,
          oh.demand_ind,
          oh.order_source_code,
          oh.order_status_desc,
          oh.order_placed_dttm      
;
