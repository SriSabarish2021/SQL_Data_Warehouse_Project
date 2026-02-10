
/* GOLD LAYER */

CREATE VIEW gold.gold_layer_customer_view 
AS
SELECT
	row_number() over(order by ci.cst_id) as customer_ID,
	ci.cst_id as customer_NUM,
	ci.cst_key as customer_key,
	ci.cst_firstname as customer_FirstName,
	ci.cst_lastname as customer_LastName,
	cz.BDATE as customer_BirthDate,
	CL.CNTRY as customer_Location,
	ci.cst_create_date AS customer_entryDate,
	case when ci.cst_gndr != 'Not Interested' then ci.cst_gndr
	else coalesce(cz.GEN,'not interested') 
	end as customer_Gender,
	ci.cst_marital_status as customer_maritalStatus
FROM silver.crm_customer_info ci
left join silver.erp_cust_AZ cz
ON ci.cst_key=cz.CID
LEFT JOIN silver.erp_LOC_A CL
ON ci.cst_key=CL.CID





Create view gold.gold_layer_product_view_data
AS
select 
	row_number() over(ORDER BY prod_main.prd_id) as Product_ID,
	prod_main.prd_id as product_uniqID,
	prod_main.prd_number as product_key,
	prod_detail.ID,
	prod_main.prd_nm as product_name,
	prod_detail.CAT as product_cathegory,
	prod_detail.SUBCAT as product_SUBcathegory,
	prod_main.prd_cost as product_cost,
	prod_main.prd_line as product_line,
	prod_main.prd_start_dt as product_beginDATE,
	prod_main.prd_end_dt as product_endDATE,
	prod_detail.MAINTENANCE as product_maintanance

from silver.crm_prd_info as prod_main
left join silver.erp_px_CAT_G1V2 as prod_detail
on prod_main.cat_id=prod_detail.ID




	

	
select * from silver.crm_prd_info

select * from DataWareHouse.silver.erp_px_CAT_G1V2

select * from gold.gold_layer_product_view_data

select * from gold.gold_layer_customer_view



create View gold.Final_FACTS
AS
select 
	ROW_NUMBER() over(order by sls_ord_num) as sales_UNQid,
	customer_NUM,
	customer_BirthDate,
	customer_FirstName,
	customer_LastName,
	customer_maritalStatus,
	customer_Gender,
	customer_entryDate,
	product_maintanance,
	product_line,
	product_key,
	product_name,
	product_uniqID,
	sls_ord_num as sales_OrderNUM,
	sls_prd_key AS sales_ProductKey,
	sls_cust_id as sales_CustomerID,
	sls_order_dt AS sales_OrderDETAIL,
	sls_ship_dt AS sales_shipmentDETAIL,
	sls_due_dt AS sales_DueDATE,
	sls_sales AS sales_TotalSales,
	sls_quantity as sales_TotalQuant,
	sls_price as Sales_SellingCost
	
from silver.crm_sales_details_info as sales_det
left join gold.gold_layer_customer_view as customer_det
on sales_det.sls_cust_id=customer_det.customer_NUM
left join gold.gold_layer_product_view_data as product_det
on sales_det.sls_prd_key=product_det.product_key



select * from gold.Final_FACTS
