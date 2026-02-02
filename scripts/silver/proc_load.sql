
create procedure insertingDATA
AS
	begin
	BEGIN TRY 

	Truncate table DataWareHouse.silver.crm_customer_info
	insert into DataWareHouse.silver.crm_customer_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status ,
	cst_gndr,
	cst_create_date
	)
	SELECT 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstnamev,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim(cst_gndr))='S'
			then 'Single'
			when upper(trim(cst_gndr))='M'
			then 'Married'
			else 'Not Interested'
	end cst_marital_status ,
	case when upper(trim(cst_gndr))='F'
			then 'Female'
			when upper(trim(cst_gndr))='M'
			then 'Male'
			else 'Not Interested'
	end cst_gndr,
	cst_create_date
	
	from(
	select
		*,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date DESC) as rankings_date
	from DataWareHouse.bronze.crm_customer_info

	)t where rankings_date=1 


	select * from DataWareHouse.silver.crm_customer_info


	select 

	prd_key,

	count(*)
	from DataWareHouse.bronze.crm_prd_info
	group by prd_key
	having count(*)>1 or prd_key is null

	Truncate table DataWareHouse.silver.crm_prd_info
	insert into DataWareHouse.silver.crm_prd_info(
		prd_id,
		prd_key,
		cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_')as cat_id,
		replace(SUBSTRING(prd_key,7,len(prd_key)),'-','_')as prd_key,
		prd_nm,
		isnull(prd_cost,0) prd_cost,
		case when upper(trim(prd_line))='M'
			 THEN 'Mounain'
			 when upper(trim(prd_line))='R'
			 then 'Road'
			 when upper(trim(prd_line))='T'
			 then 'Touring'
			 when upper(trim(prd_line))='S'
			 then 'Othere Sales'
		else 'n/a'
		end as prd_line,
		prd_start_dt,
		cast(LEAD(prd_start_dt) over(order by prd_start_dt) as DATE) as prd_end_dt
	
	from 
	(
	select
		*,
		ROW_NUMBER() over(partition by prd_key order by prd_end_dt DESC) as rankings_date
	from DataWareHouse.bronze.crm_prd_info
	)t





	Truncate table DataWareHouse.silver.crm_sales_details_info

	insert into DataWareHouse.silver.crm_sales_details_info(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price

	)


	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			when sls_order_dt=0 OR len(sls_order_dt)!=8 
			then NULL
			else cast(cast(sls_order_dt as varchar)as date)
		END AS sls_order_dt,
		case 
			when sls_ship_dt=0 OR len(sls_ship_dt)!=8 
			then NULL
			else cast(cast(sls_ship_dt as varchar)as date)
		END AS sls_ship_dt,
		case 
			when sls_due_dt=0 OR len(sls_due_dt)!=8 
			then NULL
			else cast(cast(sls_due_dt as varchar)as date)
		END AS sls_due_dt,
		case 
			when sls_sales<=0 or sls_sales is null or sls_sales!=cast(abs(sls_price) as int)*cast(sls_quantity as int)
			then cast(sls_price as int)*cast(sls_quantity as int)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when cast(sls_sales as int)*cast(sls_quantity as int)<=0
			then abs(cast(sls_sales as int)*cast(sls_quantity as int))
			else cast(sls_sales as int)*cast(sls_quantity as int)
		end as sls_price

	from DataWareHouse.bronze.crm_sales_details_info




	select 
	*
	from DataWareHouse.silver.crm_sales_details_info





	Truncate table DataWareHouse.silver.erp_cust_AZ

	INSERT INTO DataWareHouse.silver.erp_cust_AZ(
	CID,
	BDATE,
	GEN
	)

	select 
		CASE 
			WHEN CID LIKE 'NAS%'
			THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
		END AS CID,
		CASE 
			WHEN BDATE>GETDATE() THEN NULL
			ELSE BDATE
		END AS BDATE,
		CASE WHEN UPPER(TRIM(GEN)) IN ('F','Female')
			THEN 'Female'
			when UPPER(trim(GEN)) IN ('M','Male')
			THEN 'Male'
		ELSE 'N/A'
		END AS GEN
	from DataWareHouse.bronze.erp_cust_AZ


	Truncate table DataWareHouse.silver.erp_LOC_A
	INSERT INTO DataWareHouse.silver.erp_LOC_A(
	CID,
	CNTRY
	)
	SELECT 
	REPLACE(CID,'-','')as CID,
	CASE 
		WHEN TRIM(CNTRY) IN ('USA','US','United States')
		THEN 'United States'
		WHEN TRIM(CNTRY) IN ('DE')
		THEN 'Germany'
		WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY)=' ' 
		THEN 'N/A'
		else CNTRY
		end as CNTRY
	 

	FROM DataWareHouse.bronze.erp_LOC_A
	Truncate table DataWareHouse.silver.erp_px_CAT_G1V2
	INSERT INTO DataWareHouse.silver.erp_px_CAT_G1V2(
	ID,CAT,SUBCAT,MAINTENANCE

	)
	select ID,CAT,SUBCAT,MAINTENANCE
	 from DataWareHouse.bronze.erp_px_CAT_G1V2


	 select *
	 from DataWareHouse.silver.erp_px_CAT_G1V2

	 END TRY 
	 BEGIN CATCH 
	 PRINT('There is some error occured sorry for the error')
	 end catch 
	end


	exec insertingDATA
