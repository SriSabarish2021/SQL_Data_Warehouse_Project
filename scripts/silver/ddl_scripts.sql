
/* SILVER LAYER 

SELECT TOP 1000 * from silver.crm_sales_details_info

SELECT TOP 1000 * from silver.crm_customer_info

SELECT TOP 1000 * from silver.crm_prd_info

SELECT TOP 1000 * from silver.erp_cust_AZ
*/


/* silver LAYER */

if OBJECT_ID('DataWareHouse.silver.crm_customer_info','U') is not null
	drop table DataWareHouse.silver.crm_customer_info
create table DataWareHouse.silver.crm_customer_info(
	cst_id int ,
	cst_key nvarchar(20),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	BDATE DATE,
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(20),
	cst_create_date date,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

)
if OBJECT_ID('DataWareHouse.silver.crm_sales_details_info','U') is not null
	drop table DataWareHouse.silver.crm_sales_details_info

create table DataWareHouse.silver.crm_sales_details_info(
	sls_ord_num nvarchar(20),
	sls_prd_key nvarchar(40),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

)


if OBJECT_ID('DataWareHouse.silver.crm_prd_info','U') is not null
	drop table DataWareHouse.silver.crm_prd_info

create table DataWareHouse.silver.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	cat_id nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

)


/* ERP */
if OBJECT_ID('silver.erp_cust_AZ','U') is not null
	drop table silver.erp_cust_AZ

create table silver.erp_cust_AZ(
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()

)
		
if OBJECT_ID('silver.erp_LOC_A','U') is not null
	drop table silver.erp_LOC_A

create table silver.erp_LOC_A(
	CID nvarchar(50),
	CNTRY nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()

)


if OBJECT_ID('silver.erp_px_CAT_G1V2','U') is not null
	drop table silver.erp_px_CAT_G1V2

create table silver.erp_px_CAT_G1V2(
	ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()

)
CREATE PROCEDURE silver.regular_cont_getter
AS
	begin 
		BEGIN TRY 

	
		DECLARE @strttime DATETIME;
		DECLARE @endtime DATETIME;

		SET @strttime = GETDATE();

	

		/*CRM 1*/
		print('=====================================');
		print('CRM 1');
		print('=====================================');

		truncate table silver.crm_customer_info
		BULK INSERT silver.crm_customer_info
		from 'C:\Users\HP\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);


		select *
		from silver.crm_customer_info


		/*      CRM 2		*/

		print('=====================================');
		print('CRM 2');
		print('=====================================');

		truncate table silver.crm_sales_details_info
		BULK INSERT silver.crm_sales_details_info
		from 'C:\Users\HP\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);

		select *
		from silver.crm_sales_details_info



		/*			CRM 3		*/

		print('=====================================');
		print('CRM 3');
		print('=====================================');
		truncate table silver.crm_prd_info

		BULK INSERT silver.crm_prd_info
		from 'C:\Users\HP\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);

		select *
		from silver.crm_prd_info


		/*		ERP 1		*/

	


		print('=====================================');
		print('ERP 1');
		print('=====================================');
		truncate table silver.erp_cust_AZ
		BULK INSERT silver.erp_cust_AZ
		from 'C:\Users\HP\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);

		select *
		from silver.erp_cust_AZ


		/*		ERP 2		*/



		print('=====================================');
		print('ERP 2');
		print('=====================================');
		truncate table silver.erp_LOC_A
		BULK INSERT silver.erp_LOC_A
		from 'C:\Users\HP\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);

		select *
		from silver.erp_LOC_A


		/*		ERP 3		*/
			


		print('=====================================');
		print('ERP 3');
		print('=====================================');
		truncate table silver.erp_px_CAT_G1V2

		BULK INSERT silver.erp_px_CAT_G1V2
		from 'C:\Users\HP\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);

		select *
		from silver.erp_px_CAT_G1V2
		SET @endtime = GETDATE();
		print('total time taken'+ cast(datediff(second,@strttime,@endtime)as nvarchar)+'seconds')
		END TRY
	
		BEGIN CATCH
		PRINT('================================================');
		PRINT('THER IS SOME ERROR OCCURED PLEASE FIND THE ERROR');
		PRINT('================================================');
		END CATCH

	end
/*		insertion of values completed		*/

exec silver.regular_cont_getter





