@echo off
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\TRS\01_get_data_from_bc\01_TRS_SalesLines_args_get_data_from_bc.bat"
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\TRS\02_send_data_to_cosmosdb\02_TRS_SalesLines_args_send_data_to_cosmosdb.bat"
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\TRS\03_read_from_cosmos_db_and_transform\03_TRS_SalesLines_args_read_from_cosmos_db_and_transform.bat"
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\TRS\04_read_send_to_sql\04_TRS_SalesLines_args_read_send_to_sql.bat"
