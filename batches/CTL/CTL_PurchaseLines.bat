@echo off
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\CTL\01_get_data_from_bc\01_CTL_PurchaseLines_args_get_data_from_bc.bat"
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\CTL\02_send_data_to_cosmosdb\02_CTL_PurchaseLines_args_send_data_to_cosmosdb.bat"
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\CTL\03_read_from_cosmos_db_and_transform\03_CTL_PurchaseLines_args_read_from_cosmos_db_and_transform.bat"
call "C:\Users\eviadmin\Documents\Datawarehouse\schedule_scripts\From_BC\batches\CTL\04_read_send_to_sql\04_CTL_PurchaseLines_args_read_send_to_sql.bat"
