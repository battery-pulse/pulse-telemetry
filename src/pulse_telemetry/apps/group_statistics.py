telemetry_partitions = "PARTITIONED BY device_id, test_id, month(timestamp);"
telemetry_writes = "WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY cycle_number, step_number, record_number"

statistics_step_partitions = "PARTITIONED BY year(start_time);"
statistics_step_writes = "WRITE ORDERED BY test_id, cycle_number, step_number"

statistics_cycle_partitions = "PARTITIONED BY year(start_time);"
statistics_cycle_writes = "WRITE ORDERED BY test_id, cycle_number"
