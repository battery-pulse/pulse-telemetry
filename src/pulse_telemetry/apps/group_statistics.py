telemetry_partitions = "PARTITIONED BY device_id, test_id, month(timestamp);"
telemetry_writes = "WRITE ORDERED BY cycle_number, step_number, record_number"

statistics_step_partitions = "PARTITIONED BY year(start_time);"  # Only if you have ~1000 channels
statistics_step_writes = "WRITE ORDERED BY test_id, cycle_number, step_number"

statistics_cycle_partitions = "PARTITIONED BY year(start_time);"  # Only if you have ~1000 channels
statistics_cycle_writes = "WRITE ORDERED BY test_id, cycle_number"
