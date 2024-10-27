import asyncio
import datetime
import uuid
from typing import TypedDict


class TelemetryState(TypedDict):
    device_id: str
    test_id: str
    cycle_number: int
    step_number: int
    step_type: str | None
    step_id: int | None
    record_number: int
    timestamp: str
    current__A: float
    voltage__V: float
    power__W: float
    duration__s: float
    voltage_delta__V: float
    current_delta__A: float
    power_delta__W: float
    capacity_charged__Ah: float
    capacity_discharged__Ah: float
    differential_capacity_charged__Ah_V: float | None
    differential_capacity_discharged__Ah_V: float | None
    step_duration__s: float
    step_capacity_charged__Ah: float
    step_capacity_discharged__Ah: float
    step_energy_charged__Wh: float
    step_energy_discharged__Wh: float
    auxiliary: dict[str, float] | None
    metadata: str | None
    update_ts: str


async def telemetry_generator(
    acquisition_frequency: int,  # Hz
    points_per_step: int,
    lower_voltage_limit: float = 3.0,  # V
    upper_voltage_limit: float = 4.0,  # V
    current: float = 1.0,  # A
):
    # Initializes the state
    state: TelemetryState = {
        "device_id": str(uuid.uuid4()),
        "test_id": str(uuid.uuid4()),
        "cycle_number": 1,
        "step_number": 1,
        "step_type": "Rest",
        "step_id": 0,
        "record_number": 0,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "current__A": 0.0,
        "voltage__V": lower_voltage_limit,
        "power__W": 0.0,
        "duration__s": 0.0,
        "voltage_delta__V": 0.0,
        "current_delta__A": 0.0,
        "power_delta__W": 0.0,
        "capacity_charged__Ah": 0.0,
        "capacity_discharged__Ah": 0.0,
        "differential_capacity_charged__Ah_V": 0.0,
        "differential_capacity_discharged__Ah_V": 0.0,
        "step_duration__s": 0.0,
        "step_capacity_charged__Ah": 0.0,
        "step_capacity_discharged__Ah": 0.0,
        "step_energy_charged__Wh": 0.0,
        "step_energy_discharged__Wh": 0.0,
        "auxiliary": {"temperature": 25.0},
        "metadata": '{"experiment": "testing"}',
        "update_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    step_record_number = 0  # internal state, used to determine step transitions
    voltage_delta = (upper_voltage_limit - lower_voltage_limit) / points_per_step
    capacity_energy_factor = 1.0 / acquisition_frequency / 3600

    while True:
        previous_timestamp = datetime.datetime.fromisoformat(state["timestamp"])
        previous_voltage = state["voltage__V"]
        previous_current = state["current__A"]

        # Updates time and record indexing
        new_time = datetime.datetime.now(datetime.timezone.utc)
        state.update(
            {
                "timestamp": new_time.isoformat(),
                "update_ts": new_time.isoformat(),
                "record_number": state["record_number"] + 1,
            }
        )
        step_record_number += 1

        # Updates telemetry
        match state["step_type"]:
            case "Charge":
                new_current = current
                new_voltage = state["voltage__V"] + voltage_delta
                capacity_charged = abs(new_current * capacity_energy_factor)
                capacity_discharged = 0.0
                energy_charged = abs(new_current * new_voltage * capacity_energy_factor)
                energy_discharged = 0.0
            case "Discharge":
                new_current = -current
                new_voltage = state["voltage__V"] - voltage_delta
                capacity_charged = 0.0
                capacity_discharged = abs(new_current * capacity_energy_factor)
                energy_charged = 0.0
                energy_discharged = abs(new_current * new_voltage * capacity_energy_factor)
            case "Rest":
                new_current = 0.0
                new_voltage = state["voltage__V"]
                capacity_charged = 0.0
                capacity_discharged = 0.0
                energy_charged = 0.0
                energy_discharged = 0.0

        # Calculate deltas
        voltage_delta_val = new_voltage - previous_voltage
        current_delta_val = new_current - previous_current
        power_delta_val = (new_current * new_voltage) - (previous_current * previous_voltage)

        # Calculate differential capacities (dQ/dV) for charge and discharge
        differential_capacity_charged = capacity_charged / voltage_delta_val if voltage_delta_val != 0 else None
        differential_capacity_discharged = capacity_discharged / voltage_delta_val if voltage_delta_val != 0 else None

        # Accumulate step-level values
        state.update(
            {
                "current__A": new_current,
                "voltage__V": new_voltage,
                "power__W": new_current * new_voltage,
                "duration__s": (new_time - previous_timestamp).total_seconds(),
                "voltage_delta__V": voltage_delta_val,
                "current_delta__A": current_delta_val,
                "power_delta__W": power_delta_val,
                "capacity_charged__Ah": capacity_charged,
                "capacity_discharged__Ah": capacity_discharged,
                "differential_capacity_charged__Ah_V": differential_capacity_charged,
                "differential_capacity_discharged__Ah_V": differential_capacity_discharged,
                "step_duration__s": state["step_duration__s"] + (new_time - previous_timestamp).total_seconds(),
                "step_capacity_charged__Ah": state["step_capacity_charged__Ah"] + capacity_charged,
                "step_capacity_discharged__Ah": state["step_capacity_discharged__Ah"] + capacity_discharged,
                "step_energy_charged__Wh": state["step_energy_charged__Wh"] + energy_charged,
                "step_energy_discharged__Wh": state["step_energy_discharged__Wh"] + energy_discharged,
            }
        )

        # Yields state before handling step transitions
        yield state
        await asyncio.sleep(1.0 / acquisition_frequency)

        # Handles step transitions after the state is sent
        if step_record_number == points_per_step:
            match state["step_type"]:
                case "Discharge":
                    state.update(
                        {
                            "step_type": "Rest",
                            "step_id": 0,
                            "step_number": state["step_number"] + 1,
                            "cycle_number": state["cycle_number"] + 1,
                        }
                    )
                case "Rest":
                    state.update(
                        {
                            "step_type": "Charge",
                            "step_id": 1,
                            "step_number": state["step_number"] + 1,
                        }
                    )
                case "Charge":
                    state.update(
                        {
                            "step_type": "Discharge",
                            "step_id": 2,
                            "step_number": state["step_number"] + 1,
                        }
                    )
            step_record_number = 0
            state["step_duration__s"] = 0
            state["step_capacity_charged__Ah"] = 0
            state["step_capacity_discharged__Ah"] = 0
            state["step_energy_charged__Wh"] = 0
            state["step_energy_discharged__Wh"] = 0
        else:
            continue


if __name__ == "__main__":
    import os

    from pyspark.sql import SparkSession

    from pulse_telemetry.sparklib import iceberg, telemetry
    from pulse_telemetry.utils import channel

    catalog = os.environ["PULSE_TELEMETRY_CATALOG"]
    database = os.environ["PULSE_TELEMETRY_DATABASE"]
    num_channels = int(os.environ["PULSE_TELEMETRY_NUM_CHANNELS"])
    timeout_seconds = float(os.environ["PULSE_TELEMETRY_TIMEOUT_SECONDS"])
    acquisition_frequency = int(os.environ["PULSE_TELEMETRY_ACQUISITION_FREQUENCY"])
    points_per_step = int(os.environ["PULSE_TELEMETRY_POINTS_PER_STEP"])

    spark = SparkSession.builder.appName("TelemetryGenerator").getOrCreate()

    # Create telemetry table if not exists
    iceberg.create_table_if_not_exists(
        spark=spark,
        catalog_name=catalog,
        database_name=database,
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
        write_order_columns=telemetry.telemetry_write_order,
    )

    # Runs generator and loads data into iceberg table
    local_buffer = channel.LocalBuffer()
    channel.run_with_timeout(
        source=telemetry_generator,
        sink=local_buffer,
        topic="telemetry",
        num_channels=num_channels,
        timeout_seconds=timeout_seconds,
        acquisition_frequency=acquisition_frequency,
        points_per_step=points_per_step,
        lower_voltage_limit=3,  # V
        upper_voltage_limit=4,  # V
        current=1.0,  # A
    )
    iceberg.merge_into_table(
        spark=spark,
        source_df=local_buffer.dataframe(spark, telemetry.telemetry_schema),
        catalog_name=catalog,
        database_name=database,
        table_name="telemetry",
        match_columns=telemetry.telemetry_composite_key,
    )
