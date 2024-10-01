import asyncio
import datetime
import uuid
from typing import TypedDict


class TimeseriesState(TypedDict):
    device_id: str
    test_id: str
    cycle_number: int
    step_number: int
    step_type: str
    step_id: int
    record_number: int
    timestamp: str
    date: str
    current__A: float
    voltage__V: float
    power__W: float
    capacity__Ah: float
    energy__Wh: float
    auxiliary: dict[str, float]
    metadata: str
    update_ts: str


async def timeseries_generator(
    aquisition_frequency: int,  # Hz
    points_per_step: int,
    lower_voltage_limit: float = 3.0,  # V
    upper_voltage_limit: float = 4.0,  # V
    current: float = 1.0,  # A
):
    # Initializes the state
    state: TimeseriesState = {
        "device_id": str(uuid.uuid4()),
        "test_id": str(uuid.uuid4()),
        "cycle_number": 1,
        "step_number": 1,
        "step_type": "Rest",
        "step_id": 0,
        "record_number": 0,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "date": datetime.datetime.now(datetime.timezone.utc).date().isoformat(),
        "current__A": 0.0,
        "voltage__V": lower_voltage_limit,
        "power__W": 0.0,
        "capacity__Ah": 0.0,
        "energy__Wh": 0.0,
        "auxiliary": {"temperature": 25.0},
        "metadata": '{"experiment": "testing"}',
        "update_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    step_record_number = 0  # internal state, used to determine step transitions
    voltage_delta = (upper_voltage_limit - lower_voltage_limit) / points_per_step
    capacity_energy_factor = 1.0 / aquisition_frequency / 3600

    while True:
        # Updates time and record indexing
        new_time = datetime.datetime.now(datetime.timezone.utc)
        state.update(
            {
                "timestamp": new_time.isoformat(),
                "date": new_time.date().isoformat(),
                "update_ts": new_time.isoformat(),
                "record_number": state["record_number"] + 1,
            }
        )
        step_record_number += 1  # Used for step transitions

        # Updates telemetry
        match state["step_type"]:
            case "Charge":
                new_current = current
                new_voltage = state["voltage__V"] + voltage_delta
            case "Discharge":
                new_current = -current
                new_voltage = state["voltage__V"] - voltage_delta
            case "Rest":
                new_current = 0.0
                new_voltage = state["voltage__V"]
        state.update(
            {
                "current__A": new_current,
                "voltage__V": new_voltage,
                "power__W": new_current * new_voltage,
                "capacity__Ah": new_current * capacity_energy_factor,
                "energy__Wh": new_current * new_voltage * capacity_energy_factor,
            }
        )

        # Yields state before handling step transitions
        yield state
        await asyncio.sleep(1.0 / aquisition_frequency)

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
        else:
            continue
