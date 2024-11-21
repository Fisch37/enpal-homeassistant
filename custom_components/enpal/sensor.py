"""Platform for sensor integration."""
from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta, datetime
from typing import NamedTuple
from homeassistant.components.sensor import (SensorEntity)
from homeassistant.core import HomeAssistant
from homeassistant import config_entries
from homeassistant.helpers.device_registry import DeviceEntryType
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_registry import async_get, async_entries_for_config_entry
from custom_components.enpal.const import DOMAIN
import aiohttp
import logging
from influxdb_client import InfluxDBClient

_LOGGER = logging.getLogger(__name__)
SCAN_INTERVAL = timedelta(seconds=20)

VERSION= '0.1.0'

class EnpalSensorConfig(NamedTuple):
    icon: str
    name: str
    device_class: str
    unit: str

FIELD_MAP: dict[str, EnpalSensorConfig] = {
    'Power.DC.Total': EnpalSensorConfig('mdi:solar-power', 'Enpal Solar Production Power', 'power', 'W'),
    'Power.House.Total': EnpalSensorConfig('mdi:home-lightning-bolt', 'Enpal Power House Total', 'power', 'W'),
    'Power.External.Total': EnpalSensorConfig('mdi:home-lightning-bolt', 'Enpal Power External Total', 'power', 'W'),
    'Energy.Consumption.Total.Day': EnpalSensorConfig('mdi:home-lightning-bolt', 'Enpal Energy Consumption', 'energy', 'kWh'),
    
    'Energy.External.Total.Out.Day': EnpalSensorConfig('mdi:transmission-tower-export', 'Enpal Energy External Out Day', 'energy', 'kWh'),
    'Energy.External.Total.In.Day': EnpalSensorConfig('mdi:transmission-tower-import', 'Enpal Energy External In Day', 'energy', 'kWh'),
    
    'Energy.Production.Total.Day': EnpalSensorConfig('mdi:solar-power-variant', 'Enpal Production Day', 'energy', 'kWh'),
    
    'Voltage.Phase.A': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Voltage Phase A', 'voltage', 'V'),
    'Current.Phase.A': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Ampere Phase A', 'current', 'A'),
    'Power.AC.Phase.A': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Power Phase A', 'power', 'W'),
    'Voltage.Phase.B': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Voltage Phase B', 'voltage', 'V'),
    'Current.Phase.B': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Ampere Phase B', 'current', 'A'),
    'Power.AC.Phase.B': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Power Phase B', 'power', 'W'),
    'Voltage.Phase.C': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Voltage Phase C', 'voltage', 'V'),
    'Current.Phase.C': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Ampere Phase C', 'current', 'A'),
    'Power.AC.Phase.C': EnpalSensorConfig('mdi:lightning-bolt', 'Enpal Power Phase C', 'power', 'W'),
    
    'Current.String.1': EnpalSensorConfig('mdi:sun-angle', 'Enpal Current String 1', 'current', 'A'),
    'Voltage.String.1': EnpalSensorConfig('mdi:sun-angle', 'Enpal Voltage String 1', 'voltage', 'V'),
    'Power.DC.String.1': EnpalSensorConfig('mdi:sun-angle', 'Enpal Power String 1', 'power', 'W'),
    'Current.String.2': EnpalSensorConfig('mdi:sun-angle', 'Enpal Current String 2', 'current', 'A'),
    'Voltage.String.2': EnpalSensorConfig('mdi:sun-angle', 'Enpal Voltage String 2', 'voltage', 'V'),
    'Power.DC.String.2': EnpalSensorConfig('mdi:sun-angle', 'Enpal Power String 2', 'power', 'W'),
    
    'Power.Battery.Charge.Discharge': EnpalSensorConfig('mdi:battery-charging', 'Enpal Battery Power', 'power', 'W'),
    'Energy.Battery.Charge.Level': EnpalSensorConfig('mdi:battery', 'Enpal Battery Percent', 'battery', '%'),
    'Energy.Battery.Charge.Day': EnpalSensorConfig('mdi:battery-arrow-up', 'Enpal Battery Charge Day', 'energy', 'kWh'),
    'Energy.Battery.Discharge.Day': EnpalSensorConfig('mdi:battery-arrow-down', 'Enpal Battery Discharge Day', 'energy', 'kWh'),
    'Energy.Battery.Charge.Total.Unit.1': EnpalSensorConfig('mdi:battery-arrow-up', 'Enpal Battery Charge Total', 'energy', 'kWh'),
    'Energy.Battery.Discharge.Total.Unit.1': EnpalSensorConfig('mdi:battery-arrow-down', 'Enpal Battery Discharge Total', 'energy', 'kWh'),
    
    'State.Wallbox.Connector.1.Charge': EnpalSensorConfig('mdi:ev-station', 'Wallbox Charge Percent', 'battery', '%'),
    'Power.Wallbox.Connector.1.Charging': EnpalSensorConfig('mdi:ev-station', 'Wallbox Charging Power', 'power', 'W'),
    'Energy.Wallbox.Connector.1.Charged.Total': EnpalSensorConfig('mdi:ev-station', 'Wallbox Charging Total', 'energy', 'Wh'),
}

def get_tables(ip: str, port: int, token: str):
    client = InfluxDBClient(url=f'http://{ip}:{port}', token=token, org='enpal')
    query_api = client.query_api()

    query = 'from(bucket: "solar") \
      |> range(start: -5m) \
      |> last()'

    tables = query_api.query(query)
    return tables

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: config_entries.ConfigEntry,
    async_add_entities,
):
    # Get the config entry for the integration
    config = hass.data[DOMAIN][config_entry.entry_id]
    if config_entry.options:
        config.update(config_entry.options)
    to_add = []
    if not 'enpal_host_ip' in config:
        _LOGGER.error("No enpal_host_ip in config entry")
        return
    if not 'enpal_host_port' in config:
        _LOGGER.error("No enpal_host_port in config entry")
        return
    if not 'enpal_token' in config:
        _LOGGER.error("No enpal_token in config entry")
        return

    global_config = hass.data[DOMAIN]

    tables = await hass.async_add_executor_job(get_tables, config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'])

    encountered_fields = set()
    for table in tables:
        field = table.records[0].values['_field']
        measurement = table.records[0].values['_measurement']
        
        if field in encountered_fields:
            # There may be duplicates because of different _measurement values
            # e.g. "inverter" and "powerSensor" share some fields
            # we also can't choose which measurement's field to use, 
            # since different Enpal devices have different measurements for the same fields
            # Why? idk, ask Enpal
            continue
        encountered_fields.add(field)
        try:
            field_config = FIELD_MAP[field]
        except KeyError:
            _LOGGER.debug("Encountered field %s without config. This is normal. Skipping", field)
            continue
        to_add.append(EnpalSensor(field, measurement, field_config.icon, field_config.name, config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], field_config.device_class, field_config.unit))
        encountered_fields.add(field)

    entity_registry = async_get(hass)
    entries = async_entries_for_config_entry(
        entity_registry, config_entry.entry_id
    )
    for entry in entries:
        entity_registry.async_remove(entry.entity_id)

    async_add_entities(to_add, update_before_add=True)


class EnpalSensor(SensorEntity):

    def __init__(self, field: str, measurement: str, icon:str, name: str, ip: str, port: int, token: str, device_class: str, unit: str):
        self.field = field
        self.measurement = measurement
        self.ip = ip
        self.port = port
        self.token = token
        self.enpal_device_class = device_class
        self.unit = unit
        self._attr_icon = icon
        self._attr_name = name
        self._attr_unique_id = f'enpal_{measurement}_{field}'
        self._attr_extra_state_attributes = {}


    async def async_update(self) -> None:

        # Get the IP address from the API
        try:
            client = InfluxDBClient(url=f'http://{self.ip}:{self.port}', token=self.token, org="enpal")
            query_api = client.query_api()

            query = f'from(bucket: "solar") \
              |> range(start: -5m) \
              |> filter(fn: (r) => r["_measurement"] == "{self.measurement}") \
              |> filter(fn: (r) => r["_field"] == "{self.field}") \
              |> last()'

            tables = await self.hass.async_add_executor_job(query_api.query, query)

            value = 0
            if tables:
                value = tables[0].records[0].values['_value']

            self._attr_native_value = round(float(value), 2)
            self._attr_device_class = self.enpal_device_class
            self._attr_native_unit_of_measurement	= self.unit
            self._attr_state_class = 'measurement'
            self._attr_extra_state_attributes['last_check'] = datetime.now()
            self._attr_extra_state_attributes['field'] = self.field
            self._attr_extra_state_attributes['measurement'] = self.measurement

            #if self.field == 'Energy.Consumption.Total.Day' or 'Energy.Storage.Total.Out.Day' or 'Energy.Storage.Total.In.Day' or 'Energy.Production.Total.Day' or 'Energy.External.Total.Out.Day' or 'Energy.External.Total.In.Day':
            if self._attr_native_unit_of_measurement == "kWh":
                self._attr_extra_state_attributes['last_reset'] = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                self._attr_state_class = 'total_increasing'
            if self._attr_native_unit_of_measurement == "Wh":
                self._attr_extra_state_attributes['last_reset'] = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                self._attr_state_class = 'total_increasing'

            if self.field == 'Percent.Storage.Level':
                if self._attr_native_value >= 10:
                    self._attr_icon = "mdi:battery-outline"
                if self._attr_native_value <= 19 and self._attr_native_value >= 10:
                    self._attr_icon = "mdi:battery-10"
                if self._attr_native_value <= 29 and self._attr_native_value >= 20:
                    self._attr_icon = "mdi:battery-20"
                if self._attr_native_value <= 39 and self._attr_native_value >= 30:
                    self._attr_icon = "mdi:battery-30"
                if self._attr_native_value <= 49 and self._attr_native_value >= 40:
                    self._attr_icon = "mdi:battery-40"
                if self._attr_native_value <= 59 and self._attr_native_value >= 50:
                    self._attr_icon = "mdi:battery-50"
                if self._attr_native_value <= 69 and self._attr_native_value >= 60:
                    self._attr_icon = "mdi:battery-60"
                if self._attr_native_value <= 79 and self._attr_native_value >= 70:
                    self._attr_icon = "mdi:battery-70"
                if self._attr_native_value <= 89 and self._attr_native_value >= 80:
                    self._attr_icon = "mdi:battery-80"
                if self._attr_native_value <= 99 and self._attr_native_value >= 90:
                    self._attr_icon = "mdi:battery-90"
                if self._attr_native_value == 100:
                    self._attr_icon = "mdi:battery"

        except Exception as e:
            _LOGGER.error(f'{e}')
            self._state = 'Error'
            self._attr_native_value = None
            self._attr_extra_state_attributes['last_check'] = datetime.now()