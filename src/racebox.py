#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2024
# @generate at 2024-12-5 10:55:53
# comment: 读取RaceBox 数据并存入数据库

import asyncio
import struct
from datetime import datetime
from bleak import BleakScanner, BleakClient
import json
import os
import configparser
import psycopg2
import psycopg2.extras as extras
import uuid
from loguru import logger

import folium
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium.map import FeatureGroup
from folium.raster_layers import TileLayer

# 数据库连接定义
config = configparser.ConfigParser()
config.read("../conf/db.cnf")

pg_host = config.get("racebox", "host")
pg_database = config.get("racebox", "database")
pg_user = config.get("racebox", "user")
pg_password = config.get("racebox", "password")
pg_port = int(config.get("racebox", "port"))
amap_key = config.get("amap", "amap_key")

# 日志配置
logDir = os.path.expanduser("../log/")
if not os.path.exists(logDir):
    os.mkdir(logDir)
logFile = os.path.join(logDir, "racebox.log")
# logger.remove(handler_id=None)

logger.add(
    logFile,
    colorize=True,
    rotation="1 days",
    retention="3 days",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
    diagnose=True,
    level="INFO",
)

# RaceBox 接口定义 UUIDs
UART_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
RX_CHAR_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
TX_CHAR_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"
NMEA_TX_UUID = "00001103-0000-1000-8000-00805f9b34fb"
DOWNLOAD_COMMAND = bytes([0xB5, 0x62, 0xFF, 0x23, 0x00, 0x00, 0x22, 0x65])  # Command to initiate data download

ins_data = """insert into lc_racebox(itow, imp_stamp, year, month, day, hour, minute, second, validity_flags, time_accuracy, 
            nanoseconds, fix_status, fix_status_flags, date_time_flags, numberof_svs, longitude, latitude, wgs_altitude, 
            msl_altitude, horizontal_accuracy, vertical_accuracy, speed, heading, speed_accuracy, heading_accuracy, pdop, 
            lat_lon_flags, battery_voltage, gforce_x, gforce_y, gforce_z, rotation_rate_x, rotation_rate_y, rotation_rate_z) 
            values %s on conflict (itow) do update set year=excluded.year, month=excluded.month, day=excluded.day, 
            hour=excluded.hour, minute=excluded.minute, second=excluded.second, validity_flags=excluded.validity_flags, 
            time_accuracy=excluded.time_accuracy, nanoseconds=excluded.nanoseconds, fix_status=excluded.fix_status,
            fix_status_flags=excluded.fix_status_flags, date_time_flags=excluded.date_time_flags, 
            numberof_svs=excluded.numberof_svs, longitude=excluded.longitude, latitude=excluded.latitude, 
            wgs_altitude=excluded.wgs_altitude, msl_altitude=excluded.msl_altitude, horizontal_accuracy=excluded.horizontal_accuracy, 
            vertical_accuracy=excluded.vertical_accuracy, speed=excluded.speed, heading=excluded.heading, 
            speed_accuracy=excluded.speed_accuracy, heading_accuracy=excluded.heading_accuracy, pdop=excluded.pdop, 
            lat_lon_flags=excluded.lat_lon_flags, battery_voltage=excluded.battery_voltage, gforce_x=excluded.gforce_x, 
            gforce_y=excluded.gforce_y, gforce_z=excluded.gforce_z, rotation_rate_x=excluded.rotation_rate_x, 
            rotation_rate_y=excluded.rotation_rate_y, rotation_rate_z=excluded.rotation_rate_z;
            """

ins_imp = """insert into imp_racebox(imp_stamp, file_name, duration) values (%s, %s, %s);
          """

# 定义标志位的掩码
# validity_flags
VALID_DATE_MASK = 0x01  # 00000001
VALID_TIME_MASK = 0x02  # 00000010
FULLY_RESOLVED_MASK = 0x04  # 00000100
VALID_MAG_DECLINATION_MASK = 0x08  # 00001000

# fix_status_flags
VALID_FIX_MASK = 0x01  # 00000001
DIFFERENTIAL_CORRECTIONS_MASK = 0x02  # 00000010
POWER_STATE_MASK = 0x1C  # 00011100
VALID_HEADING_MASK = 0x20  # 00100000
CARRIER_PHASE_RANGE_SOLUTION_MASK = 0xC0  # 11000000

# date_time_flags
AVAILABLE_CONFIRMATION_MASK = 0x20  # 00100000
CONFIRMED_UTC_DATE_MASK = 0x40  # 01000000
CONFIRMED_UTC_TIME_MASK = 0x80  # 10000000

# lat_lon_flags
INVALID_LAT_LON_MASK = 0x01  # 00000001
DIFFERENTIAL_CORRECTION_AGE_MASK = 0x1E  # 00011110

# battery_status_voltage
CHARGING_MASK = 0x80  # 10000000
BATTERY_LEVEL_MASK = 0x7F  # 01111111
VOLTAGE_MASK = 0xFF  # 11111111

# 上次连接设备文件
DEVICE_MEMORY_FILE = "../conf/last_device.json"

# 本次插入数据uuid
time_uuid = uuid.uuid1()

# 建立数据库连接
con = psycopg2.connect(database=pg_database,
                       user=pg_user,
                       password=pg_password,
                       host=pg_host,
                       port=pg_port)

psycopg2.extras.register_uuid()


def speed_to_color(speed, max_speed):
    cmap = plt.get_cmap('cool')  # Blue for slow speeds, red for fast speeds
    norm_speed = min(speed / max_speed, 1.0)  # Normalize speed between 0 and 1
    return mcolors.to_hex(cmap(norm_speed))


# Function to plot the GPS path on an interactive folium map
def plot_gps_path(in_data, map_name):
    map_start_time = datetime.now()
    longitudes = []
    latitudes = []
    speeds = []
    for row in in_data:
        longitudes.append(float(row[15]))
        latitudes.append(float(row[16]))
        speeds.append(float(row[21]))

    # Check if there are fewer than 1000 rows of data, and skip if true
    if len(longitudes) < 1000 or len(latitudes) < 1000:
        logger.info(f"因点数据少于1000，跳过生成地图： {map_name}！")
        return

    if len(longitudes) == 0 or len(latitudes) == 0:
        logger.info(f"没有定位点，跳过生成地图 {map_name}！")
        return

    # Create the folium map, centered on the average of the data points
    avg_lat = np.mean(latitudes)
    avg_lon = np.mean(longitudes)
    map_folium = folium.Map(location=[avg_lat, avg_lon], zoom_start=13)

    amap_layer = TileLayer(
        'https://webrd01.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={x}&y={y}&z={z}',
        attr='高德地图',
        overlay=True,
        control=True,
        **{'Amap_key': amap_key}
    )

    map_folium.add_child(amap_layer)

    # Calculate maximum speed for normalization
    max_speed = max(speeds)

    # Add GPS points as polylines with varying colors (based on speed)
    for i in range(1, len(latitudes)):
        start_point = [latitudes[i - 1], longitudes[i - 1]]
        end_point = [latitudes[i], longitudes[i]]
        speed = speeds[i]

        # Map speed to color
        color = speed_to_color(speed, max_speed)

        # Add the polyline segment to the map with thinner lines and increased opacity
        folium.PolyLine(
            [start_point, end_point],
            color=color,
            weight=3,  # Reduce the path width to 3 for a thinner line
            opacity=1.0  # Increase opacity to make the path fully solid
        ).add_to(map_folium)

    map_folium.save(map_name)

    logger.info(f"地图 {map_name.replace('../file/','')} 生成完成，耗时 {(datetime.now() - map_start_time).total_seconds()} 秒！")


def format_filename(in_first_record, in_last_record):
    """生成文件名"""
    first_year = in_first_record[2]
    first_month = f"{in_first_record[3]:02d}"
    first_day = f"{in_first_record[4]:02d}"
    first_hour = f"{in_first_record[5]:02d}"
    first_minute = f"{in_first_record[6]:02d}"
    first_second = f"{in_first_record[7]:02d}"
    last_year = in_last_record[2]
    last_month = f"{in_last_record[3]:02d}"
    last_day = f"{in_last_record[4]:02d}"
    last_hour = f"{in_last_record[5]:02d}"
    last_minute = f"{in_last_record[6]:02d}"
    last_second = f"{in_last_record[7]:02d}"
    first_timestamp = f"{first_year}{first_month}{first_day}{first_hour}{first_minute}{first_second}"
    last_timestamp = f"{last_year}{last_month}{last_day}{last_hour}{last_minute}{last_second}"
    return f"{first_timestamp}_{last_timestamp}"


def save_last_device(device):
    """保存最后一次连接设备信息"""
    device_info = {
        "address": device.address,
        "name": device.name
    }
    with open(DEVICE_MEMORY_FILE, 'w') as f:
        json.dump(device_info, f)


def get_last_device():
    """获取最后一次连接成功设备信息"""
    try:
        if os.path.exists(DEVICE_MEMORY_FILE):
            with open(DEVICE_MEMORY_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"设备读取错误: {e}")
    return None


async def scan_and_connect():
    """查询RaceBox设备并连接"""
    try:
        # 先尝试连接上次连接的设备
        last_device = get_last_device()
        if last_device:
            logger.info(f"开始连接上次连接设备: {last_device['name']}")
            try:
                device = await BleakScanner.find_device_by_address(last_device['address'], timeout=10)
                if device:
                    logger.info(f"找到上次连接设备: {device.name}")
                    await connect_and_download(device)
                else:
                    logger.info("未找到上次连接设备, 扫描新设备...")
            except Exception as e:
                logger.info(f"上次连接设备连接失败！: {e}")
                logger.info("扫描新设备...")

        # 上次连接设备连接失败，扫描新设备
        devices = await BleakScanner.discover(timeout=10)
        racebox_devices = [device for device in devices if device.name and "RaceBox" in device.name]

        if racebox_devices:
            logger.info(f"扫描到 {len(racebox_devices)} 个RaceBox设备")
            for device in racebox_devices:
                logger.info(f"连接设备 {device.name} 中...")
                # 保存连接成功设备
                save_last_device(device)
                try:
                    await connect_and_download(device)
                    break  # 仅连接扫描出的第一个设备
                except Exception as e:
                    logger.info(f"连接设备失败： {device.name}: {e}")
        else:
            logger.error("未找到 RaceBox 设备")
    except asyncio.CancelledError:
        logger.info("\n用户取消操作！")
        raise


def parse_message(packet):
    """处理二进制数据"""
    payload = packet[6:86]
    parsed_data = struct.unpack('<I H B B B B B B I i B B B B i i i i I I i i I I H B B h h h h h h', payload[:80])

    # validity_flags parse
    validity_flags_byte = parsed_data[7]
    valid_date = (validity_flags_byte & VALID_DATE_MASK) != 0
    valid_time = (validity_flags_byte & VALID_TIME_MASK) != 0
    fully_resolved = (validity_flags_byte & FULLY_RESOLVED_MASK) != 0
    valid_magnetic_declination = (validity_flags_byte & VALID_MAG_DECLINATION_MASK) != 0

    # fix_status_flags parse
    fix_status_flags_byte = parsed_data[11]
    valid_fix = (fix_status_flags_byte & VALID_FIX_MASK) != 0
    differential_corrections_applied = (fix_status_flags_byte & DIFFERENTIAL_CORRECTIONS_MASK) != 0
    power_state = (fix_status_flags_byte & POWER_STATE_MASK) >> 2  # 提取Bits 4..2
    valid_heading = (fix_status_flags_byte & VALID_HEADING_MASK) != 0
    carrier_phase_range_solution = (fix_status_flags_byte & CARRIER_PHASE_RANGE_SOLUTION_MASK) >> 6  # 提取Bits 7..6

    # datetime_flags
    datetime_flags_byte = parsed_data[12]
    available_confirmation = (datetime_flags_byte & AVAILABLE_CONFIRMATION_MASK) != 0
    confirmed_utc_date = (datetime_flags_byte & CONFIRMED_UTC_DATE_MASK) != 0
    confirmed_utc_time = (datetime_flags_byte & CONFIRMED_UTC_TIME_MASK) != 0

    # lat_lon_flags
    lat_lon_flags_byte = parsed_data[25]
    invalid_lat_lon = (lat_lon_flags_byte & INVALID_LAT_LON_MASK) != 0
    differential_correction_age = (lat_lon_flags_byte & DIFFERENTIAL_CORRECTION_AGE_MASK) >> 1

    # battery_status_voltage
    battery_status_voltage_byte = parsed_data[26]
    # 提取电池电量百分比（适用于RaceBox Mini和Mini S）
    # battery_level = battery_status_voltage_byte & BATTERY_LEVEL_MASK
    # 计算输入电压（适用于RaceBox Micro）
    input_voltage = battery_status_voltage_byte & VOLTAGE_MASK

    record = (
        parsed_data[0],  # "iTOW"
        time_uuid,  # 导入标签
        parsed_data[1],  # "Year"
        parsed_data[2],  # "Month"
        parsed_data[3],  # "Day"
        parsed_data[4],  # "Hour"
        parsed_data[5],  # "Minute"
        parsed_data[6],  # "Second"
        extras.Json({
            "Valid Date": valid_date,
            "Valid Time": valid_time,
            "Fully Resolved": fully_resolved,
            "Valid Magnetic Declination": valid_magnetic_declination
        }),  # "Validity Flags"
        parsed_data[8],  # "Time Accuracy"
        parsed_data[9],  # "Nanoseconds"
        parsed_data[10],  # "Fix Status"
        extras.Json({
            "Valid Fix": valid_fix,
            "Differential Corrections Applied": differential_corrections_applied,
            "Power State": power_state,
            "Valid Heading": valid_heading,
            "Carrier Phase Range Solution": carrier_phase_range_solution
        }),  # "Fix Status Flags"
        extras.Json({
            "Available Confirmation of Date/Time Validity": available_confirmation,
            "Confirmed UTC Date Validity": confirmed_utc_date,
            "Confirmed UTC Time Validity": confirmed_utc_time
        }),  # "Date/Time Flags"
        parsed_data[13],  # "Number of SVs"
        parsed_data[14] / 1e7,  # "Longitude"
        parsed_data[15] / 1e7,  # "Latitude"
        parsed_data[16] / 1000,  # "WGS Altitude"
        parsed_data[17] / 1000,  # "MSL Altitude"
        parsed_data[18] / 1000,  # "Horizontal Accuracy"
        parsed_data[19] / 1000,  # "Vertical Accuracy"
        parsed_data[20] / 1000,  # "Speed"
        parsed_data[21] / 100000,  # "Heading"
        parsed_data[22],  # "Speed Accuracy"
        parsed_data[23] / 1e5,  # "Heading Accuracy"
        parsed_data[24],  # "PDOP"
        extras.Json({
            "Invalid Latitude, Longitude, WGS Altitude, and MSL Altitude": invalid_lat_lon,
            "Differential Correction Age": differential_correction_age
        }),  # "Lat/Lon Flags"
        input_voltage / 10,  # "Battery Status or Input Voltage"
        parsed_data[27] / 1000,  # "G-Force X"
        parsed_data[28] / 1000,  # "G-Force Y"
        parsed_data[29] / 1000,  # "G-Force Z"
        parsed_data[30] / 100,  # "Rotation rate X"
        parsed_data[31] / 100,  # "Rotation rate Y"
        parsed_data[32] / 100  # "Rotation rate Z"
    )
    return record


def validate_checksum(buffer):
    """根据结构协议校验数据"""
    ck_a, ck_b = 0, 0
    for byte in buffer[2:-2]:
        ck_a = (ck_a + byte) & 0xFF
        ck_b = (ck_b + ck_a) & 0xFF
    return ck_a == buffer[-2] and ck_b == buffer[-1]


def insert_db(in_sql, in_data):
    try:
        cur = con.cursor()
        cur.execute(in_sql, in_data)
        con.commit()
    except Exception as e:
        logger.error(e)
    finally:
        cur.close()


async def connect_and_download(device):
    """建立已扫描连接并下载数据"""
    session_data = []
    buffer = bytearray()
    total_records = 0
    download_complete = asyncio.Event()
    session_num = 0
    down_start_time = datetime.now()
    session_start_time = datetime.now()
    first_record = None
    last_record = None

    async with (BleakClient(device.address, timeout=20) as client):
        try:
            await client.disconnect()
            await client.connect()

            services = client.services

            if UART_UUID not in [str(service.uuid) for service in services]:
                logger.error(f"设备 {device.name} 无 UART 服务！")
                return

            def notification_handler(sender, data):
                nonlocal buffer, session_data, total_records, session_num, down_start_time, session_start_time, first_record, last_record
                buffer.extend(data)

                # 处理传输数据
                while len(buffer) >= 8:
                    if buffer[:2] == bytes([0xB5, 0x62]):
                        message_class, message_id = buffer[2], buffer[3]
                        packet_length = struct.unpack('<H', buffer[4:6])[0]
                        full_packet_length = packet_length + 8

                        if len(buffer) < full_packet_length:
                            break

                        if validate_checksum(buffer[:full_packet_length]):
                            if message_class == 0xFF:
                                if message_id == 0x23:  # 开始下载
                                    total_records = struct.unpack('<I', buffer[6:10])[0]
                                    logger.info(f"总计 {total_records} 条记录")
                                elif message_id == 0x21:  # 历史数据
                                    record = parse_message(buffer[:full_packet_length])
                                    session_data.append(record)
                                    if first_record is None:
                                        first_record = record
                                    last_record = record
                                elif message_id == 0x01:  # 实时数据
                                    record = parse_message(buffer[:full_packet_length])
                                    session_data.append(record)
                                    if first_record is None:
                                        first_record = record
                                    last_record = record
                                elif message_id == 0x02:
                                    logger.info(f"下载完成，耗时 {(datetime.now() - down_start_time).total_seconds()} 秒！")
                                    download_complete.set()
                                elif message_id == 0x26:
                                    if first_record:
                                        session_num += 1
                                        duration = (datetime.now() - session_start_time).total_seconds()
                                        file_name = format_filename(first_record, last_record)
                                        imp_data = (time_uuid, file_name, duration)
                                        insert_db(ins_imp, imp_data)
                                        logger.info(f"已处理第{session_num}段数据，耗时 {duration} 秒！")
                                    session_start_time = datetime.now()
                                    first_record = None
                            buffer = buffer[full_packet_length:]

            await client.start_notify(TX_CHAR_UUID, notification_handler)
            await client.write_gatt_char(RX_CHAR_UUID, DOWNLOAD_COMMAND)
            logger.info(f"正在从设备 {device.name} 下载数据...")

            # 等待下载完成
            await download_complete.wait()
            await client.stop_notify(TX_CHAR_UUID)
        except Exception as e:
            logger.error(e)
        finally:
            await client.disconnect()

    # 保存数据
    if session_data:
        print(session_data[0])
        print(session_data[1])
        try:
            map_record = session_data[0]
            map_name = (f"../file/map_{map_record[2]}{map_record[3]:02d}{map_record[4]:02d}"
                        f"{map_record[5]:02d}{map_record[6]:02d}{map_record[7]:02d}.html")
            plot_gps_path(session_data, map_name)

            insert_start_time = datetime.now()
            cur = con.cursor()
            extras.execute_values(cur, ins_data, session_data, page_size=1000)
            con.commit()
            logger.info(f"定位数据入库成功，耗时 {(datetime.now() - insert_start_time).total_seconds()} 秒！")
        except Exception as e:
            logger.error(e)
        finally:
            cur.close()


start_time = datetime.now()
asyncio.run(scan_and_connect())
logger.info(f"所有操作完成，总计耗时 {(datetime.now() - start_time).total_seconds()} 秒！")
