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
# import psycopg2
# import psycopg2.extras as extras
import uuid
from loguru import logger
import taos

import folium
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium.raster_layers import TileLayer
import math

# 数据库连接定义
config = configparser.ConfigParser()
config.read("../conf/db.cnf")

pg_host = config.get("racebox", "host")
pg_database = config.get("racebox", "database")
pg_user = config.get("racebox", "user")
pg_password = config.get("racebox", "password")
pg_port = int(config.get("racebox", "port"))

td_host = config.get("tdengine", "host")
td_database = config.get("tdengine", "database")
td_port = int(config.get("tdengine", "port"))
td_user = config.get("tdengine", "user")
td_password = config.get("tdengine", "password")
td_timezone = config.get("tdengine", "timezone")

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

exists_imp = """select count(1) from imp_racebox where file_name = """

ins_imp = """insert into imp_racebox(imp_stamp, file_name, duration) values (%s, %s, %s);"""

ins_data = """insert into lc_racebox(itow, imp_stamp, year, month, day, hour, minute, second, time_accuracy, nanoseconds,  
            fix_status, numberof_svs, longitude, latitude, wgs_altitude, msl_altitude, horizontal_accuracy, 
            vertical_accuracy, speed, heading, speed_accuracy, heading_accuracy, pdop, gforce_x, gforce_y, gforce_z, 
            rotation_rate_x, rotation_rate_y, rotation_rate_z) values %s;
            """

ins_taos_sql = """insert into eadm.lc_racebox values"""


# 上次连接设备文件
DEVICE_MEMORY_FILE = "../conf/last_device.json"

# 本次插入数据uuid
time_uuid = uuid.uuid1()

# 建立数据库连接
# con = psycopg2.connect(database=pg_database,
#                        user=pg_user,
#                        password=pg_password,
#                        host=pg_host,
#                        port=pg_port)
#
# psycopg2.extras.register_uuid()

con_taos = taos.connect(host=td_host,
                       database=td_database,
                       port=td_port,
                       user=td_user,
                       password=td_password,
                       timezone=td_timezone)

def insert_db(in_sql, in_data):
    try:
        cur = con.cursor()
        cur.execute(in_sql, in_data)
        con.commit()
    except Exception as e:
        logger.error(e)
    finally:
        cur.close()


def load_db(in_sql, in_data):
    try:
        cur = con.cursor()
        extras.execute_values(cur, in_sql, in_data, page_size=1000)
        con.commit()
    except Exception as e:
        logger.error(e)
    finally:
        cur.close()


def select_db(sel_sql, in_filter):
    try:
        cur = con.cursor()
        cur.execute(sel_sql, (in_filter,))
        res = cur.fetchone()[0]
        return res
    except Exception as e:
        logger.error(e)
        return 0
    finally:
        cur.close()

def load_taos(in_sql, in_data, batch_size):
    try:
        cur_taos = con_taos.cursor()
        params = []
        for i, record in enumerate(in_data, 1):
            try:
                microsecond = (record[9] // 1000) % 1_000_000
                ts = datetime(
                    year=record[2],
                    month=record[3],
                    day=record[4],
                    hour=record[5],
                    minute=record[6],
                    second=record[7],
                    microsecond=microsecond
                ).isoformat()
                params.append((
                    ts,
                    record[0],
                    str(record[1]),
                    record[2],
                    record[3],
                    record[4],
                    record[5],
                    record[6],
                    record[7],
                    record[8],
                    record[9],
                    record[10],
                    record[11],
                    record[12],
                    record[13],
                    record[14],
                    record[15],
                    record[16],
                    record[17],
                    record[18],
                    record[19],
                    record[20],
                    record[21],
                    record[22],
                    record[23],
                    record[24],
                    record[25],
                    record[26],
                    record[27],
                    record[28]
                ))

                if i % batch_size == 0:
                    cur_taos.execute_many(in_sql, params)
                    params = []
            except ValueError as e:
                logger.error(f"Invalid datetime parameters in record: {e}")
                continue
        if params:
            cur_taos.execute_many(in_sql, params)
    except Exception as e:
        logger.error(f"TDengine写入失败: {e}")
    finally:
        cur_taos.close()


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


async def scan_device_connect():
    devices = await BleakScanner.discover()
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


async def last_device_connect():
    try:
        # 先尝试连接上次连接的设备
        last_device = get_last_device()
        if last_device:
            logger.info(f"开始连接上次连接设备: {last_device['name']}")
            try:
                device = await BleakScanner.find_device_by_address(last_device['address'])
                if device:
                    logger.info(f"找到上次连接设备: {device.name}")
                    await connect_and_download(device)
                else:
                    logger.info("未找到上次连接设备, 扫描新设备...")
                    await scan_device_connect()
            except Exception as e:
                logger.info(f"上次连接设备连接失败！: {e}")
                logger.info("扫描新设备...")
                await scan_device_connect()
        else:
            logger.error("未找到 RaceBox 设备")
    except asyncio.CancelledError:
        logger.info("\n用户取消操作！")
        raise


def parse_message(packet):
    """处理二进制数据"""
    payload = packet[6:86]
    parsed_data = struct.unpack('<I H B B B B B B I i B B B B i i i i I I i i I I H B B h h h h h h', payload[:80])
    # lng, lat = wgs84_to_gcj02((parsed_data[14] / 1e7), (parsed_data[15] / 1e7))
    if int(parsed_data[10]) != 0:
        record = (
            parsed_data[0],  # "iTOW"
            time_uuid,  # 导入标签
            parsed_data[1],  # "Year"
            parsed_data[2],  # "Month"
            parsed_data[3],  # "Day"
            parsed_data[4],  # "Hour"
            parsed_data[5],  # "Minute"
            parsed_data[6],  # "Second"
            parsed_data[8],  # "Time Accuracy"
            parsed_data[9],  # "Nanoseconds"
            parsed_data[10],  # "Fix Status"
            parsed_data[13],  # "Number of SVs"
            parsed_data[14] / 1e7,  # "Longitude"
            parsed_data[15] / 1e7,  # "Latitude"
            parsed_data[16] / 1000,  # "WGS Altitude"
            parsed_data[17] / 1000,  # "MSL Altitude"
            parsed_data[18] / 1000,  # "Horizontal Accuracy"
            parsed_data[19] / 1000,  # "Vertical Accuracy"
            parsed_data[20] / 100 * 60,  # "Speed"
            parsed_data[21] / 100000,  # "Heading"
            parsed_data[22],  # "Speed Accuracy"
            parsed_data[23] / 1e5,  # "Heading Accuracy"
            parsed_data[24],  # "PDOP"
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
                                    if record:
                                        session_data.append(record)
                                        if first_record is None:
                                            first_record = record
                                        last_record = record
                                elif message_id == 0x01:  # 实时数据
                                    record = parse_message(buffer[:full_packet_length])
                                    if record:
                                        session_data.append(record)
                                        if first_record is None:
                                            first_record = record
                                    last_record = record
                                elif message_id == 0x02:
                                    # logger.info(
                                    #     f"下载完成，耗时 {(datetime.now() - down_start_time).total_seconds()} 秒！")
                                    download_complete.set()
                                elif message_id == 0x26:
                                    if first_record:
                                        session_num += 1
                                        duration = (datetime.now() - session_start_time).total_seconds()
                                        file_name = format_filename(first_record, last_record)
                                        session_len = len(session_data)
                                        
                                        # pg
                                        # exists_data = select_db(exists_imp, file_name)
                                        # if exists_data == 0:
                                        #     if session_len > 0:
                                        #         imp_data = (time_uuid, file_name, duration)
                                        #         insert_db(ins_imp, imp_data)
                                        #         load_db(ins_data, session_data)
                                        #         logger.info(f"已处理第{session_num}段数据文件名：{file_name}，共计{session_len}条，耗时 {duration} 秒！")
                                        #     else:
                                        #         logger.info(f"第{session_num}段数据文件名：{file_name}，共计{session_len}条，处理耗时 {duration} 秒，无数据，已跳过！")
                                        # else:
                                        #     logger.info(f"第{session_num}段数据文件名：{file_name}，共计{session_len}条，处理耗时 {duration} 秒，数据库中已存在，已跳过！")
                                        
                                        # taos    
                                        try:
                                            taos_exists_sql = f'select count(*) from eadm.imp_racebox where file_name = \'{file_name}\';'
                                            taos_exists = con_taos.query(taos_exists_sql).fetch_all()
                                            if taos_exists[0][0] == 0:
                                                if session_len > 0:
                                                    ins_taos_imp = (f'insert into eadm.imp_racebox(ts,imp_stamp,file_name,durations) '
                                                                    f'values(\'now()\',\'{time_uuid}\',\'{file_name}\',{duration});')
                                                    con_taos.execute(ins_taos_imp)
                                                    load_taos(ins_taos_sql, session_data, 1000)
                                                    logger.info(f"已处理第{session_num}段数据文件名：{file_name}，共计{session_len}条，耗时 {duration} 秒！")
                                                else:
                                                    logger.info(f"第{session_num}段数据文件名：{file_name}，共计{session_len}条，处理耗时 {duration} 秒，无数据，已跳过！")
                                            else:
                                                logger.info(f"第{session_num}段数据文件名：{file_name}，共计{session_len}条，处理耗时 {duration} 秒，数据库中已存在，已跳过！")
                                        except Exception as e:
                                            logger.error(f"TDengine写入失败: {e}")

                                        session_start_time = datetime.now()
                                        first_record = None
                                        session_data = []
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

start_time = datetime.now()
asyncio.run(last_device_connect())
logger.info(f"所有操作完成，总计耗时 {(datetime.now() - start_time).total_seconds()} 秒！")
