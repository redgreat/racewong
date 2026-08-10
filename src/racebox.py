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
import openpyxl
import glob

import folium
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium.raster_layers import TileLayer
import math
import sys

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 数据库连接定义
config = configparser.ConfigParser()
config.read(os.path.join(PROJECT_ROOT, "conf", "db.cnf"))

pg_host = config.get("racebox", "host")
pg_database = config.get("racebox", "database")
pg_user = config.get("racebox", "user")
pg_password = config.get("racebox", "password")
pg_port = int(config.get("racebox", "port"))

amap_key = config.get("amap", "amap_key")
auto_erase = config.getboolean("racebox", "auto_erase", fallback=False)

# Excel备份目录
EXCEL_BACKUP_DIR = os.path.join(PROJECT_ROOT, "data", "excel_backup")

# 日志配置
logDir = os.path.join(PROJECT_ROOT, "log")
if not os.path.exists(logDir):
    os.mkdir(logDir)
logFile = os.path.join(logDir, "racebox.log")

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
DOWNLOAD_COMMAND = bytes([0xB5, 0x62, 0xFF, 0x23, 0x00, 0x00, 0x22, 0x65])
STATUS_QUERY_COMMAND = bytes([0xB5, 0x62, 0xFF, 0x22, 0x00, 0x00, 0x21, 0x62])
ERASE_MEMORY_COMMAND = bytes([0xB5, 0x62, 0xFF, 0x24, 0x00, 0x00, 0x23, 0x68])

exists_imp = """select count(1) from imp_racebox where file_name = %s"""

ins_imp = """insert into imp_racebox(imp_stamp, file_name, duration) values (%s, %s, %s);"""

ins_data = """insert into lc_racebox(itow, imp_stamp, year, month, day, hour, minute, second, time_accuracy, nanoseconds,  
            fix_status, numberof_svs, longitude, latitude, wgs_altitude, msl_altitude, horizontal_accuracy, 
            vertical_accuracy, speed, heading, speed_accuracy, heading_accuracy, pdop, gforce_x, gforce_y, gforce_z, 
            rotation_rate_x, rotation_rate_y, rotation_rate_z) values %s;
            """

# 上次连接设备文件
DEVICE_MEMORY_FILE = os.path.join(PROJECT_ROOT, "conf", "last_device.json")

# 本次插入数据uuid
time_uuid = uuid.uuid1()

# PostgreSQL数据库连接
con = None
pg_available = False


def init_pg_connection():
    """初始化PostgreSQL数据库连接"""
    global con, pg_available
    try:
        con = psycopg2.connect(
            database=pg_database,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
        psycopg2.extras.register_uuid()
        pg_available = True
        logger.info("PostgreSQL数据库连接成功")
    except Exception as e:
        pg_available = False
        logger.warning(f"PostgreSQL数据库连接失败，将使用Excel备份: {e}")


def insert_db(in_sql, in_data):
    cur = None
    try:
        cur = con.cursor()
        cur.execute(in_sql, in_data)
        con.commit()
    except Exception as e:
        logger.error(e)
    finally:
        if cur:
            cur.close()


def load_db(in_sql, in_data):
    cur = None
    try:
        cur = con.cursor()
        extras.execute_values(cur, in_sql, in_data, page_size=1000)
        con.commit()
    except Exception as e:
        logger.error(e)
    finally:
        if cur:
            cur.close()


def select_db(sel_sql, in_filter):
    cur = None
    try:
        cur = con.cursor()
        cur.execute(sel_sql, (in_filter,))
        res = cur.fetchone()[0]
        return res
    except Exception as e:
        logger.error(e)
        return 0
    finally:
        if cur:
            cur.close()


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
            save_last_device(device)
            try:
                await connect_and_download(device)
                break
            except Exception as e:
                logger.info(f"连接设备失败： {device.name}: {e}")


async def last_device_connect():
    try:
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
    if int(parsed_data[10]) != 0:
        record = (
            parsed_data[0],
            time_uuid,
            parsed_data[1],
            parsed_data[2],
            parsed_data[3],
            parsed_data[4],
            parsed_data[5],
            parsed_data[6],
            parsed_data[8],
            parsed_data[9],
            parsed_data[10],
            parsed_data[13],
            parsed_data[14] / 1e7,
            parsed_data[15] / 1e7,
            parsed_data[16] / 1000,
            parsed_data[17] / 1000,
            parsed_data[18] / 1000,
            parsed_data[19] / 1000,
            parsed_data[20] / 100 * 60,
            parsed_data[21] / 100000,
            parsed_data[22],
            parsed_data[23] / 1e5,
            parsed_data[24],
            parsed_data[27] / 1000,
            parsed_data[28] / 1000,
            parsed_data[29] / 1000,
            parsed_data[30] / 100,
            parsed_data[31] / 100,
            parsed_data[32] / 100
        )
        return record


def validate_checksum(buffer):
    """根据结构协议校验数据"""
    ck_a, ck_b = 0, 0
    for byte in buffer[2:-2]:
        ck_a = (ck_a + byte) & 0xFF
        ck_b = (ck_b + ck_a) & 0xFF
    return ck_a == buffer[-2] and ck_b == buffer[-1]


def save_session_to_excel(session_data, file_name):
    """将session数据保存到Excel文件"""
    if not os.path.exists(EXCEL_BACKUP_DIR):
        os.makedirs(EXCEL_BACKUP_DIR)
    
    excel_path = os.path.join(EXCEL_BACKUP_DIR, f"{file_name}.xlsx")
    
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "RaceBox Data"
    
    headers = [
        "itow", "imp_stamp", "year", "month", "day", "hour", "minute", "second",
        "time_accuracy", "nanoseconds", "fix_status", "numberof_svs", "longitude",
        "latitude", "wgs_altitude", "msl_altitude", "horizontal_accuracy",
        "vertical_accuracy", "speed", "heading", "speed_accuracy", "heading_accuracy",
        "pdop", "gforce_x", "gforce_y", "gforce_z", "rotation_rate_x",
        "rotation_rate_y", "rotation_rate_z"
    ]
    ws.append(headers)
    
    for record in session_data:
        row = list(record)
        row[1] = str(row[1])  # UUID转字符串
        ws.append(row)
    
    wb.save(excel_path)
    logger.info(f"数据已保存到Excel: {excel_path}")


def load_excel_to_pg(excel_path):
    """将Excel数据导入到PostgreSQL，成功后删除Excel文件"""
    try:
        wb = openpyxl.load_workbook(excel_path)
        ws = wb.active
        
        records = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            records.append(tuple(row))
        
        if not records:
            logger.warning(f"Excel文件 {excel_path} 无数据")
            os.remove(excel_path)
            return False
        
        file_name = os.path.splitext(os.path.basename(excel_path))[0]
        
        exists_count = select_db(exists_imp, file_name)
        if exists_count > 0:
            logger.info(f"文件 {file_name} 数据库中已存在，删除Excel文件")
            os.remove(excel_path)
            return True
        
        imp_data = (time_uuid, file_name, 0)
        insert_db(ins_imp, imp_data)
        load_db(ins_data, records)
        
        logger.info(f"从Excel导入数据成功: {file_name}, 共 {len(records)} 条记录")
        os.remove(excel_path)
        logger.info(f"已删除Excel备份: {excel_path}")
        return True
    except Exception as e:
        logger.error(f"从Excel导入数据失败: {excel_path}, 错误: {e}")
        return False


def sync_excel_backups():
    """同步所有待处理的Excel备份文件到数据库"""
    if not pg_available:
        logger.info("数据库不可用，跳过Excel同步")
        return
    
    if not os.path.exists(EXCEL_BACKUP_DIR):
        return
    
    excel_files = glob.glob(os.path.join(EXCEL_BACKUP_DIR, "*.xlsx"))
    if not excel_files:
        return
    
    logger.info(f"发现 {len(excel_files)} 个待同步的Excel备份文件")
    for excel_file in sorted(excel_files):
        load_excel_to_pg(excel_file)


def process_session_data(session_data, file_name, duration, session_num, session_len):
    """处理session数据：优先写入PG，失败则保存Excel"""
    if not session_data or session_len == 0:
        logger.info(f"第{session_num}段数据文件名：{file_name}，共计{session_len}条，无数据，已跳过！")
        return
    
    if pg_available:
        try:
            exists_data = select_db(exists_imp, file_name)
            if exists_data == 0:
                imp_data = (time_uuid, file_name, duration)
                insert_db(ins_imp, imp_data)
                load_db(ins_data, session_data)
                logger.info(f"已处理第{session_num}段数据文件名：{file_name}，共计{session_len}条，耗时 {duration} 秒，已写入PG！")
            else:
                logger.info(f"第{session_num}段数据文件名：{file_name}，共计{session_len}条，数据库中已存在，已跳过！")
            return
        except Exception as e:
            logger.warning(f"PostgreSQL写入失败，回退到Excel保存: {e}")
    
    save_session_to_excel(session_data, file_name)
    logger.info(f"第{session_num}段数据文件名：{file_name}，共计{session_len}条，已保存到Excel！")


async def query_recording_status(device):
    """查询设备录制状态"""
    status_received = asyncio.Event()
    status_result = {}

    async with BleakClient(device.address, timeout=20) as client:
        try:
            await client.connect()

            def status_handler(sender, data):
                nonlocal status_result
                buffer = bytearray(data)
                while len(buffer) >= 8:
                    if buffer[:2] == bytes([0xB5, 0x62]):
                        msg_cls, msg_id = buffer[2], buffer[3]
                        pkt_len = struct.unpack('<H', buffer[4:6])[0]
                        full_len = pkt_len + 8
                        if len(buffer) < full_len:
                            break
                        if validate_checksum(buffer[:full_len]):
                            if msg_cls == 0xFF and msg_id == 0x22:
                                payload = buffer[6:6 + pkt_len]
                                if len(payload) >= 12:
                                    status_result = {
                                        "recording": payload[0] != 0,
                                        "memory_level": payload[1],
                                        "security_enabled": bool(payload[2] & 0x01),
                                        "security_unlocked": bool(payload[2] & 0x02),
                                        "stored_messages": struct.unpack('<I', payload[4:8])[0],
                                        "max_messages": struct.unpack('<I', payload[8:12])[0],
                                    }
                                status_received.set()
                            elif msg_cls == 0xFF and msg_id == 0x03:
                                logger.error("设备不支持录制状态查询（可能是RaceBox Mini非S/Micro）")
                                status_received.set()
                        buffer = buffer[full_len:]

            await client.start_notify(TX_CHAR_UUID, status_handler)
            await client.write_gatt_char(RX_CHAR_UUID, STATUS_QUERY_COMMAND)

            try:
                await asyncio.wait_for(status_received.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error("查询状态超时")
                return None

            await client.stop_notify(TX_CHAR_UUID)
        except Exception as e:
            logger.error(f"查询状态失败: {e}")
            return None
        finally:
            await client.disconnect()

    if status_result:
        mem_pct = status_result["memory_level"]
        stored = status_result["stored_messages"]
        max_msg = status_result["max_messages"]
        logger.info(f"设备状态: 录制={'是' if status_result['recording'] else '否'}, "
                     f"内存 {mem_pct}% ({stored}/{max_msg}条), "
                     f"安全锁={'已启用' if status_result['security_enabled'] else '未启用'}")
    return status_result


async def erase_device_memory(device):
    """清除设备内存，显示进度"""
    erase_complete = asyncio.Event()
    erase_success = False

    async with BleakClient(device.address, timeout=20) as client:
        try:
            await client.connect()

            def erase_handler(sender, data):
                nonlocal erase_success
                buffer = bytearray(data)
                while len(buffer) >= 8:
                    if buffer[:2] == bytes([0xB5, 0x62]):
                        msg_cls, msg_id = buffer[2], buffer[3]
                        pkt_len = struct.unpack('<H', buffer[4:6])[0]
                        full_len = pkt_len + 8
                        if len(buffer) < full_len:
                            break
                        if validate_checksum(buffer[:full_len]):
                            if msg_cls == 0xFF and msg_id == 0x24:
                                # 进度更新
                                if pkt_len >= 1:
                                    progress = buffer[6]
                                    logger.info(f"  清除进度: {progress}%")
                            elif msg_cls == 0xFF and msg_id == 0x02:
                                # ACK - 操作完成
                                ack_payload = buffer[6:6 + pkt_len]
                                if len(ack_payload) >= 2 and ack_payload[0] == 0xFF and ack_payload[1] == 0x24:
                                    erase_success = True
                                    erase_complete.set()
                            elif msg_cls == 0xFF and msg_id == 0x03:
                                # NACK - 操作失败
                                logger.error("清除失败，可能内存已锁定或设备不支持")
                                erase_complete.set()
                        buffer = buffer[full_len:]

            await client.start_notify(TX_CHAR_UUID, erase_handler)
            await client.write_gatt_char(RX_CHAR_UUID, ERASE_MEMORY_COMMAND)
            logger.info("开始清除设备内存...")

            try:
                await asyncio.wait_for(erase_complete.wait(), timeout=120.0)
            except asyncio.TimeoutError:
                logger.error("清除超时")
                return False

            await client.stop_notify(TX_CHAR_UUID)
        except Exception as e:
            logger.error(f"清除内存失败: {e}")
            return False
        finally:
            await client.disconnect()

    if erase_success:
        logger.info("设备内存清除完成！")
    return erase_success


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

                while len(buffer) >= 8:
                    if buffer[:2] == bytes([0xB5, 0x62]):
                        message_class, message_id = buffer[2], buffer[3]
                        packet_length = struct.unpack('<H', buffer[4:6])[0]
                        full_packet_length = packet_length + 8

                        if len(buffer) < full_packet_length:
                            break

                        if validate_checksum(buffer[:full_packet_length]):
                            if message_class == 0xFF:
                                if message_id == 0x23:
                                    total_records = struct.unpack('<I', buffer[6:10])[0]
                                    logger.info(f"总计 {total_records} 条记录")
                                elif message_id == 0x21:
                                    record = parse_message(buffer[:full_packet_length])
                                    if record:
                                        session_data.append(record)
                                        if first_record is None:
                                            first_record = record
                                        last_record = record
                                elif message_id == 0x01:
                                    record = parse_message(buffer[:full_packet_length])
                                    if record:
                                        session_data.append(record)
                                        if first_record is None:
                                            first_record = record
                                    last_record = record
                                elif message_id == 0x02:
                                    download_complete.set()
                                elif message_id == 0x26:
                                    if first_record:
                                        session_num += 1
                                        duration = (datetime.now() - session_start_time).total_seconds()
                                        file_name = format_filename(first_record, last_record)
                                        session_len = len(session_data)
                                        
                                        process_session_data(session_data, file_name, duration, session_num, session_len)
                                        
                                        session_start_time = datetime.now()
                                        first_record = None
                                        session_data = []
                            buffer = buffer[full_packet_length:]

            await client.start_notify(TX_CHAR_UUID, notification_handler)
            await client.write_gatt_char(RX_CHAR_UUID, DOWNLOAD_COMMAND)
            logger.info(f"正在从设备 {device.name} 下载数据...")

            await download_complete.wait()
            await client.stop_notify(TX_CHAR_UUID)
        except Exception as e:
            logger.error(e)
        finally:
            await client.disconnect()


async def find_and_connect(operation_fn, operation_name):
    """查找设备并执行指定操作，返回 True 表示操作成功"""
    last_device = get_last_device()
    if last_device:
        logger.info(f"开始连接上次连接设备: {last_device['name']}")
        try:
            device = await BleakScanner.find_device_by_address(last_device['address'])
            if device:
                logger.info(f"找到上次连接设备: {device.name}")
                await operation_fn(device)
                return True
            else:
                logger.info("未找到上次连接设备, 扫描新设备...")
        except Exception as e:
            logger.info(f"上次连接设备连接失败: {e}")
            logger.info("扫描新设备...")
    else:
        logger.info("无历史设备记录")

    # 扫描新设备
    devices = await BleakScanner.discover()
    racebox_devices = [d for d in devices if d.name and "RaceBox" in d.name]

    if racebox_devices:
        logger.info(f"扫描到 {len(racebox_devices)} 个RaceBox设备")
        for device in racebox_devices:
            logger.info(f"连接设备 {device.name} 中...")
            save_last_device(device)
            try:
                await operation_fn(device)
                return True
            except Exception as e:
                logger.info(f"连接设备失败: {device.name}: {e}")
    else:
        logger.error("未找到任何 RaceBox 设备")

    logger.warning(f"{operation_name} 失败：未找到可用设备")
    return False


async def main_operation():
    """主操作入口，根据命令行参数选择操作模式"""
    mode = sys.argv[1] if len(sys.argv) > 1 else "download"

    if mode == "status":
        await find_and_connect(query_recording_status, "状态查询")
    elif mode == "erase":
        await find_and_connect(erase_device_memory, "清除内存")
    elif mode == "full":
        ok = await find_and_connect(connect_and_download, "下载数据")
        if ok:
            logger.info("下载完成，开始清除设备内存...")
            await find_and_connect(erase_device_memory, "清除内存")
        else:
            logger.warning("下载未成功，跳过清除内存")
    elif mode == "download":
        ok = await find_and_connect(connect_and_download, "下载数据")
        if ok and auto_erase:
            logger.info("已配置 auto_erase，开始清除设备内存...")
            await find_and_connect(erase_device_memory, "清除内存")
        elif not ok:
            logger.warning("下载未成功，跳过后续操作")
    else:
        logger.error(f"未知操作模式: {mode}")
        print("用法: python racebox.py [download|status|erase|full]")
        print("  download  - 下载数据（默认）")
        print("  status    - 查询设备状态")
        print("  erase     - 清除设备内存")
        print("  full      - 下载数据后自动清除内存")


# 初始化数据库连接
init_pg_connection()

# 同步本地Excel备份到数据库
sync_excel_backups()

start_time = datetime.now()
try:
    asyncio.run(main_operation())
except asyncio.CancelledError:
    logger.info("\n用户取消操作！")

if con:
    con.close()

logger.info(f"所有操作完成，总计耗时 {(datetime.now() - start_time).total_seconds()} 秒！")
