import asyncio
import struct
import csv
import time
from bleak import BleakScanner, BleakClient
import sys
import json
import os
from math import radians, sin, cos, sqrt, atan2

# UUIDs from t2he RaceBox protocol
UART_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
RX_CHAR_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
TX_CHAR_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"
NMEA_TX_UUID = "00001103-0000-1000-8000-00805f9b34fb"
DOWNLOAD_COMMAND = bytes([0xB5, 0x62, 0xFF, 0x23, 0x00, 0x00, 0x22, 0x65])  # Command to initiate data download

# CSV headers based on the parsed data structure
CSV_HEADERS = [
    "iTOW", "Year", "Month", "Day", "Hour", "Minute", "Second", "Validity Flags", "Time Accuracy",
    "Nanoseconds", "Fix Status", "Fix Status Flags", "Date/Time Flags", "Number of SVs", "Longitude",
    "Latitude", "WGS Altitude", "MSL Altitude", "Horizontal Accuracy", "Vertical Accuracy", "Speed",
    "Heading", "Speed Accuracy", "Heading Accuracy", "PDOP", "Lat/Lon Flags", "Battery Status or Input Voltage",
    "G-Force X", "G-Force Y", "G-Force Z", "Rotation rate X", "Rotation rate Y", "Rotation rate Z"
]

# Add these constants at the top with other constants
DEVICE_MEMORY_FILE = "../conf/last_device.json"


def format_filename_from_first_record(first_record, device_name):
    """Generate filename using the first record's date."""
    year = first_record['Year']
    month = f"{first_record['Month']:02d}"
    day = f"{first_record['Day']:02d}"
    hour = f"{first_record['Hour']:02d}"
    minute = f"{first_record['Minute']:02d}"
    second = f"{first_record['Second']:02d}"
    timestamp = f"{year}{month}{day}_{hour}{minute}{second}"
    return f"{device_name.replace(' ', '_')}_{timestamp}.csv"


# Function to save parsed data into CSV with first record's date and device name
def save_to_csv(data_list, device_name):
    if data_list:
        first_record = data_list[0]
        file_name = format_filename_from_first_record(first_record, device_name)
        with open(file_name, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
            writer.writeheader()
            writer.writerows(data_list)
        print(f"Data saved to {file_name}")


def save_last_device(device):
    """Save the last successfully connected device."""
    device_info = {
        "address": device.address,
        "name": device.name
    }
    with open(DEVICE_MEMORY_FILE, 'w') as f:
        json.dump(device_info, f)


def get_last_device():
    """Get the last successfully connected device info."""
    try:
        if os.path.exists(DEVICE_MEMORY_FILE):
            with open(DEVICE_MEMORY_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error reading device memory: {e}")
    return None


async def scan_and_connect(monitor_only=False, measure=False):
    """Scan for RaceBox devices and connect to them."""
    try:
        # Try to connect to last known device first
        last_device = get_last_device()
        if last_device:
            print(f"Attempting to connect to last known device: {last_device['name']}")
            try:
                device = await BleakScanner.find_device_by_address(last_device['address'])
                if device:
                    print(f"Found last device: {device.name} - {device.address}")
                    await connect_and_download(device)
                else:
                    print("Last known device not found, scanning for devices...")
            except Exception as e:
                print(f"Error connecting to last device: {e}")
                print("Scanning for devices...")

        # If last device connection failed or doesn't exist, scan for new devices
        devices = await BleakScanner.discover()
        racebox_devices = [device for device in devices if device.name and "RaceBox" in device.name]

        if racebox_devices:
            print(f"Found {len(racebox_devices)} RaceBox devices.")
            for device in racebox_devices:
                print(f"Connecting to {device.name} - {device.address}")
                # Save successful connection
                save_last_device(device)
                try:
                    if measure:
                        await measure_distances(device)
                    elif monitor_only:
                        await read_current_position(device)
                    else:
                        await connect_and_download(device)
                    break  # Connect to first available device only
                except Exception as e:
                    print(f"Failed to connect to {device.name}: {e}")
        else:
            print("No RaceBox devices found.")
    except asyncio.CancelledError:
        print("\nOperation cancelled by user")
        raise


def parse_message(packet):
    """Parse the 80-byte history data packet (0xFF 0x21) from an 88-byte message."""
    payload = packet[6:86]
    parsed_data = struct.unpack('<I H B B B B B B I i B B B B i i i i I I i i I I H B B h h h h h h', payload[:80])
    record = {
        "iTOW": parsed_data[0],
        "Year": parsed_data[1],
        "Month": parsed_data[2],
        "Day": parsed_data[3],
        "Hour": parsed_data[4],
        "Minute": parsed_data[5],
        "Second": parsed_data[6],
        "Validity Flags": parsed_data[7],
        "Time Accuracy": parsed_data[8],
        "Nanoseconds": parsed_data[9],
        "Fix Status": parsed_data[10],
        "Fix Status Flags": parsed_data[11],
        "Date/Time Flags": parsed_data[12],
        "Number of SVs": parsed_data[13],
        "Longitude": parsed_data[14] / 1e7,
        "Latitude": parsed_data[15] / 1e7,
        "WGS Altitude": parsed_data[16] / 1000,
        "MSL Altitude": parsed_data[17] / 1000,
        "Horizontal Accuracy": parsed_data[18] / 1000,
        "Vertical Accuracy": parsed_data[19] / 1000,
        "Speed": parsed_data[20] / 1000,
        "Heading": parsed_data[21] / 100000,
        "Speed Accuracy": parsed_data[22],
        "Heading Accuracy": parsed_data[23],
        "PDOP": parsed_data[24],
        "Lat/Lon Flags": parsed_data[25],
        "Battery Status or Input Voltage": parsed_data[26],
        "G-Force X": parsed_data[27] / 1000,
        "G-Force Y": parsed_data[28] / 1000,
        "G-Force Z": parsed_data[29] / 1000,
        "Rotation rate X": parsed_data[30] / 100,
        "Rotation rate Y": parsed_data[31] / 100,
        "Rotation rate Z": parsed_data[32] / 100
    }
    return record


def validate_checksum(buffer):
    """Validates the checksum as per the protocol."""
    ck_a, ck_b = 0, 0
    for byte in buffer[2:-2]:
        ck_a = (ck_a + byte) & 0xFF
        ck_b = (ck_b + ck_a) & 0xFF
    return ck_a == buffer[-2] and ck_b == buffer[-1]


async def connect_and_download(device):
    session_data = []  # Data for the current session
    buffer = bytearray()
    total_records = 0
    download_complete = asyncio.Event()  # Event to track download completion

    async with BleakClient(device) as client:
        await client.disconnect()
        await client.connect()  # Establish the connection

        services = client.services  # Use services property instead of deprecated get_services

        if UART_UUID not in [str(service.uuid) for service in services]:
            print(f"Device {device.name} does not have the UART service.")
            return

        def notification_handler(sender, data):
            nonlocal buffer, session_data, total_records
            buffer.extend(data)

            # Process the buffer
            while len(buffer) >= 8:  # Minimum packet size to check for message class and ID
                if buffer[:2] == bytes([0xB5, 0x62]):
                    message_class, message_id = buffer[2], buffer[3]
                    packet_length = struct.unpack('<H', buffer[4:6])[0]
                    full_packet_length = packet_length + 8

                    if len(buffer) < full_packet_length:
                        break  # Wait for more data if the full packet hasn't been received yet

                    if validate_checksum(buffer[:full_packet_length]):
                        if message_class == 0xFF:
                            if message_id == 0x23:  # Download data start
                                total_records = struct.unpack('<I', buffer[6:10])[0]
                                print(f"Expecting {total_records} records.")
                            elif message_id == 0x21:  # History data
                                record = parse_message(buffer[:full_packet_length])
                                session_data.append(record)
                            elif message_id == 0x01:  # Live data
                                record = parse_message(buffer[:full_packet_length])
                                session_data.append(record)
                            elif message_id == 0x02:  # ACK indicating download complete
                                print("Download complete.")
                                download_complete.set()  # Set the event when download is done
                            elif message_id == 0x03:  # NACK
                                print("NACK received")
                            elif message_id == 0x26:  # Session change - save current session data
                                print("Standalone recording state changed.")
                                # if session_data:
                                #     save_to_csv(session_data, device.name)
                                #     session_data = []  # Start a new session
                        buffer = buffer[full_packet_length:]

        await client.start_notify(TX_CHAR_UUID, notification_handler)
        await client.write_gatt_char(RX_CHAR_UUID, DOWNLOAD_COMMAND)
        print(f"Downloading data from {device.name}...")

        # Wait for the download to complete
        await download_complete.wait()
        await client.stop_notify(TX_CHAR_UUID)

    # Save the final session's data
    if session_data:
        save_to_csv(session_data, device.name)
    await client.disconnect()


async def main():
    await scan_and_connect()


if __name__ == "__main__":
    main()
