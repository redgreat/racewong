pub const UART_SERVICE_UUID: &str = "6e400001-b5a3-f393-e0a9-e50e24dcca9e";
pub const RX_CHAR_UUID: &str = "6e400002-b5a3-f393-e0a9-e50e24dcca9e";
pub const TX_CHAR_UUID: &str = "6e400003-b5a3-f393-e0a9-e50e24dcca9e";

pub const CMD_STATUS: [u8; 8] = [0xB5, 0x62, 0xFF, 0x22, 0x00, 0x00, 0x21, 0x62];
pub const CMD_DOWNLOAD: [u8; 8] = [0xB5, 0x62, 0xFF, 0x23, 0x00, 0x00, 0x22, 0x65];
pub const CMD_ERASE: [u8; 8] = [0xB5, 0x62, 0xFF, 0x24, 0x00, 0x00, 0x23, 0x68];

pub const SYNC1: u8 = 0xB5;
pub const SYNC2: u8 = 0x62;
pub const CLASS_FF: u8 = 0xFF;

pub const MSG_DOWNLOAD_START: u8 = 0x23;
pub const MSG_RECORD_HISTORY: u8 = 0x21;
pub const MSG_RECORD_LIVE: u8 = 0x01;
pub const MSG_SESSION_BOUNDARY: u8 = 0x26;
pub const MSG_ACK: u8 = 0x02;
pub const MSG_NACK: u8 = 0x03;
pub const MSG_STATUS: u8 = 0x22;
pub const MSG_ERASE_PROGRESS: u8 = 0x24;

pub const RECORD_SIZE: usize = 80;
pub const RBX2_MAGIC: &[u8; 4] = b"RBX2";
pub const RBX2_HEADER_SIZE: usize = 136;
pub const RBX2_RECORD_SIZE: usize = 80;
pub const RBX2_VERSION: u8 = 2;
pub const RBX2_MAX_BATCH_RECORDS: usize = 720;
pub const DEVICE_FIELD_SIZE: usize = 48;

pub const SPEED_KMH_PER_UNIT: f64 = 0.6;

#[derive(Debug, Clone)]
pub struct Packet {
    pub class: u8,
    pub id: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct DeviceStatus {
    pub recording: bool,
    pub memory_level: u8,
    pub security_enabled: bool,
    pub stored_messages: u32,
    pub max_messages: u32,
}

pub struct FrameReader {
    buf: Vec<u8>,
}

impl FrameReader {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn next_packet(&mut self) -> Option<Packet> {
        loop {
            if self.buf.len() < 8 {
                return None;
            }
            if !(self.buf[0] == SYNC1 && self.buf[1] == SYNC2) {
                self.buf.remove(0);
                continue;
            }
            let payload_len = u16::from_le_bytes([self.buf[4], self.buf[5]]) as usize;
            let full_len = payload_len + 8;
            if self.buf.len() < full_len {
                return None;
            }
            if !checksum_ok(&self.buf[..full_len]) {
                self.buf.remove(0);
                continue;
            }
            let packet = Packet {
                class: self.buf[2],
                id: self.buf[3],
                payload: self.buf[6..full_len - 2].to_vec(),
            };
            self.buf.drain(..full_len);
            if packet.class == CLASS_FF {
                return Some(packet);
            }
        }
    }
}

pub fn checksum_ok(buf: &[u8]) -> bool {
    if buf.len() < 8 || buf[0] != SYNC1 || buf[1] != SYNC2 {
        return false;
    }
    let (mut ck_a, mut ck_b) = (0u8, 0u8);
    for &b in &buf[2..buf.len() - 2] {
        ck_a = ck_a.wrapping_add(b);
        ck_b = ck_b.wrapping_add(ck_a);
    }
    ck_a == buf[buf.len() - 2] && ck_b == buf[buf.len() - 1]
}

fn u16le(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}

fn u32le(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

fn i32le(b: &[u8]) -> i32 {
    i32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

fn i16le(b: &[u8]) -> i16 {
    i16::from_le_bytes([b[0], b[1]])
}

#[derive(Debug, Clone)]
pub struct Record {
    pub raw: [u8; RECORD_SIZE],
    pub itow: u32,
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub time_accuracy: u32,
    pub nanoseconds: i32,
    pub fix_status: u8,
    pub satellites: u8,
    pub longitude: f64,
    pub latitude: f64,
    pub wgs_altitude: f64,
    pub msl_altitude: f64,
    pub horizontal_accuracy: f64,
    pub vertical_accuracy: f64,
    pub speed_kmh: f64,
    pub heading: f64,
    pub speed_accuracy: u32,
    pub heading_accuracy: f64,
    pub pdop: u16,
    pub gforce_x: f64,
    pub gforce_y: f64,
    pub gforce_z: f64,
    pub rotation_rate_x: f64,
    pub rotation_rate_y: f64,
    pub rotation_rate_z: f64,
}

impl Record {
    pub fn decode(payload: &[u8]) -> Option<Record> {
        if payload.len() < RECORD_SIZE {
            return None;
        }
        let p = &payload[..RECORD_SIZE];
        let mut raw = [0u8; RECORD_SIZE];
        raw.copy_from_slice(p);
        let fix_status = p[20];
        if fix_status == 0 {
            return None;
        }
        Some(Record {
            raw,
            itow: u32le(&p[0..4]),
            year: u16le(&p[4..6]),
            month: p[6],
            day: p[7],
            hour: p[8],
            minute: p[9],
            second: p[10],
            time_accuracy: u32le(&p[12..16]),
            nanoseconds: i32le(&p[16..20]),
            fix_status,
            satellites: p[23],
            longitude: i32le(&p[24..28]) as f64 / 1e7,
            latitude: i32le(&p[28..32]) as f64 / 1e7,
            wgs_altitude: i32le(&p[32..36]) as f64 / 1000.0,
            msl_altitude: i32le(&p[36..40]) as f64 / 1000.0,
            horizontal_accuracy: u32le(&p[40..44]) as f64 / 1000.0,
            vertical_accuracy: u32le(&p[44..48]) as f64 / 1000.0,
            speed_kmh: i32le(&p[48..52]) as f64 * SPEED_KMH_PER_UNIT,
            heading: i32le(&p[52..56]) as f64 / 1e5,
            speed_accuracy: u32le(&p[56..60]),
            heading_accuracy: u32le(&p[60..64]) as f64 / 1e5,
            pdop: u16le(&p[64..66]),
            gforce_x: i16le(&p[68..70]) as f64 / 1000.0,
            gforce_y: i16le(&p[70..72]) as f64 / 1000.0,
            gforce_z: i16le(&p[72..74]) as f64 / 1000.0,
            rotation_rate_x: i16le(&p[74..76]) as f64 / 100.0,
            rotation_rate_y: i16le(&p[76..78]) as f64 / 100.0,
            rotation_rate_z: i16le(&p[78..80]) as f64 / 100.0,
        })
    }

    pub fn utc_numeric(&self) -> u64 {
        (self.year as u64) * 10_000_000_000
            + (self.month as u64) * 100_000_000
            + (self.day as u64) * 1_000_000
            + (self.hour as u64) * 10_000
            + (self.minute as u64) * 100
            + self.second as u64
    }

    pub fn local_date_numeric(&self) -> u32 {
        (self.year as u32) * 10_000 + (self.month as u32) * 100 + self.day as u32
    }
}

pub fn session_file_name(first: &Record, last: &Record) -> String {
    format!(
        "{:04}{:02}{:02}{:02}{:02}{:02}_{:04}{:02}{:02}{:02}{:02}{:02}",
        first.year, first.month, first.day, first.hour, first.minute, first.second, last.year, last.month, last.day,
        last.hour, last.minute, last.second
    )
}

#[derive(Debug, Clone)]
pub struct Rbx2Batch {
    pub records_flat: Vec<u8>,
    pub count: usize,
    pub sync_offset: usize,
    pub sync_total: u32,
    pub sync_final: bool,
    pub session_index: u32,
    pub session_offset: usize,
    pub session_total: usize,
    pub session_start_utc: u64,
    pub session_end_utc: u64,
    pub session_start_itow: u32,
    pub session_start_nano: i32,
    pub sync_id: [u8; 16],
    pub device: String,
    pub sync_date: u32,
}

fn put_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

pub fn crc32_ieee(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

pub fn build_rbx2_frame(batch: &Rbx2Batch) -> Vec<u8> {
    let mut frame = vec![0u8; RBX2_HEADER_SIZE + batch.records_flat.len()];
    frame[0..4].copy_from_slice(RBX2_MAGIC);
    frame[4] = RBX2_VERSION;
    let session_final = batch.session_total > 0 && batch.session_offset + batch.count == batch.session_total;
    let sync_final = batch.sync_final && session_final;
    frame[5] = (if sync_final { 1u8 } else { 0u8 }) | (if session_final { 2u8 } else { 0u8 });
    put_u16(&mut frame, 6, RBX2_HEADER_SIZE as u16);
    put_u16(&mut frame, 8, RBX2_RECORD_SIZE as u16);
    put_u16(&mut frame, 10, batch.count as u16);
    put_u32(&mut frame, 12, batch.sync_offset as u32);
    put_u32(&mut frame, 16, batch.sync_total);
    put_u32(&mut frame, 20, batch.sync_date);
    frame[24..40].copy_from_slice(&batch.sync_id);
    let dev = batch.device.as_bytes();
    let n = dev.len().min(DEVICE_FIELD_SIZE - 1);
    frame[40..40 + n].copy_from_slice(&dev[..n]);
    put_u32(&mut frame, 88, batch.session_index);
    put_u32(&mut frame, 92, batch.session_offset as u32);
    put_u32(&mut frame, 96, batch.session_total as u32);
    put_u64(&mut frame, 100, batch.session_start_utc);
    put_u64(&mut frame, 108, batch.session_end_utc);
    put_u32(&mut frame, 116, batch.session_start_itow);
    put_u32(&mut frame, 120, batch.session_start_nano as u32);
    frame[RBX2_HEADER_SIZE..].copy_from_slice(&batch.records_flat);
    let payload_crc = crc32_ieee(&batch.records_flat);
    put_u32(&mut frame, 128, payload_crc);
    let header_crc = crc32_ieee(&frame[0..132]);
    put_u32(&mut frame, 132, header_crc);
    frame
}

pub fn parse_status(payload: &[u8]) -> Option<DeviceStatus> {
    if payload.len() < 12 {
        return None;
    }
    Some(DeviceStatus {
        recording: payload[0] != 0,
        memory_level: payload[1],
        security_enabled: (payload[2] & 0x01) != 0,
        stored_messages: u32le(&payload[4..8]),
        max_messages: u32le(&payload[8..12]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(class: u8, id: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![0xB5, 0x62, class, id, payload.len() as u8, 0x00];
        v.extend_from_slice(payload);
        let (mut a, mut b) = (0u8, 0u8);
        for &byte in &v[2..] {
            a = a.wrapping_add(byte);
            b = b.wrapping_add(a);
        }
        v.push(a);
        v.push(b);
        v
    }

    #[test]
    fn checksum_detects_corruption() {
        let ok = frame(0xFF, 0x22, &[1, 2, 3]);
        assert!(checksum_ok(&ok));
        let mut bad = ok.clone();
        bad[6] ^= 0xFF;
        assert!(!checksum_ok(&bad));
    }

    #[test]
    fn frame_reader_reassembles_fragments() {
        let mut reader = FrameReader::new();
        let a = frame(0xFF, 0x21, &[0xAA, 0xBB]);
        for chunk in a.chunks(3) {
            reader.push(chunk);
        }
        reader.push(&[0x00, 0x01, 0x02]);
        let packet = reader.next_packet().unwrap();
        assert_eq!(packet.class, 0xFF);
        assert_eq!(packet.id, 0x21);
        assert_eq!(packet.payload, vec![0xAA, 0xBB]);
    }

    #[test]
    fn download_start_frame_reports_count() {
        let mut reader = FrameReader::new();
        let payload: Vec<u8> = 0x1234u32.to_le_bytes().into_iter().collect();
        let f = frame(0xFF, 0x23, &payload);
        reader.push(&f);
        let packet = reader.next_packet().unwrap();
        let total = u32le(&packet.payload);
        assert_eq!(total, 0x1234);
    }

    fn sample_record() -> Vec<u8> {
        let mut r = vec![0u8; 80];
        r[0..4].copy_from_slice(&123456u32.to_le_bytes());
        r[4..6].copy_from_slice(&2026u16.to_le_bytes());
        r[6] = 10;
        r[7] = 11;
        r[8] = 22;
        r[9] = 8;
        r[10] = 30;
        r[12..16].copy_from_slice(&42u32.to_le_bytes());
        r[16..20].copy_from_slice(&(-7i32).to_le_bytes());
        r[20] = 3;
        r[23] = 12;
        r[24..28].copy_from_slice(&(113_543_210i32).to_le_bytes());
        r[28..32].copy_from_slice(&(22_187_654i32).to_le_bytes());
        r[32..36].copy_from_slice(&(12_345i32).to_le_bytes());
        r[36..40].copy_from_slice(&(12_000i32).to_le_bytes());
        r[40..44].copy_from_slice(&1500u32.to_le_bytes());
        r[44..48].copy_from_slice(&2000u32.to_le_bytes());
        r[48..52].copy_from_slice(&2778i32.to_le_bytes());
        r[52..56].copy_from_slice(&(45_000i32).to_le_bytes());
        r[56..60].copy_from_slice(&5u32.to_le_bytes());
        r[60..64].copy_from_slice(&(2_000u32).to_le_bytes());
        r[64..66].copy_from_slice(&250u16.to_le_bytes());
        r[68..70].copy_from_slice(&(980i16).to_le_bytes());
        r[70..72].copy_from_slice(&(-120i16).to_le_bytes());
        r[72..74].copy_from_slice(&(150i16).to_le_bytes());
        r[74..76].copy_from_slice(&(500i16).to_le_bytes());
        r[76..78].copy_from_slice(&(-800i16).to_le_bytes());
        r[78..80].copy_from_slice(&(200i16).to_le_bytes());
        r
    }

    #[test]
    fn record_decodes_all_fields() {
        let rec = Record::decode(&sample_record()).expect("should decode");
        assert_eq!(rec.year, 2026);
        assert_eq!(rec.month, 10);
        assert_eq!(rec.day, 11);
        assert_eq!(rec.hour, 22);
        assert_eq!(rec.minute, 8);
        assert_eq!(rec.second, 30);
        assert_eq!(rec.itow, 123456);
        assert_eq!(rec.nanoseconds, -7);
        assert_eq!(rec.fix_status, 3);
        assert_eq!(rec.satellites, 12);
        assert_eq!(rec.longitude, 11.354321);
        assert_eq!(rec.latitude, 2.2187654);
        assert_eq!(rec.wgs_altitude, 12.345);
        assert_eq!(rec.msl_altitude, 12.0);
        assert_eq!(rec.horizontal_accuracy, 1.5);
        assert_eq!(rec.vertical_accuracy, 2.0);
        assert_eq!(rec.speed_kmh, 2778.0 * SPEED_KMH_PER_UNIT);
        assert_eq!(rec.heading, 0.45);
        assert_eq!(rec.speed_accuracy, 5);
        assert_eq!(rec.heading_accuracy, 0.02);
        assert_eq!(rec.pdop, 250);
        assert_eq!(rec.gforce_x, 0.98);
        assert_eq!(rec.gforce_y, -0.12);
        assert_eq!(rec.gforce_z, 0.15);
        assert_eq!(rec.rotation_rate_x, 5.0);
        assert_eq!(rec.rotation_rate_y, -8.0);
        assert_eq!(rec.rotation_rate_z, 2.0);
        assert_eq!(rec.utc_numeric(), 2026_10_11_22_08_30);
    }

    #[test]
    fn record_without_fix_is_dropped() {
        let mut raw = sample_record();
        raw[20] = 0;
        assert!(Record::decode(&raw).is_none());
    }

    #[test]
    fn session_file_name_matches_python() {
        let first = Record::decode(&sample_record()).unwrap();
        let mut raw2 = sample_record();
        raw2[8] = 22;
        raw2[9] = 45;
        raw2[10] = 12;
        let last = Record::decode(&raw2).unwrap();
        assert_eq!(session_file_name(&first, &last), "20261011220830_20261011224512");
    }

    #[test]
    fn rbx2_frame_matches_des_wong_layout() {
        let rec = Record::decode(&sample_record()).unwrap();
        let mut records_flat = Vec::new();
        records_flat.extend_from_slice(&rec.raw);
        records_flat.extend_from_slice(&rec.raw);
        let sync_id = [7u8; 16];
        let batch = Rbx2Batch {
            records_flat: records_flat.clone(),
            count: 2,
            sync_offset: 5,
            sync_total: 100,
            sync_final: true,
            session_index: 3,
            session_offset: 0,
            session_total: 2,
            session_start_utc: 2026_10_11_22_08_30,
            session_end_utc: 2026_10_11_22_45_12,
            session_start_itow: 123456,
            session_start_nano: -7,
            sync_id,
            device: "RaceBox-ABCDEF".to_string(),
            sync_date: 20261011,
        };
        let frame = build_rbx2_frame(&batch);
        assert_eq!(frame.len(), RBX2_HEADER_SIZE + 160);
        assert_eq!(&frame[0..4], b"RBX2");
        assert_eq!(frame[4], 2);
        assert_eq!(frame[5], 0b11);
        assert_eq!(u16::from_le_bytes([frame[6], frame[7]]), 136);
        assert_eq!(u16::from_le_bytes([frame[8], frame[9]]), 80);
        assert_eq!(u16::from_le_bytes([frame[10], frame[11]]), 2);
        assert_eq!(u32::from_le_bytes([frame[12], frame[13], frame[14], frame[15]]), 5);
        assert_eq!(u32::from_le_bytes([frame[16], frame[17], frame[18], frame[19]]), 100);
        assert_eq!(frame[20..24], [0x93, 0x28, 0x35, 0x01]);
        assert_eq!(&frame[24..40], &sync_id);
        assert_eq!(&frame[40..54], b"RaceBox-ABCDEF");
        assert_eq!(frame[53], b'F');
        assert_eq!(frame[54], 0);
        assert_eq!(frame[87], 0);
        assert_eq!(&frame[88..92], [3, 0, 0, 0]);
        assert_eq!(&frame[92..96], [0, 0, 0, 0]);
        assert_eq!(&frame[96..100], [2, 0, 0, 0]);
        assert_eq!(&frame[136..], records_flat.as_slice());
        let payload_crc = crc32_ieee(&records_flat);
        assert_eq!(u32::from_le_bytes([frame[128], frame[129], frame[130], frame[131]]), payload_crc);
        let header_crc = crc32_ieee(&frame[0..132]);
        assert_eq!(u32::from_le_bytes([frame[132], frame[133], frame[134], frame[135]]), header_crc);
    }

    #[test]
    fn status_parses_device_report() {
        let payload = [1u8, 42, 0x01, 0, 0x10, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let status = parse_status(&payload).unwrap();
        assert!(status.recording);
        assert_eq!(status.memory_level, 42);
        assert!(status.security_enabled);
        assert_eq!(status.stored_messages, 16);
        assert_eq!(status.max_messages, 256);
    }
}
