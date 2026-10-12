use btleplug::api::{Central, Characteristic, Manager as _, Peripheral as _, ScanFilter, WriteType};
use btleplug::platform::{Adapter, Manager, Peripheral};
use futures_util::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::time::Duration;

pub type NotificationStream = Pin<Box<dyn Stream<Item = btleplug::api::ValueNotification> + Send>>;

pub async fn first_adapter() -> Result<Adapter, String> {
    let manager = Manager::new().await.map_err(|e| format!("蓝牙管理器初始化失败: {e}"))?;
    let mut adapters = manager.adapters().await.map_err(|e| format!("蓝牙适配器枚举失败: {e}"))?;
    if adapters.is_empty() {
        return Err("未找到蓝牙适配器，请确认蓝牙已开启".to_string());
    }
    Ok(adapters.remove(0))
}

pub async fn find_device(
    adapter: &Adapter,
    name_filter: &str,
    last_address: Option<&str>,
    timeout: Duration,
    mut on_device: impl FnMut(&str, &str),
) -> Result<Peripheral, String> {
    adapter
        .start_scan(ScanFilter::default())
        .await
        .map_err(|e| format!("启动扫描失败: {e}"))?;
    let deadline = std::time::Instant::now() + timeout;
    let result = loop {
        let peripherals = adapter.peripherals().await.map_err(|e| format!("扫描失败: {e}"))?;
        let mut preferred: Option<Peripheral> = None;
        let mut named: Option<Peripheral> = None;
        for p in peripherals {
            if let Ok(Some(props)) = p.properties().await {
                let address = props.address.to_string();
                let name = props.local_name.clone().unwrap_or_default();
                if !name.is_empty() {
                    on_device(&name, &address);
                }
                if let Some(last) = last_address {
                    if address.eq_ignore_ascii_case(last) {
                        preferred = Some(p.clone());
                    }
                }
                if !name_filter.is_empty() && name.contains(name_filter) {
                    if preferred.is_none() {
                        named = Some(p.clone());
                    }
                }
            }
        }
        if preferred.is_some() {
            break preferred;
        }
        if named.is_some() {
            break named;
        }
        if std::time::Instant::now() >= deadline {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    };
    let _ = adapter.stop_scan().await;
    match result {
        Some(p) => Ok(p),
        None => Err(format!(
            "扫描超时（{} 秒），未找到 {} 设备",
            timeout.as_secs(),
            if name_filter.is_empty() { "蓝牙" } else { name_filter }
        )),
    }
}

pub async fn device_info(peripheral: &Peripheral) -> (String, String) {
    match peripheral.properties().await {
        Ok(Some(props)) => (props.local_name.unwrap_or_default(), props.address.to_string()),
        _ => (String::new(), peripheral.address().to_string()),
    }
}

pub async fn connect(peripheral: &Peripheral) -> Result<(Characteristic, Characteristic), String> {
    peripheral.connect().await.map_err(|e| format!("连接设备失败: {e}"))?;
    peripheral
        .discover_services()
        .await
        .map_err(|e| format!("服务发现失败: {e}"))?;
    let uart_service: btleplug::api::Service = peripheral
        .services()
        .into_iter()
        .find(|s| s.uuid.to_string() == crate::protocol::UART_SERVICE_UUID)
        .ok_or_else(|| "设备无 RaceBox UART 服务".to_string())?;
    let mut rx_char = None;
    let mut tx_char = None;
    for c in peripheral.characteristics() {
        if c.service_uuid != uart_service.uuid {
            continue;
        }
        match c.uuid.to_string().as_str() {
            crate::protocol::RX_CHAR_UUID => rx_char = Some(c),
            crate::protocol::TX_CHAR_UUID => tx_char = Some(c),
            _ => {}
        }
    }
    match (rx_char, tx_char) {
        (Some(rx), Some(tx)) => Ok((rx, tx)),
        _ => Err("设备缺少 RaceBox UART 收发特征".to_string()),
    }
}

pub async fn disconnect(peripheral: &Peripheral) {
    let _ = peripheral.disconnect().await;
}

pub async fn write_command(peripheral: &Peripheral, characteristic: &Characteristic, command: &[u8]) -> Result<(), String> {
    match peripheral
        .write(characteristic, command, WriteType::WithResponse)
        .await
    {
        Ok(()) => Ok(()),
        Err(e) => match peripheral
            .write(characteristic, command, WriteType::WithoutResponse)
            .await
        {
            Ok(()) => Ok(()),
            Err(_) => Err(format!("写命令失败: {e}")),
        },
    }
}

pub async fn subscribe(peripheral: &Peripheral, characteristic: &Characteristic) -> Result<(), String> {
    peripheral
        .subscribe(characteristic)
        .await
        .map_err(|e| format!("订阅通知失败: {e}"))
}

pub async fn unsubscribe(peripheral: &Peripheral, characteristic: &Characteristic) {
    let _ = peripheral.unsubscribe(characteristic).await;
}

pub async fn notification_stream(peripheral: &Peripheral) -> Result<NotificationStream, String> {
    peripheral
        .notifications()
        .await
        .map_err(|e| format!("获取通知流失败: {e}"))
}

pub async fn next_notification(stream: &mut NotificationStream, timeout: Duration) -> Option<Vec<u8>> {
    match tokio::time::timeout(timeout, stream.next()).await {
        Ok(Some(note)) => Some(note.value),
        _ => None,
    }
}
