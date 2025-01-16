-- @author wangcw
-- @copyright (c) 2024, redgreat
-- created : 2024-12-5 15:30:23
-- 运维脚本

set time zone 'asia/shanghai';

insert into lc_racebox(itow, year, month, day, hour, minute, second, validity_flags, time_accuracy, nanoseconds,
fix_status, fix_status_flags, date_time_flags, numberof_svs, longitude, latitude, wgs_altitude, msl_altitude,
horizontal_accuracy, vertical_accuracy, speed, heading, speed_accuracy, heading_accuracy, pdop, lat_lon_flags,
battery_voltage, gforce_x, gforce_y, gforce_z, rotation_rate_x, rotation_rate_y, rotation_rate_z) values %s
on conflict (itow) do update set year=excluded.year, month=excluded.month, day=excluded.day, hour=excluded.hour,
minute=excluded.minute, second=excluded.second, validity_flags=excluded.validity_flags,
time_accuracy=excluded.time_accuracy, nanosecond=excluded.nanoseconds,
fix_status=excluded.fix_status, fix_status_flags=excluded.fix_status_flags,
date_time_flags=excluded.date_time_flags, numberof_svs=excluded.numberof_svs,
longitude=excluded.longitude, latitude=excluded.latitude, wgs_altitude=excluded.wgs_altitude,
msl_altitud=excluded.msl_altitude, horizontal_accuracy=excluded.horizontal_accuracy,
vertical_accuracy=excluded.vertical_accuracy, speed=excluded.speed, heading=excluded.heading,
speed_accuracy=excluded.speed_accuracy, heading_accuracy=excluded.heading_accuracy, pdop=excluded.pdop,
lat_lon_flag=excluded.lat_lon_flags, battery_voltage=excluded.battery_voltage, gforce_x=excluded.gforce_x,
gforce_y=excluded.gforce_y, gforce_z=excluded.gforce_z, rotation_rate_x=excluded.rotation_rate_x,
rotation_rate_y=excluded.rotation_rate_y, rotation_rate_z=excluded.rotation_rate_z;

insert into imp_racebox(imp_stamp, file_name, duration);

select * from lc_racebox;

select * from imp_racebox;

select * from lc_racebox where imp_stamp='beea6fee-b36d-11ef-bce9-d4f32d1e055e';

delete from imp_racebox where imp_stamp='39f6f9f7-b369-11ef-8802-d4f32d1e055e';
delete from lc_racebox where imp_stamp='39f6f9f7-b369-11ef-8802-d4f32d1e055e';

SELECT COUNT(1) FROM imp_racebox;

SELECT * FROM lc_racebox ORDER BY itow DESC;

SELECT * FROM imp_racebox ORDER BY insert_time desc;

SELECT COUNT(1) FROM lc_racebox WHERE imp_stamp='c1f18674-d3cb-11ef-ac2b-d4f32d1e055e';

SELECT * FROM lc_racebox WHERE imp_stamp='c1f18674-d3cb-11ef-ac2b-d4f32d1e055e';

DELETE FROM lc_racebox WHERE imp_stamp='b69af8bb-d3a0-11ef-aa1b-d4f32d1e055e';

DELETE FROM imp_racebox WHERE imp_stamp='b69af8bb-d3a0-11ef-aa1b-d4f32d1e055e';

SELECT COUNT(1) FROM lc_racebox WHERE year=2025 and month=1 and day in (15, 16);

SELECT * FROM pg_stat_activity;

SELECT pid, usename, datname, client_addr, state, query
FROM pg_stat_activity;

