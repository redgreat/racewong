-- @author wangcw
-- @copyright (c) 2024, redgreat
-- created : 2024-12-5 15:30:23
-- 运维脚本

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