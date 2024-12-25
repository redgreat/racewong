-- @author wangcw
-- @copyright (c) 2024, redgreat
-- created : 2024-12-5 15:30:23
-- postgres表结构设计

-- 设置查询路径
alter role wangcw set search_path to wangcw, public;

--设置 本地时区
set time zone 'asia/shanghai';

drop table if exists lc_racebox;
create table lc_racebox (
  itow int,
  imp_stamp uuid,
  year int,
  month int,
  day int,
  hour int,
  minute int,
  second int,
  -- validity_flags json,
  time_accuracy int,
  nanoseconds int,
  fix_status int,
  -- fix_status_flags json,
  -- date_time_flags json,
  numberof_svs int,
  longitude decimal(18,7),
  latitude decimal(18,7),
  wgs_altitude decimal(10,3),
  msl_altitude decimal(10,3),
  horizontal_accuracy decimal(10,3),
  vertical_accuracy decimal(10,3),
  speed decimal(10,3),
  heading decimal(10,5),
  speed_accuracy int,
  heading_accuracy int,
  pdop int,
  -- lat_lon_flags json,
  -- battery_voltage int,
  gforce_x decimal(10,3),
  gforce_y decimal(10,3),
  gforce_z decimal(10,3),
  rotation_rate_x decimal(10,2),
  rotation_rate_y decimal(10,2),
  rotation_rate_z decimal(10,2)
);

alter table lc_racebox owner to wangcw;
alter table lc_racebox drop constraint if exists pk_racebox_itow cascade;
alter table lc_racebox add constraint pk_racebox_itow primary key (itow);

drop index if exists non_racebox_imp_stamp;
create index non_racebox_imp_stamp on lc_racebox using btree (imp_stamp asc nulls last);
drop index if exists non_racebox_year;
create index non_racebox_year on lc_racebox using btree (year asc nulls last);
drop index if exists non_racebox_month;
create index non_racebox_month on lc_racebox using btree (month asc nulls last);
drop index if exists non_racebox_day;
create index non_racebox_day on lc_racebox using btree (day asc nulls last);
drop index if exists non_racebox_hour;
create index non_racebox_hour on lc_racebox using btree (hour asc nulls last);
drop index if exists non_racebox_minute;
create index non_racebox_minute on lc_racebox using btree (minute asc nulls last);
drop index if exists non_racebox_second;
create index non_racebox_second on lc_racebox using btree (second asc nulls last);

comment on column lc_racebox.itow is 'GPS星历时间戳';
comment on column lc_racebox.imp_stamp is '导入标记';
comment on column lc_racebox.year is '年(UTC)';
comment on column lc_racebox.month is '月(UTC)';
comment on column lc_racebox.day is '日(UTC)';
comment on column lc_racebox.hour is '时(UTC)';
comment on column lc_racebox.minute is '分(UTC)';
comment on column lc_racebox.second is '秒(UTC)';
-- comment on column lc_racebox.validity_flags is '数据验证标记';
comment on column lc_racebox.time_accuracy is '精确时间戳';
comment on column lc_racebox.nanoseconds is '毫秒';
comment on column lc_racebox.fix_status is 'GPS修正0:no fix 2:2D fix 3:3D fix';
-- comment on column lc_racebox.fix_status_flags is 'GPS修正标记';
-- comment on column lc_racebox.date_time_flags is '时间日期标记';
comment on column lc_racebox.numberof_svs is 'GPS卫星数量';
comment on column lc_racebox.longitude is 'GPS经度';
comment on column lc_racebox.latitude is 'GPS维度';
comment on column lc_racebox.wgs_altitude is 'WGS海拔(米)';
comment on column lc_racebox.msl_altitude is 'MSL海拔(米)';
comment on column lc_racebox.horizontal_accuracy is '水平精度';
comment on column lc_racebox.vertical_accuracy is '垂直精度';
comment on column lc_racebox.speed is '速度';
comment on column lc_racebox.heading is '方向角';
comment on column lc_racebox.speed_accuracy is '精确速度(毫米/秒)';
comment on column lc_racebox.heading_accuracy is '精确方向角';
comment on column lc_racebox.pdop is '定位点经度误差';
-- comment on column lc_racebox.lat_lon_flags is '经纬度标记';
-- comment on column lc_racebox.battery_voltage is '电池电压';
comment on column lc_racebox.gforce_x is '前后加速度/milli-g';
comment on column lc_racebox.gforce_y is '左右加速度/milli-g';
comment on column lc_racebox.gforce_z is '上下加速度/milli-g';
comment on column lc_racebox.rotation_rate_x is '前后旋转速度/centi-degrees per second';
comment on column lc_racebox.rotation_rate_y is '左右旋转速度/centi-degrees per second';
comment on column lc_racebox.rotation_rate_z is '上下旋转速度/centi-degrees per second';
comment on table lc_racebox is '业务数据_racebox信息';

--  导入记录
drop table if exists imp_racebox;
create table imp_racebox(
  id serial,
  imp_stamp uuid,
  file_name varchar(100),
  duration int,
  insert_time timestamptz default current_timestamp
);
alter table imp_racebox owner to wangcw;
alter table imp_racebox drop constraint if exists pk_imp_racebox_id cascade;
alter table imp_racebox add constraint pk_imp_racebox_id primary key (id);

drop index if exists non_racebox_imp_stamp;
create index non_racebox_imp_stamp on imp_racebox using btree (imp_stamp asc nulls last);

comment on column imp_racebox.id is '自增主键';
comment on column imp_racebox.imp_stamp is '导入标识';
comment on column imp_racebox.file_name is '导入文件名称(设备名称+时间日期)';
comment on column imp_racebox.duration is '导入花费时长';
comment on table imp_racebox is '业务数据_RaceBox设备信息导出记录';
