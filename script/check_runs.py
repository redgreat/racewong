"""把 cloud_one.bin 按固件状态机分跑，和「服务端已接受」的 test_withruns.bin 逐字段对照。

只用来验证 app/src/racebox/session.rs 的 detect_runs / track_setpoint /
encode_run_suffix。注意增量记录是相对**上一条键记录**的（官方
`DeltaRecordFromStream(reader, lastKeyRecord)` 就是这么写的），
按上一条记录累加会算错速度。
"""
import math
import struct
import sys

STATIONARY = 277.778
END_RUN_RECORDS = 10
SETPOINTS = [(0, 60), (0, 100), (0, 150), (0, 200)]
SUFFIX_FIXED = 20
SUFFIX_PER = 12


def load_points(path):
    blob = open(path, "rb").read()
    hl = struct.unpack_from("<H", blob, 4)[0]
    data_len = struct.unpack_from("<I", blob, 12)[0]
    body = blob[hl:hl + data_len]
    pts = []
    p = 0
    key = None
    while p < len(body):
        pre = body[p]
        if pre == 243:
            p += 3
            continue
        if pre == 0:
            itow, lat, lng, alt, spd = struct.unpack_from("<IiiiI", body, p + 1)
            key = (itow, lat, lng, alt, spd)
            p += 35
            pts.append(key)
        else:
            dt, dlat, dlng, dalt, dspd = struct.unpack_from("<Hhhhh", body, p + 1)
            pts.append((key[0] + dt, key[1] + dlat, key[2] + dlng, key[3] + dalt, key[4] + dspd))
            p += 25
    return blob, hl, pts


def filt(mmps):
    return 0.0 if mmps < STATIONARY else float(mmps)


def detect_runs(pts):
    runs = []
    running = False
    stationary = 0
    start = 0
    for i, p in enumerate(pts):
        v = filt(p[4])
        if not running:
            if v >= STATIONARY:
                running = True
                start = i
                stationary = 0
        elif v >= STATIONARY:
            stationary = 0
        else:
            stationary += 1
            if stationary > END_RUN_RECORDS:
                end = i - stationary
                if end >= start:
                    runs.append((start, end))
                running = False
    if running:
        runs.append((start, len(pts) - 1))
    return runs


def dist_m(a, b):
    R = 6371000.0
    p1 = math.radians(a[0])
    p2 = math.radians(b[0])
    dp = p2 - p1
    dl = math.radians(b[1] - a[1])
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def track(pts, run, s_kph, e_kph):
    st, en = run
    s_th = s_kph * 1000.0 / 3.6
    e_th = e_kph * 1000.0 / 3.6
    frm = max(st, 1)
    if frm > en:
        return None
    started = False
    s_itow = 0
    s_ll = (0.0, 0.0)
    s_alt = 0.0
    for i in range(frm, en + 1):
        prev = filt(pts[i - 1][4])
        new = filt(pts[i][4])
        if not started:
            if prev <= s_th <= new and (s_th != 0.0 or new != 0.0):
                started = True
                s_itow = pts[i - 1][0]
                s_ll = (pts[i - 1][1] / 1e7, pts[i - 1][2] / 1e7)
                s_alt = pts[i - 1][3] / 1000.0
            continue
        if prev <= e_th <= new and prev < new:
            frac = (e_th - prev) / (new - prev)
            dt = pts[i][0] - pts[i - 1][0]
            e_itow = pts[i - 1][0] + int(dt * frac)
            d = dist_m(s_ll, (pts[i][1] / 1e7, pts[i][2] / 1e7))
            slope = (pts[i][3] / 1000.0 - s_alt) * 100.0 / d if d > 0 else 0.0
            return dict(
                time_ms=max(0, e_itow - s_itow),
                slope=int(slope * 10),
                start_idx=i - st,
                n_rec=i - st + 1,
                dist=int(round(d)),
            )
    return None


def decode_suffix(blob):
    hl = struct.unpack_from("<H", blob, 4)[0]
    data_len = struct.unpack_from("<I", blob, 12)[0]
    suf_len = struct.unpack_from("<H", blob, 10)[0]
    n_runs = struct.unpack_from("<H", blob, 82)[0]
    suf = blob[hl + data_len:hl + data_len + suf_len]
    runs = []
    at = 0
    for _ in range(n_runs):
        st, nr, top = struct.unpack_from("<IIh", suf, at)
        at += SUFFIX_FIXED
        sps = []
        for _ in SETPOINTS:
            t, sl, si, nn, d = struct.unpack_from("<IhHHH", suf, at)
            at += SUFFIX_PER
            sps.append((t, sl, si, nn, d))
        runs.append(dict(start=st, n=nr, top=top, sps=sps))
    return suf, runs, at


def main():
    src, ref = sys.argv[1], sys.argv[2]
    _, hl, pts = load_points(src)
    print(f"记录数 {len(pts)}  头长 {hl}")
    runs = detect_runs(pts)
    tops = [max(filt(pts[i][4]) for i in range(a, b + 1)) / 1000 * 3.6 for a, b in runs]
    print(f"算出 {len(runs)} 跑；极速 >=60 的 {sum(1 for t in tops if t >= 60)} 跑")

    mine = []
    for (st, en), top in zip(runs, tops):
        sps = [track(pts, (st, en), a, b) for a, b in SETPOINTS]
        hit = [s for s in sps if s]
        mine.append(dict(start=st, n=en - st + 1, top=top, sps=sps, hit=len(hit)))

    hit_runs = [m for m in mine if m["hit"]]
    print(f"其中至少跑进一个目标点的 {len(hit_runs)} 跑")

    print(f"\n{'#':>3} {'start':>6} {'n':>6} {'top':>6} {'hit':>3}  setpoints")
    for k, m in enumerate(mine[:40]):
        sp = " ".join(
            f"{SETPOINTS[j][0]}-{SETPOINTS[j][1]}:{s['time_ms']/1000:.2f}s" for j, s in enumerate(m["sps"]) if s
        )
        print(f"{k:>3} {m['start']:>6} {m['n']:>6} {m['top']:>6.1f} {m['hit']:>3}  {sp}")

    if len(sys.argv) > 2:
        rblob = open(ref, "rb").read()
        _, rruns, used = decode_suffix(rblob)
        print(f"\n参考文件 {len(rruns)} 跑（尾部 {used} 字节）—— 那份是早期脚本的粗略分跑，"
              f"0-60 记成了 40 秒，只用来确认结构")


main()
