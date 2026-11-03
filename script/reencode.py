"""把 cloud_one.bin 按 session.rs 的逻辑重新编码，用来验证（本机没有 MSVC，跑不了 cargo test）。

**这是调试脚本，逻辑必须和 app/src/racebox/session.rs 保持一致。**
改动编码器时两边一起改，别把它当成第二个实现。

用法：
  python script/reencode.py <源会话.bin> <输出.bin>      # 重新编码
  python script/reencode.py <源会话.bin> <输出.bin> --upload
"""
import math
import struct
import sys
import zlib

STATIONARY = 277.778
END_RUN_RECORDS = 10
SETPOINTS = [(0, 30), (0, 60), (0, 100), (0, 150)]
RUN_START = 243
EVENT = 242
SYSTEM = 240
SYSTEM_EVERY = 50


def load(src):
    blob = open(src, "rb").read()
    hl = struct.unpack_from("<H", blob, 4)[0]
    dl = struct.unpack_from("<I", blob, 12)[0]
    body = blob[hl:hl + dl]
    pts, pos, p, key = [], [], 0, None
    while p < len(body):
        pre = body[p]
        if pre in (240, 242):
            p += 8 if pre == SYSTEM else 14
            continue
        if pre == RUN_START:
            p += 3
            continue
        pos.append(p)
        if pre == 0:
            itow, lat, lng, alt, spd = struct.unpack_from("<IiiiI", body, p + 1)
            g = struct.unpack_from("<hhh", body, p + 23)
            key = [itow, lat, lng, alt, spd, g[0], g[1], g[2]]
            p += 35
        else:
            # 增量记录是相对**上一条键记录**的，不是上一条记录
            dt, dlat, dlng, dalt, dspd = struct.unpack_from("<Hhhhh", body, p + 1)
            g = struct.unpack_from("<hhh", body, p + 13)
            cur = (key[0] + dt, key[1] + dlat, key[2] + dlng, key[3] + dalt, key[4] + dspd,
                   g[0], g[1], g[2])
            p += 25
            pts.append(cur)
            continue
        pts.append(tuple(key))
    return blob, hl, body, pts, pos


def filt(v):
    return 0.0 if v < STATIONARY else float(v)


def detect_runs(pts):
    runs, running, stationary, start = [], False, 0, 0
    for i, p in enumerate(pts):
        v = filt(p[4])
        if not running:
            if v >= STATIONARY:
                running, start, stationary = True, i, 0
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
    p1, p2 = math.radians(a[0]), math.radians(b[0])
    dl = math.radians(b[1] - a[1])
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def track(pts, run, s_kph, e_kph):
    a, b = run
    s_th, e_th = s_kph * 1000.0 / 3.6, e_kph * 1000.0 / 3.6
    started, start_at = False, a
    for i in range(max(a, 1), b + 1):
        prev, new = filt(pts[i - 1][4]), filt(pts[i][4])
        if not started:
            if prev <= s_th <= new and (s_th != 0.0 or new != 0.0):
                started, start_at = True, max(i - 1, a)
            continue
        if prev <= e_th <= new and prev < new:
            frac = (e_th - prev) / (new - prev)
            dt = pts[i][0] - pts[i - 1][0]
            end_itow = pts[i - 1][0] + int(dt * frac)
            d = dist_m((pts[start_at][1] / 1e7, pts[start_at][2] / 1e7),
                       (pts[i][1] / 1e7, pts[i][2] / 1e7))
            slope = (pts[i][3] / 1000.0 - pts[start_at][3] / 1000.0) * 100.0 / d if d > 0 else 0.0
            return dict(time_ms=max(0, end_itow - pts[start_at][0]), slope=int(slope * 10),
                        start_idx=start_at - a, n_rec=i - start_at + 1,
                        dist=min(65535, int(round(d))), end_idx=i)
    return None


def encode(src, out_path):
    blob, hl, body, pts, pos = load(src)
    runs = detect_runs(pts)
    print(f"记录 {len(pts)}  跑次 {len(runs)}")

    suffix = bytearray()
    best, best_run, events = [0] * len(SETPOINTS), [-1] * len(SETPOINTS), []
    for ri, (a, b) in enumerate(runs):
        seg = pts[a:b + 1]
        top = max(filt(p[4]) for p in seg) / 1000 * 3.6
        mg = max(math.hypot(p[5] / 1000.0, p[6] / 1000.0) for p in seg)
        suffix += struct.pack("<IIhhhHHH", a, b - a + 1, int(top * 100), int(mg * 1000),
                              -32768, 0, 65535, 0)
        for si, (s, e) in enumerate(SETPOINTS):
            r = track(pts, (a, b), s, e)
            if r:
                suffix += struct.pack("<IhHHH", r["time_ms"], r["slope"], r["start_idx"],
                                      r["n_rec"], r["dist"])
                events.append((r["end_idx"], si, r["time_ms"], r["slope"], r["start_idx"],
                               r["n_rec"], r["dist"]))
                if best_run[si] < 0 or r["time_ms"] < best[si]:
                    best[si], best_run[si] = r["time_ms"], ri
            else:
                suffix += bytes(12)
    events.sort(key=lambda e: e[0])

    starts = {r[0]: k for k, r in enumerate(runs)}
    ev_at = {}
    for e in events:
        ev_at.setdefault(e[0], []).append(e)

    new_body = bytearray()
    n_sys = 0
    for i in range(len(pts)):
        if i in starts:
            new_body += bytes([RUN_START]) + struct.pack("<H", starts[i])
        if i + 1 < len(pos):
            new_body += body[pos[i]:pos[i + 1]]
        else:
            new_body += body[pos[i]:]
        for e in ev_at.get(i, []):
            new_body += bytes([EVENT, e[1]]) + struct.pack("<IhHHH", *e[2:])
        # 源文件里没有精度/卫星数，所以这里不会写 240（和 Rust 一致：hacc>0 才写）
        if i % SYSTEM_EVERY == 0:
            pass

    h = bytearray(blob[:hl])
    h[68] = 1      # DragSessionMode::SpeedMetric
    h[69] = 0      # FtRolloutEnabled
    h[70] = len(SETPOINTS)
    h[71] = 0      # 圣诞树
    struct.pack_into("<i", h, 72, pts[0][1])
    struct.pack_into("<i", h, 76, pts[0][2])
    struct.pack_into("<h", h, 80, int(max(p[4] for p in pts) / 1000 * 3.6 * 100))
    for i, (f, t) in enumerate(SETPOINTS):
        at = 84 + i * 6
        struct.pack_into("<HH", h, at, f, t)
        h[at + 4] = 0x01 | (15 << 4)
        h[at + 5] = 0
    struct.pack_into("<H", h, 10, len(suffix))
    struct.pack_into("<I", h, 12, len(new_body))
    struct.pack_into("<I", h, 20, len(pts))
    struct.pack_into("<I", h, 24, n_sys)
    struct.pack_into("<I", h, 28, len(events))
    struct.pack_into("<H", h, 82, len(runs))
    struct.pack_into("<4i", h, 108, *best)
    struct.pack_into("<4h", h, 124, *best_run)
    struct.pack_into("<I", h, hl - 8, zlib.crc32(bytes(new_body) + bytes(suffix)) & 0xFFFFFFFF)
    struct.pack_into("<I", h, hl - 4, zlib.crc32(bytes(h[:hl - 4])) & 0xFFFFFFFF)

    full = bytes(h) + bytes(new_body) + bytes(suffix)
    open(out_path, "wb").write(full)
    print(f"写出 {out_path}  {len(full)} 字节（头 {hl} 记录 {len(new_body)} 尾部 {len(suffix)}）")
    print(f"跑次 {len(runs)}  事件 {len(events)}  系统 {n_sys}")
    print("成绩", [b / 1000 for b in best], "所在跑次", best_run)
    return out_path


if __name__ == "__main__":
    src = sys.argv[1]
    dst = sys.argv[2]
    encode(src, dst)
    if "--upload" in sys.argv:
        sys.path.insert(0, r"D:\wangcw\Documents\racebox-re\tools")
        import json
        import rbcloud
        c = rbcloud.Client(*(lambda d: (d["email"], d["password"]))(
            json.load(open(r"C:\Users\wangcw\.rbcloud.json"))))
        c.login()
        s = c.upload_file(dst)
        print("上传成功", json.dumps({k: s.get(k) for k in
                                     ("id", "sessionType", "bestResults", "dateTimeStartedUTC")},
                                    ensure_ascii=False))
