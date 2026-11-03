"""拆开一个 RB V2 会话文件：头部 / 记录流（含 241 系统、242 事件、243 跑次）/ 尾部。

用法：python script/inspect_session.py <file.bin> [--records]

只依赖标准库，用来对照手机 App 写出来的文件和我们自己写的有哪些字段不同。
"""
import struct
import sys

PREFIX_KEY = 0
PREFIX_SYSTEM = 240
PREFIX_EVENT = 242
PREFIX_RUN_START = 243


def decode(path, show_records=False):
    b = open(path, "rb").read()
    hl = struct.unpack_from("<H", b, 4)[0]
    suffix_len = struct.unpack_from("<H", b, 10)[0]
    data_len = struct.unpack_from("<I", b, 12)[0]
    n_sensor = struct.unpack_from("<I", b, 20)[0]
    n_sys = struct.unpack_from("<I", b, 24)[0]
    n_event = struct.unpack_from("<I", b, 28)[0]
    stype = b[16]
    print(f"文件 {path}")
    print(f"  头长 {hl}  类型 {stype}  记录 {data_len} 字节 / {n_sensor} 条"
          f"  系统 {n_sys}  事件 {n_event}  尾部 {suffix_len}")
    print(f"  dragMode {b[68]}  ftRollout {b[69]}  目标点 {b[70]}  圣诞树 {b[71]}")
    print(f"  极速 {struct.unpack_from('<h', b, 80)[0] / 100:.2f} km/h  "
          f"跑次 {struct.unpack_from('<H', b, 82)[0]}")
    n = b[70]
    for i in range(n):
        at = 84 + i * 6
        s, e = struct.unpack_from("<HH", b, at)
        dist = (e, "m") if b[at + 4] & 0x02 else (e, "km/h")
        print(f"  目标点 {i}: {s} -> {dist[0]} {dist[1]}  flags 0x{b[at + 4]:02x}")
    at = 84 + n * 6
    best = struct.unpack_from(f"<{n}i", b, at)
    at += n * 4
    best_run = struct.unpack_from(f"<{n}h", b, at)
    print(f"  成绩 {[x / 1000 for x in best]}  所在跑次 {list(best_run)}")

    # ---- 记录流 ----
    body = b[hl:hl + data_len]
    p, key, idx, rec_i = 0, None, 0, 0
    pts, events, runstarts, systems = [], [], [], []
    while p < len(body):
        pre = body[p]
        if pre == PREFIX_RUN_START:
            n_run = struct.unpack_from("<H", body, p + 1)[0]
            runstarts.append((rec_i, n_run))
            p += 3
            continue
        if pre == PREFIX_EVENT:
            sp = body[p + 1]
            t = struct.unpack_from("<I", body, p + 2)[0]
            slope = struct.unpack_from("<h", body, p + 6)[0]
            si, nn, tail = struct.unpack_from("<HHH", body, p + 8)
            events.append(dict(rec=rec_i, sp=sp, time=t, slope=slope, si=si, n=nn, tail=tail))
            p += 14
            continue
        if pre == PREFIX_SYSTEM:
            bat, storage, ant = body[p + 1], body[p + 2], body[p + 3]
            hacc = struct.unpack_from("<I", body, p + 4)[0] / 1000.0
            systems.append(dict(rec=rec_i, batt=bat >> 1, storage=storage,
                                siv=(ant >> 1) & 0x7F, ext=ant & 1, hacc=hacc))
            p += 8
            continue
        if pre == 0:
            itow, lat, lng, alt, spd = struct.unpack_from("<IiiiI", body, p + 1)
            gx, gy, gz = struct.unpack_from("<hhh", body, p + 23)
            key = (itow, lat, lng, alt, spd, gx, gy, gz)
            p += 35
            idx = 0
        else:
            dt, dlat, dlng, dalt, dspd = struct.unpack_from("<Hhhhh", body, p + 1)
            gx, gy, gz = struct.unpack_from("<hhh", body, p + 13)
            key = (key[0] + dt, key[1] + dlat, key[2] + dlng, key[3] + dalt, key[4] + dspd,
                   gx, gy, gz)
            p += 25
            idx = pre
        pts.append(key)
        rec_i += 1

    print(f"  解析：{len(pts)} 条采样（声明 {n_sensor}）"
          f"，{len(systems)} 条系统（声明 {n_sys}）"
          f"，{len(events)} 条事件（声明 {n_event}）"
          f"，{len(runstarts)} 条跑次标记")
    if len(pts) != n_sensor:
        print("  !! 采样条数与头部不一致")
    for rec, n_run in runstarts:
        print(f"    跑次 {n_run} 从记录 {rec} 开始")
    if systems:
        last = systems[-1]
        print(f"    最后一条系统记录：记录{last['rec']} 电量{last['batt']}% "
              f"卫星{last['siv']} 水平精度{last['hacc']:.2f} m")
    for e in events:
        print(f"    事件 记录{e['rec']:>6} 目标点{e['sp']} 成绩{e['time'] / 1000:7.3f}s "
              f"坡度{e['slope'] / 10:5.1f}% 起点{e['si']:>5} 条数{e['n']:>5} 尾{float(e['tail']):.0f}")

    # ---- 尾部 ----
    suf = b[hl + data_len:hl + data_len + suffix_len]
    at = 0
    n_runs = struct.unpack_from("<H", b, 82)[0]
    for r in range(n_runs):
        st, nr, top, mg, rt, fti, ftt, res = struct.unpack_from("<IIhhhHHH", suf, at)
        at += 20
        print(f"  跑次 {r}: 起点{st} 条数{nr} 极速{top / 100:.2f} 最大G{mg / 1000:.3f} "
              f"反应{rt} ftIdx{fti} ftTime{ftt}")
        for j in range(n):
            t, sl, si, nn, d = struct.unpack_from("<IhHHH", suf, at)
            at += 12
            if t or nn or d:
                print(f"      目标点{j}: {t / 1000:.3f}s 坡度{sl / 10:.1f}% 起点{si} 条数{nn} 距离{d}m")
        if show_records:
            seg = pts[st:st + nr]
            if seg:
                gmax = max((abs(g[5]), abs(g[6]), abs(g[7])) for g in seg)
                mag = max((g[5] ** 2 + g[6] ** 2 + g[7] ** 2) ** 0.5 for g in seg)
                print(f"      实测 gx/gy/gz 最大 {[x / 1000 for x in gmax]} 合成 {mag / 1000:.3f}")
    print(f"  尾部解析用 {at} / {suffix_len} 字节")


if __name__ == "__main__":
    decode(sys.argv[1], "--records" in sys.argv)
