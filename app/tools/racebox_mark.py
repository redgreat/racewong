"""RaceBox 官网标记（marker）的矢量数据与光栅化。

图形取自 https://www.racebox.pro/assets/img/RaceBoxLogo.svg —— 那条 marker path，
bbox 是 (3.7, 2.65) → (50.75, 37.35)，宽高比约 1.356（比正方略宽）。
字标是另外 8 条 `fill="#4B4B4B"` 的 path，这里不用。

`.venv` 里没有 cairosvg / svglib / resvg，为一枚图标装一套 SVG 渲染栈不划算，
所以自己把 path 拍平成多边形（三次贝塞尔每段采样 24 点 → 348 个点）。

`app/tools/make_icon.py`（exe / 窗口 / 托盘图标）和
`app/tools/make_installer_art.py`（安装向导的横幅与背景图）都从这里取图，
保证两处用的是同一个形状。
"""

import re

from PIL import Image, ImageDraw

# 品牌红：官网 CSS 里出现 71 次的主色
BRAND = (234, 33, 39, 255)

MARKER_PATH = (
    "M27.229,2.655c-12.971,0-23.524,10.553-23.524,23.525v2.479v3.701c0,2.754,2.232,4.986,4.985,4.986"
    "s4.986-2.232,4.986-4.986v-5.125V26.18c0-7.474,6.08-13.553,13.553-13.553h12.36c0.658,0,1.194,0.535,1.194,1.194"
    "c0,0.658-0.536,1.193-1.194,1.193h-12.36c-6.156,0-11.165,5.008-11.165,11.166c0,6.156,5.009,11.166,11.165,11.166h18.54"
    "c2.754,0,4.986-2.232,4.986-4.986s-2.232-4.986-4.986-4.986h-18.54c-0.658,0-1.194-0.535-1.194-1.193s0.536-1.193,1.194-1.193"
    "h12.36c6.157,0,11.166-5.009,11.166-11.166c0-6.157-5.009-11.166-11.166-11.166H27.229z"
)

NUM = re.compile(r"[-+]?(?:\d*\.\d+|\d+\.?)(?:[eE][-+]?\d+)?")
CMD = re.compile(r"([MmCcSsHhVvLlZz])")
# 每条贝塞尔采样多少点：8 倍超采样下 24 点已经看不出折角
BEZIER_STEPS = 24


def _tokenize(d):
    out = []
    for part in CMD.split(d):
        part = part.strip()
        if not part:
            continue
        if CMD.fullmatch(part):
            out.append(part)
        else:
            out.append([float(x) for x in NUM.findall(part)])
    return out


def _bezier(p0, p1, p2, p3, n=BEZIER_STEPS):
    pts = []
    for i in range(1, n + 1):
        t = i / n
        u = 1 - t
        x = u**3 * p0[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t**3 * p3[0]
        y = u**3 * p0[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t**3 * p3[1]
        pts.append((x, y))
    return pts


def flatten(d=MARKER_PATH):
    """把 SVG path 拍平成点列（只支持 logo 里用到的命令）。"""
    toks = _tokenize(d)
    pts, cur, start = [], (0.0, 0.0), (0.0, 0.0)
    last_ctrl = None
    cmd = None
    i = 0
    while i < len(toks):
        tok = toks[i]
        i += 1
        if isinstance(tok, str):
            cmd = tok
            if cmd in "Zz":
                pts.append(start)
                cur = start
                last_ctrl = None
            continue
        nums = tok
        j = 0
        while j < len(nums):
            if cmd in "Mm":
                x, y = nums[j], nums[j + 1]
                j += 2
                if cmd == "m":
                    x, y = cur[0] + x, cur[1] + y
                cur = (x, y)
                start = cur
                pts.append(cur)
                last_ctrl = None
                # 后续坐标对按 lineto 处理（SVG 的 m 隐式 l 规则）
                cmd = "l" if cmd == "m" else "L"
            elif cmd in "Cc":
                x1, y1, x2, y2, x, y = nums[j : j + 6]
                j += 6
                if cmd == "c":
                    x1, y1 = cur[0] + x1, cur[1] + y1
                    x2, y2 = cur[0] + x2, cur[1] + y2
                    x, y = cur[0] + x, cur[1] + y
                pts += _bezier(cur, (x1, y1), (x2, y2), (x, y))
                last_ctrl = (x2, y2)
                cur = (x, y)
            elif cmd in "Ss":
                x2, y2, x, y = nums[j : j + 4]
                j += 4
                if cmd == "s":
                    x2, y2 = cur[0] + x2, cur[1] + y2
                    x, y = cur[0] + x, cur[1] + y
                if last_ctrl:
                    x1, y1 = 2 * cur[0] - last_ctrl[0], 2 * cur[1] - last_ctrl[1]
                else:
                    x1, y1 = cur
                pts += _bezier(cur, (x1, y1), (x2, y2), (x, y))
                last_ctrl = (x2, y2)
                cur = (x, y)
            elif cmd in "Vv":
                y = nums[j]
                j += 1
                y = cur[1] + y if cmd == "v" else y
                cur = (cur[0], y)
                pts.append(cur)
                last_ctrl = None
            elif cmd in "Hh":
                x = nums[j]
                j += 1
                x = cur[0] + x if cmd == "h" else x
                cur = (x, cur[1])
                pts.append(cur)
                last_ctrl = None
            elif cmd in "Ll":
                x, y = nums[j], nums[j + 1]
                j += 2
                if cmd == "l":
                    x, y = cur[0] + x, cur[1] + y
                cur = (x, y)
                pts.append(cur)
                last_ctrl = None
            else:
                raise SystemExit("unhandled path command: %s" % cmd)
    return pts


POINTS = flatten()
MINX = min(p[0] for p in POINTS)
MINY = min(p[1] for p in POINTS)
MAXX = max(p[0] for p in POINTS)
MAXY = max(p[1] for p in POINTS)
BBOX = (MINX, MINY, MAXX, MAXY)


def render_square(size, color=BRAND, pad=0.04, ss=8):
    """按目标尺寸渲染成正方形画布（等比居中，四周留 `pad` 比例的边）。"""
    w = size * ss
    bw, bh = MAXX - MINX, MAXY - MINY
    inner = w * (1 - 2 * pad)
    k = min(inner / bw, inner / bh)
    ox = (w - bw * k) / 2 - MINX * k
    oy = (w - bh * k) / 2 - MINY * k
    scaled = [(p[0] * k + ox, p[1] * k + oy) for p in POINTS]
    img = Image.new("RGBA", (w, w), (0, 0, 0, 0))
    ImageDraw.Draw(img).polygon(scaled, fill=color)
    return img.resize((size, size), Image.LANCZOS)


def render_height(height, color=BRAND, ss=8):
    """按目标高度渲染，画布紧贴图形 bbox（宽高比保持 1.356）。"""
    bw, bh = MAXX - MINX, MAXY - MINY
    w = max(1, round(bw * height / bh))
    W, H = w * ss, height * ss
    k = H / bh
    scaled = [((p[0] - MINX) * k, (p[1] - MINY) * k) for p in POINTS]
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(img).polygon(scaled, fill=color)
    return img.resize((w, height), Image.LANCZOS)
