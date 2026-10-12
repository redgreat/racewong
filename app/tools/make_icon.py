"""生成 racewong-app 的 Windows 图标与运行时图标。

用法: .venv\\Scripts\\python.exe app\\tools\\make_icon.py
输出: app/assets/icon.ico (16..256), app/assets/icon256.png
"""
import math
import os

from PIL import Image, ImageDraw

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets")
ICO_SIZES = [16, 24, 32, 48, 64, 256]
RGBA_PNG = 256

BG_TOP = (32, 96, 220, 255)
BG_BOTTOM = (12, 38, 96, 255)
TRACK = (255, 255, 255, 255)
TRACK_EDGE = (176, 214, 255, 255)
PIN = (255, 92, 46, 255)
PIN_RING = (255, 255, 255, 255)


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def draw_at(size: int) -> Image.Image:
    scale = 4
    s = size * scale
    img = Image.new("RGBA", (s, s), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)

    radius = int(s * 0.22)
    grad = Image.new("RGBA", (1, s))
    gp = grad.load()
    for y in range(s):
        t = y / max(s - 1, 1)
        gp[0, y] = (
            int(lerp(BG_TOP[0], BG_BOTTOM[0], t)),
            int(lerp(BG_TOP[1], BG_BOTTOM[1], t)),
            int(lerp(BG_TOP[2], BG_BOTTOM[2], t)),
            255,
        )
    grad = grad.resize((s, s))
    mask = Image.new("L", (s, s), 0)
    ImageDraw.Draw(mask).rounded_rectangle([0, 0, s - 1, s - 1], radius=radius, fill=255)
    img.paste(grad, (0, 0), mask)

    u = s / 256.0

    def P(x: float, y: float):
        return (x * u, y * u)

    track_w = max(int(13 * u), 1)
    pts = []
    for i in range(161):
        t = i / 160.0
        x = 42 + t * 172
        y = 178 - 62 * math.sin(t * math.pi * 1.15) - 26 * t
        pts.append(P(x, y))

    d.line(pts, fill=TRACK_EDGE, width=track_w + max(int(5 * u), 1), joint="curve")
    d.line(pts, fill=TRACK, width=track_w, joint="curve")

    for x, y, r, fill in [
        (42, 190, 15, TRACK),
        (214, 92, 19, PIN),
        (214, 92, 9, TRACK),
    ]:
        d.ellipse(
            [P(x - r, y - r), P(x + r, y + r)],
            fill=fill,
        )
    if size >= 48:
        ring_w = max(int(5 * u), 1)
        d.ellipse(
            [P(214 - 32, 92 - 32), P(214 + 32, 92 + 32)],
            outline=PIN_RING,
            width=ring_w,
        )

    return img.resize((size, size), Image.LANCZOS)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    png = draw_at(RGBA_PNG)
    png.save(os.path.join(OUT_DIR, "icon256.png"))

    frames = [draw_at(s) for s in ICO_SIZES]
    frames[-1].save(
        os.path.join(OUT_DIR, "icon.ico"),
        format="ICO",
        sizes=[(s, s) for s in ICO_SIZES],
        append_images=frames[:-1],
    )
    print("written:", os.path.join(OUT_DIR, "icon.ico"))
    print("written:", os.path.join(OUT_DIR, "icon256.png"))


if __name__ == "__main__":
    main()
