"""生成 MSI 安装向导用的两张位图。

WiX 的 `WixUIBannerBmp` / `WixUIDialogBmp` 是**按 493 宽设计、由 MSI 缩到 370 显示**的
（缩放比 370/493 ≈ 0.7505，WiX 自带素材也是这个尺寸），所以这里的坐标都在
493 宽的坐标系里算，屏幕上看到的是乘 0.75 之后的大小。

* `banner.bmp`  493×58  → 各对话框顶部的横幅（显示为 370×44）
* `dialog.bmp`  493×312 → 欢迎 / 完成页的整幅背景（显示为 370×234）
  左边 180px（屏幕上 135px）是深色品牌条，右边留白给正文——
  135 正好是 WixUI 里 Title / Description 控件的 X 起点，两者对齐才不会压字。

必须存成 24 位 BMP：Bitmap 控件不认 PNG/带 alpha 的图。

用法: .venv\\Scripts\\python.exe app\\tools\\make_installer_art.py
输出: app/installer/banner.bmp, app/installer/dialog.bmp
"""

import os

from PIL import Image, ImageDraw, ImageFont

import racebox_mark

OUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "installer"
)

WHITE = (255, 255, 255)
CHARCOAL = (44, 44, 44)
BRAND = (234, 33, 39)
TEXT = (44, 44, 44)
# 横幅底色：比正文的纯白浅一档，像一条独立的标题带
BAND = (247, 247, 249)

FONT_CANDIDATES = [
    r"C:\Windows\Fonts\msyhbd.ttc",
    r"C:\Windows\Fonts\msyh.ttc",
    "/System/Library/Fonts/PingFang.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
]

NAME = "RaceBox同步"


def load_font(size):
    for path in FONT_CANDIDATES:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    raise SystemExit("找不到可用的中文字体，装一个 Noto Sans CJK 或换 FONT_CANDIDATES")


def make_banner():
    """顶部横幅：浅灰底 + 右上角的品牌标记。

    只放标记、不放产品名，位置也压到最右：WixUI 的每个对话框都把 `Title`
    （X=15,Y=6）和 `Description`（X=25,Y=23，宽 280）**透明地画在这张图上**，
    左侧那 305px 是文字区，logo 放左边会压字。中文描述比英文长，留够余量最稳。
    WiX 自带的横幅也是这个套路——图形在右，左侧全留白。
    """
    w, h = 493, 58
    img = Image.new("RGB", (w, h), BAND)
    mark_h = 30
    mark = racebox_mark.render_height(mark_h)
    img.paste(mark, (w - 18 - mark.width, (h - mark_h) // 2), mark)
    return img


def make_dialog():
    """欢迎 / 完成页背景：左侧深色品牌条，右侧纯白给正文。"""
    w, h = 493, 312
    # 屏幕上 128px：WixUI 的 Title / Description 从 X=135 起，留 7px 让正文不贴边
    strip = 170
    img = Image.new("RGB", (w, h), WHITE)
    d = ImageDraw.Draw(img)

    d.rectangle([0, 0, strip - 1, h - 1], fill=CHARCOAL)

    mark_h = 56
    mark = racebox_mark.render_height(mark_h)
    cx = strip // 2
    mark_y = 100
    img.paste(mark, (cx - mark.width // 2, mark_y), mark)

    font = load_font(21)
    d.text((cx, mark_y + mark_h + 26), NAME, font=font, fill=(236, 236, 238), anchor="mm")

    # 品牌条底部的红色收边
    d.rectangle([0, h - 4, strip - 1, h - 1], fill=BRAND)
    return img


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for name, img in (("banner.bmp", make_banner()), ("dialog.bmp", make_dialog())):
        path = os.path.join(OUT_DIR, name)
        # 24 位 BMP：Bitmap 控件不认带 alpha 的图
        img.convert("RGB").save(path, "BMP")
        print("written:", path, img.size, img.mode)


if __name__ == "__main__":
    main()
