"""生成 racewong-app 的 Windows 图标与运行时图标。

图形用 RaceBox 官网（https://www.racebox.pro）的品牌标记：形状和拍平逻辑在
同目录的 `racebox_mark.py` 里，和安装包横幅共用同一份矢量数据。

用法: .venv\\Scripts\\python.exe app\\tools\\make_icon.py
输出: app/assets/icon.ico (16..256), app/assets/icon256.png
"""

import os

import racebox_mark

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets")
ICO_SIZES = [16, 24, 32, 48, 64, 256]
RGBA_PNG = 256


def draw_at(size: int):
    """按目标尺寸渲染：8 倍超采样后 LANCZOS 缩下来，小尺寸边缘才不毛。"""
    return racebox_mark.render_square(size)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    print(
        "marker points:",
        len(racebox_mark.POINTS),
        "bbox:",
        tuple(round(v, 2) for v in racebox_mark.BBOX),
    )

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
