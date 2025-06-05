import ast
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import ipywidgets as widgets
from pathlib import Path
from collections.abc import Iterable
from PIL import Image
import hashlib
# NAS 설정 -------------------------------------------------
NAS_ROOT = Path("/mnt/AI_NAS/datalake")
STAGING = NAS_ROOT / "_staging"



def get_sha256_size(img_input):
    """
    img_input: 파일 경로(str) 또는 PIL.Image.Image 객체 모두 지원
    리턴: (hash, width, height)
    """
    if isinstance(img_input, str):
        img = Image.open(img_input).convert("RGB")
    elif isinstance(img_input, Image.Image):
        img = img_input.convert("RGB")
    else:
        raise ValueError("img_input은 파일 경로나 PIL.Image 객체여야 합니다.")
    width, height = img.size
    arr = np.array(img)
    hash_val = hashlib.sha256(
        arr.tobytes() + str(arr.shape).encode() + str(arr.dtype).encode()
    ).hexdigest()
    return hash_val, width, height


class KIEVisualizer:
    def __init__(
        self,
        parquet_path,
        base_dir=None,
        margin=450,
    ):
        self.df = pd.read_parquet(parquet_path)
        self.base = Path(base_dir or Path(parquet_path).parent)
        self.margin = margin
        self.key_c = (0.9, 0.1, 0.1)
        self.val_c = (0.1, 0.1, 0.9)

    @staticmethod
    def _loads(
        raw,
    ):
        if isinstance(raw, (dict, list)):
            return raw
        try:
            return ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {}

    def _triples(
        self,
        d,
    ):
        for k, vs in d.items():

            if vs is None:
                continue

            if isinstance(vs, np.ndarray):
                if vs.size == 0:
                    continue
            elif isinstance(vs, (list, tuple)):
                if not vs:
                    continue
            elif isinstance(vs, dict):
                if not vs:
                    continue
            elif hasattr(vs, "__len__") and len(vs) == 0:
                continue

            if isinstance(vs, dict):
                vs = [vs]
            elif not isinstance(vs, Iterable):
                continue

            for v in vs:
                if not isinstance(v, dict):
                    continue

                bb = list(v.get("bbox", []))
                if len(bb) == 4 and any(float(c) != 0 for c in bb):
                    yield k, v.get("value", ""), bb

    def _img(
        self,
        rel,
    ):
        p = Path(rel)
        if p.is_absolute():
            return p
        return (self.base / p).resolve()

    def _draw(
        self,
        idx,
    ):
        r = self.df.iloc[idx]
        img = Image.open(self._img(r["image_path"]))
        W, H = img.size
        fig, ax = plt.subplots(figsize=(10, 10 * H / (W + self.margin)))
        ax.imshow(img)
        ax.set_xlim(0, W + self.margin)
        ax.set_ylim(H, 0)

        for k, val, bb in self._triples(self._loads(r["label"])):
            x1, y1, x2, y2 = (bb[0] * W, bb[1] * H, bb[2] * W, bb[3] * H)

            ax.add_patch(
                patches.Rectangle(
                    (x1, y1), x2 - x1, y2 - y1, lw=2, ec=self.val_c, fc="none"
                )
            )
            cx, cy = (x1 + x2) / 2, (y1 + y2) / 2

            if val is not None and str(val).strip() != "":
                ax.text(
                    x1,
                    y1 - 5,
                    str(val),
                    color="white",
                    fontsize=8,
                    ha="left",
                    va="bottom",
                    bbox=dict(
                        boxstyle="round,pad=0.2",
                        fc=self.val_c,
                        ec=self.val_c,
                        lw=1.0,
                        alpha=0.7,
                    ),
                )

            tx = W + 20
            txt = ax.text(
                tx,
                cy,
                k,
                color="white",
                fontsize=9,
                ha="left",
                va="center",
                bbox=dict(
                    boxstyle="round,pad=0.3",
                    fc=self.key_c,
                    ec=self.key_c,
                    lw=1.5,
                ),
            )
            bb_key = txt.get_window_extent(fig.canvas.get_renderer())
            inv = ax.transData.inverted()
            bb_key_data = inv.transform(
                [[bb_key.x0, bb_key.y0], [bb_key.x1, bb_key.y1]]
            )
            kx1, ky1 = bb_key_data[0]
            kx2, ky2 = bb_key_data[1]
            kcx, kcy = (kx1 + kx2) / 2, (ky1 + ky2) / 2
            ax.annotate(
                "",
                xy=(cx, cy),
                xytext=(kx1, kcy),
                arrowprops=dict(arrowstyle="-|>", color=self.val_c, lw=1.5),
            )

        ax.axis("off")
        plt.show()

    def render(
        self,
    ):
        slider = widgets.IntSlider(
            0,
            0,
            len(self.df) - 1,
            1,
            continuous_update=False,
        )
        widgets.interact(lambda i: self._draw(i), i=slider)
