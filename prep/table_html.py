import random
import imgkit
import random
import re
from pathlib import Path

# import sys
# sys.path.insert(0, '/home/eric/workspace/datalake/')
from export.utils import html_to_doctags
from managers.datalake_client import DatalakeClient


class HTMLRenderer(object):
    def __init__(
        self,
        font_dir: str = "/mnt/AI_NAS/OCR/Font/",
    ) -> None:
        self.font_paths = [i.as_posix() for i in Path(font_dir).glob("*")]

    # def add_grid_to_table(
    #     html: str,
    # ) -> str:
    #     css = """
    #     <style>
    #     table, th, td {
    #         border: 1px solid black;
    #         border-collapse: collapse;
    #     }
    #     th, td {
    #         padding: 4px;
    #         text-align: center;
    #     }
    #     </style>
    #     """
    #     return css + html

    def style_static_table_with_random_word_styles(
        self,
        html: str,
        grid=True,
        zebra=True,
        header=True,
        padding=True,
        shadow=False,
        font_path: str = None,
        style_words=True,
        bold_prop: float = 0.3,
        color_underline_prob: float = 0.3,
    ) -> str:
        def get_random_pastel_color():
            r = random.randint(150, 220)
            g = random.randint(150, 220)
            b = random.randint(150, 220)
            return f"rgb({r}, {g}, {b})"

        def randomly_style_words(text: str) -> str:
            words = re.findall(r"\S+|\s+", text)
            styled_words = []
            for word in words:
                if word.isspace():
                    styled_words.append(word)
                    continue

                styled = word

                # 확률적으로 bold
                if random.random() < bold_prop:
                    styled = f"<b>{styled}</b>"

                # 확률적으로 색상/밑줄 스타일 적용
                if random.random() < color_underline_prob:
                    style_choice = random.choice(["blue_underline", "underline", "red"])
                    if style_choice == "blue_underline":
                        styled = f"<span style='color:blue;text-decoration:underline'>{styled}</span>"
                    elif style_choice == "underline":
                        styled = f"<span style='text-decoration:underline'>{styled}</span>"
                    elif style_choice == "red":
                        styled = f"<span style='color:red'>{styled}</span>"

                styled_words.append(styled)

            return "".join(styled_words)

        styles = ["<style>", "table { border-collapse: collapse; }"]

        if font_path:
            font_path = Path(font_path).absolute().as_posix()
            styles.append(f"""
            @font-face {{
                font-family: 'CustomFont';
                src: url('file://{font_path}') format('truetype');
            }}
            * {{
                font-family: 'CustomFont', sans-serif;
            }}
            """)

        if grid:
            styles.append("td, th { border: 1px solid #333; }")
        if zebra:
            styles.append("tr:nth-child(even) { background-color: #f9f9f9; }")
        if header:
            pastel_color = get_random_pastel_color()
            styles.append(f"th {{ background-color: {pastel_color}; color: black; font-weight: bold; }}")
        if padding:
            styles.append("td, th { padding: 8px; text-align: center; }")
        if shadow:
            styles.append("table { box-shadow: 6px 6px 6px rgba(0,0,0,0.5); }")

        styles.append("</style>")

        # 스타일 적용
        if style_words:
            html = re.sub(
                r">(.*?)<",
                lambda m: f">{randomly_style_words(m.group(1))}<",
                html,
                flags=re.DOTALL,
            )

        return "\n".join(styles) + html

    def render(
        self,
        html: str,
        # save_path: str = False,
        engine: str = "imgkit",
    ):
        # print(html)
        html_temp = self.style_static_table_with_random_word_styles(
            html,
            font_path=random.choice(self.font_paths),
        )

        if engine == "imgkit":
            image_bytes = imgkit.from_string(
                html_temp,
                # output_path=save_path,
                output_path=False,
                options={
                    "enable-local-file-access": "",
                    "quiet": "",
                    # "width": "700",  # auto-fit to content
                    # "disable-smart-width": "",  # disables auto-width guessing
                },
            )
        return image_bytes


def main(
    batch_size: int = 32,
    mod: str = "table",
) -> None:

    manager = DatalakeClient()

    search_results = manager.search_catalog(
        variants=[
            "table_html",
        ]
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    dataset = manager.to_dataset(
        search_results,
        absolute_paths=False,
    )
    dataset = dataset.select(range(64))
    dataset = dataset.map(
        lambda batch: {
            "label": [
                html_to_doctags(
                    i,
                ) for i in batch["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
    )

    renderer = HTMLRenderer()
    dataset = dataset.map(
        lambda batch: {
            "image_bytes": [
                renderer.render(
                    i,
                    # save_path="/home/eric/workspace/imgkit.png",
                ) for i in batch["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
    )

    set(dataset["dataset"])
    df = dataset.to_pandas()
    for dataset_name, df_group in df.groupby("dataset"):
        _, _ = manager.upload_task_data(
            data_file=df_group ,
            provider=df_group["provider"].unique()[0],
            dataset=dataset_name,
            task="document_conversion",
            variant="table_image_otsl",
            meta={
                "lang": df_group["lang"].unique()[0],
                "src": df_group["src"].unique()[0],
                "mod": mod,
            },
            overwrite=True,
        )

    job_id = manager.trigger_nas_processing()
    manager.wait_for_job_completion(
        job_id,
    )
    manager.build_catalog_db(
        force_rebuild=True,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main,
    )
# html = "<table><tr><th>(전년동월비,%)</th><th>'19.8</th><th>9</th><th>10</th><th>11</th><th>12</th><th>20.1</th><th>2</th><th>3</th><th>4</th><th>5</th><th>6</th><th>7</th><th>8</th><th>9</th></tr><tr><td>농산물및 석유류제외</td><td>0.9</td><td>0.6</td><td>0.8</td><td>0.6</td><td>0.7</td><td>0.9</td><td>0.6</td><td>0.7</td><td>0.3</td><td>0.5</td><td>0.6</td><td>0.7</td><td>0.8</td><td>0.9</td></tr><tr><td>식료품및 에너지제외</td><td>0.8</td><td>0.5</td><td>0.6</td><td>0.5</td><td>0.6</td><td>0.8</td><td>0.5</td><td>0.4</td><td>0.1</td><td>0.1</td><td>0.2</td><td>0.4</td><td>0.4</td><td>0.6</td></tr></table>"
