import os
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = "tourism_images/images"
OUTPUT_DIR = "tourism_images/resized"
log_txt = "resize_log.txt"


def resize_image(
    image_path,
    max_size=2048,
):
    try:
        with Image.open(image_path) as img:
            
            w, h = img.size
            scale = min(max_size / w, max_size / h, 1.0)  # 최대값 초과만 축소
            save_path = Path(OUTPUT_DIR) / Path(image_path).relative_to(BASE_DIR)
            save_path.parent.mkdir(parents=True, exist_ok=True)  # 디렉토리 생성
            if scale < 1.0:
                new_w, new_h = int(w * scale), int(h * scale)
                img = img.resize((new_w, new_h), Image.LANCZOS)
                img.save(save_path)
                with open(log_txt, "a") as log_file:
                    # 이미지 경로\t원본 크기\t새 크기
                    log_file.write(f"{image_path}\t{w}x{h}\t{new_w}x{new_h}\n")
                return f"Resized: {save_path} ({w}x{h} -> {new_w}x{new_h})"
            else:
                img.save(save_path)
                with open(log_txt, "a") as log_file:
                    log_file.write(f"{image_path}\t{w}x{h}\t{w}x{h}\n")
                return f"Skipped (no resize): {save_path} ({w}x{h})"
    except Exception as e:
        return f"Error: {save_path} ({str(e)})"


def main():
    root = Path(BASE_DIR)
    image_paths = list(root.glob("**/*.jpg"))
    print(f"Found {len(image_paths)} images.")

    with ThreadPoolExecutor(max_workers=50 or 4) as executor:
        futures = {executor.submit(resize_image, str(p)): p for p in image_paths}
        for future in as_completed(futures):
            print(future.result())


if __name__ == "__main__":
    main()