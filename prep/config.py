from pathlib import Path

PARTITION_ORDER = {
    "ocr":        ["lang", "src"],
    "kie":        ["lang", "src"],
    "vqa":        ["lang", "src"],
    "layout":     ["lang", "src"],
    "document_conversion": ["lang", "src", "mod"],
}

ALLOWED_VALUES = {
    "lang": {"ko", "en", "ja", "multi"},  # multi = 다국어 혼합
    "src":  {"real", "synthetic"},
    "mod":  {"page", "table", "chart"},
}

ALLOWED_PROVIDERS = [
    "aihub", "huggingface", "opensource", "inhouse"
]


def build_images_root(
    base_root: Path,
    provider: str,
    dataset: str,
) -> Path:
    return base_root / f"provider={provider}" / f"dataset={dataset}" / "images"


def build_dst_root(
    base_root: Path,
    provider: str,
    dataset: str,
    task: str,
    variant: str,
    parts: dict,
) -> Path:
    segs = [
        f"provider={provider}",
        f"dataset={dataset}",
        f"task={task}",
        f"variant={variant}",
    ] + [f"{k}={parts[k]}" for k in PARTITION_ORDER[task] if k in parts]
    return base_root.joinpath(*segs)


def validate_provider(
    provider: str,
) -> None:
    if provider not in ALLOWED_PROVIDERS:
        raise ValueError(
            f"unknown provider '{provider}'. "
            f"allowed: {ALLOWED_PROVIDERS}"
        )


def validate_parts(
    task: str,
    parts: dict,
) -> None:
    """
    · task 에 정의된 키가 모두 존재하는지
    · 허용 값 범위를 벗어나지 않는지 검사
    실패 시 ValueError
    """
    if task not in PARTITION_ORDER:
        raise ValueError(
            f"unknown task '{task}'. "
            f"allowed: {list(PARTITION_ORDER.keys())}"
        )

    required = PARTITION_ORDER[task]
    missing  = [k for k in required if k not in parts]
    if missing:
        raise ValueError(f"missing partition keys for {task}: {missing}")

    unknown = [k for k in parts if k not in required]
    if unknown:
        raise ValueError(f"unexpected partition keys for {task}: {unknown}")

    for k, v in parts.items():
        if k in ALLOWED_VALUES and v not in ALLOWED_VALUES[k]:
            raise ValueError(f"{k}='{v}' not in {sorted(ALLOWED_VALUES[k])}")


def parse_to_parts(
    text: str,
):
    """
    "lang=ko,src=real" → {"lang":"ko", "src":"real"}
    공백·대소문자 자동 정리
    """
    parts = {}
    if not text:
        return parts
    for pair in text.split(","):
        k, v = pair.split("=", 1)
        parts[k.strip()] = v.strip()
    return parts


def get_partitions(
    task: str,
    parts: dict,
) -> list:
    """
    task 에 정의된 순서로 정렬된 파티션 리스트 반환
    예) ["lang=ko", "src=real"]
    """
    if task not in PARTITION_ORDER:
        raise ValueError(
            f"unknown task '{task}'. "
            f"allowed: {list(PARTITION_ORDER.keys())}"
        )
    order = PARTITION_ORDER[task]
    return [f"{k}={parts[k]}" for k in order if k in parts]
