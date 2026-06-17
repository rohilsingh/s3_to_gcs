def build_gcs_key(s3_key: str, pattern: dict) -> str:
    destination_base = pattern["destination_base"]
    if not destination_base:
        destination_base = s3_key.split("/", 1)[0].rstrip("/") + "/"

    destination_prefix_base = ""
    if pattern["destination_prefix_base"]:
        destination_prefix_base = pattern["destination_prefix_base"].rstrip("/") + "/"

    remainder = s3_key.split("/", 1)[1] if "/" in s3_key else s3_key
    return destination_prefix_base + destination_base + remainder
