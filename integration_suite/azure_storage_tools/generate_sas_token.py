"""Utility for generating Azure Blob SAS URLs with minimal dependencies."""

from datetime import datetime, timedelta
import argparse

from azure.storage.blob import BlobSasPermissions, generate_container_sas


def build_sas_url(account_name: str, account_key: str, container: str, blob: str, hours: int = 1) -> str:
    """Return a full SAS URL with read permissions."""
    sas_token = generate_container_sas(
        account_name=account_name,
        container_name=container,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=hours),
    )
    return f"https://{account_name}.blob.core.windows.net/{container}/{blob}?{sas_token}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-name", required=True)
    parser.add_argument("--account-key", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--blob", required=True)
    parser.add_argument("--hours", type=int, default=1)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(
        build_sas_url(
            account_name=args.account_name,
            account_key=args.account_key,
            container=args.container,
            blob=args.blob,
            hours=args.hours,
        )
    )
