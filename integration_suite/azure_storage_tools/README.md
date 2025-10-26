# Azure Storage Tools

Helper script extracted from automation that distributed reports via Azure Blob
SAS URLs. The original code embedded customer storage credentials; this version
keeps the logic intact but replaces secrets with placeholders.

## Usage
```bash
python generate_sas_token.py \
  --account-name <storage-account> \
  --account-key <base64-key> \
  --container export \
  --blob report.xlsx
```

Wrap the `build_sas_url` function inside your orchestration framework or CLI.
Do not commit real access keys to version control.
