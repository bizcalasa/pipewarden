"""Human-readable formatting for checksum reports."""

from __future__ import annotations

from pipewarden.checksum import ChecksumRecord, ChecksumReport


def format_record(record: ChecksumRecord) -> str:
    lines = [f"Table : {record.table}"]
    lines.append(f"  Schema checksum : {record.schema_checksum[:16]}...")
    if record.data_checksum:
        lines.append(f"  Data checksum   : {record.data_checksum[:16]}...")
    else:
        lines.append("  Data checksum   : n/a")
    return "\n".join(lines)


def format_checksum_report(report: ChecksumReport) -> str:
    if not report.records:
        return "Checksum report: no entries."
    parts = ["=== Checksum Report ==="]
    for rec in report.records:
        parts.append(format_record(rec))
    return "\n".join(parts)


def checksum_summary(report: ChecksumReport) -> str:
    n = len(report.records)
    with_data = sum(1 for r in report.records if r.data_checksum)
    return (
        f"Checksum report: {n} table(s) tracked, "
        f"{with_data} with data checksums."
    )
