"""Human-readable formatting for audit log entries."""

from __future__ import annotations

from typing import List

from pipewarden.audit import AuditEntry, AuditLog


def format_entry(entry: AuditEntry) -> str:
    status = "OK" if entry.is_valid else "FAIL"
    source_part = f" [{entry.source}]" if entry.source else ""
    return (
        f"[{status}] {entry.table}{source_part} "
        f"rows={entry.row_count} errors={entry.error_count} "
        f"at {entry.run_at}"
    )


def format_audit_log(log: AuditLog) -> str:
    if not log.entries:
        return "No audit entries recorded."
    lines = [format_entry(e) for e in log.entries]
    return "\n".join(lines)


def format_table_history(log: AuditLog, table: str) -> str:
    entries = log.entries_for(table)
    if not entries:
        return f"No audit history for table '{table}'."
    header = f"Audit history for '{table}' ({len(entries)} run(s)):"
    lines = [header] + [f"  {format_entry(e)}" for e in entries]
    return "\n".join(lines)


def audit_summary(log: AuditLog) -> str:
    total = len(log.entries)
    if total == 0:
        return "Audit log is empty."
    passed = sum(1 for e in log.entries if e.is_valid)
    failed = total - passed
    tables = len(log.table_names())
    return (
        f"Audit log: {total} run(s) across {tables} table(s) — "
        f"{passed} passed, {failed} failed."
    )
