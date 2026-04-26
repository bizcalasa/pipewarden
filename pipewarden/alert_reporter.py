"""Formatting helpers for alert output."""

from typing import List
from pipewarden.alert import Alert


def format_alert(alert: Alert) -> str:
    """Return a single-line string describing a fired alert."""
    return f"[ALERT] ({alert.rule_name}) table='{alert.table}' — {alert.message}"


def format_alert_block(alerts: List[Alert]) -> str:
    """Return a formatted block listing all alerts, or an OK message."""
    if not alerts:
        return "No alerts fired."
    lines = [f"Alerts fired: {len(alerts)}"]
    for alert in alerts:
        lines.append(f"  {format_alert(alert)}")
    return "\n".join(lines)


def alert_summary(alerts: List[Alert]) -> str:
    """Return a one-line summary suitable for pipeline logs."""
    if not alerts:
        return "alert_summary: OK — 0 alerts"
    tables = sorted({a.table for a in alerts})
    table_list = ", ".join(tables)
    return (
        f"alert_summary: {len(alerts)} alert(s) across "
        f"{len(tables)} table(s): {table_list}"
    )
