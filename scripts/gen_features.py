#!/usr/bin/env python3
"""
gen_features.py — Generate FEATURES.md from executor/features.go

Usage:
    python3 scripts/gen_features.py > FEATURES.md
"""

import re
import sys
from pathlib import Path
from collections import OrderedDict

FEATURES_GO = Path(__file__).parent.parent / "executor" / "features.go"

STATUS_ICON = {
    "supported":   "✅ Supported",
    "unsupported": "❌ Unsupported",
    "partial":     "⚠️ Partial",
}

# Category display order
CATEGORY_ORDER = [
    "Storage Engine",
    "DML",
    "DDL",
    "SQL Statement",
    "Transaction",
    "Stored Program",
    "Partitioning",
    "Index",
    "Auth",
    "Prepared Statement",
    "Metadata",
    "Replication",
    "CTE",
    "Function",
    "Charset",
]


def parse_features(path: Path):
    text = path.read_text(encoding="utf-8")

    # Match each Feature{...} literal (may span multiple lines)
    block_re = re.compile(
        r'\{Name:\s*"([^"]*)"'
        r',\s*Category:\s*"([^"]*)"'
        r',\s*Status:\s*(\w+)'
        r',\s*Description:\s*"([^"]*)"\s*\}',
        re.MULTILINE,
    )

    features = []
    for m in block_re.finditer(text):
        name, category, status_const, description = m.groups()
        # Map Go constant names to string values
        status_map = {
            "Supported": "supported",
            "Unsupported": "unsupported",
            "Partial": "partial",
        }
        status = status_map.get(status_const, status_const.lower())
        features.append({
            "name": name,
            "category": category,
            "status": status,
            "description": description,
        })
    return features


def group_by_category(features):
    groups = OrderedDict()
    # Insert in preferred order first
    for cat in CATEGORY_ORDER:
        groups[cat] = []
    # Then any leftover categories not in the order list
    for f in features:
        cat = f["category"]
        if cat not in groups:
            groups[cat] = []
        groups[cat].append(f)
    # Remove empty categories
    return {k: v for k, v in groups.items() if v}


def render_table(features):
    lines = []
    lines.append("| Feature | Status | Description |")
    lines.append("|---|---|---|")
    for f in features:
        icon = STATUS_ICON.get(f["status"], f["status"])
        desc = f["description"].replace("|", "\\|")
        lines.append(f'| {f["name"]} | {icon} | {desc} |')
    return "\n".join(lines)


def main():
    features = parse_features(FEATURES_GO)
    if not features:
        print("ERROR: no features parsed from", FEATURES_GO, file=sys.stderr)
        sys.exit(1)

    groups = group_by_category(features)

    total = len(features)
    supported = sum(1 for f in features if f["status"] == "supported")
    partial = sum(1 for f in features if f["status"] == "partial")
    unsupported = sum(1 for f in features if f["status"] == "unsupported")

    out = []
    out.append("# mylite Feature Support")
    out.append("")
    out.append("Auto-generated from `executor/features.go`. Do not edit manually.")
    out.append("")
    out.append(
        f"**Summary:** {total} features — "
        f"✅ {supported} supported, "
        f"⚠️ {partial} partial, "
        f"❌ {unsupported} unsupported"
    )
    out.append("")

    # Table of contents
    out.append("## Categories")
    out.append("")
    for cat in groups:
        anchor = cat.lower().replace(" ", "-").replace("/", "").replace("(", "").replace(")", "")
        count = len(groups[cat])
        out.append(f"- [{cat}](#{anchor}) ({count})")
    out.append("")

    for cat, feats in groups.items():
        out.append(f"## {cat}")
        out.append("")
        out.append(render_table(feats))
        out.append("")

    print("\n".join(out))


if __name__ == "__main__":
    main()
