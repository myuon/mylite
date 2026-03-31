#!/usr/bin/env python3
"""Safe skiplist management for mylite MTR test suite."""

import argparse
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIPLIST_PATH = os.path.join(REPO_ROOT, "cmd", "mtrrun", "skiplist.json")
TESTDATA_BASE = os.path.join(REPO_ROOT, "testdata", "dolt-mysql-tests", "files", "suite")


def load_skiplist():
    with open(SKIPLIST_PATH, "r") as f:
        return json.load(f)


def save_skiplist(data):
    text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    # Validate before writing
    json.loads(text)
    with open(SKIPLIST_PATH, "w") as f:
        f.write(text)


def cmd_remove(args):
    data = load_skiplist()
    original_count = len(data["skips"])
    to_remove = set(args.tests)

    found = set()
    remaining = []
    for entry in data["skips"]:
        if entry["test"] in to_remove:
            found.add(entry["test"])
        else:
            remaining.append(entry)

    not_found = to_remove - found
    for name in sorted(not_found):
        print(f"WARNING: '{name}' was not in the skiplist (ignored)")

    removed_count = original_count - len(remaining)
    data["skips"] = remaining
    save_skiplist(data)

    print(f"Removed {removed_count} entries. Remaining: {len(remaining)}")
    if removed_count > 10:
        print(f"WARNING: Removed {removed_count} entries (>10). This may be an error.")


def cmd_add(args):
    data = load_skiplist()

    # Check if already exists (idempotent)
    for entry in data["skips"]:
        if entry["test"] == args.test:
            print(f"WARNING: '{args.test}' is already in the skiplist (not adding duplicate)")
            return

    data["skips"].append({"test": args.test, "reason": args.reason})
    save_skiplist(data)
    print(f"Added '{args.test}'. Total entries: {len(data['skips'])}")


def cmd_count(args):
    data = load_skiplist()
    entries = data["skips"]
    if args.suite:
        entries = [e for e in entries if e["test"].startswith(args.suite + "/")]
    print(len(entries))


def cmd_list(args):
    data = load_skiplist()
    entries = data["skips"]
    if args.suite:
        entries = [e for e in entries if e["test"].startswith(args.suite + "/")]
    for entry in entries:
        print(f"{entry['test']}\t{entry['reason']}")


def cmd_validate(args):
    data = load_skiplist()
    missing = 0
    for entry in data["skips"]:
        test = entry["test"]
        parts = test.split("/", 1)
        if len(parts) != 2:
            print(f"INVALID FORMAT: '{test}' (expected suite/testname)")
            missing += 1
            continue
        suite, testname = parts
        test_file = os.path.join(TESTDATA_BASE, suite, "t", testname + ".test")
        if not os.path.exists(test_file):
            print(f"MISSING: {test} -> {test_file}")
            missing += 1
    if missing == 0:
        print("All entries have corresponding test files.")
    else:
        print(f"\n{missing} entries have missing test files.")


def main():
    parser = argparse.ArgumentParser(description="Manage mylite MTR skiplist")
    sub = parser.add_subparsers(dest="command", required=True)

    p_remove = sub.add_parser("remove", help="Remove tests from skiplist")
    p_remove.add_argument("tests", nargs="+", help="Test names to remove (suite/testname)")

    p_add = sub.add_parser("add", help="Add a test to the skiplist")
    p_add.add_argument("test", help="Test name (suite/testname)")
    p_add.add_argument("reason", help="Reason for skipping")

    p_count = sub.add_parser("count", help="Count skiplist entries")
    p_count.add_argument("suite", nargs="?", help="Filter by suite name")

    p_list = sub.add_parser("list", help="List skiplist entries")
    p_list.add_argument("suite", nargs="?", help="Filter by suite name")

    sub.add_parser("validate", help="Check for entries with missing test files")

    args = parser.parse_args()
    {"remove": cmd_remove, "add": cmd_add, "count": cmd_count,
     "list": cmd_list, "validate": cmd_validate}[args.command](args)


if __name__ == "__main__":
    main()
