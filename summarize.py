#!/usr/bin/env python3
"""Print a Markdown summary of the last build. Used for the Actions job summary."""

import json
import os
import sys

STATS_PATH = os.path.join("reports", "stats.json")


def main():
    if not os.path.exists(STATS_PATH):
        print("## IPTV playlist update\n\nNo stats produced - see the build log.")
        return 0

    with open(STATS_PATH, "r", encoding="utf-8") as fh:
        stats = json.load(fh)

    print("## IPTV playlist update\n")

    if stats.get("aborted"):
        print(
            f"**Aborted by the safety gate.** Would have written "
            f"{stats['would_have_written']:,} channels versus {stats['previous']:,} "
            f"previously (floor {stats['floor']:,}). The existing playlist was left alone."
        )
    else:
        print(
            f"**{stats['total_channels']:,} channels** in {stats['elapsed_seconds']}s - "
            f"{stats['dead_streams']:,} dead, "
            f"{stats.get('kept_geo_blocked', 0):,} region-locked but kept, "
            f"{stats.get('kept_on_grace', 0):,} kept on grace.\n"
        )
        print("| Group | Channels |")
        print("| --- | ---: |")
        for group, count in list(stats.get("groups", {}).items())[:30]:
            print(f"| {group} | {count} |")
        print()

    print("<details><summary>Sources</summary>\n")
    for name, info in stats.get("sources", {}).items():
        if info.get("ok"):
            print(f"- `{name}`: ok ({info.get('channels', 0):,} entries)")
        else:
            print(f"- `{name}`: **FAILED** - {info.get('error', 'unknown')}")
    print("\n</details>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
