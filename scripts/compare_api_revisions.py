#!/usr/bin/env python3
"""
Sanity-check tool for API revision migrations.

For every endpoint the tap calls, hits it with one revision's ACTUAL query
params and with another revision's ACTUAL query params (mirroring what the
tap itself sends on each branch), then diffs:
  - the top-level attribute keys returned for the first record of each stream
  - whether resource `id` values for the same underlying records are stable
    across revisions (a full-table stream whose IDs change format on upgrade
    will not update existing destination rows in place -- it creates a fresh
    set of rows, breaking any historical joins on the old ID).

Does NOT modify tap_klaviyo code and is safe to re-run for future revision
bumps. Reads credentials from a local config.json (gitignored) and never
prints the api_key or full record bodies -- only attribute-key diffs, ID
samples, and HTTP status codes.

Usage:
    python scripts/compare_api_revisions.py \\
        --config config.json --old 2024-10-15 --new 2026-07-15.pre
"""
import argparse
import json
import requests

BASE = "https://a.klaviyo.com/api"


def headers_for(api_key, version):
    return {"Authorization": f"Klaviyo-API-Key {api_key}", "revision": version}


def fetch(api_key, url, params, version):
    return requests.get(url, params=params, headers=headers_for(api_key, version), timeout=60)


def top_level_keys(record):
    attrs = record.get("attributes", {}) or {}
    return sorted(attrs.keys())


def get_one_metric_id(api_key, version):
    r = fetch(api_key, f"{BASE}/metrics", {}, version)
    r.raise_for_status()
    data = r.json().get("data", [])
    return data[0]["id"] if data else None


def build_cases(metric_id):
    """(name, url, old_params, new_params) -- params mirror each branch's
    actual STREAM_PARAMS_MAP / query construction, not a forced-identical
    query, so we're comparing what the tap really sends on each revision."""
    return [
        (
            "global_exclusions (profiles)",
            f"{BASE}/profiles",
            {
                "filter": "equals(subscriptions.email.marketing.suppression.reason,'HARD_BOUNCE')",
                "additional-fields[profile]": "subscriptions,predictive_analytics",
            },
            {
                "filter": "equals(subscriptions.email.marketing.suppression.reason,'HARD_BOUNCE')",
                "additional-fields[profile]": "subscriptions,predictive_analytics",
            },
        ),
        ("lists", f"{BASE}/lists", {"include": "tags"}, {"include": "tags"}),
        ("metrics", f"{BASE}/metrics", {}, {}),
        (
            "events (incremental)",
            f"{BASE}/events",
            {
                "filter": f"equals(metric_id,\"{metric_id}\"),greater-or-equal(timestamp,1577836800)",
                "include": "profile,metric",
                "sort": "datetime",
            },
            {
                "filter": f"equals(metric_id,\"{metric_id}\"),greater-or-equal(timestamp,1577836800)",
                "include": "profile,metric",
                "sort": "datetime",
            },
        ),
        (
            "campaigns",
            f"{BASE}/campaigns",
            {
                "filter": "equals(messages.channel,'email')",
                "include": "tags,campaign-messages",
            },
            {"fields[campaign]": "created_at,definition,updated_at"},
        ),
    ]


def run(api_key, old_version, new_version):
    metric_id = get_one_metric_id(api_key, new_version)
    results = []

    for name, url, old_params, new_params in build_cases(metric_id):
        print(f"=== {name} ===")
        try:
            r_old = fetch(api_key, url, old_params, old_version)
            r_new = fetch(api_key, url, new_params, new_version)
        except requests.RequestException as e:
            print(f"  REQUEST ERROR: {e}")
            results.append((name, "REQUEST_ERROR", str(e)))
            continue

        print(f"  old params={old_params}")
        print(f"  old ({old_version}): HTTP {r_old.status_code}")
        print(f"  new params={new_params}")
        print(f"  new ({new_version}): HTTP {r_new.status_code}")

        if r_old.status_code != 200 or r_new.status_code != 200:
            for label, resp in (("old", r_old), ("new", r_new)):
                if resp.status_code != 200:
                    print(f"  {label} error body: {resp.text[:300]}")
            results.append((name, "HTTP_ERROR", None))
            print()
            continue

        data_old = r_old.json().get("data", [])
        data_new = r_new.json().get("data", [])

        keys_old = top_level_keys(data_old[0]) if data_old else []
        keys_new = top_level_keys(data_new[0]) if data_new else []
        added = sorted(set(keys_new) - set(keys_old))
        removed = sorted(set(keys_old) - set(keys_new))

        ids_old = sorted(x["id"] for x in data_old)
        ids_new = sorted(x["id"] for x in data_new)
        ids_stable = ids_old == ids_new if (data_old and data_new) else None

        if not data_old or not data_new:
            print(f"  (record counts: old={len(data_old)} new={len(data_new)})")

        if added or removed:
            print(f"  FIELD DIFF -> added: {added} | removed: {removed}")
        else:
            print("  no top-level attribute differences")

        if ids_stable is False:
            print(f"  ID FORMAT CHANGED -> old sample: {ids_old[:3]} | new sample: {ids_new[:3]}")
        elif ids_stable is True:
            print("  ids stable across revisions")

        status = "OK"
        if added or removed:
            status = "FIELD_DIFF"
        if ids_stable is False:
            status = "ID_MISMATCH" if status == "OK" else f"{status}+ID_MISMATCH"
        results.append((name, status, {"added": added, "removed": removed, "ids_stable": ids_stable}))
        print()

    print("\n=== SUMMARY ===")
    for name, status, detail in results:
        print(f"{name}: {status}" + (f" {detail}" if detail else ""))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json", help="Path to a tap config.json with an api_key")
    parser.add_argument("--old", default="2024-10-15", help="Older/master API revision to compare against")
    parser.add_argument("--new", default="2026-07-15.pre", help="Newer/pre-release API revision to compare")
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    run(config["api_key"], args.old, args.new)
