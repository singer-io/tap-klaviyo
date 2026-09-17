#!/usr/bin/env python3
"""
Sanity-check tool for API revision migrations.

For every endpoint the tap calls, hits it with one revision's ACTUAL query
params and with another revision's ACTUAL query params (mirroring what the
tap itself sends on each branch), then diffs:
  - the top-level attribute keys returned for the first comparable record of
    each stream
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
CAMPAIGN_MESSAGES_SAMPLE_CAMPAIGNS = 5
# Mirror the tap's actual flat endpoint request size; the smaller campaign
# sample only limits how many legacy nested requests this script makes.
CAMPAIGN_MESSAGES_PAGE_SIZE = 100


def headers_for(api_key, version):
    return {"Authorization": f"Klaviyo-API-Key {api_key}", "revision": version}


def fetch(api_key, url, params, version):
    return requests.get(url, params=params, headers=headers_for(api_key, version), timeout=60)


def top_level_keys(record):
    attrs = record.get("attributes", {}) or {}
    return sorted(attrs.keys())


def campaign_name(record):
    attrs = record.get("attributes", {}) or {}
    definition = attrs.get("definition", {}) or {}
    return definition.get("name") or attrs.get("name")


def campaign_message_name(record):
    attrs = record.get("attributes", {}) or {}
    definition = attrs.get("definition", {}) or {}
    return definition.get("name") or attrs.get("name")


def campaign_message_channel(record):
    if record.get("channel"):
        return record["channel"]
    attrs = record.get("attributes", {}) or {}
    return attrs.get("channel")


def relationship_id(record, relationship_name):
    data = ((record.get("relationships", {}) or {}).get(relationship_name, {}) or {}).get("data")
    if isinstance(data, dict):
        return data.get("id")
    return None


def business_key(case_name, record):
    attrs = record.get("attributes", {}) or {}
    if case_name == "campaigns":
        return (
            campaign_name(record),
            attrs.get("created_at"),
            attrs.get("updated_at"),
        )
    if case_name == "campaign_messages":
        return (
            campaign_message_name(record),
            attrs.get("created") or attrs.get("created_at"),
            attrs.get("updated") or attrs.get("updated_at"),
            campaign_message_channel(record),
        )
    return None


def records_by_business_key(case_name, records):
    keyed_records = {}
    for record in records:
        key = business_key(case_name, record)
        if key is not None and key not in keyed_records:
            keyed_records[key] = record
    return keyed_records


def comparable_records(case_name, data_old, data_new):
    keyed_old = records_by_business_key(case_name, data_old)
    keyed_new = records_by_business_key(case_name, data_new)
    shared_keys = [key for key in keyed_old if key in keyed_new]
    if shared_keys:
        shared_key = shared_keys[0]
        return keyed_old[shared_key], keyed_new[shared_key]
    return (
        data_old[0] if data_old else None,
        data_new[0] if data_new else None,
    )


def compare_ids(case_name, data_old, data_new):
    keyed_old = records_by_business_key(case_name, data_old)
    keyed_new = records_by_business_key(case_name, data_new)
    if keyed_old and keyed_new:
        shared_keys = [key for key in keyed_old if key in keyed_new]
        if not shared_keys:
            return None, []
        mismatches = [
            {
                "business_key": key,
                "old_id": keyed_old[key]["id"],
                "new_id": keyed_new[key]["id"],
            }
            for key in shared_keys
            if keyed_old[key]["id"] != keyed_new[key]["id"]
        ]
        return not mismatches, mismatches[:3]

    ids_old = sorted(x["id"] for x in data_old)
    ids_new = sorted(x["id"] for x in data_new)
    return (ids_old == ids_new if (data_old and data_new) else None), []


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


def get_sample_campaign_ids(api_key, version):
    r = fetch(
        api_key,
        f"{BASE}/campaigns",
        {
            "filter": "equals(messages.channel,'email')",
            "page[size]": CAMPAIGN_MESSAGES_SAMPLE_CAMPAIGNS,
        },
        version,
    )
    r.raise_for_status()
    return [campaign["id"] for campaign in r.json().get("data", [])]


def get_nested_campaign_messages(api_key, version, campaign_ids):
    records = []
    for campaign_id in campaign_ids:
        response = fetch(api_key, f"{BASE}/campaigns/{campaign_id}/campaign-messages", {}, version)
        if response.status_code != 200:
            return response, []
        for record in response.json().get("data", []):
            record["campaign_id"] = campaign_id
            records.append(record)
    return None, records


def variation_lookup(included):
    return {
        record["id"]: record
        for record in included
        if record.get("type") == "campaign-variation"
    }


def variation_channel(variation):
    definition = (variation.get("attributes", {}) or {}).get("definition", {}) or {}
    details = definition.get("details", {}) or {}
    return details.get("channel")


def message_variation_channels(record, variations):
    refs = ((record.get("relationships", {}) or {}).get("campaign-variations", {}) or {}).get("data", [])
    channels = []
    for ref in refs:
        channel = variation_channel(variations.get(ref.get("id"), {}))
        if channel is not None:
            channels.append(channel)
    return channels


def get_flat_campaign_messages(body, channel=None):
    variations = variation_lookup(body.get("included", []))
    messages = []
    for record in body.get("data", []):
        normalized = dict(record)
        channels = message_variation_channels(record, variations)
        if channel is not None:
            if channel not in channels:
                continue
            normalized["channel"] = channel
        elif channels:
            normalized["channel"] = channels[0]
        messages.append(normalized)
    return messages


def run_comparison(name, data_old, data_new):
    old_record, new_record = comparable_records(name, data_old, data_new)
    keys_old = top_level_keys(old_record) if old_record else []
    keys_new = top_level_keys(new_record) if new_record else []
    added = sorted(set(keys_new) - set(keys_old))
    removed = sorted(set(keys_old) - set(keys_new))
    ids_stable, id_mismatches = compare_ids(name, data_old, data_new)

    if not data_old or not data_new:
        print(f"  (record counts: old={len(data_old)} new={len(data_new)})")

    if added or removed:
        print(f"  FIELD DIFF -> added: {added} | removed: {removed}")
    else:
        print("  no top-level attribute differences")

    if ids_stable is False:
        print(f"  ID FORMAT CHANGED -> sample mismatches: {id_mismatches}")
    elif ids_stable is True:
        print("  ids stable across revisions")
    else:
        print("  unable to compare ids for matching business keys")

    status = "OK"
    if added or removed:
        status = "FIELD_DIFF"
    if ids_stable is False:
        status = "ID_MISMATCH" if status == "OK" else f"{status}+ID_MISMATCH"
    return status, {"added": added, "removed": removed, "ids_stable": ids_stable}


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
        status, detail = run_comparison(name, data_old, data_new)
        results.append((name, status, detail))
        print()

    print("=== campaign_messages ===")
    try:
        old_campaign_ids = get_sample_campaign_ids(api_key, old_version)
        old_error, data_old = get_nested_campaign_messages(api_key, old_version, old_campaign_ids)
        r_new = fetch(
            api_key,
            f"{BASE}/campaign-messages",
            {"include": "campaign-variations", "page[size]": CAMPAIGN_MESSAGES_PAGE_SIZE},
            new_version,
        )
    except requests.RequestException as e:
        print(f"  REQUEST ERROR: {e}")
        results.append(("campaign_messages", "REQUEST_ERROR", str(e)))
        print()
    else:
        print(f"  old params={{'campaign_ids': {old_campaign_ids}}}")
        if old_error is not None:
            print(f"  old ({old_version}): HTTP {old_error.status_code}")
            print(f"  old error body: {old_error.text[:300]}")
            results.append(("campaign_messages", "HTTP_ERROR", None))
            print()
        else:
            print(f"  old ({old_version}): fetched nested messages for {len(old_campaign_ids)} campaigns")
            print(
                "  new params={'include': 'campaign-variations', "
                f"'page[size]': {CAMPAIGN_MESSAGES_PAGE_SIZE}}} + "
                "client-side channel filter=email"
            )
            print(f"  new ({new_version}): HTTP {r_new.status_code}")
            if r_new.status_code != 200:
                print(f"  new error body: {r_new.text[:300]}")
                results.append(("campaign_messages", "HTTP_ERROR", None))
                print()
            else:
                status, detail = run_comparison(
                    "campaign_messages",
                    data_old,
                    get_flat_campaign_messages(r_new.json(), channel="email"),
                )
                results.append(("campaign_messages", status, detail))
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
