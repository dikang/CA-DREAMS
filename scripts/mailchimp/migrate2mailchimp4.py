#!/usr/bin/env python3
"""
Migrate HubSpot contacts + M2 Conferences custom-object records to Mailchimp.

What it does:
- Searches HubSpot contacts modified in the last LOOKBACK_HOURS
- Searches HubSpot M2 Conferences records modified in the last LOOKBACK_HOURS
- For both sources:
    - requires non-empty tags field
    - upserts the Mailchimp member by email
    - appends only missing Mailchimp tags
    - does not remove existing Mailchimp tags
"""

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import requests


# =========================
# Configuration
# =========================

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "1"))

# Contact source (HubSpot standard object)
HUBSPOT_CONTACT_TAGS_PROPERTY = os.getenv("HUBSPOT_CONTACT_TAGS_PROPERTY", "tags")
HUBSPOT_CONTACT_EMAIL_PROPERTY = os.getenv("HUBSPOT_CONTACT_EMAIL_PROPERTY", "email")
HUBSPOT_CONTACT_FIRSTNAME_PROPERTY = os.getenv("HUBSPOT_CONTACT_FIRSTNAME_PROPERTY", "firstname")
HUBSPOT_CONTACT_LASTNAME_PROPERTY = os.getenv("HUBSPOT_CONTACT_LASTNAME_PROPERTY", "lastname")
HUBSPOT_CONTACT_MODIFIED_PROPERTY = os.getenv("HUBSPOT_CONTACT_MODIFIED_PROPERTY", "lastmodifieddate")

# M2 Conferences source (custom object)
# IMPORTANT: set this to the custom object's objectTypeId (e.g. 2-12345678)
# or the fully qualified name, not the display label "M2 Conferences".
HUBSPOT_M2_OBJECT_TYPE = "2-231331339"
HUBSPOT_M2_TAGS_PROPERTY = os.getenv("HUBSPOT_M2_TAGS_PROPERTY", "mailchimp_tags")
HUBSPOT_M2_EMAIL_PROPERTY = os.getenv("HUBSPOT_M2_EMAIL_PROPERTY", "email")
HUBSPOT_M2_FIRSTNAME_PROPERTY = os.getenv("HUBSPOT_M2_FIRSTNAME_PROPERTY", "first_name")
HUBSPOT_M2_LASTNAME_PROPERTY = os.getenv("HUBSPOT_M2_LASTNAME_PROPERTY", "last_name")
HUBSPOT_M2_MODIFIED_PROPERTY = os.getenv("HUBSPOT_M2_MODIFIED_PROPERTY", "hs_lastmodifieddate")

# If a tags field contains multiple tags, split on these separators.
TAG_SPLIT_REGEX = re.compile(r"[,\n;|]+")

NEW_MEMBER_STATUS_IF_NEW = os.getenv("NEW_MEMBER_STATUS_IF_NEW", "subscribed")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

MAILCHIMP_DC = "us5"
MAILCHIMP_AUDIENCE_ID = "ba9097c194"


def require_env(name, value):
    if not value:
        raise RuntimeError("Missing required environment variable: {}".format(name))
    return value


HUBSPOT_TOKEN = require_env("HUBSPOT_TOKEN", os.environ.get("HUBSPOT_TOKEN"))
MAILCHIMP_API_KEY = require_env("MAILCHIMP_API_KEY", os.environ.get("MAILCHIMP_API_KEY"))
MAILCHIMP_AUDIENCE_ID = require_env("MAILCHIMP_AUDIENCE_ID", MAILCHIMP_AUDIENCE_ID)

if not MAILCHIMP_DC:
    if "-" not in MAILCHIMP_API_KEY:
        raise RuntimeError("MAILCHIMP_API_KEY must end with a data center suffix like -us5")
    MAILCHIMP_DC = MAILCHIMP_API_KEY.split("-")[-1]


# =========================
# HTTP helpers
# =========================

def hs_headers():
    return {
        "Authorization": "Bearer {}".format(HUBSPOT_TOKEN),
        "Content-Type": "application/json",
    }


def mc_auth():
    return ("anystring", MAILCHIMP_API_KEY)


def mc_base_url():
    return "https://{}.api.mailchimp.com/3.0".format(MAILCHIMP_DC)


def subscriber_hash(email):
    return hashlib.md5(email.strip().lower().encode("utf-8")).hexdigest()


def split_tags(raw_value):
    if not raw_value:
        return []

    parts = [p.strip() for p in TAG_SPLIT_REGEX.split(raw_value) if p.strip()]

    seen = set()
    out = []
    for tag in parts:
        key = tag.lower()
        if key not in seen:
            seen.add(key)
            out.append(tag)
    return out


def hubspot_search_objects(object_type, modified_property, property_names):
    """
    Generic HubSpot CRM search for any object type.
    object_type:
      - 'contacts' for contacts
      - custom object type ID like '2-12345678'
      - or the custom object's fully qualified name
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    url = "https://api.hubapi.com/crm/v3/objects/{}/search".format(object_type)

    payload = {
        "limit": 100,
        "properties": property_names,
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": modified_property,
                        "operator": "GTE",
                        "value": str(cutoff_ms),
                    }
                ]
            }
        ],
    }

    results = []
    after = None

    while True:
        if after is not None:
            payload["after"] = after

        resp = requests.post(url, headers=hs_headers(), json=payload, timeout=30)

        if resp.status_code != 200:
            print("STATUS:", resp.status_code)
            print("BODY:", resp.text)
            print("PAYLOAD:", json.dumps(payload, indent=2))
            resp.raise_for_status()

        data = resp.json()
        results.extend(data.get("results", []))

        paging = data.get("paging", {})
        next_after = paging.get("next", {}).get("after")
        if not next_after:
            break
        after = next_after

    return results


def mailchimp_upsert_member(email, first_name, last_name):
    url = "{}/lists/{}/members/{}".format(
        mc_base_url(), MAILCHIMP_AUDIENCE_ID, subscriber_hash(email)
    )

    payload = {
        "email_address": email,
        "status_if_new": NEW_MEMBER_STATUS_IF_NEW,
        "merge_fields": {
            "FNAME": first_name or "",
            "LNAME": last_name or "",
            "SOURCE": "HubSpot",   # optional; keep if you want it
        },
    }

    if DRY_RUN:
        print("[DRY RUN] Upsert member:", json.dumps(payload, indent=2))
        return

    resp = requests.put(url, auth=mc_auth(), json=payload, timeout=30)

    if resp.status_code not in (200, 201):
        print("MAILCHIMP UPSERT FAILED")
        print("STATUS:", resp.status_code)
        print("BODY:", resp.text)
        print("PAYLOAD:", json.dumps(payload, indent=2))
        resp.raise_for_status()

#    print("Mailchimp upsert OK for {}".format(email))


def mailchimp_get_member(email):
    url = "{}/lists/{}/members/{}".format(
        mc_base_url(), MAILCHIMP_AUDIENCE_ID, subscriber_hash(email)
    )
    resp = requests.get(url, auth=mc_auth(), timeout=30)

    if resp.status_code == 404:
        return None

    resp.raise_for_status()
    return resp.json()


def mailchimp_get_member_tags(email):
    url = "{}/lists/{}/members/{}/tags".format(
        mc_base_url(), MAILCHIMP_AUDIENCE_ID, subscriber_hash(email)
    )
    resp = requests.get(url, auth=mc_auth(), timeout=30)

    if resp.status_code == 404:
        return []

    resp.raise_for_status()
    data = resp.json()
    tags = data.get("tags", [])

    out = []
    for item in tags:
        name = item.get("name")
        if name:
            out.append(name)
    return out


def mailchimp_add_tags(email, tags_to_add):
    if not tags_to_add:
        return

    url = "{}/lists/{}/members/{}/tags".format(
        mc_base_url(), MAILCHIMP_AUDIENCE_ID, subscriber_hash(email)
    )
    payload = {
        "tags": [{"name": tag, "status": "active"} for tag in tags_to_add]
    }

    if DRY_RUN:
        print("[DRY RUN] Add tags for {}: {}".format(email, json.dumps(payload, indent=2)))
        return

    resp = requests.post(url, auth=mc_auth(), json=payload, timeout=30)
    resp.raise_for_status()


def sync_one_record(props, source_name, email_property, first_name_property, last_name_property, tags_property):
    email = (props.get(email_property) or "").strip()
    first_name = (props.get(first_name_property) or "").strip()
    last_name = (props.get(last_name_property) or "").strip()
    raw_tags = props.get(tags_property) or ""

    pstr = str(email) + str(", ") + str(raw_tags)

    if not email:
        print("Not an email, skip!")
        return 0

    if not raw_tags.strip():
#        print("Skipping {} from {}: empty tags field".format(email, source_name))
        print("No tag, skip!")
        return 0

    print("sync_one_record: %s" % (pstr))

    source_tags = split_tags(raw_tags)

#    print("\n--- Syncing {} ({}) ---".format(email, source_name))
#    print("Raw tags: {!r}".format(raw_tags))
#    print("Parsed tags: {}".format(source_tags))

    before_member = mailchimp_get_member(email)
#    if before_member is None:
#        print("Mailchimp before upsert: NOT FOUND")
#    else:
#        print("Mailchimp before upsert: FOUND, status={}".format(before_member.get("status")))

    mailchimp_upsert_member(email, first_name, last_name)

    after_member = mailchimp_get_member(email)
#    if after_member is None:
#        print("Mailchimp after upsert: NOT FOUND")
#    else:
#        print("Mailchimp after upsert: FOUND, status={}".format(after_member.get("status")))

    current_tags = set(tag.lower() for tag in mailchimp_get_member_tags(email))
#    print("Mailchimp current tags: {}".format(sorted(current_tags)))

    missing_tags = [tag for tag in source_tags if tag.lower() not in current_tags]

    if missing_tags:
        mailchimp_add_tags(email, missing_tags)
        print("Record: %s, %s, %s; tag: %s" % (first_name, last_name, email, missing_tags))
#        print("Added tags: {}".format(missing_tags))
        return 1
    else:
        print("No new tags needed")
        return 0


def main():
    print("Searching HubSpot contacts...")
    print("  Modified within last {} hours".format(LOOKBACK_HOURS))

    contacts = hubspot_search_objects(
        "contacts",
        HUBSPOT_CONTACT_MODIFIED_PROPERTY,
        [
            HUBSPOT_CONTACT_EMAIL_PROPERTY,
            HUBSPOT_CONTACT_FIRSTNAME_PROPERTY,
            HUBSPOT_CONTACT_LASTNAME_PROPERTY,
            HUBSPOT_CONTACT_TAGS_PROPERTY,
        ],
    )
    print("Found {} contact(s)".format(len(contacts)))

    count = 0
    for contact in contacts:
        try:
            props = contact.get("properties", {})
            count = count + sync_one_record(
                props,
                "Contact",
                HUBSPOT_CONTACT_EMAIL_PROPERTY,
                HUBSPOT_CONTACT_FIRSTNAME_PROPERTY,
                HUBSPOT_CONTACT_LASTNAME_PROPERTY,
                HUBSPOT_CONTACT_TAGS_PROPERTY,
            )
        except Exception as e:
            props = contact.get("properties", {})
            print("ERROR syncing contact {}: {}".format(props.get("email", "(no email)"), e), file=sys.stderr)
    print("[%d] records from contacts have been udpated" % count)

    if HUBSPOT_M2_OBJECT_TYPE:
        print("\nSearching M2 Conferences custom object...")
#        print("  object type: {}".format(HUBSPOT_M2_OBJECT_TYPE))
        m2_records = hubspot_search_objects(
            HUBSPOT_M2_OBJECT_TYPE,
            HUBSPOT_M2_MODIFIED_PROPERTY,
            [
                HUBSPOT_M2_EMAIL_PROPERTY,
                HUBSPOT_M2_FIRSTNAME_PROPERTY,
                HUBSPOT_M2_LASTNAME_PROPERTY,
                HUBSPOT_M2_TAGS_PROPERTY,
            ],
        )
        print("Found {} M2 Conference record(s)".format(len(m2_records)))

        count = 0
        for record in m2_records:
            try:
                props = record.get("properties", {})
                count = count + sync_one_record(
                    props,
                    "M2 Conferences",
                    HUBSPOT_M2_EMAIL_PROPERTY,
                    HUBSPOT_M2_FIRSTNAME_PROPERTY,
                    HUBSPOT_M2_LASTNAME_PROPERTY,
                    HUBSPOT_M2_TAGS_PROPERTY,
                )
            except Exception as e:
                props = record.get("properties", {})
                print("ERROR syncing M2 record {}: {}".format(props.get(HUBSPOT_M2_EMAIL_PROPERTY, "(no email)"), e), file=sys.stderr)
        print("[%d] records from M2 Conferences have been udpated" % count)
    else:
        print("\nSkipping M2 Conferences because HUBSPOT_M2_OBJECT_TYPE is not set.")


if __name__ == "__main__":
    main()
