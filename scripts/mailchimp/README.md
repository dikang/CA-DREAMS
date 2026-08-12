# Scripts to update Mailchimp contacts from HubSpot

This scripts reads the records in HubSpot contacts and a custom object 'M2 Conferences'.
If the record has non-empty 'mailchimp_tags', the record and the 'mailchimp_tags' are updated in Mailchimp audience.
This scripts looks at the updated records in HubSpot for the past 48 hours.
This script must run periodically in less than 48 hours to avoid missing records.

## ▶️ Usage

```bash
python3 migrate2mailchimp4-git.py

