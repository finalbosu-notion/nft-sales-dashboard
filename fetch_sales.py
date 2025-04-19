#!/usr/bin/env python3
"""Magic Eden (EVM) → Notion sales importer
Environment variables:
  NOTION_TOKEN, SALES_DB_ID, SUMMARY_DB_ID, ME_COLLECTION, ME_CHAIN, UNIT_DIVISOR
"""
import os, requests, datetime, pytz, logging, sys

NOTION_TOKEN  = os.getenv("NOTION_TOKEN")
SALES_DB      = os.getenv("SALES_DB_ID")
SUMMARY_DB    = os.getenv("SUMMARY_DB_ID")
COLLECTION    = os.getenv("ME_COLLECTION")
CHAIN         = os.getenv("ME_CHAIN", "ethereum")
UNIT_DIVISOR  = int(os.getenv("UNIT_DIVISOR", "1000000000000000000"))

if not all([NOTION_TOKEN, SALES_DB, SUMMARY_DB, COLLECTION]):
    sys.exit("Missing environment variables.")

API = f"https://api-mainnet.magiceden.dev/v3/rtp/{CHAIN}/collections/{COLLECTION}/activities"

HDRS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

TZ = pytz.timezone("Europe/Amsterdam")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def latest_sale_ts():
    url = f"https://api.notion.com/v1/databases/{SALES_DB}/query"
    payload = {"sorts":[{"property":"Sale Time","direction":"descending"}],"page_size":1}
    r = requests.post(url, headers=HDRS, json=payload, timeout=30)
    r.raise_for_status()
    res = r.json().get("results", [])
    if not res:
        return 0
    iso = res[0]["properties"]["Sale Time"]["date"]["start"]
    return int(datetime.datetime.fromisoformat(iso).timestamp())

def fetch_sales(since):
    params = {"type":"sale","limit":200}
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    return [s for s in r.json() if s.get("blockTime",0) > since]

def ensure_summary(date_iso):
    url = f"https://api.notion.com/v1/databases/{SUMMARY_DB}/query"
    payload = {"filter":{"property":"Date","date":{"equals":date_iso}}}
    res = requests.post(url, json=payload, headers=HDRS, timeout=30).json()["results"]
    if res:
        return res[0]["id"]
    create = {
        "parent":{"database_id":SUMMARY_DB},
        "properties":{
            "Name":{"title":[{"text":{"content":date_iso}}]},
            "Date":{"date":{"start":date_iso}},
            "Royalty %":{"number":5}
        }
    }
    r = requests.post("https://api.notion.com/v1/pages", json=create, headers=HDRS, timeout=30)
    r.raise_for_status()
    return r.json()["id"]

def create_sale(sale, summary_id):
    ts = datetime.datetime.fromtimestamp(sale["blockTime"], TZ)
    raw_price = sale.get("price") or sale.get("tx",{}).get("price",0)
    price_eth = raw_price / UNIT_DIVISOR
    page = {
        "parent":{"database_id":SALES_DB},
        "properties":{
            "NFT / Tx Hash":{"title":[{"text":{"content":sale["signature"][:12]}}]},
            "Sale Time":{"date":{"start":ts.isoformat()}},
            "Price ETH":{"number":price_eth},
            "Summary":{"relation":[{"id":summary_id}]}
        }
    }
    r = requests.post("https://api.notion.com/v1/pages", json=page, headers=HDRS, timeout=30)
    r.raise_for_status()

def main():
    since = latest_sale_ts()
    sales = fetch_sales(since)
    if not sales:
        logging.info("No new sales.")
        return
    for s in sorted(sales, key=lambda x: x["blockTime"]):
        date_iso = datetime.datetime.fromtimestamp(s["blockTime"], TZ).date().isoformat()
        sid = ensure_summary(date_iso)
        create_sale(s, sid)
    logging.info("Imported %d sales.", len(sales))

if __name__ == "__main__":
    main()
