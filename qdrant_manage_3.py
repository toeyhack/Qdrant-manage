#!/usr/bin/env python3
"""
Qdrant Vector Database Management Tool (v3)
Features:
  ✅ List all collections
  ✅ View summary of a collection (group by doc_id)
  ✅ Inspect full payloads with --limit
  ✅ Delete specific vectors by field/value
  ✅ Delete all vectors in a collection safely
"""

import argparse
import requests
import json
import sys

def make_url(host, port, path, https=False):
    protocol = "https" if https else "http"
    return f"{protocol}://{host}:{port}{path}"

# -------------------------------
# Collection Management
# -------------------------------
def list_collections(args, headers):
    url = make_url(args.host, args.port, "/collections", args.https)
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f"❌ Failed to list collections: {r.text}")
        return
    data = r.json()
    print("\n📦 Collections found:")
    for c in data.get("result", {}).get("collections", []):
        print(f" - {c['name']}")

# -------------------------------
# View (Summary)
# -------------------------------
def view_collection(args, headers):
    if not args.collection:
        print("❌ Please specify --collection")
        return
    url = make_url(args.host, args.port, f"/collections/{args.collection}/points/scroll", args.https)
    payload = {"limit": args.batch_size, "with_payload": True, "with_vector": False}
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code != 200:
        print(f"❌ Failed to fetch data: {r.text}")
        return
    result = r.json().get("result", {})
    points = result.get("points", [])
    print(f"\n🔍 Previewing collection: {args.collection}")
    if not points:
        print("No points found.")
        return

    doc_map = {}
    for p in points:
        payload = p.get("payload", {})
        doc_id = payload.get("doc_id") or payload.get("source") or payload.get("filename") or "UNKNOWN"
        doc_map.setdefault(doc_id, []).append(payload.get("text") or "")

    print(f"\nFound {len(doc_map)} document(s):\n")
    for doc_id, chunks in doc_map.items():
        sample = (chunks[0][:100] + "...") if chunks and chunks[0] else "(no text)"
        print(f"📄 doc_id: {doc_id}\n   chunks: {len(chunks)}\n   sample: {sample}\n")

# -------------------------------
# Inspect (Full Payload)
# -------------------------------
def inspect_collection(args, headers):
    if not args.collection:
        print("❌ Please specify --collection")
        return
    url = make_url(args.host, args.port, f"/collections/{args.collection}/points/scroll", args.https)
    payload = {"limit": args.limit, "with_payload": True, "with_vector": False}
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code != 200:
        print(f"❌ Failed to inspect: {r.text}")
        return
    result = r.json().get("result", {})
    points = result.get("points", [])
    print(f"\n🔎 Inspecting collection: {args.collection} (showing up to {args.limit})\n")
    if not points:
        print("No vectors found.")
        return

    for i, p in enumerate(points, 1):
        print(f"🧠 Vector #{i} — id: {p.get('id')}")
        payload = p.get("payload", {})
        for k, v in payload.items():
            val = v
            if isinstance(v, str) and len(v) > 150:
                val = v[:150] + "..."
            print(f"   {k}: {val}")
        print("")

# -------------------------------
# Delete all vectors in collection
# -------------------------------
def delete_all(args, headers):
    if not args.collection:
        print("❌ Please specify --collection")
        return
    if not args.yes:
        confirm = input(f"⚠️  Confirm delete ALL vectors in '{args.collection}'? (y/N): ").lower()
        if confirm != "y":
            print("Cancelled.")
            return
    url = make_url(args.host, args.port, f"/collections/{args.collection}/points/delete", args.https)
    payload = {"filter": {}}
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 200:
        print(f"🗑️  All vectors deleted from collection '{args.collection}'.")
    else:
        print(f"❌ Delete failed: {r.text}")

# -------------------------------
# Delete chunk by field/value
# -------------------------------
def delete_chunk(args, headers):
    if not args.collection:
        print("❌ Please specify --collection")
        return
    if not args.chunk_field or not args.value:
        print("❌ Must specify both --chunk-field and --value for targeted delete.")
        return

    scroll_url = make_url(args.host, args.port, f"/collections/{args.collection}/points/scroll", args.https)
    payload = {
        "filter": {"must": [{"key": args.chunk_field, "match": {"value": args.value}}]},
        "with_payload": False,
        "with_vector": False,
        "limit": 1000
    }
    r = requests.post(scroll_url, headers=headers, json=payload)
    if r.status_code != 200:
        print(f"❌ Query failed: {r.text}")
        return

    result = r.json().get("result", {})
    points = result.get("points", [])
    if not points:
        print(f"⚠️ No matching vectors found for {args.chunk_field} = {args.value}")
        return

    ids = [p["id"] for p in points]
    print(f"Found {len(ids)} vectors for {args.chunk_field} = {args.value}")

    if not args.yes:
        confirm = input(f"⚠️  Confirm delete {len(ids)} vectors? (y/N): ").lower()
        if confirm != "y":
            print("Cancelled.")
            return

    delete_url = make_url(args.host, args.port, f"/collections/{args.collection}/points/delete", args.https)
    del_payload = {"points": ids}
    r = requests.post(delete_url, headers=headers, json=del_payload)
    if r.status_code == 200:
        print(f"🗑️  Successfully deleted {len(ids)} vectors.")
    else:
        print(f"❌ Delete failed: {r.text}")

# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Qdrant Vector Database Management Script")

    # Connection
    parser.add_argument("--host", default="localhost", help="Qdrant host")
    parser.add_argument("--port", type=int, default=6333, help="Qdrant port")
    parser.add_argument("--https", action="store_true", help="Use HTTPS")
    parser.add_argument("--api-key", help="Qdrant API key (optional)")

    # Actions
    parser.add_argument("--list", action="store_true", help="List all collections")
    parser.add_argument("--view", action="store_true", help="Preview summary by document")
    parser.add_argument("--inspect", action="store_true", help="Inspect full payload details")
    parser.add_argument("--delete-all", action="store_true", help="Delete ALL vectors in collection")
    parser.add_argument("--delete-chunk", action="store_true", help="Delete specific vectors by field/value")

    # Parameters
    parser.add_argument("--collection", help="Collection name")
    parser.add_argument("--chunk-field", help="Payload field to filter by (e.g., doc_id)")
    parser.add_argument("--value", help="Value to match for deletion")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for scroll")
    parser.add_argument("--limit", type=int, default=10, help="Number of vectors to show in --inspect")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")

    args = parser.parse_args()

    headers = {"Content-Type": "application/json"}
    if args.api_key:
        headers["api-key"] = args.api_key

    if args.list:
        list_collections(args, headers)
    elif args.view:
        view_collection(args, headers)
    elif args.inspect:
        inspect_collection(args, headers)
    elif args.delete_all:
        delete_all(args, headers)
    elif args.delete_chunk:
        delete_chunk(args, headers)
    else:
        parser.print_help()
