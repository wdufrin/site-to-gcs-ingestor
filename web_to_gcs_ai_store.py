#!/usr/bin/env python3
"""
Website to GCS AI Datastore Ingestor (CLI Interface)
===================================================
Thin command-line wrapper around main.execute_ingestion().

This file deliberately contains NO pipeline logic. The crawl -> stage -> upload
-> index pipeline lives in exactly one place (main.execute_ingestion) so that a
fix applied there takes effect for the CLI, the web UI and the Cloud Run
Function simultaneously. Previously each of those three had its own copy.
"""

import argparse
import json
import logging
import sys

from main import execute_ingestion, LOCAL_AUTH_INFO

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("web_to_gcs")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Crawl website(s), convert to AI Markdown, upload to GCS, and configure Gemini Enterprise / Vertex AI Search."
    )
    parser.add_argument("--url", help="Single website URL to crawl (e.g., https://docs.example.com)")
    parser.add_argument("--targets-file", help="Path to config.json or gs:// URI containing multiple targets")
    parser.add_argument("--gcs-bucket", help="Target GCS bucket name (omit with --dry-run)")
    parser.add_argument("--gcs-prefix", default="website_datastore", help="Folder prefix in GCS bucket (default: 'website_datastore')")
    parser.add_argument("--output-dir", default="./output_datastore", help="Local directory for generated files (used with --dry-run)")
    parser.add_argument("--project-id", help="Google Cloud Project ID")
    parser.add_argument("--location", default="global", help="Discovery Engine location (default: global)")
    parser.add_argument("--data-store-id", help="Vertex AI Search / Discovery Engine Data Store ID")
    parser.add_argument("--engine-id", help="Gemini Enterprise Search/Agent App (Engine) ID to attach")
    parser.add_argument("--max-pages", type=int, default=50, help="Default max pages per site")
    parser.add_argument("--max-depth", type=int, default=3, help="Default link traversal depth per site")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between requests in seconds")
    parser.add_argument(
        "--reconciliation-mode",
        choices=["auto", "incremental", "full"],
        default="auto",
        help="Datastore import reconciliation mode: 'auto' (INCREMENTAL if capped/errors, FULL if complete), 'incremental' (upsert only, never purge), or 'full' (replace index & purge unlisted pages)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Crawl and convert locally without modifying cloud resources")
    parser.add_argument("--verbose", action="store_true", help="Print a line for every URL attempted")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if not args.url and not args.targets_file:
        build_parser().error("Either --url or --targets-file must be provided.")

    config = {
        "url": args.url,
        "targets_config_uri": args.targets_file,
        "gcs_bucket": args.gcs_bucket,
        "gcs_prefix": args.gcs_prefix,
        "project_id": args.project_id,
        "location": args.location,
        "data_store_id": args.data_store_id,
        "engine_id": args.engine_id,
        "max_pages": args.max_pages,
        "max_depth": args.max_depth,
        "delay": args.delay,
        "reconciliation_mode": args.reconciliation_mode,
        "dry_run": args.dry_run,
    }

    def on_progress(event):
        if not args.verbose:
            return
        kind = event.get("kind")
        if kind == "page":
            print(f"  [ok]   {event['url']}")
        elif kind in ("skipped", "error"):
            print(f"  [skip] {event['url']}  ({event.get('reason')})")

    # Keep artifacts on disk for dry-runs so the operator can inspect them.
    staging = args.output_dir if args.dry_run else None

    try:
        result = execute_ingestion(config, progress_callback=on_progress, staging_dir=staging)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return 1

    pages = result["pages_count"]
    failed = result.get("failed_count", 0)

    print()
    print(f"Pages indexed : {pages}")
    if failed:
        print(f"Pages failed  : {failed}  (NOT in the datastore)")
    print(f"Targets       : {result['targets_count']}")
    print(f"Metadata      : {result['metadata_uri']}")
    if result.get("staging_dir"):
        print(f"Artifacts     : {result['staging_dir']}")

    if not result.get("crawl_complete"):
        print(
            "\nWARNING: the crawl was incomplete (page budget reached and/or some URLs "
            "failed). Deleted pages will NOT be purged from the index on this run."
        )

    if result.get("indexing_error"):
        print(f"\nERROR: indexing failed: {result['indexing_error']}")
        return 1

    if result.get("dry_run"):
        print("\nDry run - no cloud resources were modified.")
    else:
        print(f"\nImport operation: {result.get('import_operation')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
