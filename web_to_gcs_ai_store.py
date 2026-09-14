#!/usr/bin/env python3
"""
Website to GCS AI Datastore Ingestor (CLI Interface)
===================================================
Crawls one or multiple websites, extracts clean article/page content, converts HTML to AI-optimized
Markdown with metadata, builds a Vertex AI Search & Gemini Enterprise compliant metadata.jsonl,
uploads all artifacts directly to Google Cloud Storage (GCS), creates or updates the Discovery
Engine Data Store, sets up the schema with keyPropertyMapping (uri -> url), and triggers import.

Guarantees that citations synthesized by Gemini Enterprise assistants point to live public website URLs.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Set

from main import (
    WebsiteCrawler,
    stage_artifacts,
    upload_to_gcs,
    ensure_datastore_and_schema,
    trigger_datastore_import,
    link_datastore_to_engine,
    load_targets_config,
    GCS_AVAILABLE,
    DISCOVERYENGINE_AVAILABLE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("web_to_gcs")


def main():
    parser = argparse.ArgumentParser(
        description="Crawl website(s), convert to AI Markdown, upload to GCS, and configure Gemini Enterprise / Vertex AI Search."
    )
    parser.add_argument("--url", required=False, help="Single website URL to crawl (e.g., https://docs.example.com)")
    parser.add_argument("--targets-file", required=False, help="Path to targets.json or gs:// URI containing multiple targets")
    parser.add_argument("--gcs-bucket", required=False, help="Target GCS bucket name (omit with --dry-run)")
    parser.add_argument("--gcs-prefix", default="website_datastore", help="Folder prefix in GCS bucket (default: 'website_datastore')")
    parser.add_argument("--output-dir", default="./output_datastore", help="Local directory to store generated files")
    parser.add_argument("--project-id", required=False, help="Google Cloud Project ID")
    parser.add_argument("--location", default="global", help="Discovery Engine location (default: global)")
    parser.add_argument("--data-store-id", required=False, help="Vertex AI Search / Discovery Engine Data Store ID")
    parser.add_argument("--engine-id", required=False, help="Gemini Enterprise Search/Agent App (Engine) ID to attach")
    parser.add_argument("--max-pages", type=int, default=50, help="Default max pages per site")
    parser.add_argument("--max-depth", type=int, default=3, help="Default link traversal depth per site")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between requests in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Crawl and convert locally without modifying cloud resources")

    args = parser.parse_args()

    if not args.url and not args.targets_file:
        parser.error("Either --url or --targets-file must be provided.")

    # Resolve Targets
    config_dict = {
        "url": args.url,
        "targets_config_uri": args.targets_file,
        "max_pages": args.max_pages,
        "max_depth": args.max_depth,
        "delay": args.delay,
    }
    targets = load_targets_config(config_dict)
    if not targets:
        logger.error("No valid crawl targets found.")
        sys.exit(1)

    logger.info(f"Loaded {len(targets)} crawl target(s).")

    # Step 1: Crawl and Extract
    all_pages: List[Dict] = []
    shared_visited: Set[str] = set()

    for idx, target in enumerate(targets, start=1):
        t_url = target.get("url")
        if not t_url:
            continue
        t_name = target.get("name", f"target-{idx}")
        t_max_pages = int(target.get("max_pages", args.max_pages))
        t_max_depth = int(target.get("max_depth", args.max_depth))
        t_delay = float(target.get("delay", args.delay))
        t_includes = target.get("include_patterns", [])
        t_excludes = target.get("exclude_patterns", [])
        t_headers = dict(target.get("headers") or {})
        auth_env = target.get("auth_env_var")
        if auth_env and os.environ.get(auth_env):
            auth_header = target.get("auth_header_name", "Authorization")
            t_headers[auth_header] = os.environ[auth_env]
        t_cookies = dict(target.get("cookies") or {})

        logger.info(f"=== [{idx}/{len(targets)}] Crawling: '{t_name}' ({t_url}) [depth={t_max_depth}, max_pages={t_max_pages}] ===")
        crawler = WebsiteCrawler(
            base_url=t_url,
            max_pages=t_max_pages,
            max_depth=t_max_depth,
            delay_seconds=t_delay,
            include_patterns=t_includes,
            exclude_patterns=t_excludes,
            visited_urls=shared_visited,
            target_name=t_name,
            headers=t_headers,
            cookies=t_cookies,
        )
        pages = crawler.crawl()
        all_pages.extend(pages)
        logger.info(f"Target '{t_name}' completed with {len(pages)} extracted pages.")

    if not all_pages:
        logger.error("No pages were extracted. Please check the target URLs and network connectivity.")
        sys.exit(1)

    # Step 2: Format & Stage Artifacts
    logger.info(f"=== Step 2: Generating AI Markdown & Metadata Schema ===")
    saved_files, metadata_path = stage_artifacts(
        pages=all_pages,
        staging_dir=args.output_dir,
        bucket_name=args.gcs_bucket or "dry-run-bucket",
        prefix=args.gcs_prefix,
    )
    logger.info(f"Generated {len(saved_files)} Markdown docs in {args.output_dir}/documents")
    logger.info(f"Generated datastore metadata at {metadata_path}")

    # Step 3: Upload to Cloud Storage
    clean_prefix = args.gcs_prefix.strip("/")
    prefix_str = f"{clean_prefix}/" if clean_prefix else ""
    metadata_gcs_uri = f"gs://{args.gcs_bucket}/{prefix_str}metadata.jsonl" if args.gcs_bucket else f"file://{metadata_path}"

    if args.dry_run or not args.gcs_bucket:
        logger.info(f"=== Dry-Run Complete: Staged at {os.path.abspath(args.output_dir)} ===")
        print(f"\n[OK] Crawled {len(all_pages)} total pages across {len(targets)} target(s). Metadata JSONL ready at: {metadata_path}")
        return

    logger.info(f"=== Step 3: Uploading to GCS (gs://{args.gcs_bucket}/{prefix_str}) ===")
    if not GCS_AVAILABLE:
        logger.warning("google-cloud-storage package not found. Run `pip install google-cloud-storage` to enable automated upload.")
    else:
        upload_to_gcs(args.output_dir, args.gcs_bucket, args.gcs_prefix)
        logger.info(f"Successfully uploaded artifacts. Metadata URI: {metadata_gcs_uri}")

    # Step 4: Configure Data Store & Import
    if args.project_id and args.data_store_id:
        logger.info(f"=== Step 4: Configuring Discovery Engine Data Store ('{args.data_store_id}') ===")
        if not DISCOVERYENGINE_AVAILABLE:
            logger.warning("google-cloud-discoveryengine package not found. Run `pip install google-cloud-discoveryengine`.")
        else:
            try:
                ensure_datastore_and_schema(args.project_id, args.location, args.data_store_id)
                op_name = trigger_datastore_import(args.project_id, args.location, args.data_store_id, metadata_gcs_uri)
                logger.info(f"Import operation launched: {op_name}")

                if args.engine_id:
                    link_datastore_to_engine(args.project_id, args.location, args.engine_id, args.data_store_id)
            except Exception as e:
                logger.error(f"Failed to configure Discovery Engine: {e}")
    else:
        logger.info("\n=== Discovery Engine Setup Instructions ===")
        logger.info("To create or sync the Data Store manually:")
        logger.info(f"1. In Google Cloud Console, open Vertex AI Search / Gemini Enterprise -> Data Stores.")
        logger.info(f"2. Choose 'Create Data Store' -> 'Cloud Storage'.")
        logger.info(f"3. Specify URI: {metadata_gcs_uri}")
        logger.info(f"4. Select 'Linked unstructured documents (JSONL with metadata)'.")
        logger.info(f"5. IMPORTANT: In Schema mapping, map 'url' to property 'uri' so citations link to the live website!")

    print(f"\n✅ Pipeline Complete! Indexed {len(all_pages)} pages into {metadata_gcs_uri}")


if __name__ == "__main__":
    main()
