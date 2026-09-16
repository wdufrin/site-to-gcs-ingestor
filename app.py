#!/usr/bin/env python3
"""
Site-to-GCS & Gemini Enterprise Datastore Ingestor - Unified Web Manager
========================================================================
Interactive Web UI & Control Plane that allows users to:
1. Manage multi-target website crawling configurations (URLs, depths, exclusions).
2. Sync config directly to/from Google Cloud Storage (gs://.../config.json).
3. Trigger the live Cloud Run Function on-demand via authenticated OIDC.
4. Convert pages into clean AI-optimized Markdown and upload to GCS.
5. Automatically configure Discovery Engine Data Store with schema
   keyPropertyMapping (uri -> url) so citations link to live public URLs.
6. Export CI/CD manifests (cloudbuild.yaml) and deployment commands.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urldefrag, urljoin, urlparse

from main import (
    execute_ingestion,
    get_datastore_live_status,
    resolve_full_config,
    GCS_AVAILABLE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ingestor_ui")

# In-memory storage for jobs, logs, and document content
JOBS: Dict[str, Dict] = {}
LOGS: Dict[str, List[Dict]] = {}
DOCUMENTS: Dict[str, Dict[str, Dict]] = {}
JOB_LOCK = threading.Lock()


def add_job_log(job_id: str, level: str, message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = {"time": timestamp, "level": level, "message": message}
    with JOB_LOCK:
        if job_id not in LOGS:
            LOGS[job_id] = []
        LOGS[job_id].append(entry)


class IngestionWorker:
    def __init__(self, job_id: str, config: Dict):
        self.job_id = job_id
        self.config = resolve_full_config(config)
        self.gcs_bucket = self.config.get("gcs_bucket", "").strip()
        self.gcs_prefix = self.config.get("gcs_prefix", "website_datastore").strip()
        self.project_id = self.config.get("project_id", "").strip()
        self.location = self.config.get("location", "global").strip()
        self.data_store_id = self.config.get("data_store_id", "").strip()
        self.engine_id = self.config.get("engine_id", "").strip()
        self.default_delay = float(self.config.get("delay", 0.2))
        self.dry_run = bool(self.config.get("dry_run", False))
        self.output_dir = Path(self.config.get("output_dir", f"./datastore_output_{job_id}"))
        self.execution_mode = self.config.get("execution_mode", "local_dry" if self.dry_run else "local_live")
        self._target_index = 1
        self._target_total = 1

    def _run_remote_cloud_function(self):
        """
        Invokes the deployed Cloud Run Function (2nd Gen) via authenticated OIDC
        and surfaces the remote execution result in the local UI job logs.
        """
        self._update_status(stage="Resolving Cloud Run Function", progress=15, state="running")
        function_url = self.config.get("function_url", "").strip()
        func_name = self.config.get("function_name", "site-datastore-ingestor").strip()
        region = self.config.get("region", "us-central1").strip()

        # Validate identifiers before passing to gcloud subprocess (CWE-77 fix)
        if not re.match(r"^[a-zA-Z0-9_-]+$", func_name) or not re.match(r"^[a-zA-Z0-9-]+$", region):
            raise ValueError(f"Invalid function_name '{func_name}' or region '{region}'.")

        if not function_url:
            add_job_log(self.job_id, "INFO", f"Looking up URL for Cloud Run Function '{func_name}' in {region}...")
            f_res = subprocess.run(
                ["gcloud", "functions", "describe", func_name, "--gen2", f"--region={region}", "--format=value(serviceConfig.uri)"],
                capture_output=True, text=True,
            )
            if f_res.returncode == 0 and f_res.stdout.strip():
                function_url = f_res.stdout.strip()
            else:
                raise RuntimeError(
                    f"Could not auto-detect Cloud Run Function '{func_name}' in region '{region}'. "
                    f"Ensure it is deployed or provide 'function_url'. ({f_res.stderr.strip()})"
                )

        add_job_log(self.job_id, "INFO", f"Target Cloud Run Function URI: {function_url}")
        self._update_status(stage="Acquiring OIDC Identity Token", progress=30)

        token = ""
        t_res = subprocess.run(
            ["gcloud", "auth", "print-identity-token", f"--audiences={function_url}"],
            capture_output=True, text=True,
        )
        if t_res.returncode == 0 and t_res.stdout.strip():
            token = t_res.stdout.strip().splitlines()[-1].strip()
        else:
            t_basic = subprocess.run(["gcloud", "auth", "print-identity-token"], capture_output=True, text=True)
            if t_basic.returncode == 0 and t_basic.stdout.strip():
                token = t_basic.stdout.strip().splitlines()[-1].strip()

        if not token:
            raise RuntimeError("Could not acquire OIDC identity token via 'gcloud auth print-identity-token'.")

        add_job_log(self.job_id, "INFO", "Sending crawl & ingestion payload to remote Cloud Run Function...")
        self._update_status(stage="Executing Remote Cloud Run Function...", progress=55)

        remote_payload = dict(self.config)
        remote_payload["dry_run"] = False
        req = urllib.request.Request(
            function_url,
            data=json.dumps(remote_payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=1800) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        for stat in result.get("crawler_stats", []):
            add_job_log(
                self.job_id, "INFO",
                f"Remote target '{stat.get('target')}': {stat.get('pages_crawled', 0)} pages crawled, "
                f"{stat.get('failed_count', 0)} failed/skipped."
            )

        pages = result.get("pages_count", 0)
        failed = result.get("failed_count", 0)
        if result.get("indexing_error"):
            add_job_log(self.job_id, "ERROR", f"Remote indexing failed: {result['indexing_error']}")
            self._update_status(
                stage="Remote Cloud Run Failed During Indexing",
                progress=100, state="failed", error=result["indexing_error"],
            )
            return

        self._update_status(
            stage="Remote Cloud Run Ingestion Complete!",
            progress=100,
            state="completed",
            metadata_uri=result.get("metadata_uri", ""),
            import_operation=result.get("import_operation"),
            failed_count=failed,
        )
        add_job_log(
            self.job_id, "SUCCESS",
            f"Cloud Run Function finished! {pages} page(s) indexed into '{result.get('data_store_id')}'."
        )

    def run(self):
        """
        Drives either the local shared pipeline (main.execute_ingestion) or
        remote Cloud Run Function execution based on self.execution_mode.
        """
        try:
            if self.execution_mode == "cloud_function":
                self._run_remote_cloud_function()
                return
            self._update_status(stage="Resolving Targets", progress=5, state="running")

            def on_progress(event):
                kind = event.get("kind")

                if kind == "target_start":
                    self._target_index = event["target_index"]
                    self._target_total = event["target_total"]
                    add_job_log(
                        self.job_id, "INFO",
                        f"[{event['target_index']}/{event['target_total']}] Crawling "
                        f"'{event['target']}': {event['url']}"
                    )

                elif kind == "page":
                    page = event["page"]
                    with JOB_LOCK:
                        DOCUMENTS.setdefault(self.job_id, {})[page["id"]] = page
                        JOBS[self.job_id].setdefault("pages", []).append({
                            "id": page["id"],
                            "url": page["url"],
                            "title": page["title"],
                            "target_name": page.get("target_name", ""),
                            "word_count": page["word_count"],
                            "crawled_at": page["crawled_at"],
                        })
                    add_job_log(
                        self.job_id, "SUCCESS",
                        f"Extracted: '{page['title'][:40]}' ({page['word_count']} words)"
                    )
                    self._update_status(
                        stage=f"Target {self._target_index}/{self._target_total} "
                              f"({event['crawled']} pages)",
                        progress=min(10 + int((event["crawled"] / max(1, event["max_pages"])) * 45), 55),
                    )

                elif kind in ("skipped", "error"):
                    with JOB_LOCK:
                        JOBS[self.job_id].setdefault("failed_urls", []).append({
                            "url": event["url"], "reason": event.get("reason", ""),
                        })
                    add_job_log(self.job_id, "WARNING", f"Skipped {event['url']}: {event.get('reason')}")

                elif kind == "target_done":
                    level = "WARNING" if event.get("failed") else "SUCCESS"
                    add_job_log(
                        self.job_id, level,
                        f"Target '{event['target']}' finished: {event['pages']} pages, "
                        f"{event.get('failed', 0)} failed/skipped."
                    )

                elif kind == "stage":
                    stage_map = {
                        "staging": ("Structuring AI Markdown & Schema", 60),
                        "uploading": ("Uploading to Cloud Storage", 75),
                        "indexing": ("Configuring Discovery Engine Data Store", 85),
                    }
                    label, pct = stage_map.get(event["stage"], (event["stage"], 60))
                    self._update_status(stage=label, progress=pct)
                    add_job_log(self.job_id, "INFO", label)

            result = execute_ingestion(
                self.config,
                progress_callback=on_progress,
                staging_dir=str(self.output_dir),
            )

            failed = result.get("failed_count", 0)
            pages = result["pages_count"]

            if result.get("indexing_error"):
                # Indexing failed -- the datastore was NOT updated. Do not
                # present this to the operator as a success.
                add_job_log(self.job_id, "ERROR", f"Indexing failed: {result['indexing_error']}")
                self._update_status(
                    stage="Ingestion failed during indexing",
                    progress=100, state="failed",
                    error=result["indexing_error"],
                    metadata_uri=result["metadata_uri"],
                    failed_count=failed,
                )
                return

            rec_used = result.get("reconciliation_mode_used", "INCREMENTAL")
            rec_setting = result.get("reconciliation_mode", "auto")
            if rec_used == "FULL":
                add_job_log(
                    self.job_id, "INFO",
                    f"Datastore import triggered in FULL mode (setting: '{rec_setting}'). "
                    "Any previously indexed pages not in this batch will be purged."
                )
            else:
                reason = (
                    f"Max Pages budget ({pages} pages) reached"
                    if not result.get("crawl_complete") and failed == 0
                    else f"{failed} failed page(s)"
                    if failed > 0
                    else "Incremental mode selected"
                )
                add_job_log(
                    self.job_id, "INFO" if failed == 0 else "WARNING",
                    f"Datastore import triggered in INCREMENTAL mode ({reason}). "
                    "Existing datastore pages are preserved."
                )

            state_label = "Ingestion Complete & Grounded!" if not failed else \
                          f"Completed with {failed} failed page(s)"

            self._update_status(
                stage=state_label,
                progress=100,
                state="completed" if not failed else "completed_with_errors",
                metadata_uri=result["metadata_uri"],
                import_operation=result.get("import_operation"),
                failed_count=failed,
                crawl_complete=result.get("crawl_complete"),
            )
            add_job_log(
                self.job_id, "SUCCESS" if not failed else "WARNING",
                f"Pipeline finished: {pages} page(s) indexed, {failed} failed, "
                f"across {result['targets_count']} target(s)."
            )

        except Exception as e:
            logger.error(f"Job {self.job_id} failed: {e}", exc_info=True)
            add_job_log(self.job_id, "ERROR", f"Job failed: {str(e)}")
            self._update_status(stage="Failed", progress=100, state="failed", error=str(e))

    def _update_status(self, stage: str, progress: int, state: Optional[str] = None, **kwargs):
        with JOB_LOCK:
            if self.job_id in JOBS:
                JOBS[self.job_id]["stage"] = stage
                JOBS[self.job_id]["progress"] = progress
                if state:
                    JOBS[self.job_id]["state"] = state
                for k, v in kwargs.items():
                    JOBS[self.job_id][k] = v


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Site to GCS & Gemini Enterprise Ingestor</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/lucide@latest"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
    body {
      font-family: 'Plus Jakarta Sans', sans-serif;
      background: radial-gradient(circle at 10% 20%, rgb(15, 23, 42) 0%, rgb(8, 13, 26) 90.1%);
    }
    .font-mono { font-family: 'JetBrains Mono', monospace; }
    .glass-card {
      background: rgba(30, 41, 59, 0.72);
      backdrop-filter: blur(12px);
      border: 1px solid rgba(255, 255, 255, 0.08);
    }
    .glass-input {
      background: rgba(15, 23, 42, 0.7);
      border: 1px solid rgba(255, 255, 255, 0.12);
    }
    .glass-input:focus {
      border-color: #3b82f6;
      box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.25);
    }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: rgba(15, 23, 42, 0.5); }
    ::-webkit-scrollbar-thumb { background: rgba(100, 116, 139, 0.4); border-radius: 3px; }
  </style>
</head>
<body class="text-slate-100 min-h-screen flex flex-col antialiased selection:bg-blue-600 selection:text-white">

  <!-- Header Navbar -->
  <header class="border-b border-slate-800/80 glass-card sticky top-0 z-40">
    <div class="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
      <div class="flex items-center space-x-3">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-blue-600 to-indigo-500 flex items-center justify-center shadow-lg shadow-blue-500/20">
          <i data-lucide="sparkles" class="w-5 h-5 text-white"></i>
        </div>
        <div>
          <h1 class="font-bold text-lg leading-none flex items-center gap-2">
            Gemini Enterprise
            <span class="text-xs font-semibold px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">Datastore Ingestor</span>
          </h1>
          <p class="text-xs text-slate-400 mt-1">Multi-Site Crawler, Decoupled GCS Config & Citation Grounder</p>
        </div>
      </div>
      <div class="flex items-center space-x-4">
        <a href="https://console.cloud.google.com/gen-app-builder/data-stores" target="_blank" class="text-xs font-medium text-slate-400 hover:text-white flex items-center gap-1.5 transition">
          <i data-lucide="external-link" class="w-3.5 h-3.5"></i> Discovery Engine Console
        </a>
      </div>
    </div>
  </header>

  <!-- Main Grid -->
  <main class="max-w-7xl mx-auto px-6 py-8 flex-1 w-full grid grid-cols-1 lg:grid-cols-12 gap-8">
    
    <!-- Left Column: Ingestion Controls & Settings (5 cols) -->
    <div class="lg:col-span-5 space-y-5">

      <!-- Cloud Config Sync Toolbar (Live GCS & Cloud Run Management) -->
      <div class="glass-card rounded-2xl p-4 border border-blue-500/20 bg-blue-950/20 shadow-xl space-y-3">
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-2">
            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
            <span class="text-xs font-bold text-white uppercase tracking-wider">Live Cloud Storage Config</span>
          </div>
          <span class="text-[10px] font-mono text-slate-400 truncate max-w-[200px]" id="liveConfigLabel">gs://.../config.json</span>
        </div>
        <p class="text-[11px] text-slate-400 leading-tight">
          Cloud Run reads this configuration on every scheduled run. Change websites or depths here and sync directly without code redeployments.
        </p>
        <div class="grid grid-cols-3 gap-2">
          <button type="button" onclick="pullConfigFromGcs()" id="pullConfigBtn"
                  class="px-2 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-xl text-xs font-semibold flex items-center justify-center gap-1.5 transition border border-slate-700">
            <i data-lucide="cloud-download" class="w-3.5 h-3.5 text-blue-400"></i>
            <span>Pull GCS</span>
          </button>
          <button type="button" onclick="pushConfigToGcs()" id="pushConfigBtn"
                  class="px-2 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-xl text-xs font-semibold flex items-center justify-center gap-1.5 transition border border-slate-700">
            <i data-lucide="cloud-upload" class="w-3.5 h-3.5 text-emerald-400"></i>
            <span>Push GCS</span>
          </button>
          <button type="button" onclick="triggerCloudRunNow()" id="triggerCloudBtn"
                  class="px-2 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-semibold flex items-center justify-center gap-1.5 shadow-md shadow-indigo-500/25 transition">
            <i data-lucide="rocket" class="w-3.5 h-3.5 text-yellow-300"></i>
            <span>Run Now</span>
          </button>
        </div>
        <div id="cloudSyncStatus" class="text-[11px] text-slate-300 hidden p-2 rounded-lg bg-slate-900/80 border border-slate-800"></div>
      </div>

      <!-- Main Ingestion Setup Card -->
      <div class="glass-card rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div class="flex items-center justify-between mb-5">
          <h2 class="text-base font-bold flex items-center gap-2 text-white">
            <i data-lucide="sliders" class="w-4 h-4 text-blue-400"></i> Targets & Depth Setup
          </h2>
          <!-- Target Mode Switcher -->
          <div class="flex bg-slate-900/80 p-0.5 rounded-lg border border-slate-800 text-[11px] font-medium">
            <button type="button" id="modeSingleBtn" onclick="setTargetMode('single')" class="px-2.5 py-1 rounded-md text-slate-400 hover:text-white transition">Single URL</button>
            <button type="button" id="modeMultiBtn" onclick="setTargetMode('multi')" class="px-2.5 py-1 rounded-md bg-blue-600 text-white transition">Multi-Site</button>
          </div>
        </div>

        <form id="ingestForm" class="space-y-4" onsubmit="handleStartJob(event)">

          <!-- Mode 1: Single Target View -->
          <div id="singleTargetSection" class="space-y-3 hidden">
            <div>
              <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5 flex items-center gap-1.5">
                <i data-lucide="globe" class="w-3.5 h-3.5 text-blue-400"></i> Public Website URL
              </label>
              <input type="url" id="singleUrl" placeholder="https://docs.example.com" value="https://docs.python.org/3/tutorial/"
                     class="glass-input w-full px-3.5 py-2.5 rounded-xl text-sm text-white placeholder-slate-500 focus:outline-none transition">
            </div>
            <div class="grid grid-cols-3 gap-3">
              <div>
                <label class="block text-[11px] font-semibold text-slate-300 uppercase tracking-wider mb-1">Max Pages</label>
                <input type="number" id="singleMaxPages" value="30" min="1" max="500"
                       class="glass-input w-full px-3 py-1.5 rounded-xl text-xs text-white focus:outline-none">
              </div>
              <div>
                <label class="block text-[11px] font-semibold text-slate-300 uppercase tracking-wider mb-1">Depth</label>
                <input type="number" id="singleMaxDepth" value="2" min="1" max="10"
                       class="glass-input w-full px-3 py-1.5 rounded-xl text-xs text-white focus:outline-none">
              </div>
              <div>
                <label class="block text-[11px] font-semibold text-slate-300 uppercase tracking-wider mb-1">Delay (s)</label>
                <input type="number" id="singleDelay" value="0.2" step="0.1" min="0" max="5"
                       class="glass-input w-full px-3 py-1.5 rounded-xl text-xs text-white focus:outline-none">
              </div>
            </div>
          </div>

          <!-- Mode 2: Multi-Target Manager View -->
          <div id="multiTargetSection" class="space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-semibold text-slate-300 uppercase tracking-wider flex items-center gap-1.5">
                <i data-lucide="layers" class="w-3.5 h-3.5 text-indigo-400"></i> Managed Target Sites (<span id="targetCount">1</span>)
              </span>
              <button type="button" onclick="addTargetSite()" class="text-[11px] text-blue-400 hover:text-blue-300 font-semibold flex items-center gap-1">
                <i data-lucide="plus-circle" class="w-3.5 h-3.5"></i> Add Website
              </button>
            </div>
            <div id="targetCardsContainer" class="space-y-2.5 max-h-60 overflow-y-auto pr-1"></div>
            <div class="flex items-center justify-between text-[11px] pt-1 border-t border-slate-800">
              <button type="button" onclick="loadDefaultTargetsJson()" class="text-slate-400 hover:text-white flex items-center gap-1">
                <i data-lucide="file-text" class="w-3 h-3"></i> Load targets.json
              </button>
              <button type="button" onclick="exportTargetsJson()" class="text-slate-400 hover:text-white flex items-center gap-1">
                <i data-lucide="download" class="w-3 h-3"></i> Export JSON
              </button>
            </div>
          </div>

          <!-- GCS Bucket & Subfolder -->
          <div class="grid grid-cols-2 gap-3 pt-1">
            <div>
              <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5 flex items-center gap-1.5">
                <i data-lucide="cloud" class="w-3.5 h-3.5 text-slate-400"></i> GCS Bucket
              </label>
              <input type="text" id="gcs_bucket" value="" placeholder="my-ai-knowledge-bucket"
                     class="glass-input w-full px-3.5 py-2 rounded-xl text-xs text-white font-mono focus:outline-none transition" onchange="updateDynamicSnippets()">
            </div>
            <div>
              <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5 flex items-center gap-1.5">
                <i data-lucide="folder" class="w-3.5 h-3.5 text-slate-400"></i> GCS Prefix
              </label>
              <input type="text" id="gcs_prefix" value="website_datastore"
                     class="glass-input w-full px-3.5 py-2 rounded-xl text-xs text-white font-mono focus:outline-none transition" onchange="updateDynamicSnippets()">
            </div>
          </div>

          <!-- Google Cloud & Discovery Engine Parameters -->
          <div class="p-3.5 rounded-xl bg-slate-900/60 border border-slate-800/80 space-y-3">
            <div class="text-[11px] font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
              <i data-lucide="cpu" class="w-3.5 h-3.5 text-indigo-400"></i> Vertex AI Search & Gemini Setup
            </div>
            <div class="grid grid-cols-2 gap-3">
              <div>
                <label class="block text-[11px] text-slate-300 mb-1">GCP Project ID</label>
                <input type="text" id="project_id" value="" placeholder="my-gcp-project"
                       class="glass-input w-full px-3 py-1.5 rounded-lg text-xs text-white font-mono focus:outline-none" onchange="updateDynamicSnippets()">
              </div>
              <div>
                <label class="block text-[11px] text-slate-300 mb-1">Data Store ID</label>
                <input type="text" id="data_store_id" value="web-docs-store" placeholder="web-docs-store"
                       class="glass-input w-full px-3 py-1.5 rounded-lg text-xs text-white font-mono focus:outline-none" onchange="updateDynamicSnippets()">
              </div>
            </div>
            <div class="grid grid-cols-2 gap-3">
              <div>
                <label class="block text-[11px] text-slate-300 mb-1">Engine / App ID (Optional)</label>
                <input type="text" id="engine_id" value="" placeholder="gemini-assistant-app"
                       class="glass-input w-full px-3 py-1.5 rounded-lg text-xs text-white font-mono focus:outline-none" onchange="updateDynamicSnippets()">
              </div>
              <div>
                <label class="block text-[11px] text-slate-300 mb-1">Location</label>
                <select id="location" class="glass-input w-full px-3 py-1.5 rounded-lg text-xs text-white font-mono focus:outline-none" onchange="updateDynamicSnippets()">
                  <option value="global" selected>global</option>
                  <option value="us">us</option>
                  <option value="eu">eu</option>
                </select>
              </div>
            </div>
            <div>
              <label class="block text-[11px] text-slate-300 mb-1 flex items-center justify-between">
                <span>Recurring Sync Schedule (Cloud Scheduler)</span>
                <span class="text-[10px] text-slate-500 font-mono">Cron (UTC)</span>
              </label>
              <div class="grid grid-cols-12 gap-2">
                <div class="col-span-7">
                  <select id="schedulePreset" onchange="onSchedulePresetChange(this.value)"
                          class="glass-input w-full px-2.5 py-1.5 rounded-lg text-xs text-white focus:outline-none">
                    <option value="0 */6 * * *">Every 6 Hours</option>
                    <option value="0 */12 * * *">Every 12 Hours</option>
                    <option value="0 2 * * *" selected>Daily at 2:00 AM UTC (Default)</option>
                    <option value="0 0 * * *">Daily at Midnight UTC</option>
                    <option value="0 2 * * 0">Weekly (Sundays, 2:00 AM UTC)</option>
                    <option value="0 2 1 * *">Monthly (1st of Month, 2:00 AM)</option>
                    <option value="custom">Custom Cron Expression...</option>
                  </select>
                </div>
                <div class="col-span-5">
                  <input type="text" id="schedule" value="0 2 * * *" oninput="onScheduleInputChange(this.value)"
                         class="glass-input w-full px-2.5 py-1.5 rounded-lg text-xs text-indigo-300 font-mono text-center focus:outline-none">
                </div>
              </div>
              <p id="scheduleHumanText" class="text-[10px] text-slate-400 mt-1">Runs daily at 02:00 UTC via Cloud Scheduler.</p>
            </div>
          </div>

          <!-- Datastore Sync / Reconciliation Mode Selector -->
          <div class="pt-3 border-t border-slate-800 space-y-2">
            <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider flex items-center gap-1.5">
              <i data-lucide="refresh-cw" class="w-3.5 h-3.5 text-emerald-400"></i> Datastore Update Mode
            </label>
            <input type="hidden" id="reconciliation_mode" value="auto">
            <div class="grid grid-cols-3 gap-2" id="reconcileModeGroup">
              <button type="button" id="reconcileMode_auto" onclick="setReconcileMode('auto')"
                      class="p-2.5 rounded-xl border border-emerald-500/60 bg-emerald-950/30 text-left transition flex flex-col justify-between">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-bold text-white">Auto (Safe)</span>
                  <i data-lucide="shield-check" class="w-3.5 h-3.5 text-emerald-400"></i>
                </div>
                <p class="text-[10px] text-slate-300 mt-1 leading-tight">INCREMENTAL if capped/errors; FULL if complete</p>
              </button>
              <button type="button" id="reconcileMode_incremental" onclick="setReconcileMode('incremental')"
                      class="p-2.5 rounded-xl border border-slate-800 bg-slate-900/60 hover:border-slate-700 text-left transition flex flex-col justify-between">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-bold text-slate-200">Incremental</span>
                  <i data-lucide="plus-circle" class="w-3.5 h-3.5 text-blue-400"></i>
                </div>
                <p class="text-[10px] text-slate-400 mt-1 leading-tight">Upsert only (add/update pages, never delete)</p>
              </button>
              <button type="button" id="reconcileMode_full" onclick="setReconcileMode('full')"
                      class="p-2.5 rounded-xl border border-slate-800 bg-slate-900/60 hover:border-slate-700 text-left transition flex flex-col justify-between">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-bold text-slate-200">Full Replace</span>
                  <i data-lucide="trash-2" class="w-3.5 h-3.5 text-amber-400"></i>
                </div>
                <p class="text-[10px] text-slate-400 mt-1 leading-tight">Mirror index & purge any unlisted pages</p>
              </button>
            </div>
          </div>

          <!-- 3-Way Execution Mode Selector -->
          <div class="pt-3 border-t border-slate-800 space-y-2">
            <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider flex items-center gap-1.5">
              <i data-lucide="cpu" class="w-3.5 h-3.5 text-blue-400"></i> Execution Mode
            </label>
            <input type="checkbox" id="dry_run" class="hidden">
            <div class="grid grid-cols-3 gap-2" id="execModeGroup">
              <button type="button" id="execMode_local_live" onclick="setExecutionMode('local_live')"
                      class="p-2.5 rounded-xl border border-blue-500/60 bg-blue-950/40 text-left transition flex flex-col justify-between">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-bold text-white">Local + GCP Sync</span>
                  <i data-lucide="laptop" class="w-3.5 h-3.5 text-blue-400"></i>
                </div>
                <p class="text-[10px] text-slate-300 mt-1 leading-tight">Crawl locally & update GCS + Datastore via ADC</p>
              </button>
              <button type="button" id="execMode_local_dry" onclick="setExecutionMode('local_dry')"
                      class="p-2.5 rounded-xl border border-slate-800 bg-slate-900/60 hover:border-slate-700 text-left transition flex flex-col justify-between">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-bold text-slate-200">Local Dry-Run</span>
                  <i data-lucide="file-search" class="w-3.5 h-3.5 text-emerald-400"></i>
                </div>
                <p class="text-[10px] text-slate-400 mt-1 leading-tight">Stage Markdown & JSONL on disk (zero GCP calls)</p>
              </button>
              <button type="button" id="execMode_cloud_function" onclick="setExecutionMode('cloud_function')"
                      class="p-2.5 rounded-xl border border-slate-800 bg-slate-900/60 hover:border-slate-700 text-left transition flex flex-col justify-between">
                <div class="flex items-center justify-between">
                  <span class="text-xs font-bold text-slate-200">Cloud Run Function</span>
                  <i data-lucide="cloud-lightning" class="w-3.5 h-3.5 text-indigo-400"></i>
                </div>
                <p class="text-[10px] text-slate-400 mt-1 leading-tight">Trigger serverless Cloud Run Function via OIDC</p>
              </button>
            </div>
          </div>

          <!-- Action Button -->
          <div class="pt-2">
            <button type="submit" id="startBtn"
                    class="w-full py-3 px-4 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white rounded-xl font-semibold text-sm shadow-lg shadow-blue-500/25 flex items-center justify-center space-x-2 transition active:scale-[0.99]">
              <i data-lucide="play" class="w-4 h-4 fill-white" id="startBtnIcon"></i>
              <span id="startBtnText">Run Locally → Sync to GCS & Datastore</span>
            </button>
          </div>
        </form>
      </div>

      <!-- Citation Info Card -->
      <div class="glass-card rounded-2xl p-4 border border-slate-800">
        <h3 class="text-xs font-bold uppercase tracking-wider text-emerald-400 mb-1.5 flex items-center gap-1.5">
          <i data-lucide="shield-check" class="w-4 h-4"></i> Public Link Grounding Guaranteed
        </h3>
        <p class="text-xs text-slate-300 leading-relaxed">
          Discovery Engine schema sets <code class="text-blue-300 font-mono">keyPropertyMapping: {"uri": "url"}</code>. 
          When Gemini Enterprise grounds responses, citations route directly to live web URLs rather than <code class="text-amber-300 font-mono">gs://</code>.
        </p>
      </div>
    </div>

    <!-- Right Column: Status, Documents & Code/API Hub (7 cols) -->
    <div class="lg:col-span-7 space-y-6">

      <!-- Status & Progress Stepper Card -->
      <div class="glass-card rounded-2xl p-6 shadow-xl">
        <div class="flex items-center justify-between mb-4">
          <div>
            <span id="jobBadge" class="text-[11px] font-bold uppercase tracking-widest px-2.5 py-1 rounded-full bg-slate-800 text-slate-400 border border-slate-700">
              Idle
            </span>
            <h2 id="jobStageText" class="text-lg font-bold text-white mt-2">Ready to run</h2>
          </div>
          <div class="text-right">
            <span id="progressPctText" class="text-2xl font-black text-blue-400 font-mono">0%</span>
            <p class="text-[11px] text-slate-400">Overall Progress</p>
          </div>
        </div>

        <!-- Progress Bar -->
        <div class="w-full bg-slate-900 rounded-full h-2.5 overflow-hidden border border-slate-800/80 mb-5">
          <div id="progressBar" class="bg-gradient-to-r from-blue-500 via-indigo-500 to-emerald-400 h-2.5 rounded-full transition-all duration-300 ease-out" style="width: 0%"></div>
        </div>

        <!-- Steppers -->
        <div class="grid grid-cols-4 gap-2 text-center text-[10px] font-semibold text-slate-400">
          <div id="step1" class="p-2 rounded-lg bg-slate-900/50 border border-slate-800/50">1. Web Crawl</div>
          <div id="step2" class="p-2 rounded-lg bg-slate-900/50 border border-slate-800/50">2. AI Markdown</div>
          <div id="step3" class="p-2 rounded-lg bg-slate-900/50 border border-slate-800/50">3. GCS Sync</div>
          <div id="step4" class="p-2 rounded-lg bg-slate-900/50 border border-slate-800/50">4. Gemini DataStore</div>
        </div>
      </div>

      <!-- Live Vertex AI Datastore Health & Telemetry Card -->
      <div class="glass-card rounded-2xl p-5 shadow-xl border border-slate-800/80">
        <div class="flex items-center justify-between mb-3.5">
          <div class="flex items-center gap-2.5">
            <div class="p-2 rounded-xl bg-emerald-500/10 border border-emerald-500/30 text-emerald-400">
              <i data-lucide="database" class="w-4 h-4"></i>
            </div>
            <div>
              <div class="flex items-center gap-2">
                <h3 class="text-xs font-bold uppercase tracking-wider text-slate-200">Vertex AI Datastore Telemetry</h3>
                <span id="dsStateBadge" class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-slate-800 text-slate-400 border border-slate-700">Checking...</span>
              </div>
              <p id="dsSubtitle" class="text-[11px] text-slate-400 font-mono mt-0.5">Select Project & Data Store ID to inspect live index</p>
            </div>
          </div>
          <button onclick="fetchDatastoreStatus()" id="refreshDsBtn" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition flex items-center gap-1.5">
            <i data-lucide="refresh-cw" id="refreshDsIcon" class="w-3.5 h-3.5"></i> Refresh Status
          </button>
        </div>

        <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <!-- 1. Indexed Documents -->
          <div class="p-3 rounded-xl bg-slate-900/70 border border-slate-800/80">
            <div class="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Indexed Documents</div>
            <div id="dsDocCount" class="text-lg font-extrabold text-white font-mono mt-1">—</div>
            <div id="dsFreshestCrawl" class="text-[10px] text-slate-400 truncate mt-0.5">No crawl timestamp</div>
          </div>

          <!-- 2. Storage Size -->
          <div class="p-3 rounded-xl bg-slate-900/70 border border-slate-800/80">
            <div class="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Datastore Size</div>
            <div id="dsTotalSize" class="text-lg font-extrabold text-blue-400 font-mono mt-1">—</div>
            <div id="dsSizeBreakdown" class="text-[10px] text-slate-400 truncate mt-0.5">Markdown + metadata</div>
          </div>

          <!-- 3. Last Import / Sync -->
          <div class="p-3 rounded-xl bg-slate-900/70 border border-slate-800/80">
            <div class="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Latest Import LRO</div>
            <div id="dsLastSyncStatus" class="text-xs font-bold text-emerald-400 mt-1.5 truncate">—</div>
            <div id="dsLastSyncTime" class="text-[10px] text-slate-400 truncate mt-1">No recent sync</div>
          </div>

          <!-- 4. Citation Grounding -->
          <div class="p-3 rounded-xl bg-slate-900/70 border border-slate-800/80">
            <div class="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Citation Grounding</div>
            <div id="dsGroundingBadge" class="text-xs font-bold text-emerald-400 mt-1.5 flex items-center gap-1">
              <span>—</span>
            </div>
            <div id="dsSearchTier" class="text-[10px] text-slate-400 truncate mt-1">url → uri schema check</div>
          </div>
        </div>
      </div>

      <!-- Main Tab Controller: Extracted Docs / Logs / Code & Manual Execution -->
      <div class="glass-card rounded-2xl p-5 shadow-xl">
        <div class="flex items-center justify-between border-b border-slate-800 pb-3 mb-4">
          <div class="flex space-x-1.5">
            <button id="tabDocsBtn" onclick="switchMainTab('docs')" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-600/20 text-blue-400 border border-blue-500/30 flex items-center gap-1.5">
              <i data-lucide="file-text" class="w-3.5 h-3.5"></i> Indexed Documents (<span id="docCount">0</span>)
            </button>
            <button id="tabLogsBtn" onclick="switchMainTab('logs')" class="px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white flex items-center gap-1.5">
              <i data-lucide="terminal" class="w-3.5 h-3.5"></i> Live Logs
            </button>
            <button id="tabCodeBtn" onclick="switchMainTab('code')" class="px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white flex items-center gap-1.5">
              <i data-lucide="code" class="w-3.5 h-3.5"></i> Code, CI/CD & Deployment
            </button>
          </div>
        </div>

        <!-- Tab 1: Documents List -->
        <div id="tabDocs" class="space-y-2 max-h-96 overflow-y-auto pr-1">
          <div id="emptyDocs" class="text-center py-12 text-slate-500 text-xs">
            <i data-lucide="inbox" class="w-8 h-8 mx-auto mb-2 opacity-40"></i>
            No documents indexed yet. Run the pipeline to view extracted pages and preview markdown.
          </div>
          <div id="docsList" class="space-y-2 hidden"></div>
        </div>

        <!-- Tab 2: Logs Console -->
        <div id="tabLogs" class="hidden font-mono text-xs bg-slate-950/80 p-4 rounded-xl border border-slate-900 max-h-96 overflow-y-auto space-y-1.5 text-slate-300">
          <div id="logsConsole" class="space-y-1">
            <div class="text-slate-600">// Real-time pipeline logs will stream here...</div>
          </div>
        </div>

        <!-- Tab 3: Code & Deployment Hub -->
        <div id="tabCode" class="hidden space-y-4 max-h-[500px] overflow-y-auto pr-1">
          <div class="text-xs text-slate-400">
            Exported deployment manifests, CI/CD pipelines, CLI commands, and schemas pre-populated with your configuration:
          </div>

          <!-- Sub-Tab Navigation for Code -->
          <div class="flex space-x-2 border-b border-slate-800 pb-2 text-xs overflow-x-auto">
            <button onclick="switchCodeSubTab('cloudbuild')" id="subBtnCloudbuild" class="px-2.5 py-1 rounded bg-slate-800 text-white font-medium whitespace-nowrap">Cloud Build CI/CD</button>
            <button onclick="switchCodeSubTab('gcloud')" id="subBtnGcloud" class="px-2.5 py-1 rounded text-slate-400 hover:text-white font-medium whitespace-nowrap">Manual gcloud</button>
            <button onclick="switchCodeSubTab('config')" id="subBtnConfig" class="px-2.5 py-1 rounded text-slate-400 hover:text-white font-medium whitespace-nowrap">config.json Spec</button>
            <button onclick="switchCodeSubTab('cli')" id="subBtnCli" class="px-2.5 py-1 rounded text-slate-400 hover:text-white font-medium whitespace-nowrap">Standalone CLI</button>
            <button onclick="switchCodeSubTab('curl')" id="subBtnCurl" class="px-2.5 py-1 rounded text-slate-400 hover:text-white font-medium whitespace-nowrap">cURL Trigger</button>
            <button onclick="switchCodeSubTab('schema')" id="subBtnSchema" class="px-2.5 py-1 rounded text-slate-400 hover:text-white font-medium whitespace-nowrap">Datastore Schema</button>
          </div>

          <!-- 1. Cloud Build Sub-Tab -->
          <div id="codeSubCloudbuild" class="space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-semibold text-emerald-400">1. Automated Pipeline (cloudbuild.yaml)</span>
              <button onclick="copySnippet('snippetCloudbuild')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy YAML</button>
            </div>
            <p class="text-[11px] text-slate-400">
              Run this pipeline via Cloud Build to deploy the Cloud Run Function and configure the Cloud Scheduler cron job automatically.
            </p>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto max-h-56" id="snippetCloudbuild"></pre>
            
            <div class="flex items-center justify-between pt-1">
              <span class="text-xs font-semibold text-blue-400">2. Submit Build via CLI</span>
              <button onclick="copySnippet('snippetBuildSubmit')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy Command</button>
            </div>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto" id="snippetBuildSubmit"></pre>
          </div>

          <!-- 2. Manual gcloud Sub-Tab -->
          <div id="codeSubGcloud" class="hidden space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-semibold text-blue-400">1. Deploy Cloud Run Function (2nd Gen)</span>
              <button onclick="copySnippet('snippetDeploy')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy Command</button>
            </div>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto" id="snippetDeploy"></pre>

            <div class="flex items-center justify-between pt-2">
              <span class="text-xs font-semibold text-indigo-400">2. Configure Periodic Updates (Cloud Scheduler)</span>
              <button onclick="copySnippet('snippetScheduler')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy Command</button>
            </div>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto" id="snippetScheduler"></pre>
          </div>

          <!-- 3. Config JSON Spec Sub-Tab -->
          <div id="codeSubConfig" class="hidden space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-semibold text-amber-400">Decoupled GCS Manifest (config.json)</span>
              <button onclick="copySnippet('snippetConfig')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy JSON</button>
            </div>
            <p class="text-[11px] text-slate-400">
              Stored at <code class="text-blue-300">gs://&lt;bucket&gt;/&lt;prefix&gt;/config.json</code>. The scheduled Cloud Run Function reads this on every invocation.
            </p>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto max-h-60" id="snippetConfig"></pre>
          </div>

          <!-- 4. Standalone CLI Sub-Tab -->
          <div id="codeSubCli" class="hidden space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-semibold text-emerald-400">Run via Terminal CLI (web_to_gcs_ai_store.py)</span>
              <button onclick="copySnippet('snippetCli')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy Command</button>
            </div>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto" id="snippetCli"></pre>
          </div>

          <!-- 5. cURL Trigger Sub-Tab -->
          <div id="codeSubCurl" class="hidden space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-semibold text-yellow-400">Trigger Cloud Run Function via HTTP / cURL</span>
              <button onclick="copySnippet('snippetCurl')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy cURL</button>
            </div>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto" id="snippetCurl"></pre>
          </div>

          <!-- 6. Schema Sub-Tab -->
          <div id="codeSubSchema" class="hidden space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-semibold text-purple-400">Vertex AI Search Schema (Guarantees Web Citations)</span>
              <button onclick="copySnippet('snippetSchema')" class="text-xs px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-slate-300">Copy Schema</button>
            </div>
            <p class="text-[11px] text-slate-400">
              Notice <code class="text-blue-300">"keyPropertyMapping": "uri"</code> on <code class="text-blue-300">url</code>. This tells Discovery Engine to return the public web link for citations instead of the internal <code class="text-amber-300">gs://</code> storage path.
            </p>
            <pre class="bg-slate-950 p-3 rounded-xl border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto" id="snippetSchema"></pre>
          </div>

        </div>
      </div>

    </div>
  </main>

  <!-- Document Preview Modal -->
  <div id="docModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
    <div class="glass-card w-full max-w-3xl rounded-2xl overflow-hidden shadow-2xl border border-slate-700 flex flex-col max-h-[85vh]">
      <div class="px-6 py-4 border-b border-slate-800 flex items-center justify-between bg-slate-900/60">
        <div>
          <h3 id="modalDocTitle" class="font-bold text-sm text-white truncate max-w-lg">Document Preview</h3>
          <a id="modalDocUrl" href="#" target="_blank" class="text-xs text-blue-400 font-mono truncate hover:underline flex items-center gap-1 mt-0.5">
            <i data-lucide="external-link" class="w-3 h-3"></i> <span>https://...</span>
          </a>
        </div>
        <button onclick="closeDocModal()" class="w-8 h-8 rounded-lg bg-slate-800 hover:bg-slate-700 flex items-center justify-center text-slate-400 hover:text-white">
          <i data-lucide="x" class="w-4 h-4"></i>
        </button>
      </div>
      <div class="p-6 overflow-y-auto space-y-4">
        <div class="p-3 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1">
          <div class="text-[11px] font-bold text-emerald-400 uppercase tracking-wider">Citation & Grounding Metadata</div>
          <div class="text-slate-300 font-mono" id="modalDocMeta"></div>
        </div>
        <div>
          <div class="text-xs font-bold uppercase tracking-wider text-slate-400 mb-2">Transformed Markdown Content</div>
          <pre id="modalDocContent" class="bg-slate-950 p-4 rounded-xl border border-slate-900 font-mono text-xs text-slate-200 whitespace-pre-wrap leading-relaxed max-h-80 overflow-y-auto"></pre>
        </div>
      </div>
    </div>
  </div>

  <script>
    lucide.createIcons();
    let currentJobId = null;
    let pollInterval = null;
    let currentTargetMode = 'multi';

    let targetsList = [
      {
        name: "python-tutorial",
        url: "https://docs.python.org/3/tutorial/",
        max_depth: 2,
        max_pages: 30,
        include_patterns: ["*/tutorial/*"],
        exclude_patterns: ["*/changelog/*", "*/whatsnew/*", "*/genindex*"]
      }
    ];

    function setTargetMode(mode) {
      currentTargetMode = mode;
      const bSingle = document.getElementById('modeSingleBtn');
      const bMulti = document.getElementById('modeMultiBtn');
      const secSingle = document.getElementById('singleTargetSection');
      const secMulti = document.getElementById('multiTargetSection');

      if (mode === 'single') {
        bSingle.className = 'px-2.5 py-1 rounded-md bg-blue-600 text-white transition';
        bMulti.className = 'px-2.5 py-1 rounded-md text-slate-400 hover:text-white transition';
        secSingle.classList.remove('hidden');
        secMulti.classList.add('hidden');
      } else {
        bMulti.className = 'px-2.5 py-1 rounded-md bg-blue-600 text-white transition';
        bSingle.className = 'px-2.5 py-1 rounded-md text-slate-400 hover:text-white transition';
        secSingle.classList.add('hidden');
        secMulti.classList.remove('hidden');
        renderTargetCards();
      }
      updateDynamicSnippets();
    }

    let currentExecMode = 'local_live';

    function setExecutionMode(mode) {
      currentExecMode = mode;
      const modes = ['local_live', 'local_dry', 'cloud_function'];
      modes.forEach(m => {
        const el = document.getElementById('execMode_' + m);
        if (!el) return;
        if (m === mode) {
          el.className = 'p-2.5 rounded-xl border border-blue-500/60 bg-blue-950/40 text-left transition flex flex-col justify-between';
        } else {
          el.className = 'p-2.5 rounded-xl border border-slate-800 bg-slate-900/60 hover:border-slate-700 text-left transition flex flex-col justify-between';
        }
      });

      const dryCheck = document.getElementById('dry_run');
      if (dryCheck) dryCheck.checked = (mode === 'local_dry');

      const btnText = document.getElementById('startBtnText');
      if (btnText) {
        if (mode === 'local_live') {
          btnText.innerText = 'Run Locally → Sync to GCS & Datastore';
        } else if (mode === 'local_dry') {
          btnText.innerText = 'Run Local Dry-Run (No GCP Calls)';
        } else {
          btnText.innerText = 'Trigger Remote Cloud Run Function';
        }
      }
    }

    let currentReconcileMode = 'auto';

    function setReconcileMode(mode) {
      currentReconcileMode = mode || 'auto';
      const modes = ['auto', 'incremental', 'full'];
      modes.forEach(m => {
        const el = document.getElementById('reconcileMode_' + m);
        if (!el) return;
        if (m === currentReconcileMode) {
          const borderCol = m === 'full' ? 'border-amber-500/60 bg-amber-950/30' : (m === 'incremental' ? 'border-blue-500/60 bg-blue-950/40' : 'border-emerald-500/60 bg-emerald-950/30');
          el.className = `p-2.5 rounded-xl border ${borderCol} text-left transition flex flex-col justify-between`;
        } else {
          el.className = 'p-2.5 rounded-xl border border-slate-800 bg-slate-900/60 hover:border-slate-700 text-left transition flex flex-col justify-between';
        }
      });
      const recInput = document.getElementById('reconciliation_mode');
      if (recInput) recInput.value = currentReconcileMode;
      updateDynamicSnippets();
    }

    const CRON_DESCRIPTIONS = {
      '0 */6 * * *': 'Runs every 6 hours (00:00, 06:00, 12:00, 18:00 UTC) via Cloud Scheduler.',
      '0 */12 * * *': 'Runs every 12 hours (00:00 and 12:00 UTC) via Cloud Scheduler.',
      '0 2 * * *': 'Runs daily at 02:00 AM UTC via Cloud Scheduler.',
      '0 0 * * *': 'Runs daily at Midnight (00:00 UTC) via Cloud Scheduler.',
      '0 2 * * 0': 'Runs weekly on Sundays at 02:00 AM UTC via Cloud Scheduler.',
      '0 2 1 * *': 'Runs monthly on the 1st at 02:00 AM UTC via Cloud Scheduler.'
    };

    function describeCron(cronStr) {
      const clean = (cronStr || '').trim().replace(/\\s+/g, ' ');
      if (CRON_DESCRIPTIONS[clean]) return CRON_DESCRIPTIONS[clean];
      return clean ? `Custom schedule (${clean} UTC) via Cloud Scheduler.` : 'Enter a valid 5-field cron schedule.';
    }

    function onSchedulePresetChange(val) {
      const schedInput = document.getElementById('schedule');
      if (val !== 'custom' && schedInput) {
        schedInput.value = val;
      } else if (val === 'custom' && schedInput) {
        schedInput.focus();
      }
      const humanEl = document.getElementById('scheduleHumanText');
      if (humanEl) humanEl.innerText = describeCron(schedInput ? schedInput.value : val);
      updateDynamicSnippets();
    }

    function onScheduleInputChange(val) {
      const clean = (val || '').trim().replace(/\\s+/g, ' ');
      const presetEl = document.getElementById('schedulePreset');
      if (presetEl) {
        if (CRON_DESCRIPTIONS[clean]) {
          presetEl.value = clean;
        } else {
          presetEl.value = 'custom';
        }
      }
      const humanEl = document.getElementById('scheduleHumanText');
      if (humanEl) humanEl.innerText = describeCron(clean);
      updateDynamicSnippets();
    }

    function renderTargetCards() {
      const container = document.getElementById('targetCardsContainer');
      document.getElementById('targetCount').innerText = targetsList.length;
      container.innerHTML = '';

      targetsList.forEach((t, i) => {
        const card = document.createElement('div');
        card.className = 'p-3 rounded-xl bg-slate-900/80 border border-slate-800 text-xs space-y-2 relative';
        card.innerHTML = `
          <div class="flex items-center justify-between">
            <input type="text" value="${t.name}" onchange="updateTargetField(${i}, 'name', this.value)" placeholder="Target Name"
                   class="glass-input px-2 py-1 rounded text-xs font-semibold text-indigo-300 w-1/2 focus:outline-none">
            ${targetsList.length > 1 ? `<button type="button" onclick="removeTargetSite(${i})" class="text-slate-500 hover:text-red-400"><i data-lucide="trash-2" class="w-3.5 h-3.5"></i></button>` : ''}
          </div>
          <div>
            <input type="url" value="${t.url}" onchange="updateTargetField(${i}, 'url', this.value)" placeholder="https://..."
                   class="glass-input w-full px-2.5 py-1 rounded text-xs font-mono text-white focus:outline-none">
          </div>
          <div class="grid grid-cols-2 gap-2">
            <div>
              <label class="block text-[10px] text-slate-400">Max Depth: <span id="depthVal_${i}" class="text-blue-400 font-bold">${t.max_depth}</span></label>
              <input type="range" min="1" max="5" value="${t.max_depth}" oninput="document.getElementById('depthVal_${i}').innerText=this.value; updateTargetField(${i}, 'max_depth', parseInt(this.value, 10))" class="w-full">
            </div>
            <div>
              <label class="block text-[10px] text-slate-400">Max Pages</label>
              <input type="number" min="1" max="500" value="${t.max_pages}" onchange="updateTargetField(${i}, 'max_pages', parseInt(this.value, 10))"
                     class="glass-input w-full px-2 py-0.5 rounded text-xs font-mono text-white focus:outline-none">
            </div>
          </div>
          <div>
            <label class="block text-[10px] text-slate-400">Exclude Patterns (comma-separated):</label>
            <input type="text" value="${(t.exclude_patterns || []).join(', ')}" onchange="updateTargetField(${i}, 'exclude_patterns', this.value.split(',').map(s=>s.trim()).filter(Boolean))" placeholder="*/archive/*, */v1/*"
                   class="glass-input w-full px-2 py-0.5 rounded text-[11px] font-mono text-slate-300 focus:outline-none">
          </div>
        `;
        container.appendChild(card);
      });
      lucide.createIcons();
      updateDynamicSnippets();
    }

    function addTargetSite() {
      targetsList.push({
        name: `site-${targetsList.length + 1}`,
        url: "https://",
        max_depth: 2,
        max_pages: 30,
        include_patterns: [],
        exclude_patterns: []
      });
      renderTargetCards();
    }

    function removeTargetSite(index) {
      if (targetsList.length > 1) {
        targetsList.splice(index, 1);
        renderTargetCards();
      }
    }

    function updateTargetField(index, field, value) {
      if (targetsList[index]) {
        targetsList[index][field] = value;
        updateDynamicSnippets();
      }
    }

    // Cloud Config Sync Actions
    async function pullConfigFromGcs() {
      const bucket = document.getElementById('gcs_bucket').value.trim();
      const prefix = document.getElementById('gcs_prefix').value.trim();
      const statusEl = document.getElementById('cloudSyncStatus');
      const btn = document.getElementById('pullConfigBtn');

      statusEl.classList.remove('hidden');
      statusEl.className = 'text-[11px] text-blue-400 p-2 rounded-lg bg-slate-900/80 border border-slate-800 animate-pulse';
      statusEl.innerText = `Fetching gs://${bucket}/${prefix}/config.json ...`;

      try {
        const resp = await fetch('/api/cloud/pull-config', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ gcs_bucket: bucket, gcs_prefix: prefix })
        });
        const res = await resp.json();
        if (resp.ok && res.config) {
          const cfg = res.config;
          if (cfg.project_id) document.getElementById('project_id').value = cfg.project_id;
          if (cfg.data_store_id) document.getElementById('data_store_id').value = cfg.data_store_id;
          if (cfg.engine_id) document.getElementById('engine_id').value = cfg.engine_id;
          if (cfg.location) document.getElementById('location').value = cfg.location;
          if (cfg.schedule) {
            document.getElementById('schedule').value = cfg.schedule;
            onScheduleInputChange(cfg.schedule);
          }
          if (cfg.reconciliation_mode) setReconcileMode(cfg.reconciliation_mode);
          if (cfg.targets && cfg.targets.length > 0) {
            targetsList = cfg.targets;
            renderTargetCards();
          }
          statusEl.className = 'text-[11px] text-emerald-400 p-2 rounded-lg bg-emerald-950/30 border border-emerald-800/40 font-medium';
          statusEl.innerHTML = `✅ Successfully pulled live config from <code class="font-mono text-white">${res.uri}</code>`;
          updateDynamicSnippets();
          fetchDatastoreStatus();
        } else {
          statusEl.className = 'text-[11px] text-rose-400 p-2 rounded-lg bg-rose-950/30 border border-rose-800/40';
          statusEl.innerText = `⚠️ Could not read config from GCS: ${res.message || 'File not found'}`;
        }
      } catch (err) {
        statusEl.className = 'text-[11px] text-rose-400 p-2 rounded-lg bg-rose-950/30 border border-rose-800/40';
        statusEl.innerText = `Error: ${err}`;
      }
    }

    async function pushConfigToGcs() {
      const bucket = document.getElementById('gcs_bucket').value.trim();
      const prefix = document.getElementById('gcs_prefix').value.trim();
      const statusEl = document.getElementById('cloudSyncStatus');

      const fullConfig = {
        version: "2.0",
        project_id: document.getElementById('project_id').value.trim(),
        location: document.getElementById('location').value.trim(),
        gcs_bucket: bucket,
        gcs_prefix: prefix,
        data_store_id: document.getElementById('data_store_id').value.trim(),
        engine_id: document.getElementById('engine_id').value.trim(),
        schedule: document.getElementById('schedule').value.trim(),
        reconciliation_mode: currentReconcileMode,
        default_delay: 0.2,
        targets: currentTargetMode === 'multi' ? targetsList : [{
          name: "primary-site",
          url: document.getElementById('singleUrl').value,
          max_depth: parseInt(document.getElementById('singleMaxDepth').value, 10) || 2,
          max_pages: parseInt(document.getElementById('singleMaxPages').value, 10) || 30,
          exclude_patterns: ["*/archive/*"]
        }]
      };

      statusEl.classList.remove('hidden');
      statusEl.className = 'text-[11px] text-blue-400 p-2 rounded-lg bg-slate-900/80 border border-slate-800 animate-pulse';
      statusEl.innerText = `Pushing config to gs://${bucket}/${prefix}/config.json ...`;

      try {
        const resp = await fetch('/api/cloud/push-config', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(fullConfig)
        });
        const res = await resp.json();
        if (resp.ok) {
          statusEl.className = 'text-[11px] text-emerald-400 p-2 rounded-lg bg-emerald-950/30 border border-emerald-800/40 font-medium';
          statusEl.innerHTML = `✅ Successfully synced config to <code class="font-mono text-white">${res.uri}</code>! Future scheduled runs will use this configuration.`;
        } else {
          statusEl.className = 'text-[11px] text-rose-400 p-2 rounded-lg bg-rose-950/30 border border-rose-800/40';
          statusEl.innerText = `⚠️ Upload failed: ${res.message || 'Check permissions'}`;
        }
      } catch (err) {
        statusEl.className = 'text-[11px] text-rose-400 p-2 rounded-lg bg-rose-950/30 border border-rose-800/40';
        statusEl.innerText = `Error: ${err}`;
      }
    }

    async function triggerCloudRunNow() {
      setExecutionMode('cloud_function');
      await handleStartJob({ preventDefault: () => {} });
    }

    async function loadDefaultTargetsJson() {
      try {
        const resp = await fetch('/api/targets');
        if (resp.ok) {
          const data = await resp.json();
          if (data.targets && data.targets.length > 0) {
            targetsList = data.targets;
            renderTargetCards();
            alert(`Loaded ${data.targets.length} target(s) from targets.json`);
          }
        }
      } catch (err) {
        console.error("Error loading targets.json:", err);
      }
    }

    function exportTargetsJson() {
      const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify({ version: "2.0", targets: targetsList }, null, 2));
      const downloadAnchor = document.createElement('a');
      downloadAnchor.setAttribute("href", dataStr);
      downloadAnchor.setAttribute("download", "targets.json");
      document.body.appendChild(downloadAnchor);
      downloadAnchor.click();
      downloadAnchor.remove();
    }

    function switchMainTab(tab) {
      document.getElementById('tabDocs').classList.toggle('hidden', tab !== 'docs');
      document.getElementById('tabLogs').classList.toggle('hidden', tab !== 'logs');
      document.getElementById('tabCode').classList.toggle('hidden', tab !== 'code');

      const bDocs = document.getElementById('tabDocsBtn');
      const bLogs = document.getElementById('tabLogsBtn');
      const bCode = document.getElementById('tabCodeBtn');

      bDocs.className = tab === 'docs' ? 'px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-600/20 text-blue-400 border border-blue-500/30 flex items-center gap-1.5' : 'px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white flex items-center gap-1.5';
      bLogs.className = tab === 'logs' ? 'px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-600/20 text-blue-400 border border-blue-500/30 flex items-center gap-1.5' : 'px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white flex items-center gap-1.5';
      bCode.className = tab === 'code' ? 'px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-600/20 text-blue-400 border border-blue-500/30 flex items-center gap-1.5' : 'px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white flex items-center gap-1.5';

      if (tab === 'code') {
        updateDynamicSnippets();
      }
      lucide.createIcons();
    }

    function switchCodeSubTab(sub) {
      const subs = ['cloudbuild', 'gcloud', 'config', 'cli', 'curl', 'schema'];
      subs.forEach(s => {
        const el = document.getElementById('codeSub' + s.charAt(0).toUpperCase() + s.slice(1));
        const btn = document.getElementById('subBtn' + s.charAt(0).toUpperCase() + s.slice(1));
        if (el) el.classList.toggle('hidden', s !== sub);
        if (btn) {
          btn.className = (s === sub) 
            ? 'px-2.5 py-1 rounded bg-slate-800 text-white font-medium whitespace-nowrap'
            : 'px-2.5 py-1 rounded text-slate-400 hover:text-white font-medium whitespace-nowrap';
        }
      });
      updateDynamicSnippets();
    }

    function updateDynamicSnippets() {
      const url = document.getElementById('singleUrl').value || 'https://docs.example.com';
      const bucket = document.getElementById('gcs_bucket').value || 'my-ai-knowledge-bucket';
      const prefix = document.getElementById('gcs_prefix').value || 'website_datastore';
      const project = document.getElementById('project_id').value || 'my-gcp-project';
      const datastore = document.getElementById('data_store_id').value || 'web-docs-store';
      const engine = document.getElementById('engine_id').value || 'gemini-assistant-app';
      const location = document.getElementById('location').value || 'global';
      const schedule = document.getElementById('schedule').value || '0 2 * * *';

      const configGcsUri = `gs://${bucket}/${prefix}/config.json`;
      document.getElementById('liveConfigLabel').innerText = configGcsUri;

      // 1. Cloud Build YAML
      const cloudbuildYaml = `# cloudbuild.yaml
steps:
  # Step 1: Verify syntax and dependencies
  - name: 'python:3.11-slim'
    id: 'verify-syntax'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install -r requirements.txt --quiet
        python3 -m py_compile main.py
        echo "Syntax check passed successfully."

  # Step 2: Deploy Cloud Run Function (2nd Gen)
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk:slim'
    id: 'deploy-cloud-function'
    entrypoint: 'gcloud'
    args:
      - 'functions'
      - 'deploy'
      - 'site-datastore-ingestor'
      - '--gen2'
      - '--runtime=python311'
      - '--region=us-central1'
      - '--source=.'
      - '--entry-point=index_website_handler'
      - '--memory=2Gi'
      - '--timeout=1800s'
      - '--set-env-vars=GCP_PROJECT=$PROJECT_ID,LOCATION=${location},GCS_BUCKET=${bucket},GCS_PREFIX=${prefix},DATA_STORE_ID=${datastore},ENGINE_ID=${engine},CONFIG_URI=${configGcsUri}'

  # Step 3: Configure Cloud Scheduler with OIDC
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk:slim'
    id: 'setup-cloud-scheduler'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
          gcloud scheduler jobs create http $_SCHEDULER_JOB_NAME \\
            --location=$_REGION --schedule="$_CRON_SCHEDULE" --uri="$$FUNCTION_URI" \\
            --http-method=POST --headers="Content-Type=application/json" --message-body='{}' \\
            --oidc-service-account-email="$_SCHEDULER_SA"
        fi

substitutions:
  _FUNCTION_NAME: 'site-datastore-ingestor'
  _REGION: 'us-central1'
  _LOCATION: '${location}'
  _GCS_BUCKET: '${bucket}'
  _GCS_PREFIX: '${prefix}'
  _DATA_STORE_ID: '${datastore}'
  _ENGINE_ID: '${engine}'
  _CONFIG_URI: '${configGcsUri}'
  _CRON_SCHEDULE: '${schedule}'
  _SCHEDULER_JOB_NAME: 'site-ingestor-nightly-sync'
  _SCHEDULER_SA: 'cloud-scheduler-sa@\\${PROJECT_ID}.iam.gserviceaccount.com'`;
      document.getElementById('snippetCloudbuild').innerText = cloudbuildYaml;
      document.getElementById('snippetBuildSubmit').innerText = `gcloud builds submit --config=cloudbuild.yaml`;

      // 2. Deploy Cloud Run Function (Manual)
      const deployCmd = `gcloud functions deploy site-datastore-ingestor \\
  --gen2 \\
  --runtime=python311 \\
  --region=us-central1 \\
  --source=. \\
  --entry-point=index_website_handler \\
  --memory=2Gi \\
  --timeout=1800s \\
  --set-env-vars=GCP_PROJECT="${project}",LOCATION="${location}",GCS_BUCKET="${bucket}",GCS_PREFIX="${prefix}",DATA_STORE_ID="${datastore}",ENGINE_ID="${engine}",CONFIG_URI="${configGcsUri}",RECONCILIATION_MODE="${currentReconcileMode}"`;
      document.getElementById('snippetDeploy').innerText = deployCmd;

      // 3. Cloud Scheduler Command
      const schedCmd = `gcloud scheduler jobs create http site-ingestor-nightly-sync \\
  --schedule="${schedule}" \\
  --uri="https://us-central1-${project}.cloudfunctions.net/site-datastore-ingestor" \\
  --http-method=POST \\
  --headers="Content-Type=application/json" \\
  --message-body='{}' \\
  --oidc-service-account-email="cloud-scheduler-sa@${project}.iam.gserviceaccount.com"`;
      document.getElementById('snippetScheduler').innerText = schedCmd;

      // 4. Config JSON snippet
      const configPayload = {
        version: "2.0",
        project_id: project,
        location: location,
        gcs_bucket: bucket,
        gcs_prefix: prefix,
        data_store_id: datastore,
        engine_id: engine,
        schedule: schedule,
        reconciliation_mode: currentReconcileMode,
        default_delay: 0.2,
        targets: currentTargetMode === 'multi' ? targetsList : [{
          name: "primary-site",
          url: url,
          max_depth: parseInt(document.getElementById('singleMaxDepth').value, 10) || 2,
          max_pages: parseInt(document.getElementById('singleMaxPages').value, 10) || 30,
          exclude_patterns: ["*/archive/*"]
        }]
      };
      document.getElementById('snippetConfig').innerText = JSON.stringify(configPayload, null, 2);

      // 5. CLI Command
      const cliCmd = `.venv/bin/python web_to_gcs_ai_store.py \\
  --targets-file config.json \\
  --gcs-bucket "${bucket}" \\
  --gcs-prefix "${prefix}" \\
  --project-id "${project}" \\
  --data-store-id "${datastore}" \\
  --engine-id "${engine}" \\
  --reconciliation-mode "${currentReconcileMode}"`;
      document.getElementById('snippetCli').innerText = cliCmd;

      // 6. cURL Trigger
      document.getElementById('snippetCurl').innerText = `FUNCTION_URI=$(gcloud functions describe site-datastore-ingestor --gen2 --region=us-central1 --format='value(serviceConfig.uri)')

curl -X POST "$FUNCTION_URI" \\
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \\
  -H "Content-Type: application/json" \\
  -d '{"reconciliation_mode": "${currentReconcileMode}"}'`;

      // 7. Schema
      const schemaObj = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
          "title": {"type": "string", "keyPropertyMapping": "title"},
          "url": {"type": "string", "keyPropertyMapping": "uri"},
          "description": {"type": "string", "keyPropertyMapping": "description"},
          "author": {"type": "string"},
          "target_name": {"type": "string"},
          "crawled_at": {"type": "string"}
        }
      };
      document.getElementById('snippetSchema').innerText = JSON.stringify(schemaObj, null, 2);
    }

    function copySnippet(elementId) {
      const text = document.getElementById(elementId).innerText;
      navigator.clipboard.writeText(text);
      alert('Copied snippet to clipboard!');
    }

    async function handleStartJob(e) {
      e.preventDefault();

      let payload = {
        gcs_bucket: document.getElementById('gcs_bucket').value,
        gcs_prefix: document.getElementById('gcs_prefix').value,
        project_id: document.getElementById('project_id').value,
        data_store_id: document.getElementById('data_store_id').value,
        engine_id: document.getElementById('engine_id').value,
        location: document.getElementById('location').value,
        reconciliation_mode: currentReconcileMode,
        dry_run: (currentExecMode === 'local_dry'),
        execution_mode: currentExecMode,
      };

      if (currentTargetMode === 'multi') {
        payload.targets = targetsList;
      } else {
        payload.url = document.getElementById('singleUrl').value;
        payload.max_pages = parseInt(document.getElementById('singleMaxPages').value, 10);
        payload.max_depth = parseInt(document.getElementById('singleMaxDepth').value, 10);
        payload.delay = parseFloat(document.getElementById('singleDelay').value);
      }

      document.getElementById('startBtn').disabled = true;
      document.getElementById('startBtn').classList.add('opacity-50', 'cursor-not-allowed');

      try {
        const resp = await fetch('/api/jobs', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        currentJobId = data.job_id;

        document.getElementById('docsList').innerHTML = '';
        document.getElementById('logsConsole').innerHTML = '';
        document.getElementById('emptyDocs').classList.add('hidden');
        document.getElementById('docsList').classList.remove('hidden');

        switchMainTab('logs');
        startPolling();
      } catch (err) {
        alert('Failed to launch ingestion job: ' + err);
        document.getElementById('startBtn').disabled = false;
        document.getElementById('startBtn').classList.remove('opacity-50', 'cursor-not-allowed');
      }
    }

    function startPolling() {
      if (pollInterval) clearInterval(pollInterval);
      pollInterval = setInterval(async () => {
        if (!currentJobId) return;

        try {
          const rStatus = await fetch(`/api/jobs/${currentJobId}`);
          if (rStatus.ok) {
            const job = await rStatus.json();
            updateUIJobState(job);

            if (job.state === 'completed' || job.state === 'completed_with_errors' || job.state === 'failed') {
              clearInterval(pollInterval);
              document.getElementById('startBtn').disabled = false;
              document.getElementById('startBtn').classList.remove('opacity-50', 'cursor-not-allowed');
              fetchDatastoreStatus();
            }
          }

          const rLogs = await fetch(`/api/jobs/${currentJobId}/logs`);
          if (rLogs.ok) {
            const logData = await rLogs.json();
            updateUILogs(logData.logs || []);
          }
        } catch (e) {
          console.error("Polling error:", e);
        }
      }, 1000);
    }

    function updateUIJobState(job) {
      document.getElementById('progressBar').style.width = job.progress + '%';
      document.getElementById('progressPctText').innerText = job.progress + '%';
      document.getElementById('jobStageText').innerText = job.stage || 'Processing...';

      const badge = document.getElementById('jobBadge');
      if (job.state === 'running') {
        badge.className = 'text-[11px] font-bold uppercase tracking-widest px-2.5 py-1 rounded-full bg-blue-500/20 text-blue-400 border border-blue-500/40 animate-pulse';
        badge.innerText = 'Running';
      } else if (job.state === 'completed') {
        badge.className = 'text-[11px] font-bold uppercase tracking-widest px-2.5 py-1 rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/40';
        badge.innerText = 'Success';
      } else if (job.state === 'completed_with_errors') {
        badge.className = 'text-[11px] font-bold uppercase tracking-widest px-2.5 py-1 rounded-full bg-amber-500/20 text-amber-400 border border-amber-500/40';
        badge.innerText = 'Completed w/ Warnings';
      } else if (job.state === 'failed') {
        badge.className = 'text-[11px] font-bold uppercase tracking-widest px-2.5 py-1 rounded-full bg-rose-500/20 text-rose-400 border border-rose-500/40';
        badge.innerText = 'Failed';
      }

      // Steppers Highlight
      const prog = job.progress;
      document.getElementById('step1').className = prog >= 25 ? 'p-2 rounded-lg bg-blue-900/40 border border-blue-500/50 text-blue-300 font-bold' : 'p-2 rounded-lg bg-slate-900/50 border border-slate-800/50 text-slate-500';
      document.getElementById('step2').className = prog >= 55 ? 'p-2 rounded-lg bg-blue-900/40 border border-blue-500/50 text-blue-300 font-bold' : 'p-2 rounded-lg bg-slate-900/50 border border-slate-800/50 text-slate-500';
      document.getElementById('step3').className = prog >= 75 ? 'p-2 rounded-lg bg-blue-900/40 border border-blue-500/50 text-blue-300 font-bold' : 'p-2 rounded-lg bg-slate-900/50 border border-slate-800/50 text-slate-500';
      document.getElementById('step4').className = prog >= 100 ? 'p-2 rounded-lg bg-emerald-900/40 border border-emerald-500/50 text-emerald-300 font-bold' : 'p-2 rounded-lg bg-slate-900/50 border border-slate-800/50 text-slate-500';

      const pages = job.pages || [];
      document.getElementById('docCount').innerText = pages.length;
      if (pages.length > 0) {
        document.getElementById('emptyDocs').classList.add('hidden');
        const listEl = document.getElementById('docsList');
        listEl.classList.remove('hidden');

        listEl.innerHTML = pages.map(p => `
          <div onclick="openDocPreview('${p.id}')" class="p-3 rounded-xl bg-slate-900/70 border border-slate-800/80 hover:border-blue-500/50 hover:bg-slate-800/50 cursor-pointer transition flex items-center justify-between group">
            <div class="truncate mr-3">
              <div class="font-semibold text-xs text-white group-hover:text-blue-400 transition truncate">${p.title || 'Untitled'}</div>
              <div class="text-[11px] text-slate-400 font-mono truncate flex items-center gap-1 mt-0.5">
                ${p.target_name ? `<span class="px-1.5 py-0.2 rounded bg-indigo-950 text-indigo-300 text-[9px] border border-indigo-800/60 mr-1">${p.target_name}</span>` : ''}
                ${p.url}
              </div>
            </div>
            <div class="text-right shrink-0">
              <span class="text-[10px] font-mono px-2 py-0.5 rounded-full bg-slate-800 text-slate-300">${p.word_count} words</span>
            </div>
          </div>
        `).join('');
      }
    }

    function updateUILogs(logs) {
      const consoleEl = document.getElementById('logsConsole');
      if (logs.length === 0) return;

      consoleEl.innerHTML = logs.map(l => {
        let colorClass = 'text-slate-300';
        if (l.level === 'SUCCESS') colorClass = 'text-emerald-400 font-semibold';
        if (l.level === 'WARNING') colorClass = 'text-amber-400';
        if (l.level === 'ERROR') colorClass = 'text-rose-400 font-bold';
        return `<div><span class="text-slate-600">[${l.time}]</span> <span class="${colorClass}">${l.message}</span></div>`;
      }).join('');
      consoleEl.scrollTop = consoleEl.scrollHeight;
    }

    async function openDocPreview(docId) {
      if (!currentJobId) return;
      try {
        const resp = await fetch(`/api/jobs/${currentJobId}/document/${docId}`);
        if (resp.ok) {
          const doc = await resp.json();
          document.getElementById('modalDocTitle').innerText = doc.title || 'Untitled';
          document.getElementById('modalDocUrl').href = doc.url;
          document.getElementById('modalDocUrl').querySelector('span').innerText = doc.url;
          document.getElementById('modalDocMeta').innerText = JSON.stringify({
            id: doc.id,
            url: doc.url,
            target_name: doc.target_name || "default",
            word_count: doc.word_count,
            crawled_at: doc.crawled_at,
            grounding_key_mapping: "keyPropertyMapping: {'uri': 'url'}"
          }, null, 2);
          document.getElementById('modalDocContent').innerText = doc.content;
          document.getElementById('docModal').classList.remove('hidden');
        }
      } catch (e) {
        alert("Failed to load document preview: " + e);
      }
    }

    function closeDocModal() {
      document.getElementById('docModal').classList.add('hidden');
    }

    function formatBytes(bytes) {
      if (!bytes || bytes <= 0) return '0 B';
      const units = ['B', 'KB', 'MB', 'GB'];
      let i = 0;
      let val = bytes;
      while (val >= 1024 && i < units.length - 1) {
        val /= 1024;
        i++;
      }
      return val.toFixed(i === 0 ? 0 : 2) + ' ' + units[i];
    }

    function formatIsoTime(isoStr) {
      if (!isoStr) return '—';
      try {
        const d = new Date(isoStr);
        if (isNaN(d.getTime())) return isoStr;
        return d.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
      } catch (e) {
        return isoStr;
      }
    }

    let dsPollTimer = null;

    async function fetchDatastoreStatus() {
      const project = (document.getElementById('project_id')?.value || '').trim();
      const location = (document.getElementById('location')?.value || 'global').trim();
      const datastore = (document.getElementById('data_store_id')?.value || '').trim();

      const badge = document.getElementById('dsStateBadge');
      const subtitle = document.getElementById('dsSubtitle');
      const icon = document.getElementById('refreshDsIcon');

      if (!project || !datastore) {
        badge.className = 'text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-slate-800 text-slate-400 border border-slate-700';
        badge.innerText = 'Unconfigured';
        subtitle.innerText = 'Enter GCP Project ID & Data Store ID to inspect live index';
        return;
      }

      if (icon) icon.classList.add('animate-spin');
      badge.className = 'text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-blue-500/20 text-blue-400 border border-blue-500/40 animate-pulse';
      badge.innerText = 'Checking...';
      subtitle.innerText = `Querying ${project} / ${location} / ${datastore} ...`;

      try {
        const q = new URLSearchParams({ project_id: project, location: location, data_store_id: datastore });
        const resp = await fetch(`/api/datastore-status?${q.toString()}`);
        const data = await resp.json();

        if (!data.exists) {
          badge.className = 'text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-400 border border-amber-500/40';
          badge.innerText = data.state || 'Not Found';
          subtitle.innerText = data.error || `Data Store '${datastore}' does not exist yet (will be created on first sync).`;
          document.getElementById('dsDocCount').innerText = '0';
          document.getElementById('dsFreshestCrawl').innerText = 'Not created yet';
          document.getElementById('dsTotalSize').innerText = '0 B';
          document.getElementById('dsSizeBreakdown').innerText = '—';
          document.getElementById('dsLastSyncStatus').innerText = 'None';
          document.getElementById('dsLastSyncTime').innerText = '—';
          document.getElementById('dsGroundingBadge').innerHTML = '<span class="text-slate-400">Pending creation</span>';
          return;
        }

        // 1. State badge & subtitle
        const imp = data.latest_import;
        const isImporting = imp && imp.done === false;
        if (isImporting) {
          badge.className = 'text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-300 border border-amber-500/40 animate-pulse';
          badge.innerText = 'Import Running...';
          if (dsPollTimer) clearTimeout(dsPollTimer);
          dsPollTimer = setTimeout(fetchDatastoreStatus, 5000);
        } else {
          badge.className = 'text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/40';
          badge.innerText = data.state || 'ACTIVE';
        }
        subtitle.innerText = `${data.project_id} • ${data.location} • ${data.data_store_id}`;

        // 2. Document Count & Freshest Crawl
        document.getElementById('dsDocCount').innerText = `${(data.document_count || 0).toLocaleString()} docs`;
        document.getElementById('dsFreshestCrawl').innerText = data.latest_crawled_at
          ? `Latest: ${formatIsoTime(data.latest_crawled_at)}`
          : 'No crawl timestamp found';

        // 3. Storage Size
        document.getElementById('dsTotalSize').innerText = formatBytes(data.total_bytes || 0);
        document.getElementById('dsSizeBreakdown').innerText =
          `${formatBytes(data.unstructured_bytes || 0)} MD • ${formatBytes(data.structured_bytes || 0)} Meta`;

        // 4. Latest Import Operation
        const syncStatusEl = document.getElementById('dsLastSyncStatus');
        const syncTimeEl = document.getElementById('dsLastSyncTime');
        if (imp) {
          if (!imp.done) {
            syncStatusEl.className = 'text-xs font-bold text-amber-400 mt-1.5 truncate animate-pulse';
            syncStatusEl.innerText = `Indexing (${imp.success_count}/${imp.total_count || '?'} docs)...`;
          } else if (imp.failure_count > 0 && imp.success_count === 0) {
            syncStatusEl.className = 'text-xs font-bold text-rose-400 mt-1.5 truncate';
            syncStatusEl.innerText = `Failed (0/${imp.total_count} docs)`;
          } else {
            syncStatusEl.className = 'text-xs font-bold text-emerald-400 mt-1.5 truncate';
            syncStatusEl.innerText = `Succeeded (${imp.success_count}/${imp.total_count} docs)`;
          }
          syncTimeEl.innerText = formatIsoTime(imp.update_time || imp.create_time);
        } else {
          syncStatusEl.className = 'text-xs font-bold text-slate-400 mt-1.5 truncate';
          syncStatusEl.innerText = 'No import history';
          syncTimeEl.innerText = '—';
        }

        // 5. Citation Grounding
        const groundEl = document.getElementById('dsGroundingBadge');
        if (data.citation_grounding_active) {
          groundEl.className = 'text-xs font-bold text-emerald-400 mt-1.5 flex items-center gap-1';
          groundEl.innerHTML = '<span>url → uri Active ✅</span>';
        } else {
          groundEl.className = 'text-xs font-bold text-amber-400 mt-1.5 flex items-center gap-1';
          groundEl.innerHTML = '<span>Unmapped ⚠️</span>';
        }
        document.getElementById('dsSearchTier').innerText = `Tier: ${data.search_tier || 'STANDARD'}`;

      } catch (err) {
        badge.className = 'text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-rose-500/20 text-rose-400 border border-rose-500/40';
        badge.innerText = 'Error';
        subtitle.innerText = `Status check error: ${err}`;
      } finally {
        if (icon) icon.classList.remove('animate-spin');
      }
    }

    // Attach auto-refresh when project or datastore inputs change
    ['project_id', 'location', 'data_store_id'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', fetchDatastoreStatus);
    });

    // Initialize UI on page load
    renderTargetCards();
    updateDynamicSnippets();
    fetchDatastoreStatus();
  </script>
</body>
</html>
"""


class IngestServerHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path in ["/", "/index.html"]:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode("utf-8"))
            return

        elif parsed.path == "/api/targets":
            targets_file = Path("./targets.json")
            if targets_file.exists():
                with open(targets_file, "r", encoding="utf-8") as f:
                    content = json.load(f)
                self._send_json(content)
            else:
                self._send_json({"version": "2.0", "targets": []})
            return

        elif parsed.path == "/api/jobs":
            with JOB_LOCK:
                jobs_list = list(JOBS.values())
            self._send_json({"jobs": jobs_list})
            return

        elif parsed.path.startswith("/api/jobs/"):
            parts = parsed.path.strip("/").split("/")
            if len(parts) == 3:
                job_id = parts[2]
                with JOB_LOCK:
                    job = JOBS.get(job_id)
                if job:
                    self._send_json(job)
                else:
                    self.send_error(HTTPStatus.NOT_FOUND, "Job not found")
                return

            elif len(parts) == 4 and parts[3] == "logs":
                job_id = parts[2]
                with JOB_LOCK:
                    logs = LOGS.get(job_id, [])
                self._send_json({"job_id": job_id, "logs": logs})
                return

            elif len(parts) == 5 and parts[3] == "document":
                job_id = parts[2]
                doc_id = parts[4]
                with JOB_LOCK:
                    job_docs = DOCUMENTS.get(job_id, {})
                    doc = job_docs.get(doc_id)
                if doc:
                    self._send_json(doc)
                else:
                    self.send_error(HTTPStatus.NOT_FOUND, "Document not found")
                return

        elif parsed.path == "/api/datastore-status":
            qs = parse_qs(parsed.query)
            project_id = (qs.get("project_id", [""])[0] or os.environ.get("GCP_PROJECT", "")).strip()
            location = (qs.get("location", ["global"])[0] or "global").strip()
            data_store_id = (qs.get("data_store_id", [""])[0] or os.environ.get("DATA_STORE_ID", "")).strip()
            if not project_id or not data_store_id:
                self._send_json({
                    "exists": False,
                    "state": "UNCONFIGURED",
                    "error": "Enter GCP Project ID and Data Store ID to inspect status.",
                })
                return
            try:
                status_data = get_datastore_live_status(project_id, location, data_store_id)
                self._send_json(status_data)
            except Exception as e:
                logger.warning(f"Datastore status check failed: {e}")
                self._send_json({
                    "exists": False,
                    "state": "ERROR",
                    "error": str(e),
                    "project_id": project_id,
                    "data_store_id": data_store_id,
                })
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len).decode("utf-8") if content_len > 0 else "{}"

        # 1. Pull Config from GCS
        if self.path == "/api/cloud/pull-config":
            try:
                data = json.loads(body)
                bucket = data.get("gcs_bucket") or os.environ.get("GCS_BUCKET")
                if not bucket:
                    self.send_error(HTTPStatus.BAD_REQUEST, "GCS bucket must be specified.")
                    return
                prefix = data.get("gcs_prefix", "website_datastore").strip("/")
                gcs_uri = f"gs://{bucket}/{prefix}/config.json"

                config_obj = None
                if GCS_AVAILABLE:
                    try:
                        client = storage.Client()
                        b = client.bucket(bucket)
                        blob = b.blob(f"{prefix}/config.json")
                        if blob.exists():
                            config_obj = json.loads(blob.download_as_text())
                    except Exception as ex:
                        logger.warning(f"GCS client read failed, falling back to gcloud CLI: {ex}")

                if not config_obj:
                    res = subprocess.run(["gcloud", "storage", "cat", gcs_uri], capture_output=True, text=True)
                    if res.returncode == 0:
                        config_obj = json.loads(res.stdout)
                    else:
                        self.send_error(HTTPStatus.NOT_FOUND, f"Could not read {gcs_uri}: {res.stderr}")
                        return

                self._send_json({"status": "success", "uri": gcs_uri, "config": config_obj})
                return
            except Exception as e:
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
                return

        # 2. Push Config to GCS
        elif self.path == "/api/cloud/push-config":
            try:
                data = json.loads(body)
                bucket = data.get("gcs_bucket") or os.environ.get("GCS_BUCKET")
                if not bucket:
                    self.send_error(HTTPStatus.BAD_REQUEST, "GCS bucket must be specified.")
                    return
                prefix = data.get("gcs_prefix", "website_datastore").strip("/")
                gcs_uri = f"gs://{bucket}/{prefix}/config.json"

                with open("./config.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

                uploaded = False
                if GCS_AVAILABLE:
                    try:
                        client = storage.Client()
                        b = client.bucket(bucket)
                        blob = b.blob(f"{prefix}/config.json")
                        blob.upload_from_filename("./config.json", content_type="application/json")
                        uploaded = True
                    except Exception as ex:
                        logger.warning(f"GCS client upload failed, falling back to gcloud CLI: {ex}")

                if not uploaded:
                    res = subprocess.run(["gcloud", "storage", "cp", "./config.json", gcs_uri], capture_output=True, text=True)
                    if res.returncode != 0:
                        self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Upload to {gcs_uri} failed: {res.stderr}")
                        return

                self._send_json({"status": "success", "uri": gcs_uri})
                return
            except Exception as e:
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
                return

        # 3. Trigger Cloud Run Function via HTTP with OIDC Token
        elif self.path == "/api/cloud/trigger-run":
            try:
                data = json.loads(body)
                function_url = data.get("function_url")
                if not function_url:
                    func_name = data.get("function_name", "site-datastore-ingestor")
                    reg = data.get("region", "us-central1")
                    f_res = subprocess.run(["gcloud", "functions", "describe", func_name, "--gen2", f"--region={reg}", "--format=value(serviceConfig.uri)"], capture_output=True, text=True)
                    if f_res.returncode == 0 and f_res.stdout.strip():
                        function_url = f_res.stdout.strip()
                    else:
                        self.send_error(HTTPStatus.BAD_REQUEST, "Could not auto-detect Cloud Run Function URL. Please specify 'function_url'.")
                        return

                token = ""
                # 1. Try direct print-identity-token with audience
                t_res = subprocess.run(["gcloud", "auth", "print-identity-token", f"--audiences={function_url}"], capture_output=True, text=True)
                if t_res.returncode == 0 and t_res.stdout.strip():
                    token = t_res.stdout.strip().splitlines()[-1].strip()
                else:
                    # 2. Try with compute service account impersonation dynamically
                    sa = data.get("service_account")
                    if not sa:
                        pnum_res = subprocess.run(["gcloud", "projects", "describe", data.get("project_id") or os.environ.get("GCP_PROJECT", ""), "--format=value(projectNumber)"], capture_output=True, text=True)
                        if pnum_res.returncode == 0 and pnum_res.stdout.strip():
                            sa = f"{pnum_res.stdout.strip()}-compute@developer.gserviceaccount.com"

                    if sa:
                        t_imp = subprocess.run(
                            ["gcloud", "auth", "print-identity-token", f"--audiences={function_url}", f"--impersonate-service-account={sa}"],
                            capture_output=True, text=True
                        )
                        if t_imp.returncode == 0 and t_imp.stdout.strip():
                            valid_lines = [l.strip() for l in t_imp.stdout.splitlines() if l.strip() and not l.startswith("WARNING:")]
                            if valid_lines:
                                token = valid_lines[-1]

                if not token:
                    # 3. Fallback to basic print-identity-token
                    t_basic = subprocess.run(["gcloud", "auth", "print-identity-token"], capture_output=True, text=True)
                    token = t_basic.stdout.strip().splitlines()[-1].strip() if t_basic.stdout.strip() else ""

                if not token:
                    self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Could not acquire identity token for Cloud Run invocation.")
                    return

                req = urllib.request.Request(
                    function_url,
                    data=b"{}",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    method="POST",
                )
                try:
                    with urllib.request.urlopen(req, timeout=300) as resp:
                        resp_data = resp.read().decode("utf-8")
                        self._send_json({"status": "success", "response": json.loads(resp_data)})
                except Exception as req_ex:
                    self._send_json({"status": "error", "message": str(req_ex)})
                return
            except Exception as e:
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
                return

        # 4. Save Local targets.json
        elif self.path == "/api/targets":
            try:
                data = json.loads(body)
                with open("./targets.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                self._send_json({"status": "saved"})
            except Exception as ex:
                self.send_error(HTTPStatus.BAD_REQUEST, f"Invalid JSON: {ex}")
            return

        # 5. Local Pipeline Run
        elif self.path == "/api/jobs":
            try:
                data = json.loads(body)
            except Exception:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid JSON payload")
                return

            job_id = f"job_{int(time.time())}_{hashlib.md5(body.encode()).hexdigest()[:6]}"
            with JOB_LOCK:
                JOBS[job_id] = {
                    "id": job_id,
                    "config": data,
                    "state": "running",
                    "stage": "Initializing",
                    "progress": 0,
                    "pages": [],
                    "metadata_uri": None,
                }
                LOGS[job_id] = []
                DOCUMENTS[job_id] = {}

            worker = IngestionWorker(job_id, data)
            t = threading.Thread(target=worker.run, daemon=True)
            t.start()

            self._send_json({"job_id": job_id, "status": "started"})
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def _send_json(self, data: Dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def run_gui_server(port: int = 8085, host: str = "127.0.0.1"):
    httpd = None
    for candidate_port in range(port, port + 20):
        try:
            httpd = ReusableThreadingHTTPServer((host, candidate_port), IngestServerHandler)
            if candidate_port != port:
                print(f"Note: Port {port} was busy; automatically bound to port {candidate_port} instead.")
            port = candidate_port
            break
        except OSError:
            continue

    if httpd is None:
        raise RuntimeError(f"Could not find an open port between {port} and {port + 19}.")

    print(f"\n==================================================================")
    print(f" Gemini Enterprise Ingestor & Cloud Control Plane Active!")
    print(f" Web UI: http://{'localhost' if host in ('127.0.0.1', '0.0.0.0') else host}:{port}")
    if host != "127.0.0.1":
        print("")
        print(" *** WARNING *******************************************************")
        print(" This server has NO authentication. Binding to a non-loopback")
        print(f" address ({host}) exposes config changes and crawl triggering to")
        print(" anyone who can reach this port. Use only on a trusted network,")
        print(" or place it behind Identity-Aware Proxy.")
        print(" *******************************************************************")
    print(f"==================================================================\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        httpd.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Gemini Enterprise Ingestor Web GUI")
    parser.add_argument("--port", type=int, default=8085, help="Port to bind the web server (default: 8085)")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help=(
            "Address to bind. Defaults to 127.0.0.1 (localhost only) because the UI "
            "is unauthenticated. Use 0.0.0.0 only inside a container or trusted network."
        ),
    )
    args = parser.parse_args()
    run_gui_server(port=args.port, host=args.host)
