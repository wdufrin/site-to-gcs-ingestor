#!/usr/bin/env python3
"""
Cloud Run Function: Website to GCS & Gemini Enterprise Datastore Ingestor
========================================================================
- Crawls one or multiple public websites with custom depth and path filters.
- Extracts clean, AI-optimized Markdown with table and code fence preservation.
- Generates Vertex AI Search / Gemini Enterprise compliant metadata.jsonl.
- Uploads markdown files and metadata directly to Google Cloud Storage.
- Creates/updates the Vertex AI Search Data Store with schema keyPropertyMapping (url -> uri).
  This guarantees that Gemini Enterprise assistant citations route to public web links, NOT gs://!
- Triggers document import with FULL reconciliation mode so updated pages are refreshed
  and deleted pages are purged automatically on a periodic schedule.
- Supports multi-target management via direct payload or GCS-hosted targets.json (TARGETS_CONFIG_URI).
"""

import fnmatch
import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urldefrag, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

# Google Cloud SDKs (loaded dynamically or with graceful handling)
try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

try:
    from google.cloud import discoveryengine_v1 as discoveryengine
    DISCOVERYENGINE_AVAILABLE = True
except ImportError:
    DISCOVERYENGINE_AVAILABLE = False

try:
    import functions_framework
    FUNCTIONS_FRAMEWORK_AVAILABLE = True
except ImportError:
    FUNCTIONS_FRAMEWORK_AVAILABLE = False


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("site_ingestor")


def make_doc_id(url: str) -> str:
    """Generates a stable, alphanumeric document ID from a URL."""
    hash_suffix = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]
    parsed = urlparse(url)
    slug = re.sub(r"[^a-zA-Z0-9_-]", "_", parsed.path.strip("/"))
    if not slug:
        slug = "index"
    return f"{slug[:40]}_{hash_suffix}"


def html_to_markdown(soup_node: Tag) -> str:
    """
    Recursively converts a BeautifulSoup node into structured, clean Markdown.
    Avoids duplicate text, supports tables, code blocks, lists, quotes, and links,
    and strips noise tags and anchor permalinks.
    """
    def _convert(node) -> str:
        if isinstance(node, NavigableString):
            text = str(node)
            return re.sub(r"[ \t]+", " ", text)
        if not isinstance(node, Tag):
            return ""

        tag = node.name.lower()
        if tag in ["script", "style", "nav", "footer", "header", "aside", "noscript", "svg", "form", "iframe"]:
            return ""

        # Recursive child conversion
        child_text = "".join(_convert(c) for c in node.children)

        if tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            level = int(tag[1])
            clean_text = child_text.strip().rstrip("¶").rstrip("#").strip()
            return f"\n\n{'#' * level} {clean_text}\n\n" if clean_text else ""
        elif tag == "p":
            clean_text = child_text.strip()
            return f"\n\n{clean_text}\n\n" if clean_text else ""
        elif tag in ["strong", "b"]:
            clean_text = child_text.strip()
            return f"**{clean_text}**" if clean_text else ""
        elif tag in ["em", "i"]:
            clean_text = child_text.strip()
            return f"*{clean_text}*" if clean_text else ""
        elif tag == "code":
            if node.parent and node.parent.name == "pre":
                return child_text
            clean_text = child_text.strip()
            return f"`{clean_text}`" if clean_text else ""
        elif tag == "pre":
            code_text = node.get_text().strip()
            return f"\n\n```\n{code_text}\n```\n\n" if code_text else ""
        elif tag == "a":
            href = node.get("href", "").strip()
            clean_text = child_text.strip()
            if not clean_text or clean_text in ["¶", "#"]:
                return ""
            if href and not href.startswith("#"):
                return f"[{clean_text}]({href})"
            return clean_text
        elif tag == "li":
            clean_text = child_text.strip()
            return f"\n- {clean_text}" if clean_text else ""
        elif tag in ["ul", "ol"]:
            return f"\n{child_text}\n"
        elif tag == "blockquote":
            clean_text = child_text.strip()
            return f"\n\n> {clean_text}\n\n" if clean_text else ""
        elif tag == "table":
            rows = []
            for tr in node.find_all("tr"):
                cols = [c.get_text().strip().replace("|", "\\|") for c in tr.find_all(["th", "td"])]
                if cols:
                    rows.append(cols)
            if not rows:
                return ""
            col_count = max(len(r) for r in rows)
            padded_rows = [r + [""] * (col_count - len(r)) for r in rows]
            header = "| " + " | ".join(padded_rows[0]) + " |"
            sep = "| " + " | ".join(["---"] * col_count) + " |"
            body = "\n".join("| " + " | ".join(r) + " |" for r in padded_rows[1:])
            return f"\n\n{header}\n{sep}\n{body}\n\n"
        elif tag in ["div", "section", "article", "main", "body"]:
            return child_text
        return child_text

    raw_md = _convert(soup_node)
    # Collapse multiple blank lines
    clean_md = re.sub(r"\n{3,}", "\n\n", raw_md).strip()
    return clean_md


class WebsiteCrawler:
    def __init__(
        self,
        base_url: str,
        max_pages: int = 50,
        max_depth: int = 3,
        delay_seconds: float = 0.2,
        user_agent: str = "GeminiEnterpriseIngestor/2.0 (+https://cloud.google.com)",
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        visited_urls: Optional[Set[str]] = None,
        target_name: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
    ):
        self.base_url = self._normalize_url(base_url)
        parsed = urlparse(self.base_url)
        self.base_domain = parsed.netloc.lower()
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.delay_seconds = delay_seconds
        self.user_agent = user_agent
        self.include_patterns = [p.strip() for p in (include_patterns or []) if p.strip()]
        self.exclude_patterns = [p.strip() for p in (exclude_patterns or []) if p.strip()]
        self.visited = visited_urls if visited_urls is not None else set()
        self.target_name = target_name or ""

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        if headers and isinstance(headers, dict):
            self.session.headers.update(headers)
        if cookies and isinstance(cookies, dict):
            self.session.cookies.update(cookies)

    def _normalize_url(self, url: str) -> str:
        url, _ = urldefrag(url)
        url = url.strip()
        parsed = urlparse(url)
        if parsed.path == "/":
            url = f"{parsed.scheme}://{parsed.netloc}"
        return url

    def _is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False

        netloc = parsed.netloc.lower()
        if not (netloc == self.base_domain or netloc.endswith("." + self.base_domain)):
            return False

        ignored_extensions = (
            ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
            ".pdf", ".zip", ".tar", ".gz", ".rar", ".7z",
            ".exe", ".dmg", ".pkg", ".deb", ".rpm",
            ".mp3", ".mp4", ".mov", ".avi", ".wmv", ".wav",
            ".css", ".js", ".json", ".xml", ".woff", ".woff2", ".ttf"
        )
        if any(parsed.path.lower().endswith(ext) for ext in ignored_extensions):
            return False

        # Pattern-based exclusion filtering
        if self.exclude_patterns:
            for pattern in self.exclude_patterns:
                if fnmatch.fnmatch(url, pattern) or fnmatch.fnmatch(parsed.path, pattern):
                    return False

        # Pattern-based inclusion filtering
        if self.include_patterns:
            matched = any(
                fnmatch.fnmatch(url, pattern) or fnmatch.fnmatch(parsed.path, pattern)
                for pattern in self.include_patterns
            )
            if not matched:
                return False

        return True

    def _extract_page(self, resp: requests.Response, url: str) -> Optional[Dict]:
        # Handle encoding properly to prevent mojibake (e.g. smart quotes turning into â€™)
        if resp.encoding is None or resp.encoding.lower() == "iso-8859-1":
            resp.encoding = resp.apparent_encoding or "utf-8"

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract Title
        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        elif soup.find("h1"):
            title = soup.find("h1").get_text().strip()
        else:
            title = url

        # Extract Meta Description
        description = ""
        meta_desc = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if meta_desc and meta_desc.get("content"):
            description = meta_desc["content"].strip()

        # Extract Author
        author = ""
        meta_author = soup.find("meta", attrs={"name": "author"}) or soup.find("meta", attrs={"property": "article:author"})
        if meta_author and meta_author.get("content"):
            author = meta_author["content"].strip()

        # Locate Main Content
        main_content = (
            soup.find("article")
            or soup.find("main")
            or soup.find("div", {"id": re.compile(r"(content|main|article|body)", re.I)})
            or soup.find("div", {"class": re.compile(r"(content|main|article|body|post)", re.I)})
            or soup.body
        )

        if not main_content:
            return None

        # Convert to clean Markdown
        markdown_text = html_to_markdown(main_content)
        if len(markdown_text.strip()) < 50:
            return None

        doc_id = make_doc_id(url)
        return {
            "id": doc_id,
            "url": url,
            "title": title,
            "description": description,
            "author": author,
            "content": markdown_text,
            "word_count": len(markdown_text.split()),
            "crawled_at": datetime.now(timezone.utc).isoformat(),
            "target_name": self.target_name,
        }

    def _extract_links(self, html: str, current_url: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        base_tag = soup.find("base", href=True)
        base_url = urljoin(current_url, base_tag["href"]) if base_tag else current_url

        links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href or href.startswith(("#", "mailto:", "javascript:", "tel:")):
                continue
            abs_url = self._normalize_url(urljoin(base_url, href))
            if self._is_valid_url(abs_url):
                links.append(abs_url)
        return links

    def crawl(self) -> List[Dict]:
        queue: List[Tuple[str, int]] = [(self.base_url, 0)]
        results: List[Dict] = []

        logger.info(f"Starting crawl at {self.base_url} (Max pages: {self.max_pages}, Max depth: {self.max_depth})")

        while queue and len(results) < self.max_pages:
            url, depth = queue.pop(0)
            if url in self.visited:
                continue
            self.visited.add(url)

            logger.info(f"[{len(results) + 1}/{self.max_pages}] Fetching: {url} (depth={depth})")

            try:
                resp = self.session.get(url, timeout=12, headers={"Accept": "text/html,application/xhtml+xml"})
                if resp.status_code != 200 or "text/html" not in resp.headers.get("Content-Type", ""):
                    continue

                effective_url = resp.url or url
                page_data = self._extract_page(resp, effective_url)
                if page_data:
                    results.append(page_data)

                if depth < self.max_depth:
                    new_links = self._extract_links(resp.text, effective_url)
                    for link in new_links:
                        if link not in self.visited:
                            queue.append((link, depth + 1))

            except Exception as e:
                logger.warning(f"Error crawling {url}: {e}")

            time.sleep(self.delay_seconds)

        self.stats = {
            "target": self.target_name,
            "url": self.base_url,
            "pages_crawled": len(results),
            "max_pages_budget": self.max_pages,
            "max_depth_limit": self.max_depth,
            "queue_exhausted": len(queue) == 0,
            "remaining_queue": len(queue),
            "hit_max_pages_limit": len(results) >= self.max_pages and len(queue) > 0,
        }
        if self.stats["hit_max_pages_limit"]:
            logger.warning(
                f"Crawl capped for '{self.target_name}': reached max_pages limit ({self.max_pages}) "
                f"with {len(queue)} pending URLs remaining in queue. Increase max_pages to crawl remaining pages."
            )
        else:
            logger.info(
                f"Crawl completed for '{self.target_name}': {len(results)} valid pages crawled. "
                f"Queue fully exhausted (0 remaining). Complete coverage achieved for depth={self.max_depth}."
            )
        return results


def stage_artifacts(
    pages: List[Dict],
    staging_dir: str,
    bucket_name: str,
    prefix: str,
) -> Tuple[List[str], str]:
    """
    Saves clean markdown documents with YAML frontmatter and builds metadata.jsonl.
    Schema maps `structData.url` as the live URL for citation grounding.
    """
    out_path = Path(staging_dir)
    docs_dir = out_path / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)

    clean_prefix = prefix.strip("/")
    prefix_str = f"{clean_prefix}/" if clean_prefix else ""
    metadata_lines = []
    saved_files = []

    for page in pages:
        doc_id = page["id"]
        filename = f"{doc_id}.md"
        filepath = docs_dir / filename

        md_document = (
            f"---\n"
            f"id: {doc_id}\n"
            f"title: \"{page['title']}\"\n"
            f"url: {page['url']}\n"
            f"description: \"{page['description']}\"\n"
            f"author: \"{page.get('author', '')}\"\n"
            f"target_name: \"{page.get('target_name', '')}\"\n"
            f"crawled_at: {page['crawled_at']}\n"
            f"---\n\n"
            f"# {page['title']}\n\n"
            f"**Source URL:** {page['url']}\n\n"
            f"{page['content']}\n"
        )
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(md_document)
        saved_files.append(str(filepath))

        gcs_uri = f"gs://{bucket_name}/{prefix_str}documents/{filename}" if bucket_name else f"file://{filepath}"
        meta_entry = {
            "id": doc_id,
            "structData": {
                "title": page["title"],
                "url": page["url"],
                "description": page["description"],
                "author": page.get("author", ""),
                "target_name": page.get("target_name", ""),
                "crawled_at": page["crawled_at"],
            },
            "content": {
                "mimeType": "text/markdown",
                "uri": gcs_uri,
            },
        }
        metadata_lines.append(json.dumps(meta_entry))

    metadata_path = out_path / "metadata.jsonl"
    with open(metadata_path, "w", encoding="utf-8") as f:
        f.write("\n".join(metadata_lines) + "\n")

    return saved_files, str(metadata_path)


def upload_to_gcs(staging_dir: str, bucket_name: str, prefix: str) -> None:
    """Uploads staged markdown files and metadata.jsonl to Google Cloud Storage."""
    clean_prefix = prefix.strip("/")
    prefix_str = f"{clean_prefix}/" if clean_prefix else ""
    staging_path = Path(staging_dir)

    if GCS_AVAILABLE:
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            for file_path in staging_path.rglob("*"):
                if file_path.is_file():
                    rel_path = file_path.relative_to(staging_path).as_posix()
                    blob_name = f"{prefix_str}{rel_path}"
                    blob = bucket.blob(blob_name)
                    content_type = "text/markdown" if file_path.suffix == ".md" else "application/json"
                    blob.upload_from_filename(str(file_path), content_type=content_type)
                    logger.info(f"Uploaded -> gs://{bucket_name}/{blob_name}")
            return
        except Exception as ex:
            logger.warning(f"Python GCS client upload failed, trying gcloud CLI fallback: {ex}")

    # Fallback to gcloud storage CLI
    target_uri = f"gs://{bucket_name}/{prefix_str}"
    logger.info(f"Uploading via gcloud storage cp to {target_uri}...")
    cp_res = subprocess.run(
        ["gcloud", "storage", "cp", "-r", f"{staging_dir}/*", target_uri],
        capture_output=True,
        text=True,
        shell=True,
    )
    if cp_res.returncode != 0:
        cp_res2 = subprocess.run(["gcloud", "storage", "cp", "-r", staging_dir, target_uri], capture_output=True, text=True)
        if cp_res2.returncode != 0:
            raise RuntimeError(f"GCS upload failed: {cp_res.stderr or cp_res2.stderr}")
    logger.info(f"Successfully uploaded staged files to gs://{bucket_name}/{prefix_str} via gcloud CLI")


def ensure_datastore_and_schema(project_id: str, location: str, data_store_id: str) -> None:
    """
    Creates the Discovery Engine Data Store if it doesn't exist, and configures
    the schema with keyPropertyMapping: {"uri": "url"}.
    This forces the Gemini Enterprise assistant to cite public URLs, not gs://!
    """
    schema_dict = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "title": {"type": "string", "keyPropertyMapping": "title"},
            "url": {"type": "string", "keyPropertyMapping": "uri"},
            "description": {"type": "string", "keyPropertyMapping": "description"},
            "author": {"type": "string"},
            "target_name": {"type": "string"},
            "crawled_at": {"type": "string"},
        },
    }

    if DISCOVERYENGINE_AVAILABLE:
        try:
            client = discoveryengine.DataStoreServiceClient()
            parent = f"projects/{project_id}/locations/{location}/collections/default_collection"
            data_store_name = f"{parent}/dataStores/{data_store_id}"

            try:
                client.get_data_store(name=data_store_name)
                logger.info(f"DataStore '{data_store_id}' exists.")
            except Exception:
                logger.info(f"Creating DataStore '{data_store_id}'...")
                data_store = discoveryengine.DataStore(
                    display_name=data_store_id,
                    industry_vertical=discoveryengine.IndustryVertical.GENERIC,
                    solution_types=[discoveryengine.SolutionType.SOLUTION_TYPE_SEARCH],
                    content_config=discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED,
                )
                op = client.create_data_store(
                    parent=parent,
                    data_store=data_store,
                    data_store_id=data_store_id,
                )
                op.result()
                logger.info(f"DataStore '{data_store_id}' successfully created.")

            # Configure Schema with keyPropertyMapping (critical for citations)
            schema_client = discoveryengine.SchemaServiceClient()
            schema_name = f"{data_store_name}/schemas/default_schema"
            schema = discoveryengine.Schema(
                name=schema_name,
                json_schema=json.dumps(schema_dict),
            )
            req = discoveryengine.UpdateSchemaRequest(schema=schema)
            op = schema_client.update_schema(request=req)
            op.result()
            logger.info(f"Configured schema with keyPropertyMapping (uri -> url) on {data_store_id}")
            return
        except Exception as e:
            logger.warning(f"Python Discovery Engine schema client failed, trying REST API fallback: {e}")

    # Fallback via REST API
    token_res = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True, text=True)
    if token_res.returncode == 0 and token_res.stdout.strip():
        token = token_res.stdout.strip()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-goog-user-project": project_id,
        }
        # Update schema via PATCH
        schema_url = f"https://discoveryengine.googleapis.com/v1/projects/{project_id}/locations/{location}/collections/default_collection/dataStores/{data_store_id}/schemas/default_schema"
        resp = requests.patch(
            schema_url,
            headers=headers,
            json={"jsonSchema": json.dumps(schema_dict)},
            timeout=30,
        )
        if resp.status_code == 200:
            logger.info(f"Configured schema via REST API on {data_store_id}")
        else:
            logger.warning(f"REST schema update note ({resp.status_code}): {resp.text}")


def trigger_datastore_import(project_id: str, location: str, data_store_id: str, metadata_gcs_uri: str) -> str:
    """
    Imports documents from metadata.jsonl with FULL reconciliation mode so
    obsolete pages are removed on scheduled refreshes.
    """
    if DISCOVERYENGINE_AVAILABLE:
        try:
            doc_client = discoveryengine.DocumentServiceClient()
            parent = (
                f"projects/{project_id}/locations/{location}/collections/default_collection/"
                f"dataStores/{data_store_id}/branches/default_branch"
            )
            gcs_source = discoveryengine.GcsSource(
                input_uris=[metadata_gcs_uri],
                data_schema="document",
            )
            request = discoveryengine.ImportDocumentsRequest(
                parent=parent,
                gcs_source=gcs_source,
                reconciliation_mode=discoveryengine.ImportDocumentsRequest.ReconciliationMode.FULL,
            )
            operation = doc_client.import_documents(request=request)
            op_name = operation.operation.name
            logger.info(f"Started Discovery Engine import operation: {op_name}")
            return op_name
        except Exception as ex:
            logger.warning(f"Python Discovery Engine client failed, trying REST API fallback: {ex}")

    # Fallback: Discovery Engine REST API using gcloud access token
    token_res = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True, text=True)
    if token_res.returncode == 0 and token_res.stdout.strip():
        token = token_res.stdout.strip()
        url = (
            f"https://discoveryengine.googleapis.com/v1/projects/{project_id}/locations/{location}/"
            f"collections/default_collection/dataStores/{data_store_id}/branches/0/documents:import"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-goog-user-project": project_id,
        }
        payload = {
            "gcsSource": {"inputUris": [metadata_gcs_uri], "dataSchema": "document"},
            "reconciliationMode": "FULL",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code == 200:
            op_name = resp.json().get("name", "started")
            logger.info(f"Started Discovery Engine import via REST API: {op_name}")
            return op_name
        else:
            raise RuntimeError(f"Discovery Engine REST import failed ({resp.status_code}): {resp.text}")

    raise ImportError("google-cloud-discoveryengine missing and gcloud access token unavailable.")


def link_datastore_to_engine(project_id: str, location: str, engine_id: str, data_store_id: str) -> None:
    """Attaches the Data Store to a Gemini Enterprise Engine/App if specified."""
    if not DISCOVERYENGINE_AVAILABLE:
        return

    try:
        engine_client = discoveryengine.EngineServiceClient()
        engine_name = f"projects/{project_id}/locations/{location}/collections/default_collection/engines/{engine_id}"
        engine = engine_client.get_engine(name=engine_name)
        if data_store_id not in engine.data_store_ids:
            engine.data_store_ids.append(data_store_id)
            op = engine_client.update_engine(engine=engine)
            op.result()
            logger.info(f"Attached DataStore '{data_store_id}' to Engine '{engine_id}'")
        else:
            logger.info(f"DataStore '{data_store_id}' already attached to Engine '{engine_id}'")
    except Exception as ex:
        logger.warning(f"Engine attachment note: {ex}")


def resolve_full_config(config: Dict) -> Dict:
    """
    Resolves complete configuration from:
    1. config_uri or TARGETS_CONFIG_URI (gs://... or local file)
    2. Environment variable fallbacks (GCS_BUCKET, GCP_PROJECT, etc.)
    3. Direct payload overrides
    """
    config_uri = (
        config.get("config_uri")
        or config.get("targets_config_uri")
        or os.environ.get("CONFIG_URI")
        or os.environ.get("TARGETS_CONFIG_URI")
    )
    base_config: Dict = {}
    if config_uri:
        try:
            if config_uri.startswith("gs://"):
                if GCS_AVAILABLE:
                    parts = config_uri[5:].split("/", 1)
                    bucket_name = parts[0]
                    blob_name = parts[1] if len(parts) > 1 else ""
                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    blob = bucket.blob(blob_name)
                    if blob.exists():
                        base_config = json.loads(blob.download_as_text())
                        logger.info(f"Loaded remote configuration from {config_uri}")
                    else:
                        logger.warning(f"Config blob does not exist: {config_uri}")
                else:
                    logger.warning("google-cloud-storage not installed locally, cannot fetch remote config_uri.")
            else:
                p = Path(config_uri)
                if p.exists():
                    with open(p, "r", encoding="utf-8") as f:
                        base_config = json.load(f)
                    logger.info(f"Loaded local configuration from {config_uri}")
        except Exception as ex:
            logger.warning(f"Could not load config from {config_uri}: {ex}")

    merged = dict(base_config)
    env_mappings = [
        ("gcs_bucket", "GCS_BUCKET"),
        ("gcs_prefix", "GCS_PREFIX"),
        ("project_id", "GCP_PROJECT"),
        ("project_id", "GOOGLE_CLOUD_PROJECT"),
        ("location", "LOCATION"),
        ("data_store_id", "DATA_STORE_ID"),
        ("engine_id", "ENGINE_ID"),
        ("url", "BASE_URL"),
    ]
    for prop, env_k in env_mappings:
        if env_k in os.environ and not merged.get(prop):
            merged[prop] = os.environ[env_k]

    for k, v in config.items():
        if v is not None and v != "":
            merged[k] = v

    return merged


def load_targets_config(config: Dict) -> List[Dict]:
    """
    Resolves targets list from:
    1. config["targets"] (explicit list in resolved config)
    2. config["url"] or os.environ["BASE_URL"] (single legacy fallback)
    """
    targets_input = config.get("targets")
    if targets_input and isinstance(targets_input, list) and len(targets_input) > 0:
        return targets_input

    base_url = config.get("url") or os.environ.get("BASE_URL")
    if base_url:
        return [{
            "name": config.get("name", "primary"),
            "url": base_url,
            "max_pages": int(config.get("max_pages", 50)),
            "max_depth": int(config.get("max_depth", 3)),
            "delay": float(config.get("delay", 0.2)),
            "include_patterns": config.get("include_patterns", []),
            "exclude_patterns": config.get("exclude_patterns", []),
        }]

    return []


def execute_ingestion(config: Dict) -> Dict:
    """Core pipeline runner used by Cloud Run Function, Web UI, and CLI."""
    full_config = resolve_full_config(config)

    bucket_name = full_config.get("gcs_bucket")
    gcs_prefix = full_config.get("gcs_prefix", "website_datastore").strip("/")
    project_id = full_config.get("project_id")
    location = full_config.get("location", "global")
    data_store_id = full_config.get("data_store_id")
    engine_id = full_config.get("engine_id")
    default_max_pages = int(full_config.get("max_pages", 50))
    default_max_depth = int(full_config.get("max_depth", 3))
    default_delay = float(full_config.get("delay", 0.2))
    dry_run = bool(full_config.get("dry_run", False))

    targets = load_targets_config(full_config)
    if not targets:
        raise ValueError("No crawl targets specified. Provide 'targets' list, 'config_uri', or 'url'.")

    # In Cloud Run, write all temporary files to /tmp
    with tempfile.TemporaryDirectory(dir="/tmp") as tmp_dir:
        # Step 1: Crawl each target website
        all_pages: List[Dict] = []
        crawler_stats: List[Dict] = []
        shared_visited: Set[str] = set()

        for idx, target in enumerate(targets, start=1):
            t_url = target.get("url")
            if not t_url:
                continue
            t_name = target.get("name", f"target-{idx}")
            t_max_pages = int(target.get("max_pages", default_max_pages))
            t_max_depth = int(target.get("max_depth", default_max_depth))
            t_delay = float(target.get("delay", default_delay))
            t_includes = target.get("include_patterns", [])
            t_excludes = target.get("exclude_patterns", [])
            t_headers = dict(target.get("headers") or {})
            auth_env = target.get("auth_env_var")
            if auth_env and os.environ.get(auth_env):
                auth_header = target.get("auth_header_name", "Authorization")
                t_headers[auth_header] = os.environ[auth_env]
            t_cookies = dict(target.get("cookies") or {})

            logger.info(f"[{idx}/{len(targets)}] Crawling target '{t_name}': {t_url} (depth={t_max_depth}, max_pages={t_max_pages})")
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
            target_pages = crawler.crawl()
            all_pages.extend(target_pages)
            if hasattr(crawler, "stats"):
                crawler_stats.append(crawler.stats)
            logger.info(f"Target '{t_name}' completed with {len(target_pages)} valid pages.")

        if not all_pages:
            raise ValueError(f"No pages could be extracted from {len(targets)} target(s).")

        # Step 2: Stage Markdown & metadata.jsonl
        saved_files, metadata_path = stage_artifacts(
            pages=all_pages,
            staging_dir=tmp_dir,
            bucket_name=bucket_name or "dry-run-bucket",
            prefix=gcs_prefix,
        )

        metadata_gcs_uri = f"gs://{bucket_name}/{gcs_prefix}/metadata.jsonl" if bucket_name else f"file://{metadata_path}"

        # Step 3: Upload to Cloud Storage
        if not dry_run and bucket_name:
            upload_to_gcs(tmp_dir, bucket_name, gcs_prefix)

        # Step 4: Create/Update Data Store with Schema Key Property Mapping
        import_op = None
        if not dry_run and project_id and data_store_id:
            try:
                ensure_datastore_and_schema(project_id, location, data_store_id)
                import_op = trigger_datastore_import(project_id, location, data_store_id, metadata_gcs_uri)
                if engine_id:
                    link_datastore_to_engine(project_id, location, engine_id, data_store_id)
            except Exception as e:
                logger.error(f"Discovery Engine configuration error: {e}")

        return {
            "status": "completed",
            "targets_count": len(targets),
            "pages_count": len(all_pages),
            "crawler_stats": crawler_stats,
            "metadata_uri": metadata_gcs_uri,
            "data_store_id": data_store_id,
            "import_operation": import_op,
            "dry_run": dry_run,
        }


# Cloud Run Function HTTP Entrypoint
if FUNCTIONS_FRAMEWORK_AVAILABLE:
    @functions_framework.http
    def index_website_handler(request):
        try:
            payload = request.get_json(silent=True) or {}
            result = execute_ingestion(payload)
            return (json.dumps(result), 200, {"Content-Type": "application/json"})
        except Exception as err:
            logger.error(f"Cloud Run Function error: {err}", exc_info=True)
            return (json.dumps({"status": "error", "message": str(err)}), 500, {"Content-Type": "application/json"})
else:
    def index_website_handler(request):
        payload = request.get_json(silent=True) or {}
        result = execute_ingestion(payload)
        return (json.dumps(result), 200, {"Content-Type": "application/json"})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run website to GCS & Gemini Enterprise Ingestor")
    parser.add_argument("--url", required=False, help="Website URL to crawl (single target)")
    parser.add_argument("--targets-file", required=False, help="Path or gs:// URI to targets.json")
    parser.add_argument("--gcs-bucket", required=False, help="GCS bucket name")
    parser.add_argument("--gcs-prefix", default="website_datastore", help="GCS subfolder prefix")
    parser.add_argument("--project-id", required=False, help="Google Cloud Project ID")
    parser.add_argument("--location", default="global", help="Discovery Engine location (default: global)")
    parser.add_argument("--data-store-id", required=False, help="Discovery Engine DataStore ID")
    parser.add_argument("--engine-id", required=False, help="Gemini Enterprise Engine / App ID")
    parser.add_argument("--max-pages", type=int, default=50, help="Max pages to crawl")
    parser.add_argument("--max-depth", type=int, default=3, help="Max crawl depth")
    parser.add_argument("--delay", type=float, default=0.2, help="Crawl delay in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without GCP writes")

    args = parser.parse_args()
    config_payload = vars(args)
    if args.targets_file:
        config_payload["targets_config_uri"] = args.targets_file
    res = execute_ingestion(config_payload)
    print("\nResult:", json.dumps(res, indent=2))
