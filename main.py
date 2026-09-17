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

import contextlib
import copy
import fnmatch
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple
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
    from google.api_core.client_options import ClientOptions
    CLIENT_OPTIONS_AVAILABLE = True
except ImportError:
    CLIENT_OPTIONS_AVAILABLE = False

try:
    from google.api_core.exceptions import NotFound
except ImportError:  # pragma: no cover - only when google libs are absent
    class NotFound(Exception):  # type: ignore[no-redef]
        """Fallback so except-clauses stay valid without google-api-core."""

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


def configure_local_credentials(base_dir: Optional[Path] = None) -> Dict[str, Optional[str]]:
    """
    Auto-discovers and configures local credentials without modifying system ADC.
    Priority:
    0. Serverless check (Cloud Run / Cloud Functions) -> use metadata server.
    1. Explicit GOOGLE_APPLICATION_CREDENTIALS already set in the environment.
    2. Local .env file in base_dir or project root (e.g. GOOGLE_APPLICATION_CREDENTIALS=...).
    3. Project-local service account key (service-account.json or service_account.json).
    4. Project-local isolated gcloud directory (.gcloud_auth/application_default_credentials.json).

    Propagates GOOGLE_APPLICATION_CREDENTIALS and CLOUDSDK_CONFIG into os.environ
    so both Python SDK client libraries and child gcloud subprocesses inherit them.
    """
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent

    status: Dict[str, Optional[str]] = {
        "mode": "system_adc",
        "credentials_path": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        "cloudsdk_config": os.environ.get("CLOUDSDK_CONFIG"),
        "account": None,
        "detail": "Using machine default Application Default Credentials (ADC).",
    }

    # 0. Cloud Run / Cloud Functions serverless execution environment check
    if os.environ.get("K_SERVICE") or os.environ.get("FUNCTION_TARGET"):
        status.update({
            "mode": "cloud_run",
            "account": "Cloud Run Runtime Service Account",
            "detail": "Running in Cloud Run / Functions environment (using GCP Metadata Server).",
        })
        return status

    def _inspect_cred_file(file_path: Path) -> Tuple[Optional[str], Optional[str]]:
        if not file_path.is_file():
            return None, None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            cred_type = data.get("type", "unknown")
            account = data.get("client_email") or data.get("account")
            if not account and cred_type == "authorized_user":
                account = "Authorized User (OAuth2)"
            return cred_type, account
        except Exception:
            return None, None

    # 1. Existing environment variable
    env_cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_cred and Path(env_cred).is_file():
        cred_type, account = _inspect_cred_file(Path(env_cred))
        status.update({
            "mode": "env",
            "credentials_path": str(Path(env_cred).resolve()),
            "account": account,
            "detail": f"Using credentials from $GOOGLE_APPLICATION_CREDENTIALS ({cred_type or 'custom'}).",
        })
        return status

    # 2. Local .env file
    env_file = base_dir / ".env"
    if env_file.is_file():
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k and k not in os.environ:
                        os.environ[k] = v
        except Exception as e:
            logger.warning(f"Could not read .env file: {e}")

        env_cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if env_cred and Path(env_cred).is_file():
            cred_type, account = _inspect_cred_file(Path(env_cred))
            status.update({
                "mode": "dotenv",
                "credentials_path": str(Path(env_cred).resolve()),
                "account": account,
                "detail": f"Loaded credentials from local .env ({cred_type or 'custom'}).",
            })
            return status

    # 3. Project-local service account key
    for sa_name in ("service-account.json", "service_account.json"):
        sa_file = base_dir / sa_name
        if sa_file.is_file():
            cred_type, account = _inspect_cred_file(sa_file)
            abs_path = str(sa_file.resolve())
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = abs_path
            status.update({
                "mode": "service_account",
                "credentials_path": abs_path,
                "account": account,
                "detail": f"Using local service account key: {sa_name}",
            })
            return status

    # 4. Project-local isolated gcloud directory (.gcloud_auth or .gcloud_local)
    for gcloud_dir_name in (".gcloud_auth", ".gcloud_local"):
        gcloud_dir = base_dir / gcloud_dir_name
        if gcloud_dir.is_dir():
            abs_gcloud_dir = str(gcloud_dir.resolve())
            os.environ.setdefault("CLOUDSDK_CONFIG", abs_gcloud_dir)
            status["cloudsdk_config"] = abs_gcloud_dir

            adc_file = gcloud_dir / "application_default_credentials.json"
            if adc_file.is_file():
                abs_adc = str(adc_file.resolve())
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = abs_adc
                cred_type, account = _inspect_cred_file(adc_file)
                status.update({
                    "mode": "isolated_adc",
                    "credentials_path": abs_adc,
                    "account": account or "User Account (isolated ADC)",
                    "detail": f"Using project-isolated user credentials from {gcloud_dir_name}/",
                })
                return status

    return status


LOCAL_AUTH_INFO = configure_local_credentials()


def _get_client_options(project_id: Optional[str] = None):
    """Constructs ClientOptions with quota_project_id to prevent 403 SERVICE_DISABLED on user ADC."""
    if CLIENT_OPTIONS_AVAILABLE and project_id:
        return ClientOptions(quota_project_id=project_id)
    return None



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
        self.failed_urls: List[Dict] = []
        self.stats: Dict = {}

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

    def crawl(self, progress_callback: Optional[Callable[[Dict], None]] = None) -> List[Dict]:
        """
        Breadth-first crawl of the target site.

        progress_callback, if supplied, is invoked once per attempted URL with a
        dict describing the attempt. This exists so UIs can show live progress
        without re-implementing this loop (which previously caused the crawl
        logic to be maintained in three places).
        """
        queue: List[Tuple[str, int]] = [(self.base_url, 0)]
        results: List[Dict] = []
        self.failed_urls: List[Dict] = []

        def _emit(**event):
            if progress_callback:
                try:
                    progress_callback(event)
                except Exception as cb_err:  # never let UI errors kill a crawl
                    logger.debug(f"progress_callback raised: {cb_err}")

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
                    reason = (
                        f"HTTP {resp.status_code}"
                        if resp.status_code != 200
                        else f"non-HTML content-type ({resp.headers.get('Content-Type', 'unknown')})"
                    )
                    self.failed_urls.append({"url": url, "reason": reason})
                    _emit(kind="skipped", url=url, depth=depth, reason=reason,
                          crawled=len(results), max_pages=self.max_pages)
                    continue

                effective_url = resp.url or url
                page_data = self._extract_page(resp, effective_url)
                if page_data:
                    results.append(page_data)
                    _emit(kind="page", url=effective_url, depth=depth,
                          title=page_data.get("title", ""), page=page_data,
                          crawled=len(results), max_pages=self.max_pages)
                else:
                    # _extract_page returns None when too little text was found,
                    # which most often means the page is JavaScript-rendered.
                    reason = "no extractable content (possible JavaScript-rendered page)"
                    self.failed_urls.append({"url": url, "reason": reason})
                    logger.warning(f"Skipped {effective_url}: {reason}")
                    _emit(kind="skipped", url=effective_url, depth=depth, reason=reason,
                          crawled=len(results), max_pages=self.max_pages)

                if depth < self.max_depth:
                    new_links = self._extract_links(resp.text, effective_url)
                    for link in new_links:
                        if link not in self.visited:
                            queue.append((link, depth + 1))

            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
                self.failed_urls.append({"url": url, "reason": reason})
                logger.warning(f"Error crawling {url}: {e}")
                _emit(kind="error", url=url, depth=depth, reason=reason,
                      crawled=len(results), max_pages=self.max_pages)

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
            "failed_count": len(self.failed_urls),
            "failed_urls": self.failed_urls[:50],
        }
        if self.failed_urls:
            logger.warning(
                f"Crawl for '{self.target_name}' had {len(self.failed_urls)} failed/skipped URL(s). "
                f"These pages are NOT in the index."
            )
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


def upload_to_gcs(staging_dir: str, bucket_name: str, prefix: str, project_id: Optional[str] = None) -> None:
    """Uploads staged markdown files and metadata.jsonl to Google Cloud Storage."""
    clean_prefix = prefix.strip("/")
    prefix_str = f"{clean_prefix}/" if clean_prefix else ""
    staging_path = Path(staging_dir)

    if not GCS_AVAILABLE:
        raise RuntimeError(
            "google-cloud-storage is required to upload artifacts. "
            "Install it with: pip install google-cloud-storage"
        )

    opts = _get_client_options(project_id)
    client = storage.Client(project=project_id, client_options=opts) if (project_id or opts) else storage.Client()
    bucket = client.bucket(bucket_name)
    uploaded = 0
    for file_path in staging_path.rglob("*"):
        if file_path.is_file():
            rel_path = file_path.relative_to(staging_path).as_posix()
            blob_name = f"{prefix_str}{rel_path}"
            blob = bucket.blob(blob_name)
            content_type = "text/markdown" if file_path.suffix == ".md" else "application/json"
            blob.upload_from_filename(str(file_path), content_type=content_type)
            uploaded += 1
            logger.info(f"Uploaded -> gs://{bucket_name}/{blob_name}")

    logger.info(f"Uploaded {uploaded} file(s) to gs://{bucket_name}/{prefix_str}")


def _merge_schema(existing_schema: Dict, desired_schema: Dict) -> Tuple[Dict, List[str]]:
    """
    Non-destructively merges the desired property definitions into the live
    Discovery Engine schema.

    Discovery Engine rejects any UpdateSchema request that alters the `type` of
    an already-established field with:
        400 Schema update cannot alter the field type. Field type mismatch ...

    This happens because Vertex AI Search auto-infers field types on the first
    document import (e.g. an ISO-8601 `crawled_at` string is inferred as
    `datetime`). Blindly PATCHing our hard-coded schema would also wipe out
    Google-managed annotations (`retrievable`, `indexable`, `searchable`,
    `dynamicFacetable`) that the datastore has already applied.

    Merge rules:
      * Fields that do not exist yet are added exactly as desired.
      * Fields that already exist KEEP their established `type` and all of
        their existing annotations.
      * `keyPropertyMapping` is only added when the field does not already have
        one; an existing (differing) mapping is preserved and reported.

    Returns the merged schema and a list of human-readable change descriptions.
    An empty change list means the live schema is already correct and no
    UpdateSchema call is required.
    """
    merged = copy.deepcopy(existing_schema) if existing_schema else {}
    merged.setdefault("$schema", desired_schema.get("$schema"))
    merged.setdefault("type", "object")
    existing_props = merged.setdefault("properties", {})

    changes: List[str] = []

    for field, desired_def in desired_schema.get("properties", {}).items():
        current_def = existing_props.get(field)

        if current_def is None:
            existing_props[field] = copy.deepcopy(desired_def)
            changes.append(f"added field '{field}' ({desired_def.get('type')})")
            continue

        # NEVER alter an established field type -- Discovery Engine rejects it.
        desired_type = desired_def.get("type")
        current_type = current_def.get("type")
        if desired_type and current_type and desired_type != current_type:
            logger.info(
                f"Preserving established type for '{field}': "
                f"'{current_type}' (schema declares '{desired_type}'). "
                "Discovery Engine does not permit altering field types."
            )

        desired_kpm = desired_def.get("keyPropertyMapping")
        current_kpm = current_def.get("keyPropertyMapping")
        if desired_kpm and not current_kpm:
            current_def["keyPropertyMapping"] = desired_kpm
            changes.append(f"set keyPropertyMapping '{field}' -> '{desired_kpm}'")
        elif desired_kpm and current_kpm != desired_kpm:
            logger.warning(
                f"Field '{field}' already maps to key property '{current_kpm}' "
                f"(expected '{desired_kpm}'). Leaving it unchanged -- key property "
                "mappings cannot be re-assigned on an existing datastore. "
                "Recreate the datastore if this mapping is wrong."
            )

    return merged, changes


def ensure_datastore_and_schema(project_id: str, location: str, data_store_id: str) -> None:
    """
    Creates the Discovery Engine Data Store if it doesn't exist, and configures
    the schema with keyPropertyMapping: {"uri": "url"}.
    This forces the Gemini Enterprise assistant to cite public URLs, not gs://!

    The schema update is a MERGE, not an overwrite, so re-running against an
    existing datastore never fails with a field-type mismatch.
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

    if not DISCOVERYENGINE_AVAILABLE:
        raise RuntimeError(
            "google-cloud-discoveryengine is required to configure the data store. "
            "Install it with: pip install google-cloud-discoveryengine"
        )

    opts = _get_client_options(project_id)
    client = discoveryengine.DataStoreServiceClient(client_options=opts)
    parent = f"projects/{project_id}/locations/{location}/collections/default_collection"
    data_store_name = f"{parent}/dataStores/{data_store_id}"

    try:
        client.get_data_store(name=data_store_name)
        logger.info(f"DataStore '{data_store_id}' exists.")
    except NotFound:
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

    # Configure Schema with keyPropertyMapping (critical for citations).
    # Mapping the 'url' property to the 'uri' key property is what makes Gemini
    # Enterprise cite the live public page instead of the gs:// object path.
    schema_client = discoveryengine.SchemaServiceClient(client_options=opts)
    schema_name = f"{data_store_name}/schemas/default_schema"

    existing_schema: Dict = {}
    try:
        live = schema_client.get_schema(name=schema_name)
        if live.json_schema:
            existing_schema = json.loads(live.json_schema)
    except NotFound:
        logger.info("No default_schema found yet; creating it from scratch.")
    except json.JSONDecodeError:
        logger.warning("Live schema was not valid JSON; rebuilding from scratch.")

    merged_schema, changes = _merge_schema(existing_schema, schema_dict)

    if not changes:
        logger.info(
            f"Schema on '{data_store_id}' already correct "
            "(url -> uri key property mapping in place). No update needed."
        )
        return

    schema = discoveryengine.Schema(
        name=schema_name,
        json_schema=json.dumps(merged_schema),
    )
    req = discoveryengine.UpdateSchemaRequest(schema=schema)
    op = schema_client.update_schema(request=req)
    op.result()
    logger.info(
        f"Updated schema on '{data_store_id}': {'; '.join(changes)}"
    )


def trigger_datastore_import(
    project_id: str,
    location: str,
    data_store_id: str,
    metadata_gcs_uri: str,
    full_reconcile: bool = True,
) -> str:
    """
    Imports documents from metadata.jsonl into Discovery Engine.

    When full_reconcile=True (default when crawl_complete=True), uses
    ReconciliationMode.FULL so obsolete/deleted pages are removed on scheduled
    refreshes. When full_reconcile=False (e.g., crawl capped by max_pages budget
    or some URLs failed), uses ReconciliationMode.INCREMENTAL so existing
    datastore documents are NOT purged.
    """
    if not DISCOVERYENGINE_AVAILABLE:
        raise RuntimeError(
            "google-cloud-discoveryengine is required to import documents. "
            "Install it with: pip install google-cloud-discoveryengine"
        )

    opts = _get_client_options(project_id)
    doc_client = discoveryengine.DocumentServiceClient(client_options=opts)
    parent = (
        f"projects/{project_id}/locations/{location}/collections/default_collection/"
        f"dataStores/{data_store_id}/branches/default_branch"
    )
    gcs_source = discoveryengine.GcsSource(
        input_uris=[metadata_gcs_uri],
        data_schema="document",
    )
    mode = (
        discoveryengine.ImportDocumentsRequest.ReconciliationMode.FULL
        if full_reconcile
        else discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
    )
    request = discoveryengine.ImportDocumentsRequest(
        parent=parent,
        gcs_source=gcs_source,
        reconciliation_mode=mode,
    )
    operation = doc_client.import_documents(request=request)
    op_name = operation.operation.name
    mode_name = "FULL" if full_reconcile else "INCREMENTAL"
    logger.info(f"Started Discovery Engine import operation ({mode_name} mode): {op_name}")
    return op_name


def link_datastore_to_engine(project_id: str, location: str, engine_id: str, data_store_id: str) -> None:
    """Attaches the Data Store to a Gemini Enterprise Engine/App if specified."""
    if not DISCOVERYENGINE_AVAILABLE:
        return

    try:
        opts = _get_client_options(project_id)
        engine_client = discoveryengine.EngineServiceClient(client_options=opts)
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


def get_datastore_live_status(project_id: str, location: str, data_store_id: str) -> Dict:
    """
    Queries the live Discovery Engine REST API using Application Default
    Credentials (with X-Goog-User-Project quota header) to return real-time
    telemetry for the UI control plane:
      - Datastore state (ACTIVE / NOT_FOUND) & Search Tier
      - Exact indexed document count & freshest crawled_at timestamp
      - Storage size breakdown (unstructured Markdown + structured metadata)
      - Latest ImportDocuments LRO status (running vs done, success/total count, updateTime)
      - Citation Grounding verification (whether default_schema maps url -> uri)
    """
    import google.auth
    from google.auth.transport.requests import Request as GoogleAuthRequest

    creds, _ = google.auth.default()
    creds.refresh(GoogleAuthRequest())
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "X-Goog-User-Project": project_id,
        "Content-Type": "application/json",
    }
    host = (
        f"https://{location}-discoveryengine.googleapis.com"
        if location and location not in ("global", "")
        else "https://discoveryengine.googleapis.com"
    )
    base = f"{host}/v1/projects/{project_id}/locations/{location or 'global'}/collections/default_collection/dataStores/{data_store_id}"

    # 1. DataStore metadata & billing estimation
    ds_resp = requests.get(base, headers=headers, timeout=12)
    if ds_resp.status_code == 404:
        return {
            "exists": False,
            "state": "NOT_FOUND",
            "data_store_id": data_store_id,
            "project_id": project_id,
            "location": location,
        }
    ds_resp.raise_for_status()
    ds = ds_resp.json()

    billing = ds.get("billingEstimation", {})
    unstructured_bytes = int(billing.get("unstructuredDataSize", 0))
    structured_bytes = int(billing.get("structuredDataSize", 0))
    total_bytes = unstructured_bytes + structured_bytes

    # 2. Live Document Count & Freshest crawled_at Timestamp
    doc_count = 0
    latest_crawled_at = None
    page_token = None
    for _ in range(5):
        url = f"{base}/branches/default_branch/documents?pageSize=1000"
        if page_token:
            url += f"&pageToken={page_token}"
        d_resp = requests.get(url, headers=headers, timeout=12)
        if d_resp.status_code != 200:
            break
        d_json = d_resp.json()
        docs = d_json.get("documents", [])
        doc_count += len(docs)
        for doc in docs:
            ca = (doc.get("structData") or {}).get("crawled_at")
            if ca and (not latest_crawled_at or ca > latest_crawled_at):
                latest_crawled_at = ca
        page_token = d_json.get("nextPageToken")
        if not page_token:
            break

    # 3. Latest Import Operation (LRO status)
    ops_resp = requests.get(f"{base}/branches/0/operations?pageSize=10", headers=headers, timeout=12)
    latest_import = None
    if ops_resp.status_code == 200:
        ops = ops_resp.json().get("operations", [])
        import_ops = [
            o for o in ops
            if "import-documents" in o.get("name", "")
            or "ImportDocuments" in str(o.get("metadata", {}))
        ]
        import_ops.sort(key=lambda o: o.get("metadata", {}).get("createTime", ""), reverse=True)
        if import_ops:
            top = import_ops[0]
            meta = top.get("metadata", {})
            succ = int(meta.get("successCount", 0))
            fail = int(meta.get("failureCount", 0))
            tot = int(meta.get("totalCount", succ + fail))
            latest_import = {
                "operation_name": top.get("name", "").split("/")[-1],
                "done": bool(top.get("done", False)),
                "create_time": meta.get("createTime"),
                "update_time": meta.get("updateTime"),
                "success_count": succ,
                "failure_count": fail,
                "total_count": tot,
                "error": top.get("error"),
            }

    # 4. Schema Citation Grounding Check (url -> uri)
    schema_resp = requests.get(f"{base}/schemas/default_schema", headers=headers, timeout=12)
    grounding_active = False
    if schema_resp.status_code == 200:
        try:
            s_json = json.loads(schema_resp.json().get("jsonSchema", "{}"))
            url_prop = (s_json.get("properties") or {}).get("url") or {}
            grounding_active = (url_prop.get("keyPropertyMapping") == "uri")
        except Exception:
            pass

    return {
        "exists": True,
        "data_store_id": data_store_id,
        "project_id": project_id,
        "location": location,
        "display_name": ds.get("displayName", data_store_id),
        "state": ds.get("state", "ACTIVE"),
        "search_tier": ds.get("searchTier", "STANDARD"),
        "document_count": doc_count,
        "latest_crawled_at": latest_crawled_at,
        "unstructured_bytes": unstructured_bytes,
        "structured_bytes": structured_bytes,
        "total_bytes": total_bytes,
        "latest_import": latest_import,
        "citation_grounding_active": grounding_active,
    }


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
        ("reconciliation_mode", "RECONCILIATION_MODE"),
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


def execute_ingestion(
    config: Dict,
    progress_callback: Optional[Callable[[Dict], None]] = None,
    staging_dir: Optional[str] = None,
) -> Dict:
    """
    Core pipeline runner. This is the single implementation of the
    crawl -> stage -> upload -> index pipeline; the Cloud Run Function, the web
    UI and the CLI all call it rather than reimplementing it.

    progress_callback receives per-URL crawl events (see WebsiteCrawler.crawl)
    plus coarse {"kind": "stage", ...} events for pipeline phases.
    staging_dir lets a caller keep the generated artifacts (used by --dry-run
    and by the UI's document preview); when None a temp dir is used and cleaned.
    """
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

    def _emit(**event):
        if progress_callback:
            try:
                progress_callback(event)
            except Exception as cb_err:
                logger.debug(f"progress_callback raised: {cb_err}")

    # Staging location: a caller-supplied dir is kept (used by --dry-run and the
    # UI preview); otherwise use a temp dir. On Cloud Run /tmp is the only
    # writable path, so prefer it when present.
    if staging_dir:
        Path(staging_dir).mkdir(parents=True, exist_ok=True)
        staging_ctx: object = contextlib.nullcontext(staging_dir)
    else:
        staging_ctx = tempfile.TemporaryDirectory(dir="/tmp" if os.path.isdir("/tmp") else None)

    with staging_ctx as tmp_dir:  # type: ignore[attr-defined]
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
            _emit(kind="target_start", target=t_name, url=t_url,
                  target_index=idx, target_total=len(targets))

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
            target_pages = crawler.crawl(progress_callback=progress_callback)
            all_pages.extend(target_pages)
            crawler_stats.append(crawler.stats)
            logger.info(f"Target '{t_name}' completed with {len(target_pages)} valid pages.")
            _emit(kind="target_done", target=t_name, pages=len(target_pages),
                  failed=crawler.stats.get("failed_count", 0))

        if not all_pages:
            raise ValueError(f"No pages could be extracted from {len(targets)} target(s).")

        total_failed = sum(s.get("failed_count", 0) for s in crawler_stats)
        crawl_complete = all(
            s.get("queue_exhausted") and not s.get("failed_count") for s in crawler_stats
        )

        # Step 2: Stage Markdown & metadata.jsonl
        _emit(kind="stage", stage="staging", pages=len(all_pages))
        saved_files, metadata_path = stage_artifacts(
            pages=all_pages,
            staging_dir=tmp_dir,
            bucket_name=bucket_name or "dry-run-bucket",
            prefix=gcs_prefix,
        )

        metadata_gcs_uri = f"gs://{bucket_name}/{gcs_prefix}/metadata.jsonl" if bucket_name else f"file://{metadata_path}"

        # Step 3: Upload to Cloud Storage
        if not dry_run and bucket_name:
            _emit(kind="stage", stage="uploading", files=len(saved_files))
            upload_to_gcs(tmp_dir, bucket_name, gcs_prefix, project_id=project_id)

        # Step 4: Create/Update Data Store with Schema Key Property Mapping
        import_op = None
        indexing_error = None
        reconcile_setting = str(full_config.get("reconciliation_mode") or "auto").lower().strip()
        if reconcile_setting == "full":
            use_full_reconcile = True
        elif reconcile_setting == "incremental":
            use_full_reconcile = False
        else:
            use_full_reconcile = crawl_complete

        if not dry_run and project_id and data_store_id:
            _emit(kind="stage", stage="indexing", data_store=data_store_id)
            try:
                ensure_datastore_and_schema(project_id, location, data_store_id)
                import_op = trigger_datastore_import(
                    project_id, location, data_store_id, metadata_gcs_uri,
                    full_reconcile=use_full_reconcile,
                )
                if engine_id:
                    link_datastore_to_engine(project_id, location, engine_id, data_store_id)
            except Exception as e:
                # Surfaced in the return value so callers can stop reporting
                # unqualified success when indexing actually failed.
                indexing_error = str(e)
                logger.error(f"Discovery Engine configuration error: {e}")

        return {
            "status": "completed" if not indexing_error else "completed_with_errors",
            "targets_count": len(targets),
            "pages_count": len(all_pages),
            "failed_count": total_failed,
            "crawl_complete": crawl_complete,
            "reconciliation_mode": reconcile_setting,
            "reconciliation_mode_used": "FULL" if use_full_reconcile else "INCREMENTAL",
            "crawler_stats": crawler_stats,
            "metadata_uri": metadata_gcs_uri,
            "staging_dir": str(tmp_dir),
            "data_store_id": data_store_id,
            "import_operation": import_op,
            "indexing_error": indexing_error,
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
