#!/usr/bin/env python3
"""
Unit and integration tests for site-to-gcs-ingestor:
- UTF-8 mojibake repair ("thereâ€™s" -> "there’s") and HTTP charset handling
- Literal "null" / "None" metadata sanitization
- SSO login page ("login.microsoftonline.com", "Sign in to your account") rejection
- "This page has moved / update your bookmark" tombstone placeholder rejection
- Canonical / ITS_URL citation resolution for HTML and Solr JSON index records
- Rate-limit (HTTP 429) retry backoff and FULL reconciliation preservation when
  filtering non-content records
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests
from bs4 import BeautifulSoup

from main import (
    WebsiteCrawler,
    decode_response_text,
    detect_non_content_page,
    execute_ingestion,
    fix_mojibake,
    html_to_markdown,
    is_sso_or_login_url,
    resolve_and_clean_url,
    resolve_citation_url,
    sanitize_metadata_value,
    stage_artifacts,
    unwrap_redirect_url,
)


def _make_response(
    body: bytes,
    status_code: int = 200,
    content_type: str = "text/html",
    url: str = "https://gmone.example.com/page",
    headers: dict | None = None,
) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = body
    resp.url = url
    resp.headers["Content-Type"] = content_type
    if headers:
        for k, v in headers.items():
            resp.headers[k] = v
    # Simulate requests' default RFC 2616 behavior when charset is absent on text/*
    if "charset=" not in content_type.lower() and content_type.startswith("text/"):
        resp.encoding = "ISO-8859-1"
    return resp


class TestEncodingAndMetadataSanitization(unittest.TestCase):
    def test_fix_mojibake_repairs_smart_quotes_dashes_and_accents(self):
        corrupted = "Miles of memories: thereâ€™s a new â€œEscalade IQâ€\x9d â€” CrÃ¨me BrÃ»lÃ©e!"
        repaired = fix_mojibake(corrupted)
        self.assertEqual(
            repaired,
            "Miles of memories: there’s a new “Escalade IQ” — Crème Brûlée!",
        )

    def test_fix_mojibake_leaves_clean_unicode_untouched(self):
        clean = "Already clean: there’s a “quote” — and café résumé."
        self.assertEqual(fix_mojibake(clean), clean)

    def test_decode_response_text_handles_missing_charset_utf8_bytes(self):
        # Raw UTF-8 bytes served without charset=utf-8 in Content-Type
        utf8_html = "<p>There’s no place like north — “Escalade IQ”</p>".encode("utf-8")
        resp = _make_response(utf8_html, content_type="text/html")
        self.assertEqual(resp.encoding, "ISO-8859-1")
        decoded = decode_response_text(resp)
        self.assertIn("There’s no place like north — “Escalade IQ”", decoded)
        self.assertNotIn("â€™", decoded)

    def test_sanitize_metadata_value_drops_literal_null_and_repairs_mojibake(self):
        self.assertEqual(sanitize_metadata_value("null"), "")
        self.assertEqual(sanitize_metadata_value("NULL"), "")
        self.assertEqual(sanitize_metadata_value("None"), "")
        self.assertEqual(sanitize_metadata_value("undefined"), "")
        self.assertEqual(sanitize_metadata_value("N/A"), "")
        self.assertEqual(sanitize_metadata_value(None), "")
        self.assertEqual(sanitize_metadata_value(["null", "  "]), "")
        self.assertEqual(
            sanitize_metadata_value(["Hereâ€™s the summary"]),
            "Here’s the summary",
        )


class TestNonContentAndRedirectDetection(unittest.TestCase):
    def test_rejects_microsoft_sso_login_urls_and_pages(self):
        sso_url = (
            "https://login.microsoftonline.com/1234-5678/oauth2/v2.0/authorize"
            "?client_id=abc&response_type=code"
        )
        self.assertIsNotNone(is_sso_or_login_url(sso_url))
        reason = detect_non_content_page(
            url=sso_url,
            title="Sign in to your account",
            content="Sign in to your account",
        )
        self.assertIsNotNone(reason)
        self.assertIn("SSO", reason)

        # Even if hosted on an internal URL, a "Sign in to your account" page is rejected
        reason_internal = detect_non_content_page(
            url="https://gmone.example.com/protected/page",
            title="Sign in to your account",
            content="Please sign in to your account to continue.",
        )
        self.assertIsNotNone(reason_internal)
        self.assertIn("SSO", reason_internal)

    def test_rejects_has_moved_placeholder_pages(self):
        for title, body in [
            (
                "Leave of Absence has moved",
                "Leave of Absence has moved. Please update your bookmark to the new HR portal.",
            ),
            (
                "Leader Workspace has moved",
                "This page has moved, update your bookmark.",
            ),
            (
                "GM Benefits Portal",
                "This page has moved to a new address. Please update your bookmarks.",
            ),
        ]:
            reason = detect_non_content_page(
                url="https://gmone.example.com/links/item",
                title=title,
                content=body,
            )
            self.assertIsNotNone(reason, f"Expected placeholder '{title}' to be rejected")
            self.assertIn("placeholder", reason)

    def test_preserves_legitimate_articles_mentioning_moved(self):
        long_article = (
            "Engineering Leveling Guide for Senior Software Engineers.\n\n"
            + ("This guide explains technical leadership expectations, system architecture "
               "deliverables, cross-team collaboration, and production operational excellence. " * 25)
            + "\nNote: the legacy rubric table has moved to Section 4 below."
        )
        reason = detect_non_content_page(
            url="https://gmone.example.com/guides/leveling",
            title="Engineering Leveling Guide",
            content=long_article,
            description="Career leveling expectations for engineering tracks.",
        )
        self.assertIsNone(reason)


class TestCitationUrlAndSolrIngestion(unittest.TestCase):
    def test_resolve_citation_url_prefers_its_url(self):
        solr_doc = {
            "id": "news-101",
            "url": "https://gmone.example.com/content/news.splite.html",
            "ITS_URL": "https://gmone.example.com/content/news.detail.html",
            "title": "Miles of memories: Up north in the ESCALADE IQ",
        }
        resolved = resolve_citation_url(solr_doc)
        self.assertEqual(resolved, "https://gmone.example.com/content/news.detail.html")

    def test_unwrap_google_redirect_and_relative_links_in_html(self):
        html = """
        <article>
          <h1> Engineering Portal </h1>
          <p>Read the <a href="https://www.google.com/url?q=https://gmone.example.com/docs/spec&sa=D">Spec</a>
          and <a href="/guides/onboarding.html">Onboarding</a>
          or <a href="https://login.microsoftonline.com/common/oauth2/authorize">SSO Login</a>.</p>
        </article>
        """
        soup = BeautifulSoup(html, "html.parser")
        md = html_to_markdown(soup, base_url="https://gmone.example.com/home/index.html")
        self.assertIn("[Spec](https://gmone.example.com/docs/spec)", md)
        self.assertIn("[Onboarding](https://gmone.example.com/guides/onboarding.html)", md)
        # SSO login link should be stripped down to plain text, not an active login.microsoftonline.com link
        self.assertNotIn("login.microsoftonline.com", md)

    def test_solr_json_crawl_filters_junk_repairs_mojibake_and_uses_its_url(self):
        solr_payload = {
            "response": {
                "numFound": 4,
                "docs": [
                    {
                        "id": "1",
                        "url": "https://gmone.example.com/news/escalade.splite.html",
                        "ITS_URL": "https://gmone.example.com/news/escalade.detail.html",
                        "title": "Miles of memories: thereâ€™s an ESCALADE IQ",
                        "description": "null",
                        "content": (
                            "By Jane Doe — Thereâ€™s nothing quite like driving up north in the "
                            "all-electric ESCALADE IQ across Michigan’s upper peninsula."
                        ),
                    },
                    {
                        "id": "2",
                        "url": "https://login.microsoftonline.com/tenant/oauth2/authorize?client_id=1",
                        "title": "Sign in to your account",
                        "description": "null",
                        "content": "Sign in to your account",
                    },
                    {
                        "id": "3",
                        "url": "https://gmone.example.com/links/loa.html",
                        "title": "Leave of Absence has moved",
                        "description": "null",
                        "content": "This page has moved, update your bookmark.",
                    },
                    {
                        "id": "4",
                        "url": "https://gmone.example.com/links/leader.html",
                        "title": "Leader Workspace has moved",
                        "description": "None",
                        "content": "Leader Workspace has moved. Please update your bookmarks.",
                    },
                ],
            }
        }
        raw_bytes = json.dumps(solr_payload, ensure_ascii=False).encode("utf-8")
        mock_resp = _make_response(
            raw_bytes,
            status_code=200,
            content_type="application/json",
            url="https://gmone.example.com/solr/gmonenews/select?q=*:*&wt=json",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(requests.Session, "get", return_value=mock_resp):
                result = execute_ingestion(
                    {
                        "url": "https://gmone.example.com/solr/gmonenews/select?q=*:*&wt=json",
                        "max_pages": 50,
                        "max_depth": 1,
                        "delay": 0.0,
                        "dry_run": True,
                    },
                    staging_dir=tmpdir,
                )

            # Only 1 valid doc should be indexed; the 3 non-content docs are filtered
            self.assertEqual(result["pages_count"], 1)
            self.assertEqual(result["filtered_count"], 3)
            self.assertEqual(result["failed_count"], 0)
            # Because the 3 skipped docs were intentionally filtered non-content (not HTTP failures),
            # crawl_complete must be True so FULL reconciliation mode purges them from Discovery Engine!
            self.assertTrue(result["crawl_complete"])
            self.assertEqual(result["reconciliation_mode_used"], "FULL")

            metadata_file = Path(tmpdir) / "metadata.jsonl"
            lines = metadata_file.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)
            meta = json.loads(lines[0])
            self.assertEqual(
                meta["structData"]["url"],
                "https://gmone.example.com/news/escalade.detail.html",
            )
            self.assertEqual(
                meta["structData"]["title"],
                "Miles of memories: there’s an ESCALADE IQ",
            )
            # "null" description must be sanitized to empty string ""
            self.assertEqual(meta["structData"]["description"], "")

            doc_files = list((Path(tmpdir) / "documents").glob("*.md"))
            self.assertEqual(len(doc_files), 1)
            md_text = doc_files[0].read_text(encoding="utf-8")
            self.assertIn("There’s nothing quite like driving up north", md_text)
            self.assertNotIn("thereâ€™s", md_text)
            self.assertIn('description: ""', md_text)

    def test_rate_limit_429_retries_with_backoff(self):
        html_body = (
            "<html><head><title>Valid Guide</title><meta name='description' content='null'></head>"
            "<body><main><p>" + ("Detailed engineering architecture guide content. " * 5) + "</p></main></body></html>"
        ).encode("utf-8")
        resp_429 = _make_response(
            b"Rate limit exceeded",
            status_code=429,
            content_type="text/plain",
            headers={"Retry-After": "0"},
        )
        resp_200 = _make_response(
            html_body,
            status_code=200,
            content_type="text/html",
            url="https://gmone.example.com/guide",
        )

        crawler = WebsiteCrawler(
            base_url="https://gmone.example.com/guide",
            max_pages=5,
            max_depth=0,
            delay_seconds=0.0,
        )
        with patch.object(crawler.session, "get", side_effect=[resp_429, resp_200]) as mock_get:
            with patch("time.sleep"):
                pages = crawler.crawl()
                self.assertEqual(mock_get.call_count, 2)
                self.assertEqual(len(pages), 1)
                self.assertEqual(pages[0]["description"], "")


class TestCloudDeploymentManagement(unittest.TestCase):
    def test_rejects_malformed_or_hostile_gcp_identifiers(self):
        from app import list_cloud_deployments, manage_cloud_deployment

        for hostile in ["proj; rm -rf /", "proj$(whoami)", "../etc/passwd", ""]:
            with self.assertRaises(ValueError):
                list_cloud_deployments(hostile, "us-central1")
            with self.assertRaises(ValueError):
                manage_cloud_deployment({
                    "project_id": "valid-proj-123",
                    "region": "us-central1",
                    "name": hostile,
                    "action": "pause_scheduler",
                })

        with self.assertRaises(ValueError):
            manage_cloud_deployment({
                "project_id": "valid-proj-123",
                "region": "us-central1",
                "name": "site-ingestor-nightly-sync",
                "action": "arbitrary_command",
            })

    def test_pause_resume_and_delete_invokes_expected_gcloud_commands(self):
        from app import manage_cloud_deployment

        ok_proc = MagicMock(returncode=0, stdout="ok", stderr="")
        with patch("app.subprocess.run", return_value=ok_proc) as mock_run:
            res = manage_cloud_deployment({
                "project_id": "ancient-sandbox-322523",
                "region": "us-central1",
                "name": "site-ingestor-nightly-sync",
                "action": "pause_scheduler",
            })
            self.assertEqual(res["status"], "success")
            cmd = mock_run.call_args[0][0]
            self.assertEqual(
                cmd,
                [
                    "gcloud", "scheduler", "jobs", "pause",
                    "site-ingestor-nightly-sync",
                    "--location=us-central1",
                    "--project=ancient-sandbox-322523",
                    "--quiet",
                ],
            )


if __name__ == "__main__":
    unittest.main()

