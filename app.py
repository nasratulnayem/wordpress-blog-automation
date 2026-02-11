import json
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timezone
from html import unescape
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

load_dotenv()

try:
    from google import genai as genai_sdk
except Exception:  # noqa: BLE001
    genai_sdk = None

try:
    import google.generativeai as genai_legacy
except Exception:  # noqa: BLE001
    genai_legacy = None

ENV_WP_BASE_URL = os.getenv("WP_BASE_URL", "").rstrip("/")
ENV_WP_USERNAME = os.getenv("WP_USERNAME", "")
ENV_WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEYS_PATH = os.path.join(BASE_DIR, "api.txt")
ENV_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
ENV_GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "").strip()
ENV_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash-latest")
ENV_GEMINI_SDK = os.getenv("GEMINI_SDK", "auto").strip().lower()
ENV_CUSTOM_PROMPT = os.getenv("CUSTOM_PROMPT", "").strip()

ENV_META_TITLE_KEY = os.getenv("META_TITLE_KEY", "yoast_wpseo_title")
ENV_META_DESCRIPTION_KEY = os.getenv("META_DESCRIPTION_KEY", "yoast_wpseo_metadesc")
ENV_PRODUCT_META_TITLE_KEY = os.getenv("PRODUCT_META_TITLE_KEY", "_yoast_wpseo_title")
ENV_PRODUCT_META_DESCRIPTION_KEY = os.getenv(
    "PRODUCT_META_DESCRIPTION_KEY", "_yoast_wpseo_metadesc"
)
ENV_PRODUCT_FOCUS_KEYWORD_KEY = os.getenv(
    "PRODUCT_FOCUS_KEYWORD_KEY", "_yoast_wpseo_focuskw"
)
ENV_USE_EXCERPT_FOR_META_DESCRIPTION = os.getenv(
    "USE_EXCERPT_FOR_META_DESCRIPTION", "true"
).lower() in ("1", "true", "yes")

INBOUND_LINKS = [
    "http://www.youtube.com/channel/UC55QaSgNnXS8wKU0AA-G0Zw?sub_confirmation=1",
    "https://gplmama.com/category/wordpress-themes/",
    "https://gplmama.com/category/wordpress-plugins/",
    "https://gplmama.com/category/woocommerce-plugin/",
    "https://gplmama.com/category/shopify-themes/",
    "https://gplmama.com/category/web-design/",
    "https://facebook.com/gplmama",
    "https://pinterest.com/gplmama",
    "https://www.youtube.com/@gplmama",
    "https://wa.me/01962351470",
    "https://gplmama.com/request-a-quote/",
    "https://gplmama.com/changelog/",
    "https://gplmama.com/pricing/",
    "https://gplmama.com/membership/",
]

DEFAULT_CUSTOM_PROMPT = """
Required GPLMama context (use when relevant, stay factual, no overselling):
GPLMama is Bangladesh's affordable hub for premium digital assets. We offer lifetime access to 2,200+ assets for a one-time fee of BDT 149 with no ads or hassles. The library includes WordPress themes, plugins, WooCommerce plugins, and Shopify themes. Files are original and unmodified. New files are added regularly. The GPL model is legal and safe. We support freelancers, small businesses, and agencies. There is a commercial license for client work and resale. Local support and video guides are available.

Writing rules (apply throughout):
- Write about 2000 words.
- Use simple, plain language and short sentences.
- Avoid AI-sounding phrases and cliches (no "let's dive in", "game-changing", "unlock potential").
- Be direct and concise. Remove filler words.
- Keep the tone natural and conversational. It is ok to start sentences with "and" or "but".
- Avoid hype, buzzwords, and overpromises.
- Keep grammar simple and readable.
- Remove fluff and extra adjectives.
- Make every sentence clear and easy to understand.
- Use transitions for flow and break long paragraphs into smaller ones.
- Keep the tone friendly, helpful, and real (not robotic or academic).

Uniqueness and expertise:
- Derive 10 key questions a writer would ask to make this unique.
- Answer those questions inside the post without listing them.
- Include practical examples, light storytelling, or mini case scenarios where useful.
""".strip()

DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")
EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv("MAX_WORKERS", "3")))
POST_CACHE_TTL = int(os.getenv("POST_CACHE_TTL", "30"))
FETCH_PER_PAGE = int(os.getenv("FETCH_PER_PAGE", "100"))
POST_CACHE: Dict[str, Any] = {"posts": [], "fetched_at": 0.0, "signature": ""}


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_rewrite_status (
                product_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                updated_at TEXT,
                last_error TEXT,
                old_title TEXT,
                new_title TEXT,
                permalink TEXT
            )
            """
        )
        ensure_product_rewrite_schema(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_rewrite_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                prompt_title TEXT NOT NULL,
                response_title TEXT NOT NULL,
                prompt_description TEXT NOT NULL,
                response_description TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS post_status (
                post_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                generated_at TEXT,
                last_error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS generation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                prompt TEXT NOT NULL,
                response TEXT NOT NULL,
                meta_title TEXT,
                meta_description TEXT,
                tags TEXT
            )
            """
        )
    conn.close()


def ensure_product_rewrite_schema(conn: sqlite3.Connection) -> None:
    cols = conn.execute("PRAGMA table_info(product_rewrite_status)").fetchall()
    names = {row[1] for row in cols}  # (cid, name, type, notnull, dflt_value, pk)
    # Per-piece completion so "Do Title" and "Do Desc" can be tracked separately.
    if "title_done" not in names:
        conn.execute(
            "ALTER TABLE product_rewrite_status ADD COLUMN title_done INTEGER NOT NULL DEFAULT 0"
        )
    if "desc_done" not in names:
        conn.execute(
            "ALTER TABLE product_rewrite_status ADD COLUMN desc_done INTEGER NOT NULL DEFAULT 0"
        )
    if "seo_done" not in names:
        conn.execute(
            "ALTER TABLE product_rewrite_status ADD COLUMN seo_done INTEGER NOT NULL DEFAULT 0"
        )
    if "last_title_error" not in names:
        conn.execute(
            "ALTER TABLE product_rewrite_status ADD COLUMN last_title_error TEXT"
        )
    if "last_desc_error" not in names:
        conn.execute("ALTER TABLE product_rewrite_status ADD COLUMN last_desc_error TEXT")
    if "last_seo_error" not in names:
        conn.execute("ALTER TABLE product_rewrite_status ADD COLUMN last_seo_error TEXT")
    if "seo_title" not in names:
        conn.execute("ALTER TABLE product_rewrite_status ADD COLUMN seo_title TEXT")
    if "seo_description" not in names:
        conn.execute("ALTER TABLE product_rewrite_status ADD COLUMN seo_description TEXT")
    if "seo_focus_keyword" not in names:
        conn.execute(
            "ALTER TABLE product_rewrite_status ADD COLUMN seo_focus_keyword TEXT"
        )
    if "slug_done" not in names:
        conn.execute(
            "ALTER TABLE product_rewrite_status ADD COLUMN slug_done INTEGER NOT NULL DEFAULT 0"
        )
    if "last_slug_error" not in names:
        conn.execute("ALTER TABLE product_rewrite_status ADD COLUMN last_slug_error TEXT")
    if "old_slug" not in names:
        conn.execute("ALTER TABLE product_rewrite_status ADD COLUMN old_slug TEXT")
    if "new_slug" not in names:
        conn.execute("ALTER TABLE product_rewrite_status ADD COLUMN new_slug TEXT")

    # Backfill flags for older rows so filters (Done/Partial) behave as expected.
    conn.execute(
        "UPDATE product_rewrite_status SET title_done = 1 WHERE status = 'done' AND title_done = 0"
    )
    conn.execute(
        "UPDATE product_rewrite_status SET desc_done = 1 WHERE status = 'done' AND desc_done = 0"
    )
    conn.execute(
        "UPDATE product_rewrite_status SET seo_done = 1 WHERE status = 'done' AND seo_done = 0"
    )


@app.before_request
def ensure_setup() -> None:
    init_db()


class WordPressClient:
    def __init__(self, base_url: str, username: str, app_password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (username, app_password)

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def list_posts(self, page: int = 1, per_page: int = 50) -> List[Dict[str, Any]]:
        url = self._url("/wp-json/wp/v2/posts")
        params = {
            "per_page": per_page,
            "page": page,
            "status": "publish,draft,pending,future",
            "orderby": "date",
            "order": "desc",
        }
        resp = requests.get(url, params=params, auth=self.auth, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def list_posts_page(
        self, page: int = 1, per_page: int = 10
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        url = self._url("/wp-json/wp/v2/posts")
        params = {
            "per_page": per_page,
            "page": page,
            "status": "publish,draft,pending,future",
            "orderby": "date",
            "order": "desc",
        }
        retries = 2
        while True:
            resp = requests.get(url, params=params, auth=self.auth, timeout=30)
            try:
                resp.raise_for_status()
                break
            except requests.HTTPError as exc:
                if resp.status_code in (502, 503, 504) and retries > 0:
                    retries -= 1
                    time.sleep(1)
                    continue
                raise exc
        total_pages = int(resp.headers.get("X-WP-TotalPages", "1") or "1")
        total_posts = int(resp.headers.get("X-WP-Total", "0") or "0")
        return resp.json(), total_pages, total_posts

    def list_all_posts(self, per_page: int = 50, max_pages: int = 10) -> List[Dict[str, Any]]:
        all_posts: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            posts = self.list_posts(page=page, per_page=per_page)
            all_posts.extend(posts)
            if len(posts) < per_page:
                break
        return all_posts

    def ping(self) -> None:
        url = self._url("/wp-json/wp/v2/users/me")
        resp = requests.get(url, auth=self.auth, timeout=30)
        resp.raise_for_status()

    def get_post(self, post_id: int) -> Dict[str, Any]:
        url = self._url(f"/wp-json/wp/v2/posts/{post_id}")
        params = {"context": "edit"}
        resp = requests.get(url, params=params, auth=self.auth, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def update_post(
        self,
        post_id: int,
        content_html: str,
        meta_title: str,
        meta_description: str,
        tag_ids: List[int],
    ) -> Dict[str, Any]:
        url = self._url(f"/wp-json/wp/v2/posts/{post_id}")
        payload: Dict[str, Any] = {
            "content": content_html,
            "tags": tag_ids,
            "status": "publish",
            "password": "",
        }
        if USE_EXCERPT_FOR_META_DESCRIPTION and meta_description:
            payload["excerpt"] = meta_description
        if META_TITLE_KEY or META_DESCRIPTION_KEY:
            meta_payload = {}
            if META_TITLE_KEY:
                meta_payload[META_TITLE_KEY] = meta_title
            if META_DESCRIPTION_KEY:
                meta_payload[META_DESCRIPTION_KEY] = meta_description
            payload["meta"] = meta_payload
        resp = requests.post(url, json=payload, auth=self.auth, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def update_product_meta(self, product_id: int, meta: Dict[str, str]) -> bool:
        """
        Best-effort fallback for SEO plugins that only read WP postmeta, not WC meta_data.
        Requires the product CPT endpoint to be enabled at /wp-json/wp/v2/product/<id>.
        """
        if not meta:
            return True
        url = self._url(f"/wp-json/wp/v2/product/{product_id}")
        resp = requests.post(url, json={"meta": meta}, auth=self.auth, timeout=45)
        if resp.status_code in (404, 400, 401, 403):
            return False
        resp.raise_for_status()
        return True

    def find_or_create_tag(self, name: str) -> Optional[int]:
        name = name.strip()
        if not name:
            return None
        url = self._url("/wp-json/wp/v2/tags")
        resp = requests.get(url, params={"search": name}, auth=self.auth, timeout=30)
        resp.raise_for_status()
        for tag in resp.json():
            if tag.get("name", "").lower() == name.lower():
                return tag["id"]
        create = requests.post(url, json={"name": name}, auth=self.auth, timeout=30)
        if create.status_code == 400:
            return None
        create.raise_for_status()
        return create.json()["id"]


class WooCommerceClient:
    def __init__(self, base_url: str, username: str, app_password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (username, app_password)

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def list_products(
        self,
        page: int = 1,
        per_page: int = 100,
        after: str = "",
        before: str = "",
        orderby: str = "date",
        order: str = "desc",
    ) -> Tuple[List[Dict[str, Any]], int]:
        url = self._url("/wp-json/wc/v3/products")
        params: Dict[str, Any] = {
            "per_page": per_page,
            "page": page,
            "orderby": orderby,
            "order": order,
        }
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        resp = requests.get(url, params=params, auth=self.auth, timeout=45)
        retries = 2
        while True:
            try:
                resp.raise_for_status()
                break
            except requests.HTTPError as exc:
                if resp.status_code in (502, 503, 504) and retries > 0:
                    retries -= 1
                    time.sleep(1)
                    resp = requests.get(url, params=params, auth=self.auth, timeout=45)
                    continue
                raise exc
        total_pages = int(resp.headers.get("X-WP-TotalPages", "1") or "1")
        return resp.json(), total_pages

    def list_all_products(
        self,
        per_page: int = 100,
        after: str = "",
        before: str = "",
        max_pages: int = 100,
    ) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            batch, total_pages = self.list_products(
                page=page, per_page=per_page, after=after, before=before
            )
            products.extend(batch)
            if page >= total_pages or len(batch) < per_page:
                break
        return products

    def update_product(
        self,
        product_id: int,
        *,
        name: Optional[str] = None,
        description_html: Optional[str] = None,
        slug: Optional[str] = None,
        meta: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = self._url(f"/wp-json/wc/v3/products/{product_id}")
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if description_html is not None:
            payload["description"] = description_html
        if slug is not None:
            payload["slug"] = slug
        if meta:
            payload["meta_data"] = [{"key": k, "value": v} for k, v in meta.items() if k and v]
        resp = requests.put(url, json=payload, auth=self.auth, timeout=45)
        resp.raise_for_status()
        return resp.json()

    def get_product(self, product_id: int) -> Dict[str, Any]:
        url = self._url(f"/wp-json/wc/v3/products/{product_id}")
        resp = requests.get(url, auth=self.auth, timeout=45)
        resp.raise_for_status()
        return resp.json()


def redirect_back(default: str = "index") -> str:
    fallback = url_for(default)
    return redirect(request.referrer or fallback)


def safe_next_url(next_url: str, fallback: str) -> str:
    if next_url and next_url.startswith("/"):
        return next_url
    return fallback


class GeminiClient:
    def __init__(self, api_key: str, model_name: str, sdk_mode: str = "auto") -> None:
        self.api_key = api_key
        self.model_name = model_name
        self.sdk_mode = (sdk_mode or "auto").strip().lower()
        self.client = None
        self.model = None
        self.active_sdk = ""

        prefer_new = self.sdk_mode in ("auto", "genai")
        prefer_legacy = self.sdk_mode in ("auto", "legacy")

        if prefer_new and genai_sdk is not None:
            self.client = genai_sdk.Client(api_key=api_key)
            self.active_sdk = "genai"
        elif prefer_legacy and genai_legacy is not None:
            genai_legacy.configure(api_key=api_key)
            self.model = genai_legacy.GenerativeModel(model_name)
            self.active_sdk = "legacy"
        else:
            if self.sdk_mode == "genai":
                raise RuntimeError("GEMINI_SDK=genai but google-genai is not installed.")
            if self.sdk_mode == "legacy":
                raise RuntimeError(
                    "GEMINI_SDK=legacy but google-generativeai is not installed."
                )
            raise RuntimeError(
                "No Gemini SDK available. Install google-genai or google-generativeai."
            )

    def generate(self, prompt: str) -> str:
        if self.active_sdk == "genai":
            response = self.client.models.generate_content(
                model=self.model_name, contents=prompt
            )
            return self._extract_text(response)
        response = self.model.generate_content(prompt)
        return response.text or ""

    def _extract_text(self, response: Any) -> str:
        text = getattr(response, "text", None)
        if text:
            return text
        candidates = getattr(response, "candidates", None)
        if candidates:
            content = getattr(candidates[0], "content", None)
            parts = getattr(content, "parts", None) if content else None
            if parts:
                return getattr(parts[0], "text", "") or ""
        return ""

    def list_models(self) -> List[str]:
        models = []
        if self.active_sdk == "genai":
            try:
                for model in self.client.models.list():
                    name = getattr(model, "name", "")
                    if name:
                        models.append(name)
            except Exception:  # noqa: BLE001
                return []
            return models
        for model in genai_legacy.list_models():
            if "generateContent" in model.supported_generation_methods:
                models.append(model.name)
        return models


def is_quota_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "resourceexhausted" in message or "quota" in message or "429" in message


def compact_error_message(raw: Optional[str]) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "api_key_invalid" in lowered or "api key not valid" in lowered:
        return "Invalid API Key"
    if "resource_exhausted" in lowered or "resourceexhausted" in lowered:
        return "Quota Limit"
    if "quota" in lowered or "429" in lowered:
        return "Quota Limit"
    if "missing product seo meta keys" in lowered:
        return "SEO Keys Missing"
    if "could not generate product slug" in lowered:
        return "Slug Failed"
    if "generated title did not match rules" in lowered:
        return "Title Rule Error"
    if "generated seo meta did not match length/rules" in lowered:
        return "SEO Rule Error"
    if "json" in lowered and "invalid" in lowered:
        return "Invalid AI Output"
    return "Process Error"


class MultiKeyGemini:
    def __init__(self, runtime: Dict[str, Any]) -> None:
        self.runtime = runtime
        keys = runtime.get("gemini_api_keys") or []
        if not keys and runtime.get("gemini_api_key"):
            keys = [runtime.get("gemini_api_key", "")]
        self.keys = [key for key in keys if key.strip()]

    def _make_client(self, api_key: str) -> GeminiClient:
        return GeminiClient(
            api_key,
            self.runtime.get("gemini_model", "gemini-1.5-flash"),
            self.runtime.get("gemini_sdk", "auto"),
        )

    def generate(self, prompt: str, conn: Optional[sqlite3.Connection] = None) -> str:
        if not self.keys:
            raise RuntimeError("Gemini API key is missing.")
        last_exc: Optional[Exception] = None
        for api_key in self.keys:
            gemini = self._make_client(api_key)
            try:
                return gemini.generate(prompt)
            except Exception as exc:  # noqa: BLE001
                if "models/" in str(exc) and "not found" in str(exc):
                    models = gemini.list_models()
                    picked = choose_default_model(models)
                    if picked:
                        self.runtime["gemini_model"] = picked
                        if conn is not None:
                            conn.execute(
                                """
                                INSERT INTO app_config (key, value)
                                VALUES (?, ?)
                                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                                """,
                                ("gemini_model", picked),
                            )
                        gemini = self._make_client(api_key)
                        return gemini.generate(prompt)
                    raise
                if is_quota_error(exc):
                    last_exc = exc
                    continue
                raise
        if last_exc:
            raise last_exc
        raise RuntimeError("Gemini API key is missing.")

    def list_models(self) -> List[str]:
        if not self.keys:
            return []
        last_exc: Optional[Exception] = None
        for api_key in self.keys:
            gemini = self._make_client(api_key)
            try:
                return gemini.list_models()
            except Exception as exc:  # noqa: BLE001
                if is_quota_error(exc):
                    last_exc = exc
                    continue
                raise
        if last_exc:
            raise last_exc
        return []


def choose_default_model(models: List[str]) -> str:
    preferred = [
        "models/gemini-1.5-flash-latest",
        "models/gemini-1.5-pro-latest",
        "models/gemini-1.5-flash",
        "models/gemini-1.5-pro",
        "models/gemini-1.0-pro",
    ]
    for name in preferred:
        if name in models:
            return name
    return models[0] if models else ""


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def is_empty_content(content_html: str) -> bool:
    text = strip_html(content_html)
    text = re.sub(r"\s+", "", text)
    return not text


def normalize_title(title_html: str) -> str:
    text = strip_html(title_html)
    return unescape(text)


def normalize_tag_name(tag: str) -> str:
    cleaned = re.sub(r"\s+", " ", tag or "").strip()
    return cleaned[:80]


def pick_inbound_links() -> List[str]:
    count = 2 if len(INBOUND_LINKS) >= 2 else len(INBOUND_LINKS)
    return random.sample(INBOUND_LINKS, count)


def build_prompt(title: str, inbound_links: List[str], custom_prompt: str) -> str:
    inbound_text = "\n".join(f"- {link}" for link in inbound_links)
    required_prompt = DEFAULT_CUSTOM_PROMPT.replace("{title}", title)
    custom_text = ""
    if custom_prompt:
        cleaned_custom = custom_prompt.replace("{title}", title).strip()
        if cleaned_custom and cleaned_custom != required_prompt:
            custom_text = f"\nAdditional instructions:\n{cleaned_custom}\n"

    return f"""
You are writing a professional, SEO-focused micro blog post that answers one clear question.

Question/Topic: {title}
Method: Simple PAA content structure
- One question = one page (non-negotiable). Do not combine multiple questions into one article.
- About 2000 words total.
- Simple, direct language. Short sentences.
- No hype, no buzzwords, no clichés.
- No special characters or emojis.
- Do not use headings like Conclusion or Final thoughts.
- Start with a short intro paragraph (1-2 sentences), not a title heading.
- Use clear H2 and H3 structure.
- Put the direct answer in the first 40 words after the H2 (required).
- Add 2 to 3 related H2 sub-questions (People Also Ask style).
- Under each H2, use short H3s for supporting details, steps, or examples.
- Use lists where helpful.
- Keep tone natural, helpful, and human.
- Stay on topic and do not add unrelated content.
- Make the content valuable and practical for the reader.
- Make my role clear in the narrative (the brand/operator behind the site) without overpromising.
- Ensure each post is distinct in structure and examples, even with similar topics.
- Do not show raw URLs as text. Use anchor text for all links.
- Add a 1-line TL;DR near the top or bottom.

Internal workflow:
- Derive 6 to 8 questions a writer would ask to make this unique.
- Answer those questions inside the post content without showing them.

Links:
- Include exactly 2 outbound links to reputable, live sources.
- Include exactly 2 inbound links from this list:
- The first inbound link must be the main guide (Medium or main site) and be contextual.
- The second inbound link should point to a related PAA post (contextual).
{inbound_text}
- Include exactly 1 contextual link to gplmama.com:
  - Place it after explaining GPL.
  - Use neutral anchor text (not exact-match/affiliate language).
  - Do not place it early in the article.
  - Add rel="nofollow" to that link.

SEO:
- Provide a meta title (50-60 chars).
- Provide a meta description (140-160 chars).
- Provide 5 to 8 tags.
- Add FAQ schema (JSON-LD) with exactly 1 question and a 2-3 line answer.
- The FAQ question must match the post title/question.

Output format:
Return only valid JSON with these keys:
- content_html
- meta_title
- meta_description
- tags (array of strings)
Required site and writing requirements:
{required_prompt}
{custom_text}
""".strip()


def build_metadata_prompt(title: str, content_html: str) -> str:
    summary = strip_html(content_html)
    summary = re.sub(r"\s+", " ", summary).strip()
    summary = summary[:1200]
    return f"""
You are preparing SEO metadata for this blog post.

Title: {title}
Content summary: {summary}

Rules:
- Meta title 50 to 60 characters.
- Meta description 140 to 160 characters.
- 5 to 8 short tags, no hashtags.

Output format:
Return only valid JSON with these keys:
- meta_title
- meta_description
- tags (array of strings)
""".strip()


def extract_json(text: str) -> Dict[str, Any]:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("No JSON object found in model response.")
    return json.loads(match.group(0))


def build_json_repair_prompt(base_prompt: str, bad_response: str) -> str:
    return f"""
You returned invalid JSON. Fix it.

Original instructions:
{base_prompt}

Your previous response:
{bad_response}

Return only valid JSON with the required keys.
""".strip()


def log_generation(
    conn: sqlite3.Connection,
    post_id: int,
    prompt: str,
    response: str,
    meta_title: str,
    meta_description: str,
    tags: List[str],
) -> None:
    conn.execute(
        """
        INSERT INTO generation_log
            (post_id, created_at, prompt, response, meta_title, meta_description, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            post_id,
            datetime.now(timezone.utc).isoformat(),
            prompt,
            response,
            meta_title,
            meta_description,
            ", ".join(tags),
        ),
    )


def update_product_status(
    conn: sqlite3.Connection,
    product_id: int,
    status: str,
    error: Optional[str] = None,
    old_title: str = "",
    new_title: str = "",
    permalink: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO product_rewrite_status
            (product_id, status, updated_at, last_error, old_title, new_title, permalink)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            status=excluded.status,
            updated_at=excluded.updated_at,
            last_error=excluded.last_error,
            old_title=(CASE WHEN excluded.old_title != '' THEN excluded.old_title ELSE product_rewrite_status.old_title END),
            new_title=(CASE WHEN excluded.new_title != '' THEN excluded.new_title ELSE product_rewrite_status.new_title END),
            permalink=(CASE WHEN excluded.permalink != '' THEN excluded.permalink ELSE product_rewrite_status.permalink END)
        """,
        (
            product_id,
            status,
            datetime.now(timezone.utc).isoformat(),
            compact_error_message(error),
            old_title,
            new_title,
            permalink,
        ),
    )


def update_product_piece_flags(
    conn: sqlite3.Connection,
    product_id: int,
    *,
    title_done: Optional[int] = None,
    desc_done: Optional[int] = None,
    seo_done: Optional[int] = None,
    slug_done: Optional[int] = None,
    last_title_error: Optional[str] = None,
    last_desc_error: Optional[str] = None,
    last_seo_error: Optional[str] = None,
    last_slug_error: Optional[str] = None,
    seo_title: Optional[str] = None,
    seo_description: Optional[str] = None,
    seo_focus_keyword: Optional[str] = None,
    old_slug: Optional[str] = None,
    new_slug: Optional[str] = None,
) -> None:
    fields = []
    params: List[Any] = []
    if title_done is not None:
        fields.append("title_done = ?")
        params.append(int(title_done))
    if desc_done is not None:
        fields.append("desc_done = ?")
        params.append(int(desc_done))
    if seo_done is not None:
        fields.append("seo_done = ?")
        params.append(int(seo_done))
    if slug_done is not None:
        fields.append("slug_done = ?")
        params.append(int(slug_done))
    if last_title_error is not None:
        fields.append("last_title_error = ?")
        params.append(compact_error_message(last_title_error))
    if last_desc_error is not None:
        fields.append("last_desc_error = ?")
        params.append(compact_error_message(last_desc_error))
    if last_seo_error is not None:
        fields.append("last_seo_error = ?")
        params.append(compact_error_message(last_seo_error))
    if last_slug_error is not None:
        fields.append("last_slug_error = ?")
        params.append(compact_error_message(last_slug_error))
    if seo_title is not None:
        fields.append("seo_title = ?")
        params.append(seo_title)
    if seo_description is not None:
        fields.append("seo_description = ?")
        params.append(seo_description)
    if seo_focus_keyword is not None:
        fields.append("seo_focus_keyword = ?")
        params.append(seo_focus_keyword)
    if old_slug is not None:
        fields.append("old_slug = ?")
        params.append(old_slug)
    if new_slug is not None:
        fields.append("new_slug = ?")
        params.append(new_slug)
    if not fields:
        return
    fields.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.append(product_id)
    conn.execute(
        f"UPDATE product_rewrite_status SET {', '.join(fields)} WHERE product_id = ?",
        tuple(params),
    )


def compute_product_overall_status(row: sqlite3.Row) -> str:
    current = (row["status"] or "").strip()
    if current == "skipped":
        return "skipped"
    if current == "done":
        return "done"
    if current == "processing":
        return "processing"
    if current == "queued":
        return "queued"
    if current == "error":
        return "error"
    title_done = int(row["title_done"] or 0)
    desc_done = int(row["desc_done"] or 0)
    seo_done = int(row["seo_done"] or 0)
    slug_done = int(row["slug_done"] or 0)
    if title_done and desc_done and seo_done and slug_done:
        return "done"
    if title_done or desc_done or seo_done or slug_done:
        return "partial"
    return current or "pending"


def sync_products_to_db(start_date: str, end_date: str) -> None:
    runtime = get_runtime_config()
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
    ):
        return

    set_product_bulk_message("Sync starting...")
    conn = get_db()
    wc_client = WooCommerceClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    try:
        after = f"{start_date}T00:00:00Z"
        before = f"{end_date}T23:59:59Z"
        per_page = 100
        max_pages = 200
        first_batch, total_pages = wc_client.list_products(
            page=1, per_page=per_page, after=after, before=before
        )
        total_pages = min(total_pages, max_pages)

        discovered = 0
        inserted = 0
        skipped = 0

        def upsert(product: Dict[str, Any]) -> None:
            nonlocal discovered, inserted, skipped
            product_id = int(product.get("id") or 0)
            if not product_id:
                return
            discovered += 1
            old_title = clamp_spaces(product.get("name", "") or "")
            permalink = str(product.get("permalink", "") or "")
            old_slug = clamp_spaces(product.get("slug", "") or "")

            if product_id == 3718 or wc_is_membership_product(product):
                reason = (
                    "Skipped: excluded product_id 3718."
                    if product_id == 3718
                    else "Skipped: membership category."
                )
                with conn:
                    update_product_status(
                        conn,
                        product_id,
                        "skipped",
                        reason,
                        old_title=old_title,
                        permalink=permalink,
                    )
                    update_product_piece_flags(conn, product_id, old_slug=old_slug)
                skipped += 1
                return

            existing = conn.execute(
                "SELECT status FROM product_rewrite_status WHERE product_id = ?",
                (product_id,),
            ).fetchone()
            if existing and (existing["status"] or "") == "skipped":
                return

            # Don't overwrite done/partial progress; just ensure it stays listed.
            if existing and (existing["status"] or "") in ("done", "partial"):
                with conn:
                    conn.execute(
                        """
                        UPDATE product_rewrite_status
                        SET old_title = ?, permalink = ?, updated_at = ?
                        WHERE product_id = ?
                        """,
                        (
                            old_title,
                            permalink,
                            datetime.now(timezone.utc).isoformat(),
                            product_id,
                        ),
                    )
                    update_product_piece_flags(conn, product_id, old_slug=old_slug)
                return

            with conn:
                update_product_status(
                    conn,
                    product_id,
                    "pending",
                    None,
                    old_title=old_title,
                    permalink=permalink,
                )
                update_product_piece_flags(conn, product_id, old_slug=old_slug)
            inserted += 1

        set_product_bulk_message(f"Sync fetching 1/{total_pages}...")
        for product in first_batch:
            if product_bulk_should_stop():
                break
            upsert(product)

        for page in range(2, total_pages + 1):
            if product_bulk_should_stop():
                break
            set_product_bulk_message(f"Sync fetching {page}/{total_pages}...")
            batch, _tp = wc_client.list_products(
                page=page, per_page=per_page, after=after, before=before
            )
            for product in batch:
                if product_bulk_should_stop():
                    break
                upsert(product)
            if len(batch) < per_page:
                break

        set_product_bulk_message(
            f"Sync done. Found {discovered}. Added/updated {inserted}. Skipped {skipped}."
        )
    finally:
        conn.close()


def get_product_status_map(conn: sqlite3.Connection) -> Dict[int, sqlite3.Row]:
    rows = conn.execute("SELECT * FROM product_rewrite_status").fetchall()
    return {row["product_id"]: row for row in rows}


def product_bulk_should_stop() -> bool:
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM app_config WHERE key = ?",
        ("product_bulk_stop",),
    ).fetchone()
    conn.close()
    return bool(row and (row["value"] or "").strip() in ("1", "true", "yes"))


def set_product_bulk_flag(key: str, value: str) -> None:
    conn = get_db()
    with conn:
        set_config(conn, {key: value})
    conn.close()


def set_product_bulk_message(message: str) -> None:
    conn = get_db()
    with conn:
        set_config(
            conn,
            {
                "product_bulk_message": message,
                "product_bulk_message_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    conn.close()


def log_product_rewrite(
    conn: sqlite3.Connection,
    product_id: int,
    prompt_title: str,
    response_title: str,
    prompt_description: str,
    response_description: str,
) -> None:
    conn.execute(
        """
        INSERT INTO product_rewrite_log
            (product_id, created_at, prompt_title, response_title, prompt_description, response_description)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            product_id,
            datetime.now(timezone.utc).isoformat(),
            prompt_title,
            response_title,
            prompt_description,
            response_description,
        ),
    )


def update_status(
    conn: sqlite3.Connection, post_id: int, status: str, error: Optional[str] = None
) -> None:
    conn.execute(
        """
        INSERT INTO post_status (post_id, status, generated_at, last_error)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(post_id) DO UPDATE SET
            status=excluded.status,
            generated_at=excluded.generated_at,
            last_error=excluded.last_error
        """,
        (
            post_id,
            status,
            datetime.now(timezone.utc).isoformat(),
            error,
        ),
    )


def get_status_map(conn: sqlite3.Connection) -> Dict[int, sqlite3.Row]:
    rows = conn.execute("SELECT * FROM post_status").fetchall()
    return {row["post_id"]: row for row in rows}


def is_canceled(post_id: int) -> bool:
    conn = get_db()
    row = conn.execute(
        "SELECT status FROM post_status WHERE post_id = ?",
        (post_id,),
    ).fetchone()
    conn.close()
    return bool(row and row["status"] == "canceled")


def enqueue_post(post_id: int) -> bool:
    conn = get_db()
    status_row = conn.execute(
        "SELECT status FROM post_status WHERE post_id = ?",
        (post_id,),
    ).fetchone()
    current = status_row["status"] if status_row else ""
    if current in ("queued", "processing"):
        conn.close()
        return False
    with conn:
        update_status(conn, post_id, "queued", None)
    conn.close()
    EXECUTOR.submit(process_post, post_id)
    return True


def process_post(post_id: int) -> None:
    if is_canceled(post_id):
        return
    runtime = get_runtime_config()
    conn = get_db()
    with conn:
        update_status(conn, post_id, "processing", None)
    conn.close()

    try:
        perform_generation(post_id, runtime)
    except Exception as exc:  # noqa: BLE001
        conn = get_db()
        with conn:
            update_status(conn, post_id, "error", str(exc))
        conn.close()


def perform_generation(post_id: int, runtime: Dict[str, Any]) -> None:
    client = WordPressClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    gemini = MultiKeyGemini(runtime)
    conn = get_db()

    post = client.get_post(post_id)
    title = normalize_title(post.get("title", {}).get("rendered", ""))
    current_html = post.get("content", {}).get("rendered", "")
    if not is_empty_content(current_html):
        with conn:
            update_status(conn, post_id, "done", "Skipped: content already exists.")
        conn.close()
        return
    if is_canceled(post_id):
        return

    inbound_links = runtime["inbound_links"] or INBOUND_LINKS
    inbound_links = random.sample(inbound_links, min(2, len(inbound_links)))
    prompt = build_prompt(title, inbound_links, runtime["custom_prompt"])
    response_text = gemini.generate(prompt, conn)
    try:
        data = extract_json(response_text)
    except Exception:
        repair_prompt = build_json_repair_prompt(prompt, response_text)
        response_text = gemini.generate(repair_prompt, conn)
        data = extract_json(response_text)

    content_html = data.get("content_html", "").strip()
    meta_title = data.get("meta_title", "").strip()
    meta_description = data.get("meta_description", "").strip()
    tags = data.get("tags", [])

    if isinstance(tags, str):
        tags = [tag.strip() for tag in tags.split(",") if tag.strip()]

    if not meta_title or not meta_description or not tags:
        metadata_prompt = build_metadata_prompt(title, content_html)
        metadata_response = gemini.generate(metadata_prompt, conn)
        try:
            metadata = extract_json(metadata_response)
        except Exception:
            repair_prompt = build_json_repair_prompt(metadata_prompt, metadata_response)
            metadata_response = gemini.generate(repair_prompt, conn)
            metadata = extract_json(metadata_response)
        meta_title = metadata.get("meta_title", "").strip() or meta_title
        meta_description = (
            metadata.get("meta_description", "").strip() or meta_description
        )
        new_tags = metadata.get("tags", [])
        if isinstance(new_tags, str):
            new_tags = [tag.strip() for tag in new_tags.split(",") if tag.strip()]
        if new_tags:
            tags = new_tags

    if is_canceled(post_id):
        return

    if not content_html:
        raise ValueError("Model returned empty content.")

    tag_ids = []
    for tag in tags:
        tag_name = normalize_tag_name(str(tag))
        if tag_name:
            try:
                tag_id = client.find_or_create_tag(tag_name)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400:
                    continue
                raise
            if tag_id:
                tag_ids.append(tag_id)

    global USE_EXCERPT_FOR_META_DESCRIPTION, META_TITLE_KEY, META_DESCRIPTION_KEY
    USE_EXCERPT_FOR_META_DESCRIPTION = runtime["use_excerpt_for_meta_description"]
    META_TITLE_KEY = runtime["meta_title_key"]
    META_DESCRIPTION_KEY = runtime["meta_description_key"]
    if is_canceled(post_id):
        return
    client.update_post(post_id, content_html, meta_title, meta_description, tag_ids)
    invalidate_posts_cache()

    with conn:
        log_generation(conn, post_id, prompt, response_text, meta_title, meta_description, tags)
        update_status(conn, post_id, "done", None)
    conn.close()


def get_config(conn: sqlite3.Connection) -> Dict[str, str]:
    rows = conn.execute("SELECT key, value FROM app_config").fetchall()
    config = {row["key"]: row["value"] for row in rows}
    if "wp_base_url" not in config:
        config["wp_base_url"] = ENV_WP_BASE_URL
    if "wp_username" not in config:
        config["wp_username"] = ENV_WP_USERNAME
    if "wp_app_password" not in config:
        config["wp_app_password"] = ENV_WP_APP_PASSWORD
    if "gemini_api_key" not in config:
        config["gemini_api_key"] = ENV_GEMINI_API_KEY
    if "gemini_api_keys" not in config and ENV_GEMINI_API_KEYS:
        config["gemini_api_keys"] = ENV_GEMINI_API_KEYS
    if "gemini_model" not in config:
        config["gemini_model"] = ENV_GEMINI_MODEL
    if "gemini_sdk" not in config:
        config["gemini_sdk"] = ENV_GEMINI_SDK
    if "custom_prompt" not in config or not config["custom_prompt"].strip():
        config["custom_prompt"] = ENV_CUSTOM_PROMPT or DEFAULT_CUSTOM_PROMPT
    if "meta_title_key" not in config:
        config["meta_title_key"] = ENV_META_TITLE_KEY
    if "meta_description_key" not in config:
        config["meta_description_key"] = ENV_META_DESCRIPTION_KEY
    if "product_meta_title_key" not in config:
        config["product_meta_title_key"] = ENV_PRODUCT_META_TITLE_KEY
    if "product_meta_description_key" not in config:
        config["product_meta_description_key"] = ENV_PRODUCT_META_DESCRIPTION_KEY
    if "product_focus_keyword_key" not in config:
        config["product_focus_keyword_key"] = ENV_PRODUCT_FOCUS_KEYWORD_KEY
    if "use_excerpt_for_meta_description" not in config:
        config["use_excerpt_for_meta_description"] = (
            "true" if ENV_USE_EXCERPT_FOR_META_DESCRIPTION else "false"
        )
    if "inbound_links" not in config:
        config["inbound_links"] = "\n".join(INBOUND_LINKS)
    if "available_models" not in config:
        config["available_models"] = ""
    if "posts_per_page" not in config:
        config["posts_per_page"] = "10"
    if "max_pages" not in config:
        config["max_pages"] = "30"
    return config


def set_config(conn: sqlite3.Connection, updates: Dict[str, str]) -> None:
    for key, value in updates.items():
        conn.execute(
            """
            INSERT INTO app_config (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )


def parse_links(raw: str) -> List[str]:
    lines = [line.strip() for line in raw.splitlines()]
    return [line for line in lines if line]


def parse_api_keys(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[,\n]", raw)
    return [part.strip() for part in parts if part.strip()]

def read_api_keys_file(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            lines = [line.strip() for line in handle.readlines()]
    except FileNotFoundError:
        return []
    except OSError:
        return []
    keys = []
    for line in lines:
        if not line or line.startswith("#"):
            continue
        keys.append(line)
    return keys


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result

def get_runtime_config() -> Dict[str, Any]:
    conn = get_db()
    config = get_config(conn)
    conn.close()
    file_keys = read_api_keys_file(API_KEYS_PATH)
    gemini_api_keys = parse_api_keys(config.get("gemini_api_keys", ""))
    if not gemini_api_keys and config.get("gemini_api_key"):
        gemini_api_keys = [config.get("gemini_api_key", "").strip()]
    if file_keys:
        gemini_api_keys = dedupe_preserve_order(file_keys + gemini_api_keys)
    return {
        "wp_base_url": config.get("wp_base_url", "").rstrip("/"),
        "wp_username": config.get("wp_username", ""),
        "wp_app_password": config.get("wp_app_password", ""),
        "gemini_api_key": config.get("gemini_api_key", ""),
        "gemini_api_keys": gemini_api_keys,
        "gemini_model": config.get("gemini_model", "gemini-1.5-flash"),
        "gemini_sdk": config.get("gemini_sdk", "auto"),
        "custom_prompt": config.get("custom_prompt", "").strip(),
        "meta_title_key": config.get("meta_title_key", ""),
        "meta_description_key": config.get("meta_description_key", ""),
        "product_meta_title_key": config.get("product_meta_title_key", "").strip(),
        "product_meta_description_key": config.get("product_meta_description_key", "").strip(),
        "product_focus_keyword_key": config.get("product_focus_keyword_key", "").strip(),
        "use_excerpt_for_meta_description": config.get(
            "use_excerpt_for_meta_description", "true"
        ).lower()
        in ("1", "true", "yes"),
        "inbound_links": parse_links(config.get("inbound_links", "")),
        "available_models": parse_links(config.get("available_models", "")),
        "posts_per_page": int(config.get("posts_per_page", "10") or "10"),
        "max_pages": int(config.get("max_pages", "30") or "30"),
    }


def posts_cache_signature(runtime: Dict[str, Any]) -> str:
    return "|".join(
        [
            runtime.get("wp_base_url", ""),
            runtime.get("wp_username", ""),
            str(runtime.get("max_pages", "")),
            str(FETCH_PER_PAGE),
        ]
    )


def invalidate_posts_cache() -> None:
    POST_CACHE["posts"] = []
    POST_CACHE["fetched_at"] = 0.0
    POST_CACHE["signature"] = ""


def get_posts_cached(
    client: WordPressClient, runtime: Dict[str, Any], force: bool = False
) -> List[Dict[str, Any]]:
    signature = posts_cache_signature(runtime)
    now = time.time()
    cache_ok = (
        not force
        and POST_CACHE_TTL > 0
        and POST_CACHE["signature"] == signature
        and POST_CACHE["posts"]
        and (now - float(POST_CACHE["fetched_at"])) < POST_CACHE_TTL
    )
    if cache_ok:
        return POST_CACHE["posts"]

    per_page = min(max(FETCH_PER_PAGE, 1), 100)
    try:
        posts = client.list_all_posts(
            per_page=per_page,
            max_pages=runtime["max_pages"],
        )
    except requests.RequestException:
        if POST_CACHE["posts"]:
            return POST_CACHE["posts"]
        return []
    POST_CACHE["posts"] = posts
    POST_CACHE["fetched_at"] = now
    POST_CACHE["signature"] = signature
    return posts


def build_page_window(current: int, total: int, radius: int = 2) -> List[int]:
    if total <= 1:
        return [1]
    start = max(1, current - radius)
    end = min(total, current + radius)
    return list(range(start, end + 1))


def build_index_context(
    status_filter: str, page: int, force_refresh: bool
) -> Dict[str, Any]:
    runtime = get_runtime_config()
    conn = get_db()
    config = get_config(conn)
    has_wp_password = bool(config.get("wp_app_password"))
    has_gemini_key = bool(config.get("gemini_api_key") or config.get("gemini_api_keys"))
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
        and (runtime["gemini_api_key"] or runtime.get("gemini_api_keys"))
    ):
        posts = []
        conn.close()
        return {
            "posts": posts,
            "counts": {"total": 0, "done": 0, "pending": 0},
            "missing_config": True,
            "config": config,
            "has_wp_password": has_wp_password,
            "has_gemini_key": has_gemini_key,
            "status_filter": status_filter,
            "auto_refresh": False,
            "current_page": page,
            "total_pages": 1,
            "pages": [1],
        }

    client = WordPressClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    posts = get_posts_cached(client, runtime, force=force_refresh)

    status_map = get_status_map(conn)

    decorated = []
    done = 0
    processing = 0
    queued = 0
    errors = 0
    canceled = 0

    for post in posts:
        post_id = post["id"]
        content_html = post.get("content", {}).get("rendered", "")
        empty = is_empty_content(content_html)
        status_row = status_map.get(post_id)

        if status_row:
            status = status_row["status"]
            generated_at = status_row["generated_at"]
            last_error = status_row["last_error"]
        else:
            status = "pending" if empty else "done"
            generated_at = None
            last_error = None

        if last_error and status != "canceled":
            status = "error"

        if status == "done":
            done += 1
        elif status == "processing":
            processing += 1
        elif status == "queued":
            queued += 1
        elif status == "error":
            errors += 1
        elif status == "canceled":
            canceled += 1
        else:
            pass

        item = (
            {
                "id": post_id,
                "title": normalize_title(post.get("title", {}).get("rendered", "")),
                "status": status,
                "generated_at": generated_at,
                "last_error": last_error,
                "link": post.get("link", ""),
                "empty": empty,
            }
        )
        matches = status_filter == "all" or status_filter == status
        if status_filter == "pending":
            matches = status == "pending"
        if matches:
            decorated.append(item)

    conn.close()

    total_items = len(decorated)
    per_page = max(1, runtime["posts_per_page"])
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages
    start = (page - 1) * per_page
    end = start + per_page
    decorated_page = decorated[start:end]

    pending_all = max(0, len(posts) - done)
    counts = {
        "total": len(posts),
        "done": done,
        "pending": pending_all,
        "processing": processing,
        "queued": queued,
        "errors": errors,
        "canceled": canceled,
    }

    return {
        "posts": decorated_page,
        "counts": counts,
        "missing_config": False,
        "status_filter": status_filter,
        "auto_refresh": (processing + queued) > 0,
        "current_page": page,
        "total_pages": total_pages,
        "pages": build_page_window(page, total_pages),
        "config": config,
        "has_wp_password": has_wp_password,
        "has_gemini_key": has_gemini_key,
    }


def clamp_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def remove_free_words(text: str) -> str:
    cleaned = re.sub(r"\bfree\b", "", text or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"\s+[-|:]\s+", " - ", cleaned).strip()
    return cleaned


_WOOCOMMERCE_TYPO_RE = re.compile(
    r"\bwoo\s*com+\s*er(?:ce|se)\b|\bwoocomerce\b|\bwoocommerse\b|\bwoocomerce\b",
    flags=re.IGNORECASE,
)


def normalize_woocommerce_spelling(text: str, *, proper_case: bool = True) -> str:
    """
    Fix common WooCommerce misspellings in human-visible text.
    """
    if not text:
        return ""
    replacement = "WooCommerce" if proper_case else "woocommerce"
    return _WOOCOMMERCE_TYPO_RE.sub(replacement, text)


def slugify(text: str) -> str:
    text = unescape(strip_html(text or ""))
    text = remove_free_words(text)
    text = re.sub(r"\bofficial\b", "", text, flags=re.IGNORECASE)
    text = text.lower()
    text = re.sub(r"['’]", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def generate_product_slug(title: str, max_len: int = 55) -> str:
    raw = slugify(title)
    if not raw:
        return ""
    stop = {
        "premium",
        "download",
        "the",
        "and",
        "or",
        "for",
        "with",
        "templates",
        "template",
        "wordpress",
        "woocommerce",
    }
    parts = [p for p in raw.split("-") if p and p not in stop]
    if not parts:
        parts = [p for p in raw.split("-") if p]
    parts = parts[:6]
    slug = "-".join(parts)
    if len(slug) > max_len:
        slug = slug[:max_len].rstrip("-")
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug


def title_len_ok(title: str) -> bool:
    n = len((title or "").strip())
    return 50 <= n <= 60


def wc_is_membership_product(product: Dict[str, Any]) -> bool:
    cats = product.get("categories") or []
    for cat in cats:
        slug = str(cat.get("slug", "") or "").strip().lower()
        name = str(cat.get("name", "") or "").strip().lower()
        if slug == "membership":
            return True
        if "membership" in name:
            return True
    return False


def wc_meta_has(product: Dict[str, Any], key: str) -> bool:
    meta_data = product.get("meta_data") or []
    for item in meta_data:
        if str(item.get("key", "")) == key:
            return True
    return False


def is_store_related_product_title(title: str) -> bool:
    text = (title or "").lower()
    store_terms = (
        "woocommerce",
        "shop",
        "store",
        "cart",
        "checkout",
        "catalog",
        "product filter",
        "product search",
        "payment",
        "stripe",
        "paypal",
        "inventory",
        "pos",
        "affiliate",
    )
    return any(term in text for term in store_terms)


def ensure_wc_product_meta(
    wc_client: WooCommerceClient,
    wp_client: WordPressClient,
    product_id: int,
    meta: Dict[str, str],
) -> Tuple[bool, str]:
    """
    Ensure meta exists after update. If WC response doesn't persist it, try WP v2 product endpoint.
    """
    if not meta:
        return False, "No meta keys configured."
    try:
        prod = wc_client.get_product(product_id)
    except Exception as exc:  # noqa: BLE001
        return False, f"Meta verify failed (fetch): {exc}"
    missing = [k for k in meta.keys() if not wc_meta_has(prod, k)]
    if not missing:
        return True, ""

    # Fallback: try updating via WP REST product endpoint.
    ok = wp_client.update_product_meta(product_id, meta)
    if not ok:
        return False, f"Meta not saved for keys: {', '.join(missing)}"
    try:
        prod2 = wc_client.get_product(product_id)
    except Exception as exc:  # noqa: BLE001
        return False, f"Meta verify failed (refetch): {exc}"
    missing2 = [k for k in meta.keys() if not wc_meta_has(prod2, k)]
    if missing2:
        return False, f"Meta not saved for keys: {', '.join(missing2)}"
    return True, ""


def build_product_title_prompt(original_title: str, store_related: bool) -> str:
    original_title = clamp_spaces(original_title)
    commerce_rule = (
        '- Only mention "WooCommerce" if the product is clearly store/ecommerce related.'
        if store_related
        else '- Do not mention "WooCommerce" in the title for this product.'
    )
    return f"""
Rewrite this product title for SEO.

Original title: {original_title}

Rules:
- Output MUST be JSON: {{ "title": "..." }}
- 1 title only.
- 50 to 60 characters (strict).
- Remove the word "free" / "Free" (and any similar "free ..." wording).
- Do not use the word "official".
- Do not use "WooCommerce Pro" (WooCommerce does not have a Pro product).
- {commerce_rule}
- If you mention WooCommerce, spell it exactly "WooCommerce" (not "woocomerce").
- Use simple words WordPress developers search on Google.
- You may use: premium, pro, download.
- Keep it natural, professional, not hype.
- Be creative: vary structure and avoid repeating common starter patterns.
- Do not add quotes or emojis.
""".strip()


def build_product_description_prompt(final_title: str) -> str:
    # Deprecated: keep for backward-compat if referenced, but new generation uses build_product_body_prompt().
    final_title = clamp_spaces(final_title)
    return build_product_body_prompt(final_title, 260, 320)


MEMBERSHIP_FOOTER_HTML = (
    "<p>"
    "Quick note: you can visit our blog and search the same assets and download them for free, "
    "but you will need to view some ads. If you do not want ads and you do not want to buy "
    "individually, check out the "
    '<a href="https://gplmama.com/membership/" rel="nofollow">GPLMama membership</a>. '
    "After membership, you get single click downloads for 2200+ assets. "
    "I charge a small amount to help maintain this platform."
    "</p>"
)


def strip_membership_mentions(html: str) -> str:
    if not html:
        return ""
    # Remove any paragraphs that contain the membership URL to avoid duplicates/placeholders.
    html = re.sub(
        r"<p[^>]*>[^<]*https?://gplmama\.com/membership/[^<]*</p>",
        "",
        html,
        flags=re.IGNORECASE,
    )
    html = re.sub(r"\s+", " ", html).strip()
    return html


def ensure_description_heading(html: str, title: str) -> str:
    title = clamp_spaces(title)
    heading = build_description_heading(title)
    if not html:
        return f"<h2>{heading}</h2>"
    trimmed = html.lstrip()
    if re.match(r"^\s*<h2\b", trimmed, flags=re.IGNORECASE):
        return html
    return f"<h2>{heading}</h2>\n{html}"


def finalize_product_description(body_html: str, title: str) -> str:
    body_html = strip_membership_mentions(body_html or "").strip()
    body_html = ensure_description_heading(body_html, title)
    # Always append our footer as the last paragraph, with a real hyperlink.
    if 'href="https://gplmama.com/membership/' not in body_html:
        return f"{body_html}\n{MEMBERSHIP_FOOTER_HTML}"
    return body_html


def build_description_heading(title: str) -> str:
    title = clamp_spaces(title)
    variants = [
        f"Why Use {title} in Your Next WordPress Build?",
        f"Need {title} for a Faster Project Launch?",
        f"Is {title} Worth Using on Client Sites?",
        f"How Can {title} Improve Your WordPress Workflow?",
        f"Thinking About {title} for Your Next Site?",
    ]
    idx = sum(ord(ch) for ch in title) % len(variants)
    return variants[idx]


def build_product_body_prompt(title: str, min_words: int, max_words: int) -> str:
    title = clamp_spaces(title)
    heading = build_description_heading(title)
    min_words = max(120, int(min_words))
    max_words = max(min_words + 20, int(max_words))
    return f"""
Write the MAIN product description body in HTML (do not include any membership/pricing/ads lines).

Title: {title}

Rules:
- Output MUST be JSON: {{ "body_html": "..." }}
- Word count for the body_html ONLY: {min_words} to {max_words} words (strict).
- First line must be: <h2>{heading}</h2>
- Then write short paragraphs and 1 <ul> list with 4 to 6 <li> items (practical use cases).
- Mention it's great for testing/staging, and can be used for client sites at own risk.
- Natural, professional tone like a cool 21-year-old web developer (not salesy).
- Use simple words. No hype, no buzzwords, no AI-cliches.
- Use active voice as much as possible. Avoid passive voice.
- Keep sentences short: aim for 12 to 18 words, avoid sentences over 20 words.
- Avoid complex words. Use plain words developers use.
- Allowed HTML tags only: <h2>, <p>, <ul>, <li>, <strong>.
- Do NOT include any URLs.
- If you mention WooCommerce, spell it exactly "WooCommerce" (not "woocomerce").
""".strip()


def build_product_seo_prompt(product_title: str, description_html: str, store_related: bool) -> str:
    product_title = clamp_spaces(product_title)
    summary = strip_html(description_html or "")
    summary = re.sub(r"\s+", " ", summary).strip()[:800]
    commerce_rule = (
        '- Use "WooCommerce" only if it naturally fits store/ecommerce intent.'
        if store_related
        else '- Do not force "WooCommerce" into meta fields for this product.'
    )
    return f"""
Generate SEO meta title, meta description, and a focus keyword for this product.

Product title: {product_title}
Description summary: {summary}

Rules:
- Output MUST be JSON: {{ "meta_title": "...", "meta_description": "...", "focus_keyword": "..." }}
- meta_title: 50 to 60 characters (strict), no "official", no "free".
- meta_description: 140 to 160 characters (strict), no "official", no "free".
- focus_keyword: 2 to 4 words, lowercase, no brand names, no "free", no "official".
- Do not use "woocommerce pro" anywhere (WooCommerce has no Pro product).
- {commerce_rule}
- Do not misspell WooCommerce (never "woocomerce").
- Use simple words WordPress devs search on Google.
""".strip()


def generate_product_description_html(
    gemini: MultiKeyGemini, conn: sqlite3.Connection, title: str
) -> Tuple[str, str, str]:
    # We always append our own membership footer, so the model only writes the body.
    footer_wc = html_word_count(MEMBERSHIP_FOOTER_HTML)
    min_body = max(180, 300 - footer_wc)
    max_body = max(min_body + 40, 400 - footer_wc)

    prompt = build_product_body_prompt(title, min_body, max_body)
    resp = gemini.generate(prompt, conn)
    try:
        data = extract_json(resp)
    except Exception:
        repair = build_json_repair_prompt(prompt, resp)
        prompt = repair
        resp = gemini.generate(prompt, conn)
        data = extract_json(resp)

    body_html = (data.get("body_html", "") or "").strip()
    description_html = finalize_product_description(body_html, title)

    wc = html_word_count(description_html)
    for _ in range(2):
        if 300 <= wc <= 400 and 'href="https://gplmama.com/membership/' in description_html:
            break
        adjust_prompt = f"""
Your body_html must be adjusted so that after appending a fixed footer, the FINAL description is 300 to 400 words.

Title: {title}
Current final word count: {wc}

Return ONLY valid JSON: {{ "body_html": "..." }}
Rules:
- body_html ONLY, no URLs, no membership text.
- First line must be: <h2>Why you need {title}?</h2>
- Use active voice. Keep sentences under 20 words.
- Keep allowed tags: <h2>, <p>, <ul>, <li>, <strong>.
""".strip()
        prompt = adjust_prompt
        resp = gemini.generate(prompt, conn)
        try:
            data = extract_json(resp)
        except Exception:
            repair = build_json_repair_prompt(prompt, resp)
            prompt = repair
            resp = gemini.generate(prompt, conn)
            data = extract_json(resp)
        body_html = (data.get("body_html", "") or "").strip()
        description_html = finalize_product_description(body_html, title)
        wc = html_word_count(description_html)

    if not description_html:
        raise ValueError("Gemini returned empty product description.")
    if 'href="https://gplmama.com/membership/' not in description_html:
        raise ValueError("Final description is missing membership hyperlink.")
    return description_html, prompt, resp


def html_word_count(html: str) -> int:
    txt = strip_html(html or "")
    parts = [p for p in re.split(r"\s+", txt.strip()) if p]
    return len(parts)


def generate_product_title_and_description(
    gemini: MultiKeyGemini, conn: sqlite3.Connection, original_title: str
) -> Tuple[str, str, str, str, str, str, str, str, str, str, str]:
    store_related = is_store_related_product_title(original_title)
    title_prompt = build_product_title_prompt(original_title, store_related)
    title_resp = gemini.generate(title_prompt, conn)
    try:
        title_data = extract_json(title_resp)
    except Exception:
        repair = build_json_repair_prompt(title_prompt, title_resp)
        title_prompt = repair
        title_resp = gemini.generate(title_prompt, conn)
        title_data = extract_json(title_resp)

    new_title = clamp_spaces(str(title_data.get("title", "") or ""))
    new_title = remove_free_words(new_title)
    new_title = normalize_woocommerce_spelling(new_title, proper_case=True)

    for _ in range(2):
        if (
            title_len_ok(new_title)
            and "official" not in new_title.lower()
            and "woocommerce pro" not in new_title.lower()
            and "woocomerce" not in new_title.lower()
        ):
            break
        revise_prompt = f"""
You must revise the product title to match the rules.

Current title: {new_title}
Character length: {len(new_title)}

Rules:
- Output MUST be JSON: {{ "title": "..." }}
- 50 to 60 characters (strict).
- Do not use the word "official".
- Do not use the word "free" (any casing).
- Do not use "WooCommerce Pro" (WooCommerce does not have a Pro product).
- {"- Only mention WooCommerce when clearly store-related." if store_related else "- Do not mention WooCommerce in this title."}
- If you mention WooCommerce, spell it exactly "WooCommerce" (not "woocomerce").
- Keep it simple and SEO-friendly for WordPress dev searches.
""".strip()
        title_prompt = revise_prompt
        title_resp = gemini.generate(title_prompt, conn)
        try:
            title_data = extract_json(title_resp)
        except Exception:
            repair = build_json_repair_prompt(title_prompt, title_resp)
            title_prompt = repair
            title_resp = gemini.generate(title_prompt, conn)
            title_data = extract_json(title_resp)
        new_title = remove_free_words(clamp_spaces(str(title_data.get("title", "") or "")))
        new_title = normalize_woocommerce_spelling(new_title, proper_case=True)

    description_html, desc_prompt, desc_resp = generate_product_description_html(
        gemini, conn, new_title
    )
    description_html = normalize_woocommerce_spelling(description_html, proper_case=True)

    if not new_title:
        raise ValueError("Gemini returned empty product title.")
    if not description_html:
        raise ValueError("Gemini returned empty product description.")
    meta_title, meta_description, focus_keyword, seo_prompt, seo_resp = generate_product_seo_meta(
        gemini, conn, new_title, description_html, store_related
    )
    return (
        new_title,
        description_html,
        title_prompt,
        title_resp,
        desc_prompt,
        desc_resp,
        meta_title,
        meta_description,
        focus_keyword,
        seo_prompt,
        seo_resp,
    )


def generate_product_seo_meta(
    gemini: MultiKeyGemini,
    conn: sqlite3.Connection,
    product_title: str,
    description_html: str,
    store_related: bool,
) -> Tuple[str, str, str, str, str]:
    def sanitize(text: str, *, proper_case: bool) -> str:
        text = clamp_spaces(text or "")
        text = remove_free_words(text)
        text = re.sub(r"\bofficial\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bwoocommerce\s+pro\b", "WooCommerce", text, flags=re.IGNORECASE)
        text = normalize_woocommerce_spelling(text, proper_case=proper_case)
        return clamp_spaces(text)

    def clamp_to_range(text: str, lo: int, hi: int, pad: str) -> str:
        text = clamp_spaces(text)
        if len(text) > hi:
            cut = text[: hi + 1]
            if " " in cut:
                cut = cut.rsplit(" ", 1)[0]
            text = cut.rstrip(" ,.-")
        if len(text) < lo:
            # Add a short pad phrase, then trim if it overshoots.
            text = clamp_spaces(f"{text} {pad}".strip())
            if len(text) > hi:
                text = text[:hi].rstrip(" ,.-")
        return clamp_spaces(text)

    def derive_focus_keyword(title: str) -> str:
        raw = slugify(title)
        stop = {"premium", "download", "pro", "the", "and", "for", "with", "wordpress", "woocommerce"}
        parts = [p for p in raw.split("-") if p and p not in stop]
        if not parts:
            parts = [p for p in raw.split("-") if p]
        parts = parts[:4]
        kw = " ".join(parts).lower()
        kw = sanitize(kw, proper_case=False).lower()
        # Ensure 2-4 words.
        words = [w for w in kw.split() if w]
        if len(words) < 2:
            words = (words + ["download"])[:2]
        if len(words) > 4:
            words = words[:4]
        return " ".join(words).strip()

    prompt = build_product_seo_prompt(product_title, description_html, store_related)
    resp = ""
    data: Dict[str, Any] = {}
    try:
        resp = gemini.generate(prompt, conn)
        try:
            data = extract_json(resp)
        except Exception:
            repair = build_json_repair_prompt(prompt, resp)
            prompt = repair
            resp = gemini.generate(prompt, conn)
            data = extract_json(resp)
    except Exception:
        # If Gemini fails, we still derive usable meta locally.
        data = {}

    meta_title = sanitize(str(data.get("meta_title", "") or product_title), proper_case=True)
    meta_description = sanitize(str(data.get("meta_description", "") or ""), proper_case=True)
    if not meta_description:
        meta_description = sanitize(strip_html(description_html)[:180], proper_case=True)

    focus_keyword = sanitize(str(data.get("focus_keyword", "") or ""), proper_case=False).lower()
    if not focus_keyword:
        focus_keyword = derive_focus_keyword(product_title)

    meta_title = clamp_to_range(meta_title, 50, 60, pad="Premium Download")
    meta_description = clamp_to_range(
        meta_description, 140, 160, pad="GPL download for WordPress developers."
    )

    focus_keyword = derive_focus_keyword(focus_keyword or product_title)
    if "woocommerce pro" in focus_keyword or "woocomerce" in focus_keyword:
        focus_keyword = derive_focus_keyword(product_title)

    return meta_title, meta_description, focus_keyword, prompt, resp


def bulk_rewrite_products(start_date: str, end_date: str) -> None:
    runtime = get_runtime_config()
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
        and (runtime["gemini_api_key"] or runtime.get("gemini_api_keys"))
    ):
        return

    set_product_bulk_flag("product_bulk_running", "1")
    set_product_bulk_flag("product_bulk_stop", "0")
    set_product_bulk_message("Starting...")
    conn = get_db()
    wc_client = WooCommerceClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    wp_client = WordPressClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    gemini = MultiKeyGemini(runtime)

    try:
        # Use Z (UTC) to avoid WP/WC parsing ambiguities.
        after = f"{start_date}T00:00:00Z"
        before = f"{end_date}T23:59:59Z"

        per_page = 100
        max_pages = 200
        first_batch, total_pages = wc_client.list_products(
            page=1, per_page=per_page, after=after, before=before
        )
        total_pages = min(total_pages, max_pages)

        # Phase 1: discover + queue (so UI shows progress immediately).
        discovered = 0
        queued = 0
        skipped = 0
        set_product_bulk_message(f"Fetching products 1/{total_pages}...")

        def handle_discovered(product: Dict[str, Any]) -> None:
            nonlocal discovered, queued, skipped
            product_id = int(product.get("id") or 0)
            if not product_id:
                return
            discovered += 1

            existing = conn.execute(
                "SELECT status FROM product_rewrite_status WHERE product_id = ?",
                (product_id,),
            ).fetchone()
            if existing and existing["status"] in ("done", "partial", "skipped"):
                return

            old_title = clamp_spaces(product.get("name", "") or "")
            permalink = str(product.get("permalink", "") or "")

            if product_id == 3718:
                with conn:
                    update_product_status(
                        conn,
                        product_id,
                        "skipped",
                        "Skipped: excluded product_id 3718.",
                        old_title=old_title,
                        permalink=permalink,
                    )
                skipped += 1
                return

            if wc_is_membership_product(product):
                with conn:
                    update_product_status(
                        conn,
                        product_id,
                        "skipped",
                        "Skipped: membership category.",
                        old_title=old_title,
                        permalink=permalink,
                    )
                skipped += 1
                return

            with conn:
                update_product_status(
                    conn,
                    product_id,
                    "queued",
                    None,
                    old_title=old_title,
                    permalink=permalink,
                )
            queued += 1

        for product in first_batch:
            if product_bulk_should_stop():
                break
            handle_discovered(product)

        for page in range(2, total_pages + 1):
            if product_bulk_should_stop():
                break
            set_product_bulk_message(f"Fetching products {page}/{total_pages}...")
            batch, _tp = wc_client.list_products(
                page=page, per_page=per_page, after=after, before=before
            )
            for product in batch:
                if product_bulk_should_stop():
                    break
                handle_discovered(product)
            if len(batch) < per_page:
                break

        if product_bulk_should_stop():
            set_product_bulk_message(
                f"Stopped during fetch. Discovered {discovered}, queued {queued}, skipped {skipped}."
            )
            return

        set_product_bulk_message(
            f"Fetched {discovered} products. Queued {queued}, skipped {skipped}. Rewriting..."
        )

        # Phase 2: process queued/error/processing rows in DB order.
        while True:
            if product_bulk_should_stop():
                break

            row = conn.execute(
                """
                SELECT product_id, old_title, permalink
                FROM product_rewrite_status
                WHERE status IN ('queued', 'error', 'processing')
                ORDER BY updated_at ASC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                break

            product_id = int(row["product_id"])
            old_title = row["old_title"] or ""
            permalink = row["permalink"] or ""
            try:
                with conn:
                    update_product_status(
                        conn,
                        product_id,
                        "processing",
                        None,
                        old_title=old_title,
                        permalink=permalink,
                    )

                set_product_bulk_message(f"Rewriting product {product_id}...")
                (
                    new_title,
                    description_html,
                    title_prompt,
                    title_resp,
                    desc_prompt,
                    desc_resp,
                    seo_title,
                    seo_description,
                    focus_keyword,
                    _seo_prompt,
                    _seo_resp,
                ) = (
                    generate_product_title_and_description(gemini, conn, old_title)
                )

                meta: Dict[str, str] = {}
                if runtime.get("product_meta_title_key"):
                    meta[str(runtime["product_meta_title_key"])] = seo_title
                if runtime.get("product_meta_description_key"):
                    meta[str(runtime["product_meta_description_key"])] = seo_description
                if runtime.get("product_focus_keyword_key"):
                    meta[str(runtime["product_focus_keyword_key"])] = focus_keyword
                if not meta:
                    raise ValueError(
                        "Missing product SEO meta keys. Set them in Settings (Yoast defaults use _yoast_wpseo_*)."
                    )

                new_slug = generate_product_slug(new_title)
                if not new_slug:
                    raise ValueError("Could not generate product slug.")

                updated = wc_client.update_product(
                    product_id,
                    name=new_title,
                    description_html=description_html,
                    slug=new_slug,
                    meta=meta or None,
                )
                ok_meta, meta_err = ensure_wc_product_meta(
                    wc_client, wp_client, product_id, meta
                )
                new_permalink = str(updated.get("permalink", "") or permalink)
                with conn:
                    log_product_rewrite(
                        conn,
                        product_id,
                        title_prompt,
                        title_resp,
                        desc_prompt,
                        desc_resp,
                    )
                    update_product_status(
                        conn,
                        product_id,
                        "done",
                        None,
                        old_title=old_title,
                        new_title=new_title,
                        permalink=new_permalink,
                    )
                    update_product_piece_flags(
                        conn,
                        product_id,
                        title_done=1,
                        desc_done=1,
                        last_title_error="",
                        last_desc_error="",
                        seo_done=1 if ok_meta else 0,
                        last_seo_error="" if ok_meta else meta_err,
                        seo_title=seo_title,
                        seo_description=seo_description,
                        seo_focus_keyword=focus_keyword,
                        slug_done=1,
                        last_slug_error="",
                        new_slug=new_slug,
                    )
                    if not ok_meta:
                        update_product_status(
                            conn,
                            product_id,
                            "partial",
                            meta_err,
                            old_title=old_title,
                            new_title=new_title,
                            permalink=new_permalink,
                        )
            except Exception as exc:  # noqa: BLE001
                with conn:
                    update_product_status(
                        conn,
                        product_id,
                        "error",
                        str(exc),
                        old_title=old_title,
                        permalink=permalink,
                    )
                time.sleep(0.5)
                continue
            time.sleep(0.25)
    finally:
        conn.close()
        set_product_bulk_message("Idle.")
        set_product_bulk_flag("product_bulk_running", "0")


def _product_filter_where(status_filter: str) -> Tuple[str, Tuple[Any, ...]]:
    status_filter = (status_filter or "all").strip().lower()
    if status_filter == "all":
        return "", ()
    if status_filter == "skipped":
        return "WHERE status = 'skipped'", ()
    if status_filter == "processing":
        return "WHERE status = 'processing'", ()
    if status_filter == "queued":
        return "WHERE status = 'queued'", ()
    if status_filter == "error":
        return "WHERE status = 'error'", ()
    if status_filter == "done":
        return (
            "WHERE status = 'done' OR (status != 'skipped' AND title_done = 1 AND desc_done = 1 AND seo_done = 1 AND slug_done = 1)",
            (),
        )
    if status_filter == "partial":
        return (
            "WHERE status = 'partial' OR (status != 'skipped' AND status != 'done' "
            "AND NOT (title_done = 1 AND desc_done = 1 AND seo_done = 1 AND slug_done = 1) "
            "AND (title_done = 1 OR desc_done = 1 OR seo_done = 1 OR slug_done = 1))",
            (),
        )
    if status_filter == "pending":
        return (
            "WHERE (status IS NULL OR status = '' OR status = 'pending') AND title_done = 0 AND desc_done = 0 AND seo_done = 0 AND slug_done = 0",
            (),
        )
    # Unknown filter, show all.
    return "", ()


def build_products_context(
    status_filter: str = "all", page: int = 1, per_page: int = 200
) -> Dict[str, Any]:
    page = max(1, int(page or 1))
    per_page = max(10, min(int(per_page or 200), 1000))
    offset = (page - 1) * per_page

    conn = get_db()
    where_sql, where_params = _product_filter_where(status_filter)

    count_row = conn.execute(
        f"SELECT COUNT(*) AS c FROM product_rewrite_status {where_sql}",
        where_params,
    ).fetchone()
    filtered_total = int(count_row["c"] or 0) if count_row else 0
    total_pages = max(1, (filtered_total + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * per_page

    rows = conn.execute(
        f"""
        SELECT product_id, status, updated_at, last_error, old_title, new_title, permalink,
               title_done, desc_done, seo_done, slug_done,
               last_title_error, last_desc_error, last_seo_error, last_slug_error,
               seo_title, seo_description, seo_focus_keyword,
               old_slug, new_slug
        FROM product_rewrite_status
        {where_sql}
        ORDER BY updated_at DESC
        LIMIT ? OFFSET ?
        """,
        where_params + (per_page, offset),
    ).fetchall()

    counts_row = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
          SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing,
          SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
          SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error,
          SUM(CASE WHEN status = 'done' OR (status != 'skipped' AND title_done = 1 AND desc_done = 1 AND seo_done = 1 AND slug_done = 1) THEN 1 ELSE 0 END) AS done,
          SUM(CASE WHEN status != 'skipped'
                    AND status != 'done'
                    AND (status = 'partial' OR (
                        NOT (title_done = 1 AND desc_done = 1 AND seo_done = 1 AND slug_done = 1)
                        AND (title_done = 1 OR desc_done = 1 OR seo_done = 1 OR slug_done = 1)
                    ))
               THEN 1 ELSE 0 END) AS partial,
          SUM(CASE WHEN (status IS NULL OR status = '' OR status = 'pending')
                    AND title_done = 0 AND desc_done = 0 AND seo_done = 0 AND slug_done = 0
               THEN 1 ELSE 0 END) AS pending
        FROM product_rewrite_status
        """
    ).fetchone()

    items = []
    for row in rows:
        status = compute_product_overall_status(row)
        items.append(
            {
                "id": row["product_id"],
                "status": status,
                "updated_at": row["updated_at"],
                "last_error": compact_error_message(row["last_error"]),
                "old_title": row["old_title"],
                "new_title": row["new_title"],
                "permalink": row["permalink"],
                "title_done": int(row["title_done"] or 0),
                "desc_done": int(row["desc_done"] or 0),
                "seo_done": int(row["seo_done"] or 0),
                "slug_done": int(row["slug_done"] or 0),
                "last_title_error": compact_error_message(row["last_title_error"]),
                "last_desc_error": compact_error_message(row["last_desc_error"]),
                "last_seo_error": compact_error_message(row["last_seo_error"]),
                "last_slug_error": compact_error_message(row["last_slug_error"]),
                "seo_title": row["seo_title"],
                "seo_description": row["seo_description"],
                "seo_focus_keyword": row["seo_focus_keyword"],
                "old_slug": row["old_slug"],
                "new_slug": row["new_slug"],
            }
        )

    config = get_config(conn)
    running_row = conn.execute(
        "SELECT value FROM app_config WHERE key = ?",
        ("product_bulk_running",),
    ).fetchone()
    msg_row = conn.execute(
        "SELECT value FROM app_config WHERE key = ?",
        ("product_bulk_message",),
    ).fetchone()
    msg_at_row = conn.execute(
        "SELECT value FROM app_config WHERE key = ?",
        ("product_bulk_message_at",),
    ).fetchone()
    conn.close()

    running = bool(running_row and (running_row["value"] or "").strip() in ("1", "true", "yes"))
    counts = {
        "total": int(counts_row["total"] or 0) if counts_row else 0,
        "pending": int(counts_row["pending"] or 0) if counts_row else 0,
        "queued": int(counts_row["queued"] or 0) if counts_row else 0,
        "processing": int(counts_row["processing"] or 0) if counts_row else 0,
        "partial": int(counts_row["partial"] or 0) if counts_row else 0,
        "done": int(counts_row["done"] or 0) if counts_row else 0,
        "skipped": int(counts_row["skipped"] or 0) if counts_row else 0,
        "error": int(counts_row["error"] or 0) if counts_row else 0,
    }

    return {
        "items": items,
        "counts": counts,
        "running": running,
        "message": (msg_row["value"] if msg_row else "") or "",
        "message_at": (msg_at_row["value"] if msg_at_row else "") or "",
        "default_start": "2026-02-04",
        "default_end": "2026-02-08",
        "status_filter": (status_filter or "all").strip().lower(),
        "current_page": page,
        "total_pages": total_pages,
        "per_page": per_page,
        "filtered_total": filtered_total,
    }


def process_single_product(product_id: int, mode: str) -> None:
    mode = (mode or "both").strip().lower()
    if mode not in ("title", "description", "both"):
        mode = "both"

    runtime = get_runtime_config()
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
        and (runtime["gemini_api_key"] or runtime.get("gemini_api_keys"))
    ):
        return

    wc_client = WooCommerceClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    wp_client = WordPressClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    gemini = MultiKeyGemini(runtime)
    conn = get_db()
    try:
        product = wc_client.get_product(product_id)
        if product_id == 3718 or wc_is_membership_product(product):
            reason = (
                "Skipped: excluded product_id 3718."
                if product_id == 3718
                else "Skipped: membership category."
            )
            with conn:
                update_product_status(
                    conn,
                    product_id,
                    "skipped",
                    reason,
                    old_title=clamp_spaces(product.get("name", "") or ""),
                    permalink=str(product.get("permalink", "") or ""),
                )
            return

        old_title = clamp_spaces(product.get("name", "") or "")
        store_related = is_store_related_product_title(old_title)
        permalink = str(product.get("permalink", "") or "")
        current_desc = str(product.get("description", "") or "")
        old_slug = clamp_spaces(product.get("slug", "") or "")

        with conn:
            update_product_status(
                conn,
                product_id,
                "processing",
                None,
                old_title=old_title,
                permalink=permalink,
            )
            update_product_piece_flags(conn, product_id, old_slug=old_slug)

        if mode == "both":
            set_product_bulk_message(f"Rewriting product {product_id} (title+desc)...")
            (
                new_title,
                description_html,
                title_prompt,
                title_resp,
                desc_prompt,
                desc_resp,
                seo_title,
                seo_description,
                focus_keyword,
                _seo_prompt,
                _seo_resp,
            ) = generate_product_title_and_description(gemini, conn, old_title)

            meta: Dict[str, str] = {}
            if runtime.get("product_meta_title_key"):
                meta[str(runtime["product_meta_title_key"])] = seo_title
            if runtime.get("product_meta_description_key"):
                meta[str(runtime["product_meta_description_key"])] = seo_description
            if runtime.get("product_focus_keyword_key"):
                meta[str(runtime["product_focus_keyword_key"])] = focus_keyword
            if not meta:
                raise ValueError(
                    "Missing product SEO meta keys. Set them in Settings (Yoast defaults use _yoast_wpseo_*)."
                )

            new_slug = generate_product_slug(new_title)
            if not new_slug:
                raise ValueError("Could not generate product slug.")

            updated = wc_client.update_product(
                product_id,
                name=new_title,
                description_html=description_html,
                slug=new_slug,
                meta=meta or None,
            )
            ok_meta, meta_err = ensure_wc_product_meta(
                wc_client, wp_client, product_id, meta
            )
            new_permalink = str(updated.get("permalink", "") or permalink)
            with conn:
                log_product_rewrite(
                    conn,
                    product_id,
                    title_prompt,
                    title_resp,
                    desc_prompt,
                    desc_resp,
                )
                update_product_status(
                    conn,
                    product_id,
                    "done",
                    None,
                    old_title=old_title,
                    new_title=new_title,
                    permalink=new_permalink,
                )
                update_product_piece_flags(
                    conn,
                    product_id,
                    title_done=1,
                    desc_done=1,
                    last_title_error="",
                    last_desc_error="",
                    seo_done=1 if ok_meta else 0,
                    last_seo_error="" if ok_meta else meta_err,
                    seo_title=seo_title,
                    seo_description=seo_description,
                    seo_focus_keyword=focus_keyword,
                    slug_done=1,
                    last_slug_error="",
                    old_slug=old_slug,
                    new_slug=new_slug,
                )
                if not ok_meta:
                    update_product_status(
                        conn,
                        product_id,
                        "partial",
                        meta_err,
                        old_title=old_title,
                        new_title=new_title,
                        permalink=new_permalink,
                    )
            return

        if mode == "title":
            set_product_bulk_message(f"Rewriting product {product_id} (title)...")
            title_prompt = build_product_title_prompt(old_title)
            title_resp = gemini.generate(title_prompt, conn)
            try:
                title_data = extract_json(title_resp)
            except Exception:
                repair = build_json_repair_prompt(title_prompt, title_resp)
                title_prompt = repair
                title_resp = gemini.generate(title_prompt, conn)
                title_data = extract_json(title_resp)
            new_title = remove_free_words(clamp_spaces(str(title_data.get("title", "") or "")))
            if not title_len_ok(new_title) or "official" in new_title.lower():
                raise ValueError("Generated title did not match rules (50-60 chars, no official).")

            seo_title, seo_description, focus_keyword, _seo_prompt, _seo_resp = generate_product_seo_meta(
                gemini, conn, new_title, current_desc, store_related
            )
            meta: Dict[str, str] = {}
            if runtime.get("product_meta_title_key"):
                meta[str(runtime["product_meta_title_key"])] = seo_title
            if runtime.get("product_meta_description_key"):
                meta[str(runtime["product_meta_description_key"])] = seo_description
            if runtime.get("product_focus_keyword_key"):
                meta[str(runtime["product_focus_keyword_key"])] = focus_keyword
            if not meta:
                raise ValueError(
                    "Missing product SEO meta keys. Set them in Settings (Yoast defaults use _yoast_wpseo_*)."
                )

            new_slug = generate_product_slug(new_title)
            if not new_slug:
                raise ValueError("Could not generate product slug.")

            updated = wc_client.update_product(
                product_id,
                name=new_title,
                description_html=current_desc,
                slug=new_slug,
                meta=meta or None,
            )
            ok_meta, meta_err = ensure_wc_product_meta(
                wc_client, wp_client, product_id, meta
            )
            new_permalink = str(updated.get("permalink", "") or permalink)
            with conn:
                update_product_status(
                    conn,
                    product_id,
                    "partial",
                    None,
                    old_title=old_title,
                    new_title=new_title,
                    permalink=new_permalink,
                )
                update_product_piece_flags(
                    conn,
                    product_id,
                    title_done=1,
                    last_title_error="",
                    seo_done=1 if ok_meta else 0,
                    last_seo_error="" if ok_meta else meta_err,
                    seo_title=seo_title,
                    seo_description=seo_description,
                    seo_focus_keyword=focus_keyword,
                    slug_done=1,
                    last_slug_error="",
                    old_slug=old_slug,
                    new_slug=new_slug,
                )
                if not ok_meta:
                    update_product_status(
                        conn,
                        product_id,
                        "partial",
                        meta_err,
                        old_title=old_title,
                        new_title=new_title,
                        permalink=new_permalink,
                    )
            return

        # description
        set_product_bulk_message(f"Rewriting product {product_id} (description)...")
        description_html, desc_prompt, desc_resp = generate_product_description_html(
            gemini, conn, old_title
        )
        seo_title, seo_description, focus_keyword, _seo_prompt, _seo_resp = generate_product_seo_meta(
            gemini, conn, old_title, description_html, store_related
        )
        meta: Dict[str, str] = {}
        if runtime.get("product_meta_title_key"):
            meta[str(runtime["product_meta_title_key"])] = seo_title
        if runtime.get("product_meta_description_key"):
            meta[str(runtime["product_meta_description_key"])] = seo_description
        if runtime.get("product_focus_keyword_key"):
            meta[str(runtime["product_focus_keyword_key"])] = focus_keyword
        if not meta:
            raise ValueError(
                "Missing product SEO meta keys. Set them in Settings (Yoast defaults use _yoast_wpseo_*)."
            )

        new_slug = generate_product_slug(old_title)
        if not new_slug:
            raise ValueError("Could not generate product slug.")

        updated = wc_client.update_product(
            product_id,
            name=old_title,
            description_html=description_html,
            slug=new_slug,
            meta=meta or None,
        )
        ok_meta, meta_err = ensure_wc_product_meta(
            wc_client, wp_client, product_id, meta
        )
        new_permalink = str(updated.get("permalink", "") or permalink)
        with conn:
            update_product_status(
                conn,
                product_id,
                "partial",
                None,
                old_title=old_title,
                new_title=str(updated.get("name", "") or old_title),
                permalink=new_permalink,
            )
            update_product_piece_flags(
                conn,
                product_id,
                desc_done=1,
                last_desc_error="",
                seo_done=1 if ok_meta else 0,
                last_seo_error="" if ok_meta else meta_err,
                seo_title=seo_title,
                seo_description=seo_description,
                seo_focus_keyword=focus_keyword,
                slug_done=1,
                last_slug_error="",
                old_slug=old_slug,
                new_slug=new_slug,
            )
            if not ok_meta:
                update_product_status(
                    conn,
                    product_id,
                    "partial",
                    meta_err,
                    old_title=old_title,
                    new_title=str(updated.get("name", "") or old_title),
                    permalink=new_permalink,
                )
    except Exception as exc:  # noqa: BLE001
        with conn:
            update_product_status(conn, product_id, "error", str(exc))
            if mode in ("title", "both"):
                update_product_piece_flags(conn, product_id, last_title_error=str(exc))
            if mode in ("description", "both"):
                update_product_piece_flags(conn, product_id, last_desc_error=str(exc))
            update_product_piece_flags(conn, product_id, last_seo_error=str(exc))
    finally:
        conn.close()


@app.route("/")
def index() -> str:
    status_filter = request.args.get("status", "all")
    try:
        page = int(request.args.get("page", "1") or "1")
    except ValueError:
        page = 1
    page = max(1, page)
    force_refresh = request.args.get("refresh") == "1"
    context = build_index_context(status_filter, page, force_refresh)
    if context.get("missing_config"):
        flash("Missing configuration. Open Settings to finish setup.")
    return render_template("index.html", **context)


@app.route("/data")
def data() -> Any:
    status_filter = request.args.get("status", "all")
    try:
        page = int(request.args.get("page", "1") or "1")
    except ValueError:
        page = 1
    page = max(1, page)
    force_refresh = request.args.get("refresh") == "1"
    context = build_index_context(status_filter, page, force_refresh)
    return jsonify(
        {
            "posts": context["posts"],
            "counts": context["counts"],
            "current_page": context["current_page"],
            "total_pages": context["total_pages"],
            "pages": context["pages"],
            "status_filter": context["status_filter"],
            "auto_refresh": context["auto_refresh"],
        }
    )


@app.route("/generate/<int:post_id>", methods=["POST"])
def generate(post_id: int) -> str:
    runtime = get_runtime_config()
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
        and (runtime["gemini_api_key"] or runtime.get("gemini_api_keys"))
    ):
        flash("Missing configuration. Open Settings to finish setup.")
        return redirect_back()

    try:
        queued = enqueue_post(post_id)
        if queued:
            flash("Post queued for generation.")
        else:
            flash("Post is already in queue or processing.")
    except Exception as exc:  # noqa: BLE001
        flash(f"Queue failed: {exc}")

    return redirect_back()


@app.route("/cancel/<int:post_id>", methods=["POST"])
def cancel(post_id: int) -> str:
    conn = get_db()
    with conn:
        update_status(conn, post_id, "canceled", "Canceled by user.")
    conn.close()
    flash("Canceled generation for this post.")
    return redirect_back()


@app.route("/bulk-generate", methods=["POST"])
def bulk_generate() -> str:
    ids = request.form.getlist("post_ids")
    if not ids:
        flash("No posts selected.")
        return redirect_back()

    queued_count = 0
    for post_id in ids:
        try:
            if enqueue_post(int(post_id)):
                queued_count += 1
        except Exception:
            continue

    flash(f"Queued {queued_count} posts.")
    return redirect_back()


@app.route("/logs/<int:post_id>")
def logs(post_id: int) -> str:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT created_at, meta_title, meta_description, tags
        FROM generation_log
        WHERE post_id = ?
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (post_id,),
    ).fetchall()
    conn.close()

    next_url = request.args.get("next", "")
    return render_template(
        "logs.html",
        post_id=post_id,
        logs=rows,
        next_url=safe_next_url(next_url, url_for("index")),
    )


@app.route("/logs/<int:post_id>/data")
def logs_data(post_id: int) -> Any:
    conn = get_db()
    rows = conn.execute(
        """
        SELECT created_at, meta_title, meta_description, tags
        FROM generation_log
        WHERE post_id = ?
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (post_id,),
    ).fetchall()
    conn.close()
    data = []
    for row in rows:
        data.append(
            {
                "created_at": row["created_at"],
                "meta_title": row["meta_title"] or "-",
                "meta_description": row["meta_description"] or "-",
                "tags": row["tags"] or "-",
            }
        )
    return jsonify({"post_id": post_id, "logs": data})


@app.route("/settings", methods=["GET", "POST"])
def settings() -> str:
    conn = get_db()
    config = get_config(conn)
    has_wp_password = bool(config.get("wp_app_password"))
    has_gemini_key = bool(config.get("gemini_api_key") or config.get("gemini_api_keys"))
    next_url = request.values.get("next", "")

    if request.method == "POST":
        wp_base_url = request.form.get("wp_base_url", "").strip().rstrip("/")
        wp_username = request.form.get("wp_username", "").strip()
        wp_app_password = request.form.get("wp_app_password", "").strip()
        gemini_api_key = request.form.get("gemini_api_key", "").strip()
        gemini_model = request.form.get("gemini_model", "").strip()
        meta_title_key = request.form.get("meta_title_key", "").strip()
        meta_description_key = request.form.get("meta_description_key", "").strip()
        product_meta_title_key = request.form.get("product_meta_title_key", "").strip()
        product_meta_description_key = request.form.get(
            "product_meta_description_key", ""
        ).strip()
        product_focus_keyword_key = request.form.get("product_focus_keyword_key", "").strip()
        use_excerpt = "true" if request.form.get("use_excerpt") == "on" else "false"
        custom_prompt = request.form.get("custom_prompt", "").strip()
        inbound_links = request.form.get("inbound_links", "").strip()

        updates: Dict[str, str] = {
            "wp_base_url": wp_base_url or config.get("wp_base_url", ""),
            "wp_username": wp_username or config.get("wp_username", ""),
            "gemini_model": gemini_model or config.get("gemini_model", "gemini-1.5-flash"),
            "meta_title_key": meta_title_key,
            "meta_description_key": meta_description_key,
            "product_meta_title_key": product_meta_title_key or config.get("product_meta_title_key", ""),
            "product_meta_description_key": product_meta_description_key or config.get("product_meta_description_key", ""),
            "product_focus_keyword_key": product_focus_keyword_key or config.get("product_focus_keyword_key", ""),
            "use_excerpt_for_meta_description": use_excerpt,
            "custom_prompt": custom_prompt or DEFAULT_CUSTOM_PROMPT,
            "inbound_links": inbound_links,
            "posts_per_page": request.form.get("posts_per_page", "50").strip() or "50",
            "max_pages": request.form.get("max_pages", "10").strip() or "10",
        }

        if wp_app_password:
            updates["wp_app_password"] = wp_app_password
        if gemini_api_key:
            updates["gemini_api_key"] = gemini_api_key

        with conn:
            set_config(conn, updates)

        flash("Settings saved.")
        conn.close()
        return redirect_back("settings")

    conn.close()
    return render_template(
        "settings.html",
        config=config,
        has_wp_password=has_wp_password,
        has_gemini_key=has_gemini_key,
        next_url=next_url,
    )


@app.route("/products")
def products() -> str:
    runtime = get_runtime_config()
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
        and (runtime["gemini_api_key"] or runtime.get("gemini_api_keys"))
    ):
        flash("Missing configuration. Open Settings to finish setup.")
    status_filter = request.args.get("status", "all")
    try:
        page = int(request.args.get("page", "1") or "1")
    except ValueError:
        page = 1
    try:
        per_page = int(request.args.get("per_page", "200") or "200")
    except ValueError:
        per_page = 200
    context = build_products_context(status_filter=status_filter, page=page, per_page=per_page)
    return render_template("products.html", **context)


@app.route("/products/sync", methods=["POST"])
def products_sync() -> str:
    start_date = request.form.get("start_date", "").strip() or "2026-02-04"
    end_date = request.form.get("end_date", "").strip() or "2026-02-08"
    try:
        datetime.fromisoformat(start_date)
        datetime.fromisoformat(end_date)
    except ValueError:
        flash("Invalid date format. Use YYYY-MM-DD.")
        return redirect_back("products")

    EXECUTOR.submit(sync_products_to_db, start_date, end_date)
    flash(f"Sync started for {start_date} to {end_date}.")
    return redirect_back("products")


@app.route("/products/do/<int:product_id>", methods=["POST"])
def products_do(product_id: int) -> str:
    mode = request.form.get("mode", "both").strip().lower()

    conn = get_db()
    row = conn.execute(
        "SELECT status FROM product_rewrite_status WHERE product_id = ?",
        (product_id,),
    ).fetchone()
    with conn:
        if not row:
            update_product_status(conn, product_id, "queued", None)
        else:
            if (row["status"] or "") != "skipped":
                update_product_status(conn, product_id, "queued", None)
    conn.close()

    EXECUTOR.submit(process_single_product, product_id, mode)
    flash(f"Queued product {product_id} for: {mode}.")
    return redirect_back("products")


@app.route("/products/bulk-do", methods=["POST"])
def products_bulk_do() -> str:
    mode = request.form.get("mode", "both").strip().lower()
    ids = request.form.getlist("product_ids")
    if not ids:
        flash("No products selected.")
        return redirect_back("products")

    queued = 0
    conn = get_db()
    with conn:
        for raw in ids:
            try:
                pid = int(raw)
            except ValueError:
                continue
            row = conn.execute(
                "SELECT status FROM product_rewrite_status WHERE product_id = ?",
                (pid,),
            ).fetchone()
            if row and (row["status"] or "") == "skipped":
                continue
            update_product_status(conn, pid, "queued", None)
            EXECUTOR.submit(process_single_product, pid, mode)
            queued += 1
    conn.close()

    flash(f"Queued {queued} products for: {mode}.")
    return redirect_back("products")


@app.route("/products/start", methods=["POST"])
def products_start() -> str:
    start_date = request.form.get("start_date", "").strip() or "2026-02-04"
    end_date = request.form.get("end_date", "").strip() or "2026-02-08"
    try:
        datetime.fromisoformat(start_date)
        datetime.fromisoformat(end_date)
    except ValueError:
        flash("Invalid date format. Use YYYY-MM-DD.")
        return redirect_back("products")

    ctx = build_products_context()
    if ctx.get("running"):
        flash("A product rewrite job is already running. Click Stop or wait for it to finish.")
        return redirect_back("products")

    EXECUTOR.submit(bulk_rewrite_products, start_date, end_date)
    flash(f"Started product rewrite job for {start_date} to {end_date}.")
    return redirect_back("products")


@app.route("/products/stop", methods=["POST"])
def products_stop() -> str:
    set_product_bulk_flag("product_bulk_stop", "1")
    flash("Stop requested. The current product will finish, then the job will stop.")
    return redirect_back("products")


@app.route("/fetch-models", methods=["POST"])
def fetch_models() -> str:
    runtime = get_runtime_config()
    if not (runtime["gemini_api_key"] or runtime.get("gemini_api_keys")):
        flash("Gemini API key is missing.")
        return redirect_back("settings")

    try:
        gemini = MultiKeyGemini(runtime)
        models = gemini.list_models()
    except Exception as exc:  # noqa: BLE001
        flash(f"Model fetch failed: {exc}")
        return redirect_back("settings")

    picked = choose_default_model(models)
    conn = get_db()
    with conn:
        updates = {"available_models": "\n".join(models)}
        if picked:
            updates["gemini_model"] = picked
        set_config(conn, updates)
    conn.close()

    if picked:
        flash(f"Found {len(models)} models. Selected {picked}.")
    else:
        flash(f"Found {len(models)} models.")
    return redirect_back("settings")


@app.route("/test-connection", methods=["POST"])
def test_connection() -> str:
    runtime = get_runtime_config()
    wp_ok = False
    gemini_ok = False
    wp_error = ""
    gemini_error = ""

    if runtime["wp_base_url"] and runtime["wp_username"] and runtime["wp_app_password"]:
        try:
            client = WordPressClient(
                runtime["wp_base_url"],
                runtime["wp_username"],
                runtime["wp_app_password"],
            )
            client.ping()
            wp_ok = True
        except Exception as exc:  # noqa: BLE001
            wp_error = str(exc)

    if runtime["gemini_api_key"] or runtime.get("gemini_api_keys"):
        try:
            gemini = MultiKeyGemini(runtime)
            gemini.generate("Return the word OK only.")
            gemini_ok = True
        except Exception as exc:  # noqa: BLE001
            gemini_error = str(exc)

    if wp_ok:
        flash("WordPress connection ok.")
    else:
        flash(f"WordPress connection failed: {wp_error or 'Missing credentials.'}")

    if gemini_ok:
        flash("Gemini connection ok.")
    else:
        flash(f"Gemini connection failed: {gemini_error or 'Missing API key.'}")

    return redirect_back("settings")


if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug_mode, use_reloader=False, threaded=True)
