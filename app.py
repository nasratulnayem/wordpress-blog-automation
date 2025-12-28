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

import google.generativeai as genai
import requests
from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

load_dotenv()

ENV_WP_BASE_URL = os.getenv("WP_BASE_URL", "").rstrip("/")
ENV_WP_USERNAME = os.getenv("WP_USERNAME", "")
ENV_WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
ENV_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
ENV_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash-latest")
ENV_CUSTOM_PROMPT = os.getenv("CUSTOM_PROMPT", "").strip()

ENV_META_TITLE_KEY = os.getenv("META_TITLE_KEY", "yoast_wpseo_title")
ENV_META_DESCRIPTION_KEY = os.getenv("META_DESCRIPTION_KEY", "yoast_wpseo_metadesc")
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
About GPLMama (site context):
GPLMama is Bangladesh's affordable hub for premium digital assets. We offer lifetime access to 2,200+ assets for a one-time fee of BDT 149 and give 5 free downloads to new users. The library includes popular WordPress themes, plugins, WooCommerce plugins, and Shopify themes. Files are original and unmodified. New files are added regularly. The GPL model is legal and safe. We support freelancers, small businesses, and agencies. There is a commercial license for client work and resale. Local support and video guides are available.

When relevant, weave this context into the post naturally and professionally. Do not oversell. Keep claims factual and grounded.
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


def redirect_back(default: str = "index") -> str:
    fallback = url_for(default)
    return redirect(request.referrer or fallback)


def safe_next_url(next_url: str, fallback: str) -> str:
    if next_url and next_url.startswith("/"):
        return next_url
    return fallback


class GeminiClient:
    def __init__(self, api_key: str, model_name: str) -> None:
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)

    def generate(self, prompt: str) -> str:
        response = self.model.generate_content(prompt)
        return response.text or ""

    def list_models(self) -> List[str]:
        models = []
        for model in genai.list_models():
            if "generateContent" in model.supported_generation_methods:
                models.append(model.name)
        return models


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
    custom_text = ""
    if custom_prompt:
        custom_text = (
            f"\nAdditional instructions:\n{custom_prompt.replace('{title}', title)}\n"
        )

    return f"""
You are writing a professional blog post.

Topic: {title}

Rules:
- 1900 to 2200 words.
- Use plain language and short sentences.
- No hype, no buzzwords, no clichés.
- No special characters or emojis.
- Do not use headings like Conclusion or Final thoughts.
- Start with a short intro paragraph, not a title heading.
- Do not include any H1 heading.
- Keep tone natural, helpful, and human.
- Stay on topic and do not add unrelated content.
- Use clear H2 and H3 structure.
- Show evidence of research with specific, accurate details about the topic.
- Make the content valuable and practical for the reader.
- Make my role clear in the narrative (the brand/operator behind the site) without overpromising.
- Ensure each post is distinct in structure and examples, even with similar topics.
- Use lists where helpful.
- Do not show raw URLs as text. Use anchor text for all links.

Internal workflow:
- Derive 10 questions a writer would ask to make this unique.
- Answer those questions inside the post content without showing them.

Links:
- Include exactly 4 outbound links to reputable, live sources.
- Include exactly 2 inbound links from this list:
{inbound_text}

SEO:
- Provide a meta title (50-60 chars).
- Provide a meta description (140-160 chars).
- Provide 5 to 8 tags.

Output format:
Return only valid JSON with these keys:
- content_html
- meta_title
- meta_description
- tags (array of strings)
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
    gemini = GeminiClient(runtime["gemini_api_key"], runtime["gemini_model"])
    conn = get_db()

    post = client.get_post(post_id)
    title = normalize_title(post.get("title", {}).get("rendered", ""))
    if is_canceled(post_id):
        return

    inbound_links = runtime["inbound_links"] or INBOUND_LINKS
    inbound_links = random.sample(inbound_links, min(2, len(inbound_links)))
    prompt = build_prompt(title, inbound_links, runtime["custom_prompt"])
    try:
        response_text = gemini.generate(prompt)
    except Exception as exc:  # noqa: BLE001
        if "models/" in str(exc) and "not found" in str(exc):
            models = gemini.list_models()
            picked = choose_default_model(models)
            if picked:
                runtime["gemini_model"] = picked
                conn.execute(
                    """
                    INSERT INTO app_config (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    ("gemini_model", picked),
                )
                gemini = GeminiClient(runtime["gemini_api_key"], picked)
                response_text = gemini.generate(prompt)
            else:
                raise
        else:
            raise
    try:
        data = extract_json(response_text)
    except Exception:
        repair_prompt = build_json_repair_prompt(prompt, response_text)
        response_text = gemini.generate(repair_prompt)
        data = extract_json(response_text)

    content_html = data.get("content_html", "").strip()
    meta_title = data.get("meta_title", "").strip()
    meta_description = data.get("meta_description", "").strip()
    tags = data.get("tags", [])

    if isinstance(tags, str):
        tags = [tag.strip() for tag in tags.split(",") if tag.strip()]

    if not meta_title or not meta_description or not tags:
        metadata_prompt = build_metadata_prompt(title, content_html)
        metadata_response = gemini.generate(metadata_prompt)
        try:
            metadata = extract_json(metadata_response)
        except Exception:
            repair_prompt = build_json_repair_prompt(metadata_prompt, metadata_response)
            metadata_response = gemini.generate(repair_prompt)
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
    if "gemini_model" not in config:
        config["gemini_model"] = ENV_GEMINI_MODEL
    if "custom_prompt" not in config or not config["custom_prompt"].strip():
        config["custom_prompt"] = ENV_CUSTOM_PROMPT or DEFAULT_CUSTOM_PROMPT
    if "meta_title_key" not in config:
        config["meta_title_key"] = ENV_META_TITLE_KEY
    if "meta_description_key" not in config:
        config["meta_description_key"] = ENV_META_DESCRIPTION_KEY
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


def get_runtime_config() -> Dict[str, Any]:
    conn = get_db()
    config = get_config(conn)
    conn.close()
    return {
        "wp_base_url": config.get("wp_base_url", "").rstrip("/"),
        "wp_username": config.get("wp_username", ""),
        "wp_app_password": config.get("wp_app_password", ""),
        "gemini_api_key": config.get("gemini_api_key", ""),
        "gemini_model": config.get("gemini_model", "gemini-1.5-flash"),
        "custom_prompt": config.get("custom_prompt", "").strip(),
        "meta_title_key": config.get("meta_title_key", ""),
        "meta_description_key": config.get("meta_description_key", ""),
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
    has_gemini_key = bool(config.get("gemini_api_key"))
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
        and runtime["gemini_api_key"]
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
        and runtime["gemini_api_key"]
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
    has_gemini_key = bool(config.get("gemini_api_key"))
    next_url = request.values.get("next", "")

    if request.method == "POST":
        wp_base_url = request.form.get("wp_base_url", "").strip().rstrip("/")
        wp_username = request.form.get("wp_username", "").strip()
        wp_app_password = request.form.get("wp_app_password", "").strip()
        gemini_api_key = request.form.get("gemini_api_key", "").strip()
        gemini_model = request.form.get("gemini_model", "").strip()
        meta_title_key = request.form.get("meta_title_key", "").strip()
        meta_description_key = request.form.get("meta_description_key", "").strip()
        use_excerpt = "true" if request.form.get("use_excerpt") == "on" else "false"
        custom_prompt = request.form.get("custom_prompt", "").strip()
        inbound_links = request.form.get("inbound_links", "").strip()

        updates: Dict[str, str] = {
            "wp_base_url": wp_base_url or config.get("wp_base_url", ""),
            "wp_username": wp_username or config.get("wp_username", ""),
            "gemini_model": gemini_model or config.get("gemini_model", "gemini-1.5-flash"),
            "meta_title_key": meta_title_key,
            "meta_description_key": meta_description_key,
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


@app.route("/fetch-models", methods=["POST"])
def fetch_models() -> str:
    runtime = get_runtime_config()
    if not runtime["gemini_api_key"]:
        flash("Gemini API key is missing.")
        return redirect_back("settings")

    try:
        gemini = GeminiClient(runtime["gemini_api_key"], runtime["gemini_model"])
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

    if runtime["gemini_api_key"]:
        try:
            gemini = GeminiClient(runtime["gemini_api_key"], runtime["gemini_model"])
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
