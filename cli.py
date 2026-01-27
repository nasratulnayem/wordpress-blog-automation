import random
import sys
import time
from typing import Any, Dict, List

import requests

from app import (
    WordPressClient,
    get_db,
    get_runtime_config,
    get_status_map,
    init_db,
    is_empty_content,
    normalize_title,
    perform_generation,
    update_status,
)


def fetch_all_posts(client: WordPressClient, max_pages: int, per_page: int) -> List[Dict[str, Any]]:
    all_posts: List[Dict[str, Any]] = []
    page = 1
    total_pages = max_pages
    posts, total_pages, _ = client.list_posts_page(page=page, per_page=per_page)
    all_posts.extend(posts)
    if not posts:
        return all_posts
    page += 1
    while page <= total_pages:
        posts, _, _ = client.list_posts_page(page=page, per_page=per_page)
        all_posts.extend(posts)
        if not posts:
            break
        page += 1
    return all_posts


def update_post_status(post_id: int, status: str, error: str | None) -> None:
    conn = get_db()
    with conn:
        update_status(conn, post_id, status, error)
    conn.close()


def print_banner() -> None:
    print("=" * 78)
    print(":: AUTO BLOG POST GENERATOR :: TERMINAL MODE ::")
    print(":: TARGET: EMPTY POSTS ONLY ::")
    print("=" * 78)


def print_counts(total: int, filled: int, empty: int, skipped_done: int) -> None:
    pending = max(0, empty - skipped_done)
    print(f"TOTAL      : {total}")
    print(f"WITH TEXT  : {filled}")
    print(f"EMPTY      : {empty}")
    print(f"SKIPPED    : {skipped_done}")
    print(f"QUEUED     : {pending}")
    print("-" * 78)


def should_skip_done(status_map: Dict[int, Any], post_id: int) -> bool:
    row = status_map.get(post_id)
    if not row:
        return False
    return row["status"] == "done"


def main() -> int:
    init_db()
    runtime = get_runtime_config()
    if not (
        runtime["wp_base_url"]
        and runtime["wp_username"]
        and runtime["wp_app_password"]
        and (runtime["gemini_api_key"] or runtime.get("gemini_api_keys"))
    ):
        print("Missing configuration. Set WordPress and Gemini credentials.")
        return 1

    max_pages = int(runtime.get("max_pages", 30) or 30)
    per_page = 100

    client = WordPressClient(
        runtime["wp_base_url"], runtime["wp_username"], runtime["wp_app_password"]
    )
    base_url = runtime["wp_base_url"]
    try:
        posts = fetch_all_posts(client, max_pages=max_pages, per_page=per_page)
    except requests.RequestException:
        print("Failed to load posts. Check network or WordPress credentials.")
        return 1

    empty_posts: List[Dict[str, Any]] = []
    filled = 0
    for post in posts:
        content_html = post.get("content", {}).get("rendered", "")
        if is_empty_content(content_html):
            empty_posts.append(post)
        else:
            filled += 1

    conn = get_db()
    status_map = get_status_map(conn)
    conn.close()

    skipped_done = 0
    todo: List[Dict[str, Any]] = []
    for post in empty_posts:
        post_id = int(post.get("id", 0))
        if should_skip_done(status_map, post_id):
            skipped_done += 1
            continue
        todo.append(post)

    random.shuffle(todo)

    print_banner()
    print_counts(len(posts), filled, len(empty_posts), skipped_done)
    if not todo:
        print("Nothing to do. All empty posts are already processed.")
        return 0

    total_todo = len(todo)
    for index, post in enumerate(todo, start=1):
        post_id = int(post.get("id", 0))
        title = normalize_title(post.get("title", {}).get("rendered", ""))
        print(f"[{index:>4}/{total_todo:<4}] POST #{post_id} :: {title}")

        attempt = 0
        wait = 5
        while True:
            attempt += 1
            try:
                current = client.get_post(post_id)
                current_content = current.get("content", {}).get("rendered", "")
                if not is_empty_content(current_content):
                    update_post_status(post_id, "done", None)
                    print("  STATUS : ALREADY FILLED -> SKIP")
                    break
            except requests.RequestException as exc:
                update_post_status(post_id, "error", str(exc))
                print(f"  STATUS : RETRYING ({attempt})")
                time.sleep(wait)
                wait = min(wait * 2, 300)
                continue

            update_post_status(post_id, "processing", None)
            try:
                perform_generation(post_id, runtime)
                refreshed = client.get_post(post_id)
                refreshed_content = refreshed.get("content", {}).get("rendered", "")
                if is_empty_content(refreshed_content):
                    raise RuntimeError("Content still empty after update.")
                link = refreshed.get("link", "") or f"{base_url}/?p={post_id}"
                print(f"  STATUS : DONE -> {link}")
                break
            except Exception as exc:  # noqa: BLE001
                update_post_status(post_id, "error", str(exc))
                print(f"  STATUS : RETRYING ({attempt})")
                time.sleep(wait)
                wait = min(wait * 2, 300)

    print("=" * 78)
    print("COMPLETE :: ALL EMPTY POSTS PROCESSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
