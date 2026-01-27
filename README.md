# Auto Blog Post Generator (WordPress + Gemini)

A professional, SEO-focused WordPress content generator that creates full blog posts, meta titles, meta descriptions, and tags using Gemini AI, then updates posts via the WordPress REST API. Built with Flask, SQLite, and a configurable prompt system.

Keywords: WordPress AI content generator, blog post automation, Gemini API, SEO meta title and description, WordPress REST API, Flask automation tool, bulk content creation, tag generation.

## What this tool does
- Fetches WordPress posts (publish, draft, pending, future).
- Detects empty posts and generates ~2000-word articles from the title.
- Produces SEO metadata (meta title, meta description) and 5–8 tags.
- Inserts content and metadata back into WordPress.
- Tracks status (queued, processing, done, error, canceled) and keeps a generation log.
- Supports multiple Gemini API keys with automatic quota fallback.
- Includes a web UI and a CLI batch mode.

## Quick start

1. Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Create your `.env`:

```bash
cp .env.example .env
```

3. Configure WordPress and Gemini credentials (or use the Settings UI):
   - `WP_BASE_URL`, `WP_USERNAME`, `WP_APP_PASSWORD`
   - `GEMINI_API_KEY` or `GEMINI_API_KEYS` (comma or newline list)
   - `GEMINI_MODEL` (default `gemini-1.5-flash-latest`)
   - `GEMINI_SDK` = `auto` (prefers `google-genai`, falls back to `google-generativeai`)
   - `CUSTOM_PROMPT` (supports `{title}` substitution)

4. Run the app:

```bash
python app.py
```

Open `http://127.0.0.1:5000`.

## How the system works (end-to-end process)
1. **Load settings** from `.env` and the SQLite database (`data.db`). Database values override `.env` when set.
2. **Fetch posts** from WordPress using the REST API and cache them briefly to reduce API calls.
3. **Detect empty content** by stripping HTML and checking for real text.
4. **Queue generation** when you click Generate or Bulk Generate in the UI (or run the CLI).
5. **Build prompt** using the post title, inbound links, SEO rules, and your custom instructions.
6. **Generate content** through Gemini (auto SDK selection). If quota fails, the app retries with another key.
7. **Validate JSON** response; if invalid, auto-repair via a strict JSON-fix prompt.
8. **Ensure metadata** exists; if missing, request a dedicated metadata prompt and merge results.
9. **Create/resolve tags** via WordPress REST API.
10. **Update the post** with HTML content, tags, and SEO meta fields (or excerpt fallback).
11. **Log results** (prompt, response, meta) to `generation_log` and update status.

## Code structure and key functions

### `app.py` (Flask web app)
- **Database**
  - `get_db()`, `init_db()` create and connect to SQLite (`app_config`, `post_status`, `generation_log`).
- **WordPress client**
  - `WordPressClient` handles listing posts, fetching a post, updating content/meta, and creating tags.
- **Gemini client**
  - `GeminiClient` selects SDK (`google-genai` or `google-generativeai`) and calls `generate`.
  - `MultiKeyGemini` rotates API keys on quota errors and can list available models.
- **Prompt system**
  - `build_prompt()` assembles the full content prompt with rules, SEO, links, and your custom prompt.
  - `build_metadata_prompt()` builds a fallback prompt for meta title/description/tags.
  - `build_json_repair_prompt()` repairs invalid JSON.
- **Generation pipeline**
  - `enqueue_post()` schedules jobs and prevents duplicates.
  - `process_post()` runs the generation with status updates.
  - `perform_generation()` does the full end-to-end flow (prompt → AI → tags → update WP).
- **UI routes**
  - `/` main dashboard with filters and counts.
  - `/generate/<id>` single post generation.
  - `/bulk-generate` batch queue.
  - `/cancel/<id>` cancel a queued/processing post.
  - `/settings` configuration UI.
  - `/fetch-models` Gemini model discovery.
  - `/test-connection` verifies WordPress + Gemini access.
  - `/logs/<id>` shows recent generation metadata.

### `cli.py` (terminal batch mode)
- Scans empty posts and processes them in random order.
- Retries on transient WordPress errors with backoff.
- Uses the same `perform_generation()` pipeline as the web app.

## Settings UI
Manage credentials, prompts, inbound links, and SEO meta keys without editing files.
Secrets are stored locally in SQLite (`data.db`).
Use **Test Connections** to verify WordPress and Gemini access.
Use **Fetch Models** to populate and select available Gemini models.

## WordPress metadata notes
- `META_TITLE_KEY` and `META_DESCRIPTION_KEY` must be registered for REST updates (Yoast and similar plugins usually add these).
- If you do not use a meta plugin, set `USE_EXCERPT_FOR_META_DESCRIPTION=true` to store the description in the post excerpt.

## Inbound links
Inbound links are stored in Settings and default to the list in `app.py`.
Each post includes exactly two inbound links and four outbound links to reputable sources.

## Full prompt template (generic, reusable)
Use this as a starting point for `CUSTOM_PROMPT`. Replace placeholders as needed.

```text
You are writing a professional blog post.

Topic: {title}

Rules:
- About 2000 words.
- Use plain language and short sentences.
- No hype, no buzzwords, no clichés.
- No special characters or emojis.
- Do not use headings like Conclusion or Final thoughts.
- Start with a short intro paragraph, not a title heading.
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
- {inbound_link_1}
- {inbound_link_2}

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

Required site and writing requirements:
{custom_requirements}
```

Note: the built-in default prompt in `app.py` is brand-specific. Replace it in Settings or via `CUSTOM_PROMPT` to match your own site and requirements.

## Safety
- The app updates posts directly. Use a staging site for testing.
