# Auto Blog Post Generator

A small Flask tool that generates WordPress post content from existing titles using Gemini, then writes content, tags, and metadata back to WordPress.

## What it does
- Lists your WordPress posts.
- Generates a 2000-word professional article from the post title.
- Writes content, meta title, meta description, and tags to the post.
- Tracks generation history and status.

## Setup

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Create a `.env` file from the example:

```bash
cp .env.example .env
```

3. Update `.env` with your WordPress and Gemini credentials (optional if you use the Settings UI).
   - If you want to inject your own writing prompt, put it in `CUSTOM_PROMPT` or paste it in Settings.
   - You can use `{title}` in `CUSTOM_PROMPT` to include the post title.

4. Run the app:

```bash
python app.py
```

Open `http://127.0.0.1:5000`.

## Settings UI
Use the Settings page to manage credentials, prompt text, and inbound links without editing files.
Secrets are stored in the local SQLite database (`data.db`).
Use the “Test Connections” button to verify WordPress and Gemini access.
If you get a model error, click “Fetch Models” and pick a model from the list.

## WordPress metadata notes
- `META_TITLE_KEY` and `META_DESCRIPTION_KEY` must be registered for REST updates. Many SEO plugins add these keys.
- If you do not use a meta plugin, you can keep `USE_EXCERPT_FOR_META_DESCRIPTION=true` to store the description in the post excerpt.

## Inbound links
Inbound links are pulled from the list in `app.py`. The prompt asks Gemini to include exactly two per post.

## Safety
- The app updates posts directly. Use a staging site for testing.
