
# Lead Master Complete Starter

This repo contains a fully-working solo lead-tracking app.

## Quick start
1. Create a virtual environment:
    ```
    python -m venv .venv && .venv\Scripts\activate   # Windows
    ```
2. Install requirements:
    ```
    pip install -r requirements.txt
    ```
3. Add your OpenAI key to `secrets_template.txt` (line must be TOML style).

4. Run locally:
    ```
    streamlit run app.py
    ```

## GitHub Actions
The `.github/workflows` folder contains hourly and nightly jobs. They install requirements and run `fetch_signals.py`.

## Streamlit Cloud
Use `app.py` as main file and paste the contents of `secrets_template.txt` into the Secrets pane.
