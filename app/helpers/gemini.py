import os
import json
from functools import lru_cache

from google import genai
from app.settings import settings


# ---------------------------------------------------------
# 🔹 Gemini Client
# ---------------------------------------------------------
client = genai.Client(api_key=settings.gemini_api_key)


# ---------------------------------------------------------
# 🔹 Utils
# ---------------------------------------------------------
def clean_text(text):
    if not text:
        return ""
    return str(text).strip().lower()


@lru_cache(maxsize=500)
def get_gemini_autocompletion(keyword: str):
    prompt = f"""
    You are an expert in ecommerce search optimization.

    Return ONLY JSON:

    [
      {{
        "original": "{keyword}",
        "synonyms": ["..."]
      }}
    ]
    """

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )

        raw_text = response.text.strip()
        print("📩 Gemini raw:", raw_text)

        try:
            data = json.loads(raw_text)
        except Exception:
            print("❌ JSON parse error")
            return []

        if isinstance(data, list) and len(data) > 0:
            syns = data[0].get("synonyms", [])

            # normalize + dedupe
            syns = list({s.strip().lower() for s in syns if s})

            print("✅ Gemini synonyms:", syns)
            return syns

        return []

    except Exception as e:
        print("❌ Gemini error:", e)
        return []
