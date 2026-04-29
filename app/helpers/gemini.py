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
            model="gemini-3-flash-preview",
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


from functools import lru_cache


@lru_cache(maxsize=500)
def get_gemini_synonyms(keyword: str) -> str:
    """
    Returns ONLY:
    - original keyword
    - singular form
    - plural form
    No synonyms, no extra words.
    """

    system_instruction = (
        "You are a strict linguistic processor.\n"
        "Return ONLY the singular and plural forms of the given word.\n"
        "Rules:\n"
        "1. Output ONLY space-separated words.\n"
        "2. Do NOT include synonyms.\n"
        "3. Do NOT include explanations.\n"
        "4. Maximum 2 words.\n"
        "5. If input is already singular/plural, include both forms.\n"
        "6. If no plural exists, return the word once."
    )

    prompt = f"Word: {keyword}"

    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            config={
                "system_instruction": system_instruction,
                "temperature": 0.0,  # make it strict/deterministic
            },
            contents=prompt,
        )

        result_text = response.text.strip().lower()
        result_text = result_text.replace('"', "").replace("'", "")

        words = result_text.split()

        # HARD GUARD: keep only 2 words max
        words = words[:2]

        # Ensure original keyword is included
        if keyword.lower() not in words:
            words.insert(0, keyword.lower())

        # Deduplicate
        final_string = " ".join(dict.fromkeys(words))

        print(f"✅ Processed '{keyword}': {final_string}")
        return final_string

    except Exception as e:
        print(f"❌ Gemini error: {e}")
        return keyword
