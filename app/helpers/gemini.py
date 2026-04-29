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


@lru_cache(maxsize=500)
def get_gemini_synonyms(keyword: str) -> str:
    keyword = keyword.strip().lower()

    system_instruction = (
        "Be extremely fast and minimal.\n"
        "Return ONLY singular and plural forms of the word.\n"
        "Output only space-separated words.\n"
        "Max 2 words.\n"
        "IMPORTANT FAST RULE:\n"
        "- If you are not 100% confident about singular or plural form,\n"
        "  immediately return ONLY the original word.\n"
        "- Do NOT try to guess or reason deeply.\n"
        "- Prefer speed over completeness.\n"
    )

    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            config={
                "system_instruction": system_instruction,
                "temperature": 0.0,
            },
            contents=f"Word: {keyword}",
        )

        words = response.text.strip().lower().split()[:2]

        if not words:
            return keyword

        if keyword not in words:
            words.insert(0, keyword)

        return " ".join(dict.fromkeys(words))

    except Exception:
        return keyword
