from sentence_transformers import SentenceTransformer

# model = SentenceTransformer("all-MiniLM-L6-v2")


def get_embedding(text: str):
    if not text:
        return None
    return model.encode(text).tolist()