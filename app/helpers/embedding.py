# from sentence_transformers import SentenceTransformer


# model = None

# def get_model():
#     global model
#     if model is None:
#         from sentence_transformers import SentenceTransformer
#         model = SentenceTransformer("all-MiniLM-L6-v2")
#     return model

# # model = SentenceTransformer("all-MiniLM-L6-v2")


# def get_embedding(text: str):
#     if not text:
#         return None
#     model = get_model()
#     return model.encode(text).tolist()