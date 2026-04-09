import time

from app.celery_app import celery_app


@celery_app.task
def test_import_products(file_path: str):
    print(f"Starting import for {file_path}")

    # simulate heavy work
    time.sleep(10)

    # 👉 your real logic:
    # - read CSV
    # - bulk index to Elasticsearch

    print("Import completed")

    return {"status": "done"}
