import base64
import hashlib
import hmac
import json

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from app.settings import settings

router = APIRouter()

# 🔐 Your Shopify App Secret (store in env in production)
SHOPIFY_SECRET = settings.shopify_api_secret


# =========================
# 🔐 HMAC VERIFICATION
# =========================
def verify_shopify_hmac(request_body: bytes, hmac_header: str):
    """
    Verify Shopify webhook HMAC signature
    """
    if not hmac_header:
        return False

    digest = hmac.new(
        SHOPIFY_SECRET.encode("utf-8"), request_body, hashlib.sha256
    ).digest()

    computed_hmac = base64.b64encode(digest).decode()

    return hmac.compare_digest(computed_hmac, hmac_header)


# =========================
# SAFE JSON PARSER
# =========================
async def safe_parse_json(request: Request):
    body = await request.body()

    if not body:
        return {}, body

    try:
        return json.loads(body), body
    except json.JSONDecodeError:
        return {}, body


# =========================
# CUSTOMER DATA REQUEST
# =========================
@router.post("/customers/data_request")
@router.post("/customers/data_request/")
async def customer_data_request(request: Request):
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    payload, raw_body = await safe_parse_json(request)

    # 🔐 Verify Shopify request authenticity
    if not verify_shopify_hmac(raw_body, hmac_header):
        raise HTTPException(status_code=401, detail="Invalid HMAC")

    print("📩 Data request received:", payload)

    # Shopify expects 200 OK
    return JSONResponse(content={}, status_code=200)


# =========================
# CUSTOMER REDACT (DELETE DATA)
# =========================
@router.post("/customers/redact")
@router.post("/customers/redact/")
async def customer_redact(request: Request):
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    payload, raw_body = await safe_parse_json(request)

    # 🔐 Verify HMAC
    if not verify_shopify_hmac(raw_body, hmac_header):
        raise HTTPException(status_code=401, detail="Invalid HMAC")

    print("🗑️ Customer redact request received:", payload)

    """
    Payload typically looks like:
    {
        "shop_id": 123456,
        "shop_domain": "test-shop.myshopify.com",
        "customer": {
            "id": 12345,
            "email": "customer@example.com"
        },
        "orders_to_redact": [123, 456]
    }
    """

    # TODO (if you had DB data):
    # - delete customer records
    # - delete logs tied to customer.id/email
    # - anonymize analytics data

    return JSONResponse(content={}, status_code=200)


# =========================
# 🧨 SHOP REDEX (FULL DELETE)
# =========================
@router.post("/shop/redact")
@router.post("/shop/redact/")
async def shop_redact(request: Request):
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    payload, raw_body = await safe_parse_json(request)

    if not verify_shopify_hmac(raw_body, hmac_header):
        raise HTTPException(status_code=401, detail="Invalid HMAC")

    shop_id = payload.get("shop_id")
    shop_domain = payload.get("shop_domain")

    print("🔥 SHOP REDECT TRIGGERED")
    print("Shop ID:", shop_id)
    print("Shop Domain:", shop_domain)

    # =========================
    # 🧹 YOUR CLEANUP LOGIC HERE
    # =========================

    # Example actions:
    # - delete products linked to shop_id
    # - delete shop settings
    # - delete sync logs
    # - clear cache keys: f"shop:{shop_id}:*"

    # Example pseudo:
    # db.products.delete_many({"shop_id": shop_id})
    # db.shops.delete_one({"shop_id": shop_id})
    # redis.delete_pattern(f"shop:{shop_id}:*")

    return JSONResponse(content={}, status_code=200)