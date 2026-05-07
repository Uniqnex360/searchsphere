import os
import secrets
import hashlib
import hmac
import requests
from urllib.parse import parse_qsl, urlencode
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select

from urllib.parse import urlencode
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import RedirectResponse

from app.database import get_session
from app.models import ShopifyAuth

router = APIRouter()

SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = os.getenv("SHOPIFY_API_SECRET")
SHOPIFY_SCOPES = os.getenv("SHOPIFY_SCOPES")

FRONTEND_URL = os.getenv("FRONTEND_URL")
BACKEND_URL = os.getenv("BACKEND_URL")


def verify_hmac(query_params: dict, hmac_to_check: str):
    params = {k: v for k, v in query_params.items() if k != "hmac" and k != "signature"}

    sorted_params = urlencode(sorted(params.items()), doseq=True)

    generated_hmac = hmac.new(
        SHOPIFY_API_SECRET.encode(),
        sorted_params.encode(),
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(generated_hmac, hmac_to_check)


# -----------------------------------
# STEP 1: START INSTALL
# -----------------------------------
@router.get("/auth/")
def auth(shop: str):

    state = secrets.token_hex(16)

    params = {
        "client_id": SHOPIFY_API_KEY,
        "scope": SHOPIFY_SCOPES,
        "redirect_uri": f"{BACKEND_URL}/auth/callback",
        "state": state,
    }

    install_url = f"https://{shop}/admin/oauth/authorize?" + urlencode(params)

    return RedirectResponse(install_url)


# -----------------------------
# OAuth Callback
# -----------------------------
@router.get("/auth/callback")
@router.get("/auth/callback/")  # handles both trailing slash cases
async def auth_callback(request: Request, session: AsyncSession = Depends(get_session)):
    query_params = dict(request.query_params)

    shop = query_params.get("shop")
    code = query_params.get("code")
    received_hmac = query_params.get("hmac")

    # -----------------------------
    # 1. Validate required params
    # -----------------------------
    if not shop or not code or not received_hmac:
        raise HTTPException(status_code=400, detail="Missing Shopify parameters")

    # -----------------------------
    # 2. Verify HMAC
    # -----------------------------
    if not verify_hmac(query_params, received_hmac):
        raise HTTPException(status_code=400, detail="Invalid HMAC")

    # -----------------------------
    # 3. Exchange code for access token
    # -----------------------------
    token_url = f"https://{shop}/admin/oauth/access_token"

    payload = {
        "client_id": SHOPIFY_API_KEY,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code,
    }

    response = requests.post(token_url, json=payload)

    if response.status_code != 200:
        raise HTTPException(
            status_code=400, detail=f"Token exchange failed: {response.text}"
        )

    token_data = response.json()
    access_token = token_data.get("access_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Missing access token")

    # -----------------------------
    # 4. Save / Update DB (UPSERT)
    # -----------------------------
    result = await session.exec(select(ShopifyAuth).where(ShopifyAuth.shop == shop))
    existing = result.first()

    if existing:
        existing.access_token = access_token
        existing.is_active = True
    else:
        session.add(
            ShopifyAuth(
                shop=shop,
                access_token=access_token,
                is_active=True,
            )
        )

    await session.commit()

    # -----------------------------
    # 5. Redirect to frontend
    # -----------------------------
    return {"message": "OAuth success", "shop": shop}


def hmac_module(data: str):
    return hmac.new(
        SHOPIFY_API_SECRET.encode(),
        data.encode(),
        hashlib.sha256,
    ).hexdigest()
