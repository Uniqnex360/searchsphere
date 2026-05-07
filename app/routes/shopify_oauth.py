import os
import secrets
import hashlib
import hmac
import requests
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select

from urllib.parse import urlencode
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import RedirectResponse

from app.database import get_session
from app.models import ShopifyAuth

router = APIRouter()

SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = os.getenv("SHOPIFY_API_SECRET")
SHOPIFY_SCOPES = os.getenv("SHOPIFY_SCOPES")

FRONTEND_URL = os.getenv("FRONTEND_URL")
BACKEND_URL = os.getenv("BACKEND_URL")


# -----------------------------------
# STEP 1: START INSTALL
# -----------------------------------
@router.get("/auth")
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


@router.get("/auth/callback")
async def auth_callback(
    shop: str, code: str, hmac: str, session: AsyncSession = Depends(get_session)
):
    # -----------------------------------
    # STEP 1: Verify HMAC (IMPORTANT: real Shopify uses full query string)
    # -----------------------------------
    query_string = f"code={code}&shop={shop}"
    generated_hmac = hmac_module(query_string)

    if generated_hmac != hmac:
        raise HTTPException(status_code=400, detail="Invalid HMAC")

    # -----------------------------------
    # STEP 2: Exchange code for access token
    # -----------------------------------
    token_url = f"https://{shop}/admin/oauth/access_token"

    payload = {
        "client_id": SHOPIFY_API_KEY,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code,
    }

    response = requests.post(token_url, json=payload)
    token_data = response.json()

    access_token = token_data.get("access_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Failed to get access token")

    # -----------------------------------
    # STEP 3: Save / Update DB (UPSERT)
    # -----------------------------------
    result = await session.exec(select(ShopifyAuth).where(ShopifyAuth.shop == shop))
    existing = result.first()

    if existing:
        existing.access_token = access_token
        existing.is_active = True
    else:
        new_auth = ShopifyAuth(
            shop=shop,
            access_token=access_token,
            is_active=True,
        )
        session.add(new_auth)

    await session.commit()

    # -----------------------------------
    # STEP 4: Redirect to frontend
    # -----------------------------------
    return RedirectResponse(f"{FRONTEND_URL}?shop={shop}")


def hmac_module(data: str):
    return hmac.new(
        SHOPIFY_API_SECRET.encode(),
        data.encode(),
        hashlib.sha256,
    ).hexdigest()
