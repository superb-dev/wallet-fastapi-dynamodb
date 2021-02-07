from fastapi import APIRouter

from api.v1.endpoints import wallets

api_router = APIRouter()
api_router.include_router(wallets.router, prefix="/wallets", tags=["wallets"])
