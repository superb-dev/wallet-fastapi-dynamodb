from typing import Dict, List, Optional

import fastapi
from fastapi import FastAPI, requests, responses
from starlette import routing

from api.v1.api import api_router
from core.aws import AWSManager
from core.config import settings
from storage import exceptions

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="The Wallet API is an simple payments system with capabilities to "
    "transfer funds, deposit, and check balance. "
    "Build to be highly robust and highly available.",
    version=settings.PROJECT_VERSION,
    redoc_url=None,
)


@app.get("/", include_in_schema=False)
async def root(request: requests.Request) -> List[Dict[str, str]]:
    url_list = []
    for route in app.routes:
        # do not add self to the list of routes
        if isinstance(route, routing.Route) and route.name != root.__name__:
            url_list.append(
                {
                    "path": str(request.base_url.replace(path=route.path)),
                    "name": route.name,
                }
            )

    return url_list


@app.exception_handler(exceptions.BaseWalletError)
async def wallet_storage_exception_handler(
    request: fastapi.Request, exc: exceptions.BaseWalletError
) -> responses.JSONResponse:
    del request
    return responses.JSONResponse(status_code=exc.code, content={"detail": str(exc)})


app.include_router(api_router, prefix=settings.API_V1_STR)
aws_manager: Optional[AWSManager] = None


@app.on_event("startup")
async def initialize_aws_manager() -> None:
    # https://docs.aiohttp.org/en/stable/client_reference.html
    # it is suggested you use a single session for the lifetime of
    # your application to benefit from connection pooling.
    # Unfortunately, fastapi does not support singleton dependencies
    # https://github.com/tiangolo/fastapi/issues/504
    global aws_manager
    aws_manager = AWSManager()
    await aws_manager.initialize()


@app.on_event("shutdown")
async def close_aws_manager() -> None:
    global aws_manager
    if aws_manager is not None:
        await aws_manager.close()
        aws_manager = None
