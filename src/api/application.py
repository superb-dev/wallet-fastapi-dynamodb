from typing import Optional

from fastapi import FastAPI
from starlette import responses
from starlette.exceptions import HTTPException

from api.v1.api import api_router
from core.aws import AWSManager
from core.config import settings

app = FastAPI(
    title=settings.PROJECT_NAME, openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

app.include_router(api_router, prefix=settings.API_V1_STR)


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request, exc):
    if exc.status_code == 404:
        url_list = [
            {"path": str(request.base_url.replace(path=route.path)), "name": route.name}
            for route in app.routes
        ]

        return responses.JSONResponse(url_list)

    if exc.status_code in {204, 304}:
        return responses.Response(b"", status_code=exc.status_code)
    return responses.PlainTextResponse(exc.detail, status_code=exc.status_code)


aws_manager: Optional[AWSManager] = None


@app.on_event("startup")
async def initialize_aws_manager():
    # https://docs.aiohttp.org/en/stable/client_reference.html
    # it is suggested you use a single session for the lifetime of
    # your application to benefit from connection pooling.
    # Unfortunately, fastapi does not support singleton dependencies
    # https://github.com/tiangolo/fastapi/issues/504
    global aws_manager
    aws_manager = AWSManager()
    await aws_manager.initialize()


@app.on_event("shutdown")
async def close_aws_manager():
    global aws_manager
    if aws_manager is not None:
        await aws_manager.close()
        aws_manager = None
