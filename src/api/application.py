from typing import Dict, List, Optional

from fastapi import FastAPI, requests
from starlette import routing

from api.v1.api import api_router
from core.aws import AWSManager
from core.config import settings

app = FastAPI(
    title=settings.PROJECT_NAME, openapi_url=f"{settings.API_V1_STR}/openapi.json"
)


@app.get("/")
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
