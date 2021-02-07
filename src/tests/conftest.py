import asyncio

import pytest
from httpx import AsyncClient

from api.application import app
from core.aws import AWSManager
from core.config import settings
from storage.models import Wallet
from storage.storage import Storage


@pytest.fixture()
async def wallet(aws) -> Wallet:
    yield Wallet(aws=aws)


@pytest.fixture()
async def storage(aws) -> Storage:
    yield Storage(aws=aws, table_name=settings.WALLET_TABLE_NAME)


@pytest.fixture(autouse=True, scope="session")
async def dynamodb(storage):
    await storage.create_table()

    storage.delete_table()


@pytest.fixture(scope="session")
async def aws() -> AWSManager:
    async with AWSManager() as manager:
        yield manager


@pytest.fixture(scope="module")
def client() -> AsyncClient:
    with AsyncClient(app=app, base_url="http://test") as c:
        yield c


@pytest.fixture(scope="session")
def event_loop():
    # use single loop on for all tests
    # aws manager are using shared session
    return asyncio.get_event_loop()
