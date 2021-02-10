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


@pytest.fixture(scope="session")
def init_settings():
    settings.AWS_DYNAMODB_READ_CAPACITY = 10
    settings.AWS_DYNAMODB_WRITE_CAPACITY = 10
    settings.AWS_CLIENT_PARAMETER_VALIDATION = True
    settings.WALLET_TABLE_NAME = "test"


@pytest.fixture(scope="session")
async def storage(aws) -> Storage:
    storage = Storage(aws=aws, table_name=settings.WALLET_TABLE_NAME)
    await storage.create_table()
    try:
        yield storage
    finally:
        await storage.drop_table()


@pytest.fixture(scope="session")
async def aws() -> AWSManager:
    async with AWSManager() as manager:
        yield manager


@pytest.fixture(scope="module")
async def client() -> AsyncClient:
    async with AsyncClient(app=app, base_url="http://test") as c:
        yield c


@pytest.fixture(scope="session")
def event_loop():
    # use single loop on for all tests
    # aws manager are using shared session
    return asyncio.get_event_loop()
