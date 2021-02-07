from contextlib import AsyncExitStack

import aiobotocore
import aiobotocore.client
from botocore.config import Config

from core.config import settings


class AWSManager:
    """
    Provides common interface for the amazon API services

    Example:
        async with AWSManager() as manager:
            do job

    """

    def __init__(self):
        self._exit_stack = AsyncExitStack()
        self._dynamodb_client = None
        self.initialized = False

    @property
    def dynamodb(self) -> aiobotocore.client.AioBaseClient:
        if self._dynamodb_client is None or not self.initialized:
            raise ValueError(
                "Dynamodb is not initialized. "
                "It is not allowed to use without context manager"
            )

        return self._dynamodb_client

    async def __aenter__(self):
        await self.initialize()
        return self

    async def initialize(self):
        if self.initialized:
            raise ValueError("Already initialized.")

        self.initialized = True
        session = self._get_session()
        self._dynamodb_client = await self._exit_stack.enter_async_context(
            session.create_client(
                "dynamodb",
                region_name=settings.AWS_REGION_NAME,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                endpoint_url=settings.AWS_DYNAMODB_ENDPOINT_URL,
            )
        )

    async def close(self):
        if self.initialized:
            self.initialized = False
            await self._exit_stack.__aexit__(None, None, None)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.initialized:
            self.initialized = False
            await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    def _get_session(self) -> aiobotocore.AioSession:
        """
        Return a session object. Creates new if not exists.
        """
        session = aiobotocore.get_session()

        # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
        session.set_default_client_config(
            Config(
                retries={"max_attempts": settings.AWS_CLIENT_MAX_ATTEMPTS},
                connect_timeout=settings.AWS_CLIENT_CONNECT_TIMEOUT,
                read_timeout=settings.AWS_CLIENT_READ_TIMEOUT,
                max_pool_connections=settings.AWS_CLIENT_MAX_POOL_CONNECTIONS,
                parameter_validation=settings.AWS_CLIENT_PARAMETER_VALIDATION,
            )
        )

        return session
