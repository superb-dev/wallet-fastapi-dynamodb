import pytest

from core.aws import AWSManager

pytestmark = pytest.mark.asyncio


class TestAWSManager:
    async def test_init_ok(self):
        manager = AWSManager()

        assert manager.initialized is False

    async def test_as_context_manager(self):
        manager = AWSManager()
        async with manager:
            assert manager.dynamodb
            assert manager.initialized

        assert manager.initialized is False
        with pytest.raises(ValueError, match="not initialized"):
            assert manager.dynamodb

    async def test_open_close(self):
        manager = AWSManager()

        await manager.initialize()

        assert manager.initialized
        assert manager.dynamodb

        await manager.close()

        assert manager.initialized is False
        with pytest.raises(ValueError, match="not initialized"):
            assert manager.dynamodb

    async def test_reinitialize(self):
        manager = AWSManager()

        async with manager:
            assert not manager.dynamodb._endpoint.http_session.closed
            assert manager.initialized

        async with manager:
            assert not manager.dynamodb._endpoint.http_session.closed
            assert manager.initialized
        assert manager.initialized is False

    async def test_double_initialize(self):
        manager = AWSManager()

        await manager.initialize()
        with pytest.raises(ValueError, match="initialized"):
            await manager.initialize()

    async def test_call_on_not_initialized(self):
        manager = AWSManager()

        with pytest.raises(ValueError, match="not initialized"):
            assert manager.dynamodb
