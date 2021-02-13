from unittest import mock

import pytest

from commands import create_table

pytestmark = pytest.mark.asyncio


class TestCreateTableCommand:
    async def test_ok(self):
        with mock.patch(
            "core.storage.DynamoDB.table_exists",
            mock.AsyncMock(return_value=False),
        ):
            with mock.patch("core.storage.DynamoDB.create_table") as create_table_mock:
                await create_table.main()

        create_table_mock.assert_awaited_once()

    async def test_create_already_exists(
        self,
    ):
        with mock.patch(
            "core.storage.DynamoDB.table_exists",
            mock.AsyncMock(return_value=True),
        ):
            with mock.patch("core.storage.DynamoDB.create_table") as create_table_mock:
                await create_table.main()

        create_table_mock.assert_not_awaited()
