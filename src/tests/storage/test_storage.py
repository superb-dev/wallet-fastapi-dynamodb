import uuid
from unittest import mock
from unittest.mock import AsyncMock, patch

import pytest

from storage import DynamoDB, exceptions

pytestmark = pytest.mark.asyncio


class TestStorage:
    DEFAULT_ITEM_DATA = {"str": "value", "arb_number": 1, "none": None}

    @pytest.fixture()
    async def storage_item(self, storage) -> str:
        pk = str(uuid.uuid4())
        await storage.create(pk=pk, data=self.DEFAULT_ITEM_DATA)
        try:
            yield pk
        finally:
            await storage.delete(pk=pk)

    async def test_create_and_delete_ok(self, storage_item):
        """Simple create and delete item, test fixture"""

    async def test_crud_empty_ok(self, storage):
        pk = str(uuid.uuid4())
        await storage.create(pk=pk, data={})
        assert await storage.get(pk) == {}
        await storage.delete(pk=pk)

    async def test_get(self, storage, storage_item):
        assert await storage.get(pk=storage_item) == self.DEFAULT_ITEM_DATA

    async def test_get_with_fields(self, storage, storage_item):
        fields = ["str", "arb_number"]
        assert await storage.get(pk=storage_item, fields=fields) == {
            f: self.DEFAULT_ITEM_DATA[f] for f in fields
        }

    async def test_get_not_existing_field(self, storage):
        with pytest.raises(exceptions.ObjectNotFoundError, match="arb_key"):
            await storage.get("arb_key")

    async def test_get_not_restricted_field(self, storage):
        """Test against reserved keyword as fields"""
        with pytest.raises(exceptions.ValidationError, match="integer"):
            await storage.get(pk="any", fields=["integer"])

    async def test_get_not_existing_object(self, storage):
        """Object is not presented at the storage"""
        with pytest.raises(exceptions.ObjectNotFoundError, match="arb_key"):
            await storage.get("arb_key")

    async def test_get_table_do_not_exists(self, aws):
        storage = DynamoDB(aws=aws, table_name="test_get_table_do_not_exists")
        with pytest.raises(exceptions.ObjectNotFoundError, match="non-existent table"):
            await storage.get("arb_key")

    async def test_create_table_ok(self, aws):
        storage = DynamoDB(aws=aws, table_name="test_create_table_ok")
        await storage.create_table()

        assert await storage.table_exists()

        await storage.drop_table()

        assert await storage.table_exists() is False

    async def test_create_table_already_exists(self, storage):
        """Second call of the create_table should raise an error"""
        await storage.create_table()

    async def test_create_table_with_ttl_ok(self, aws):
        storage = DynamoDB(aws=aws, table_name="test_create_table_with_ttl_ok")

        with patch.object(
            storage._client, "update_time_to_live", AsyncMock()
        ) as update_time_to_live:
            await storage.create_table(ttl_attribute="ttl")

        await storage.drop_table()
        update_time_to_live.assert_awaited_once_with(
            TableName="test_create_table_with_ttl_ok",
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )

    async def test_delete_table(self, aws):
        storage = DynamoDB(aws=aws, table_name="test_delete_table")
        await storage.create_table()
        await storage.drop_table()

    async def test_delete_table_does_not_exists(self, aws):
        storage = DynamoDB(aws=aws, table_name="test_delete_table_does_not_exists")
        with pytest.raises(exceptions.ObjectNotFoundError, match="non-existent table"):
            await storage.drop_table()

    async def test_delete_on_non_existing_item(self, storage):
        pk = str(uuid.uuid4())

        with pytest.raises(exceptions.ObjectNotFoundError, match=pk):
            await storage.delete(pk)

    async def test_transaction_write_items(self, storage):
        await storage.transaction_write_items(
            [
                storage.item_factory.put_idempotency_item(
                    "test_transaction_write_items", {"arb": "value"}
                )
            ]
        )

        assert await storage.get("test_transaction_write_items") == {"arb": "value"}

    async def test_transaction_write_items_invalid(self, storage):
        pass

    async def test_transaction_write_items_more_than_batch_size(self, storage):
        with pytest.raises(ValueError):
            await storage.transaction_write_items(
                items=[{}] * (storage.MAX_TRANSACTION_WRITE_BATCH_SIZE + 1)
            )

    async def test_transaction_write_items_conflict(self, storage):
        # mock due to TransactionConflictExceptions are not thrown by
        # downloadable DynamoDB for transactional APIs.
        exc = storage._client.exceptions.TransactionConflictException(
            error_response={
                "Error": {
                    "Code": "TransactionConflictException",
                    "Message": "Conflict occurred",
                }
            },
            operation_name="Put",
        )

        with pytest.raises(
            exceptions.TransactionConflictError, match="Conflict occurred"
        ):
            with patch.object(
                storage._client,
                "transact_write_items",
                mock.AsyncMock(side_effect=exc),
            ):
                await storage.transaction_write_items([{}])

    async def test_transaction_write_items_conditions_check_failed(self, storage):
        pk = "transaction_write_items_conditions"
        await storage.create(pk=pk, data={"arb": 1})

        with pytest.raises(exceptions.TransactionMultipleError) as e:
            await storage.transaction_write_items(
                [storage.item_factory.put_idempotency_item(pk=pk, data={})]
            )

        assert len(e.value.errors) == 1
        assert isinstance(e.value.errors[0], exceptions.ConditionalCheckFailedError)

        # not changed
        assert await storage.get(pk) == {"arb": 1}

    async def test_transaction_write_items_over_provisioned(self, storage):

        exc = storage._client.exceptions.ProvisionedThroughputExceededException(
            error_response={
                "Error": {
                    "Code": "ProvisionedThroughputExceededException",
                    "Message": (
                        "The level of configured provisioned throughput for "
                        "the table was exceeded. Consider increasing your "
                        "provisioning level with the UpdateTable API"
                    ),
                }
            },
            operation_name="transact_write_items",
        )

        # todo: parse exception type
        with pytest.raises(
            exceptions.UnknownStorageError,
            match="provisioned throughput for the table was exceeded",
        ):
            with patch.object(
                storage._client,
                "transact_write_items",
                mock.AsyncMock(side_effect=exc),
            ):
                await storage.transaction_write_items([{}])

    async def test_transaction_write_items_already_in_progress(self, storage):
        exc = storage._client.exceptions.TransactionInProgressException(
            error_response={
                "Error": {
                    "Code": "TransactionInProgressException",
                    "Message": (
                        "The transaction with the given request "
                        "token is already in progress."
                    ),
                }
            },
            operation_name="transact_write_items",
        )

        # todo: parse exception type
        with pytest.raises(exceptions.UnknownStorageError, match="already in progress"):
            with patch.object(
                storage._client,
                "transact_write_items",
                mock.AsyncMock(side_effect=exc),
            ):
                await storage.transaction_write_items([{}])
