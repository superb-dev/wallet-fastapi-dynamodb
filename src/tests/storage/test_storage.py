import pytest

pytestmark = pytest.mark.asyncio


class TestStorage:
    async def test_get(self, storage):
        pass

    async def test_get_with_fields(self, storage):
        pass

    async def test_get_not_existing_field(self, storage):
        pass

    async def test_get_not_existing_object(self, storage):
        pass

    async def test_create_table_ok(self, storage):
        pass

    async def test_create_table_already_exists(self, storage):
        pass

    async def test_create_table_with_ttl_ok(self, storage):
        pass

    async def test_table_exists(self, storage):
        pass

    async def test_table_do_not_exists(self, storage):
        pass

    async def test_delete_table(self, storage):
        pass

    async def test_delete_table_does_not_exists(self, storage):
        pass

    async def test_delete_ok(self, storage):
        pass

    async def test_delete_on_not_existing_item(self, storage):
        pass

    async def test_transaction_write_items(self, storage):
        pass

    async def test_transaction_write_items_conflict(self, storage):
        pass

    async def test_transaction_write_items_conditions_check_failed(self, storage):
        pass

    async def test_transaction_write_items_over_provisioned(self, storage):
        pass

    async def test_transaction_write_items_throttling(self, storage):
        pass
