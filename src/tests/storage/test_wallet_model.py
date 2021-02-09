import pytest

pytestmark = pytest.mark.asyncio


class TestWallet:
    async def test_init(self, wallet):
        pass

    async def test_eq(self, wallet):
        pass

    async def test_create(self, wallet):
        pass

    async def test_already_exists(self, wallet):
        pass

    async def test_already_exists_for_same_user(self, wallet):
        pass

    async def test_get_balance(self, wallet):
        pass

    async def test_get_balance_deposit_in_progress(self, wallet):
        pass

    async def test_get_balance_not_existing_wallet(self, wallet):
        pass

    async def test_atomic_transfer(self, wallet):
        pass

    async def test_atomic_transfer_to_the_same_wallet(self, wallet):
        pass

    async def test_atomic_transfer_to_invalid_target_wallet(self, wallet):
        pass

    async def test_atomic_transfer_from_invalid_wallet(self, wallet):
        pass

    async def test_atomic_transfer_negative_amount(self, wallet):
        pass

    async def test_atomic_transfer_race_condition(self, wallet):
        pass

    async def test_atomic_transfer_idempotency(self, wallet):
        pass

    async def test_atomic_deposit(self, wallet):
        pass

    async def test_atomic_deposit_idempotency(self, wallet):
        pass

    async def test_atomic_deposit_not_existing_wallet(self):
        pass
