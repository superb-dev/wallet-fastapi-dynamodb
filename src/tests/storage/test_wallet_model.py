import asyncio
import copy
import uuid
from unittest import mock
from unittest.mock import patch

import pytest

from core.config import settings
from storage import exceptions
from storage.models import Transaction, TransactionType, Wallet

pytestmark = pytest.mark.asyncio


class TestWallet:
    async def test_init(self, raw_wallet):
        with pytest.raises(exceptions.WalletDoesNotExistsError):
            assert raw_wallet.pk

        assert raw_wallet.storage.table_name == settings.WALLET_TABLE_NAME

    async def test_eq(self, wallet):
        assert wallet != 1
        assert wallet is not True
        assert not wallet == {}

        second_wallet = Wallet(pk=wallet.pk)

        assert second_wallet == wallet

    async def test_eq_empty_pk(self, wallet):
        assert wallet != Wallet()
        assert Wallet() == Wallet()

    async def test_create(self, wallet):
        user_id = str(uuid.uuid4())
        await wallet.create_wallet(user_id=user_id)

        transaction_pk = Transaction.get_storage_pk(nonce=None, wallet=wallet.pk)

        transaction_response = await wallet.storage.get(transaction_pk)
        assert transaction_response["data"] == {"amount": wallet.DEFAULT_BALANCE}
        assert transaction_response["type"] == TransactionType.CREATE.value

        assert wallet.pk is not None
        assert wallet.pk != user_id

    async def test_already_exists_for_same_user(self, raw_wallet):
        user_id = str(uuid.uuid4())
        raw_wallet2 = copy.copy(raw_wallet)
        await raw_wallet.create_wallet(user_id=user_id)

        with pytest.raises(exceptions.WalletAlreadyExistsError, match=user_id):
            await raw_wallet2.create_wallet(user_id=user_id)

        # still exists
        assert await raw_wallet.get_balance() == raw_wallet.DEFAULT_BALANCE

    async def test_get_balance(self, wallet):
        assert await wallet.get_balance() == wallet.DEFAULT_BALANCE

    async def test_deposit(self, wallet):
        nonce = "test_deposit"
        await wallet.atomic_deposit(500, nonce=nonce)

        assert await wallet.get_balance() == 500

        transaction_pk = Transaction.get_storage_pk(nonce=nonce, wallet=wallet.pk)

        transaction_response = await wallet.storage.get(transaction_pk)
        assert transaction_response["data"] == {"amount": 500}

        assert transaction_response["type"] == TransactionType.DEPOSIT.value

    async def test_atomic_deposit_idempotency(self, wallet):
        nonce = "test_deposit"
        await wallet.atomic_deposit(500, nonce=nonce)

        assert await wallet.get_balance() == 500

        with pytest.raises(
            exceptions.WalletTransactionAlreadyRegisteredError, match=nonce
        ):
            await wallet.atomic_deposit(600, nonce=nonce)

        # not changed
        assert await wallet.get_balance() == 500

    async def test_atomic_deposit_not_existing_wallet(self, raw_wallet):
        nonce = "test_atomic_deposit_not_existing_wallet"

        with patch.object(
            raw_wallet.storage, "transaction_write_items", mock.AsyncMock()
        ) as transaction_write_items:
            with pytest.raises(exceptions.WalletDoesNotExistsError):
                await raw_wallet.atomic_deposit(500, nonce=nonce)

        transaction_write_items.assert_not_awaited()

    async def test_get_balance_deposit_in_progress(self, wallet):
        nonce = "test_get_balance_deposit_in_progress"

        await asyncio.wait(
            [
                wallet.get_balance(),
                wallet.get_balance(),
                wallet.atomic_deposit(500, nonce),
                wallet.get_balance(),
                wallet.get_balance(),
            ]
        )

        assert await wallet.get_balance() == 500

    async def test_get_balance_not_existing_wallet(self, raw_wallet):
        raw_wallet._pk = "test_get_balance_not_existing_wallet"
        with pytest.raises(exceptions.WalletDoesNotExistsError):
            await raw_wallet.get_balance()

    async def test_atomic_transfer(self, wallet_factory):
        nonce = "test_atomic_transfer_atomic_deposit"

        wallet = await wallet_factory()
        second_wallet = await wallet_factory()
        await wallet.atomic_deposit(500, nonce=nonce)

        nonce_transfer = "test_atomic_transfer_atomic_transfer"

        assert await wallet.get_balance() == 500
        await wallet.atomic_transfer(
            400, target_wallet=second_wallet, nonce=nonce_transfer
        )

        transaction_pk = Transaction.get_storage_pk(
            nonce=nonce_transfer, wallet=wallet.pk
        )

        transaction_response = await wallet.storage.get(transaction_pk)
        assert transaction_response["data"] == {
            "amount": 400,
            "target_wallet": second_wallet.pk,
        }

        assert transaction_response["type"] == TransactionType.TRANSFER.value

        assert await wallet.get_balance() == 100

        assert await second_wallet.get_balance() == 400

    async def test_atomic_transfer_to_the_same_wallet(self, wallet):
        with patch.object(
            wallet.storage, "transaction_write_items", mock.AsyncMock()
        ) as transaction_write_items:
            with pytest.raises(ValueError):
                await wallet.atomic_transfer(
                    400,
                    target_wallet=wallet,
                    nonce="test_atomic_transfer_to_the_same_wallet",
                )

        transaction_write_items.assert_not_awaited()

    async def test_atomic_transfer_to_invalid_target_wallet(self, wallet):
        pass

    async def test_atomic_transfer_from_invalid_wallet(self, wallet):
        pass

    async def test_atomic_transfer_negative_amount(self, wallet):
        pass

    async def test_atomic_transfer_insufficient_funds(self, wallet):
        pass

    async def test_atomic_transfer_race_condition(self, wallet):
        pass

    async def test_atomic_transfer_idempotency(self, wallet):
        pass
