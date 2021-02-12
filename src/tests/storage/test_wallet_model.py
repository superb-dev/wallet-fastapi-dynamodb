import asyncio
import copy
import itertools
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
            assert raw_wallet.wallet_id

        assert raw_wallet.storage.table_name == settings.WALLET_TABLE_NAME

    async def test_storage_not_available_on_empty(self):
        empty_wallet = Wallet()

        with pytest.raises(ValueError):
            assert empty_wallet.storage

    async def test_eq(self, wallet):
        assert wallet != 1
        assert wallet is not True
        assert not wallet == {}

        second_wallet = Wallet(wallet_id=wallet.wallet_id)

        assert second_wallet == wallet

    async def test_repr(self, wallet):
        assert repr(wallet) == f"Wallet(pk={wallet.wallet_id})"

    async def test_eq_empty_pk(self, wallet):
        assert wallet != Wallet()
        assert Wallet() == Wallet()

    async def test_create(self, wallet):
        user_id = str(uuid.uuid4())
        await wallet.create_wallet(user_id=user_id)

        transaction_pk = Transaction.get_unique_id(nonce=None, wallet=wallet.wallet_id)

        transaction_response = await wallet.storage.get(transaction_pk)
        assert transaction_response["data"] == {"amount": wallet.DEFAULT_BALANCE}
        assert transaction_response["type"] == TransactionType.CREATE.value

        assert wallet.wallet_id is not None
        assert wallet.wallet_id != user_id

    async def test_create_with_same_id_twice(self, raw_wallet):
        """If we have an issue with wallet id generator and it is generate the same id
        for different wallets.

        The collisions of the guid are possible only theoretically.
        https://stackoverflow.com/a/184897/1027110
        """
        user_id = str(uuid.uuid4())
        await raw_wallet.create_wallet(user_id=user_id)

        with pytest.raises(exceptions.WalletTransactionAlreadyRegisteredError):
            with mock.patch.object(
                raw_wallet,
                "generate_wallet_id",
                mock.Mock(return_value=raw_wallet.wallet_id),
            ):
                await raw_wallet.create_wallet(user_id=user_id)

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

        transaction_pk = Transaction.get_unique_id(nonce=nonce, wallet=wallet.wallet_id)

        transaction_response = await wallet.storage.get(transaction_pk)
        assert transaction_response["data"] == {"amount": 500}

        assert transaction_response["type"] == TransactionType.DEPOSIT.value

    async def test_deposit_negative(self, wallet):
        nonce = "test_deposit_negative"
        with pytest.raises(ValueError):
            await wallet.atomic_deposit(-500, nonce=nonce)

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

        await asyncio.gather(
            wallet.get_balance(),
            wallet.get_balance(),
            wallet.atomic_deposit(500, nonce),
            wallet.get_balance(),
            wallet.get_balance(),
        )

        # todo: can be false due to eventual consistency
        # add strong parameter to get_balance function
        assert await wallet.get_balance() == 500

    async def test_get_balance_not_existing_wallet(self, raw_wallet):
        raw_wallet._wallet_id = "test_get_balance_not_existing_wallet"
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

        transaction_pk = Transaction.get_unique_id(
            nonce=nonce_transfer, wallet=wallet.wallet_id
        )

        transaction_response = await wallet.storage.get(transaction_pk)
        assert transaction_response["data"] == {
            "amount": 400,
            "target_wallet": second_wallet.wallet_id,
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

    async def test_atomic_transfer_to_not_created_target_wallet(
        self, wallet_factory, raw_wallet
    ):
        nonce = "test_atomic_transfer_atomic_deposit"

        wallet = await wallet_factory()

        await wallet.atomic_deposit(500, nonce=nonce)

        nonce_transfer = "test_atomic_transfer_atomic_transfer"

        with patch.object(
            wallet.storage, "transaction_write_items", mock.AsyncMock()
        ) as transaction_write_items:
            with pytest.raises(exceptions.WalletDoesNotExistsError):
                await wallet.atomic_transfer(
                    400, target_wallet=raw_wallet, nonce=nonce_transfer
                )

        transaction_write_items.assert_not_awaited()

    async def test_atomic_transfer_to_not_existing_target_wallet(self, wallet):
        nonce = "test_atomic_transfer_to_not_existing_target_wallet"

        await wallet.atomic_deposit(500, nonce=nonce)

        nonce_transfer = "test_atomic_transfer_to_not_existing_target_wallet_transfer"

        raw_wallet = Wallet(wallet_id="invalid_wallet")

        with pytest.raises(exceptions.WalletDoesNotExistsError):
            await wallet.atomic_transfer(
                400, target_wallet=raw_wallet, nonce=nonce_transfer
            )

    @pytest.mark.skip(reason="todo: correct wallet error message")
    async def test_atomic_transfer_from_invalid_wallet(self, wallet, aws):
        nonce = "test_atomic_transfer_from_invalid_wallet"

        await wallet.atomic_deposit(500, nonce=nonce)

        nonce_transfer = "test_atomic_transfer_from_invalid_wallet_transfer"

        raw_wallet = Wallet(wallet_id="invalid_wallet", aws=aws)

        with pytest.raises(exceptions.WalletDoesNotExistsError):
            await raw_wallet.atomic_transfer(
                400, target_wallet=wallet, nonce=nonce_transfer
            )

    @pytest.mark.parametrize("invalid_amount", [-500, 0, -999999999999999999])
    async def test_atomic_transfer_negative_amount(
        self, wallet, wallet_factory, invalid_amount
    ):
        nonce = "test_atomic_transfer_from_invalid_wallet"

        await wallet.atomic_deposit(500, nonce=nonce)

        nonce_transfer = "test_atomic_transfer_from_invalid_wallet_transfer"
        target_wallet = await wallet_factory()

        with patch.object(
            wallet.storage, "transaction_write_items", mock.AsyncMock()
        ) as transaction_write_items:
            with pytest.raises(ValueError):
                await target_wallet.atomic_transfer(
                    invalid_amount, target_wallet=wallet, nonce=nonce_transfer
                )

        transaction_write_items.assert_not_awaited()

    async def test_atomic_transfer_insufficient_funds(self, wallet, wallet_factory):
        """Initiator has not enough funds on the wallet to complete transfer."""
        nonce = "test_atomic_transfer_from_invalid_wallet"

        await wallet.atomic_deposit(500, nonce=nonce)

        nonce_transfer = "test_atomic_transfer_from_invalid_wallet_transfer"
        target_wallet = await wallet_factory()

        with pytest.raises(exceptions.WalletInsufficientFundsError):
            await target_wallet.atomic_transfer(
                600, target_wallet=wallet, nonce=nonce_transfer
            )

    async def test_atomic_transfer_race_condition(self, wallet, wallet_factory):
        """Run concurrency - 1 transfer normally,
        and check for the raise transaction conflict error due to race condition."""
        concurrency = 5

        nonce = "test_atomic_transfer_idempotency"
        target_wallet = await wallet_factory()
        target_wallet2 = await wallet_factory()

        await wallet.atomic_deposit(concurrency, nonce=nonce)

        transfer_nonce = f"{nonce}_trasfer"

        coroutines = [
            wallet.atomic_transfer(
                1, nonce=f"{transfer_nonce}_{i}", target_wallet=target
            )
            for i, target in zip(
                range(concurrency), itertools.cycle((target_wallet, target_wallet2))
            )
        ]

        original = wallet.storage._client.transact_write_items

        call_count = 0

        # todo: integration test is needed
        # mock due to TransactionConflictExceptions are not thrown by
        # downloadable DynamoDB for transactional APIs.
        exc = wallet.storage._client.exceptions.TransactionConflictException(
            error_response={
                "Error": {
                    "Code": "TransactionConflictException",
                    "Message": "Conflict occured",
                }
            },
            operation_name="Put",
        )

        async def mocked(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == concurrency:
                raise exc
            return await original(*args, **kwargs)

        with patch.object(
            wallet.storage._client,
            "transact_write_items",
            mock.AsyncMock(side_effect=mocked),
        ):
            result = await asyncio.gather(*coroutines, return_exceptions=True)

        assert result[:-1] == [None] * (concurrency - 1)
        assert isinstance(result[-1], exceptions.TransactionConflictError)

        assert await wallet.get_balance() == 1

        # check that we do not lose any penny
        target_balance = (
            await target_wallet.get_balance() + await target_wallet2.get_balance()
        )
        assert target_balance + 1 == concurrency

    async def test_atomic_transfer_idempotency(self, wallet, wallet_factory):
        """Test that can not transfer with same nonce twice."""
        nonce = "test_atomic_transfer_idempotency"
        target_wallet = await wallet_factory()

        await wallet.atomic_deposit(600, nonce=nonce)

        transfer_nonce = f"{nonce}_trasfer"

        await wallet.atomic_transfer(
            500, nonce=transfer_nonce, target_wallet=target_wallet
        )

        assert await wallet.get_balance() == 100

        with pytest.raises(
            exceptions.WalletTransactionAlreadyRegisteredError, match=transfer_nonce
        ):
            await wallet.atomic_transfer(
                600, nonce=transfer_nonce, target_wallet=target_wallet
            )

        # not changed
        assert await wallet.get_balance() == 100
