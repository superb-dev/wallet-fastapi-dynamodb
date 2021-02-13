import datetime
import sys
from unittest import mock

from storage import models


class TestTransaction:
    def test_init(self):
        pk = models.Wallet.generate_wallet_id()
        nonce = hex(sys.maxsize)
        transaction = models.Transaction(
            wallet_id=pk,
            nonce=f"{nonce}",
            type=models.TransactionType.DEPOSIT,
            data={"balance": 100},
        )
        assert transaction.unique_id == f"{pk}_{nonce}#transaction"
        assert transaction.nonce == nonce
        assert transaction.data == {"balance": 100}

    def test_init_without_nonce(self):
        pk = models.Wallet.generate_wallet_id()
        transaction = models.Transaction(
            wallet_id=pk,
            nonce=None,
            type=models.TransactionType.DEPOSIT,
            data={"balance": 100},
        )
        assert transaction.unique_id == f"{pk}#transaction"
        assert transaction.nonce is None
        assert transaction.data == {"balance": 100}

    def test_ttl(self):
        pk = models.Wallet.generate_wallet_id()
        transaction = models.Transaction(
            wallet_id=pk, nonce=None, type=models.TransactionType.CREATE, data={}
        )

        datetime_mock = mock.Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(
            2022, 1, 1, tzinfo=datetime.timezone.utc
        )
        with mock.patch("storage.models.datetime.datetime", new=datetime_mock):
            assert transaction.ttl == 1640997000

        # cached and not change each time
        assert transaction.ttl == 1640997000

    def test_as_dict(self):
        pk = models.Wallet.generate_wallet_id()
        nonce = "abc"
        transaction = models.Transaction(
            wallet_id=pk,
            nonce=nonce,
            type=models.TransactionType.TRANSFER,
            data={"balance": 100},
        )

        datetime_mock = mock.Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(
            2022, 1, 1, tzinfo=datetime.timezone.utc
        )

        with mock.patch("storage.models.datetime.datetime", new=datetime_mock):
            assert transaction.as_dict() == {
                "data": {"balance": 100},
                "ttl": 1640997000,
                "type": "transfer",
            }

    def test_as_dict_without_nonce(self):
        pk = models.Wallet.generate_wallet_id()
        transaction = models.Transaction(
            wallet_id=pk,
            nonce=None,
            type=models.TransactionType.TRANSFER,
            data={"balance": 100},
        )

        datetime_mock = mock.Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(
            2022, 1, 1, tzinfo=datetime.timezone.utc
        )

        with mock.patch("storage.models.datetime.datetime", new=datetime_mock):
            assert transaction.as_dict() == {
                "data": {"balance": 100},
                "ttl": 1640997000,
                "type": "transfer",
            }
