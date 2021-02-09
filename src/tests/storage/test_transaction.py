import datetime
import sys
import uuid
from unittest.mock import Mock, patch

from storage.models import Transaction, TransactionType


class TestTransaction:
    def test_init(self):
        pk = str(uuid.uuid4())
        nonce = hex(sys.maxsize)
        transaction = Transaction(
            wallet=pk,
            nonce=f"{nonce}",
            type=TransactionType.DEPOSIT,
            data={"balance": 100},
        )
        assert transaction.storage_pk == f"{pk}_{nonce}#transaction"
        assert transaction.nonce == nonce
        assert transaction.data == {"balance": 100}

    def test_init_without_nonce(self):
        pk = str(uuid.uuid4())
        transaction = Transaction(
            wallet=pk, nonce=None, type=TransactionType.DEPOSIT, data={"balance": 100}
        )
        assert transaction.storage_pk == f"{pk}#transaction"
        assert transaction.nonce is None
        assert transaction.data == {"balance": 100}

    def test_ttl(self):
        pk = str(uuid.uuid4())
        transaction = Transaction(
            wallet=pk, nonce=None, type=TransactionType.CREATE, data={}
        )

        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(2022, 1, 1)
        with patch("storage.models.datetime.datetime", new=datetime_mock):
            assert transaction.ttl == 1640986200

        # cached and not change each time
        assert transaction.ttl == 1640986200

    def test_as_dict(self):
        pk = str(uuid.uuid4())
        nonce = "abc"
        transaction = Transaction(
            wallet=pk, nonce=nonce, type=TransactionType.TRANSFER, data={"balance": 100}
        )

        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(2022, 1, 1)

        with patch("storage.models.datetime.datetime", new=datetime_mock):
            assert transaction.as_dict() == {
                "data": {"balance": 100},
                "ttl": 1640986200,
                "type": "transfer",
            }

    def test_as_dict_without_nonce(self):
        pk = str(uuid.uuid4())
        transaction = Transaction(
            wallet=pk, nonce=None, type=TransactionType.TRANSFER, data={"balance": 100}
        )

        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(2022, 1, 1)

        with patch("storage.models.datetime.datetime", new=datetime_mock):
            assert transaction.as_dict() == {
                "data": {"balance": 100},
                "ttl": 1640986200,
                "type": "transfer",
            }
