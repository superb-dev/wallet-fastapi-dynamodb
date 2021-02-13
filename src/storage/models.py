import dataclasses
import datetime
import enum
import functools
import uuid
from typing import Any, Dict, Optional, Union

import core.aws
import storage
from core.config import settings
from storage import exceptions


class TransactionType(enum.Enum):
    DEPOSIT = "deposit"
    TRANSFER = "transfer"
    CREATE = "create"


@enum.unique
class StorageKeyPostfix(enum.Enum):
    """Unique storage postfix to be sure that entities do not cross"""

    TRANSACTION = "#transaction"
    WALLET = "#wallet"
    USER = "#user"


@dataclasses.dataclass()
class Transaction:
    """Transaction an history instance of depositing or transferring amount of money."""

    wallet_id: str
    nonce: Optional[str]
    type: TransactionType
    data: Dict[str, Any]

    TRANSACTION_KEY_POSTFIX: str = dataclasses.field(
        default=StorageKeyPostfix.TRANSACTION.value, init=False
    )

    @functools.cached_property
    def ttl(self) -> int:
        """Time to store at the the persistent storage,
        after that period can be moved to the data lake.
        """
        now = int(datetime.datetime.utcnow().timestamp())
        return now + int(settings.WALLET_TRANSACTION_TTL)

    @property
    def unique_id(self) -> str:
        """Unique identifier transaction identifier at the entire system"""
        return self.get_unique_id(wallet=self.wallet_id, nonce=self.nonce)

    @classmethod
    def get_unique_id(cls, wallet: str, nonce: Optional[str]) -> str:
        """Create new unique identifier transaction identifier."""
        if nonce:
            return f"{wallet}_{nonce}{cls.TRANSACTION_KEY_POSTFIX}"
        else:
            return f"{wallet}{cls.TRANSACTION_KEY_POSTFIX}"

    def as_dict(self) -> Dict[str, Any]:
        return {"ttl": self.ttl, "type": self.type.value, "data": self.data}


class Wallet:
    """Individual user wallet class to save or read data from persistent storage.

    This class contains useful functionality to check user balance,
    transfer funds or deposit money.
    """

    WALLET_KEY_POSTFIX: str = StorageKeyPostfix.WALLET.value
    USER_KEY_POSTFIX: str = StorageKeyPostfix.USER.value

    USER_WALLET_KEY = "wallet"
    BALANCE_KEY = "balance"
    DEFAULT_BALANCE = 0

    def __init__(
        self,
        aws: Optional[core.aws.AWSManager] = None,
        table_name: Optional[str] = None,
        wallet_id: Optional[Union[uuid.UUID, str]] = None,
    ):
        if table_name is None:
            table_name = settings.WALLET_TABLE_NAME

        self._table_name: str = table_name
        self._aws = aws

        if isinstance(wallet_id, uuid.UUID):
            wallet_id = str(wallet_id)
        self._wallet_id: Optional[str] = wallet_id

    @property
    def wallet_id(self) -> str:
        """Unique wallet identifier"""
        if not self._wallet_id:
            raise exceptions.WalletDoesNotExistsError("Create wallet first")

        return self._wallet_id

    @functools.cached_property
    def storage(self) -> storage.DynamoDB:
        """Inner CRUD storage"""
        if not self._aws:
            raise ValueError("AWS manager was not set")

        return storage.DynamoDB(aws=self._aws, table_name=self._table_name)

    @property
    def unique_id(self) -> str:
        """Representation of the unique identifier at the entire system."""
        return f"{self.wallet_id}{self.WALLET_KEY_POSTFIX}"

    @classmethod
    def generate_wallet_id(cls) -> str:
        """Generates unique wallet primary key."""
        return str(uuid.uuid4())

    async def create_wallet(self, user_id: str) -> None:
        """Create wallet for specified user.

        User can have only one wallet at the system, if use already has wallet will
        raise already exists error
        """
        self._wallet_id = self.generate_wallet_id()

        transaction = Transaction(
            wallet_id=self.wallet_id,
            type=TransactionType.CREATE,
            data={"amount": self.DEFAULT_BALANCE},
            nonce=None,
        )

        # todo: create separate user storage
        user_pk = f"{user_id}{self.USER_KEY_POSTFIX}"

        try:
            await self.storage.transaction_write_items(
                items=[
                    # create transaction record
                    self.storage.item_factory.put_idempotency_item(
                        pk=transaction.unique_id, data=transaction.as_dict()
                    ),
                    # create wallet
                    self.storage.item_factory.put_idempotency_item(
                        pk=self.unique_id,
                        data={self.BALANCE_KEY: self.DEFAULT_BALANCE},
                    ),
                    # create link between wallet and user
                    self.storage.item_factory.put_idempotency_item(
                        pk=user_pk, data={self.USER_WALLET_KEY: self.wallet_id}
                    ),
                ]
            )
        except exceptions.TransactionMultipleError as e:
            if e.errors[0]:
                raise exceptions.WalletTransactionAlreadyRegisteredError(
                    str(e.errors[0])
                )

            raise exceptions.WalletAlreadyExistsError(
                f"Wallet already exists for the user {user_pk}"
            )

    async def get_balance(self) -> int:
        """Reads actual user balance"""
        # todo: support both strong and eventual consistency
        try:
            response = await self.storage.get(pk=self.unique_id, fields="balance")
        except exceptions.ObjectNotFoundError:
            raise exceptions.WalletDoesNotExistsError(
                f"Wallet with {self.wallet_id=} does not exists"
            )

        return int(response["balance"])

    async def atomic_transfer(
        self, amount: int, target_wallet: "Wallet", nonce: str
    ) -> None:
        """Transfer user funds from one wallet to another.

        To initiate a transfer, the user need to specify the source and destination
        of the funding source.

        Also, transfer amount can not be greater than wallet available funds.
        Nonce argument is required to guarantee idempotency.
        """

        if target_wallet == self:
            raise ValueError("Impossible to transfer funds to self")

        transaction = Transaction(
            wallet_id=self.wallet_id,
            nonce=nonce,
            type=TransactionType.TRANSFER,
            data={"amount": amount, "target_wallet": target_wallet.wallet_id},
        )

        try:
            await self.storage.transaction_write_items(
                items=[
                    self.storage.item_factory.put_idempotency_item(
                        pk=transaction.unique_id, data=transaction.as_dict()
                    ),
                    self.storage.item_factory.update_atomic_decrement(
                        pk=self.unique_id, update_key=self.BALANCE_KEY, amount=amount
                    ),
                    self.storage.item_factory.update_atomic_increment(
                        pk=target_wallet.unique_id,
                        update_key=self.BALANCE_KEY,
                        amount=amount,
                    ),
                ]
            )
        except exceptions.TransactionMultipleError as e:
            if e.errors[0]:
                raise exceptions.WalletTransactionAlreadyRegisteredError(
                    f"Transaction with nonce {nonce} already registered."
                )

            if e.errors[1]:
                raise exceptions.WalletInsufficientFundsError(
                    "Wallet has insufficient funds to "
                    f"complete operation: {str(e.errors[1])}"
                )

            if e.errors[2]:
                raise exceptions.WalletDoesNotExistsError(
                    f"Wallet does not exists: {str(e.errors[2])}"
                )

            raise exceptions.BaseWalletError(str(e))

    async def atomic_deposit(self, amount: int, nonce: str) -> None:
        """Deposit specified amount to the wallet,
        used nonce to guarantee idempotency.
        """
        transaction = Transaction(
            type=TransactionType.DEPOSIT,
            data={"amount": amount},
            nonce=nonce,
            wallet_id=self.wallet_id,
        )

        try:
            await self.storage.transaction_write_items(
                items=[
                    self.storage.item_factory.put_idempotency_item(
                        pk=transaction.unique_id,
                        data=transaction.as_dict(),
                    ),
                    self.storage.item_factory.update_atomic_increment(
                        pk=self.unique_id,
                        update_key=self.BALANCE_KEY,
                        amount=amount,
                    ),
                ]
            )
        except exceptions.TransactionMultipleError as e:
            if e.errors[0]:
                raise exceptions.WalletTransactionAlreadyRegisteredError(
                    f"Transaction with nonce {nonce} already registered."
                )

            raise exceptions.WalletDoesNotExistsError(
                f"Wallet with {self.wallet_id=} does not exists"
            )

    def __repr__(self) -> str:
        return f"Wallet(pk={self._wallet_id})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Wallet):
            return False

        return self._wallet_id == other._wallet_id
