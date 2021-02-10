import dataclasses
import datetime
import enum
import uuid
from functools import cached_property
from typing import Any, Dict, Optional, Union

from core.aws import AWSManager
from core.config import settings
from storage import exceptions
from storage.storage import Storage


class TransactionType(enum.Enum):
    DEPOSIT = "deposit"
    TRANSFER = "transfer"
    CREATE = "create"


@dataclasses.dataclass()
class Transaction:
    TRANSACTION_KEY_POSTFIX = "#transaction"

    wallet: str
    nonce: Optional[str]
    type: TransactionType
    data: Dict[str, Any]

    @cached_property
    def ttl(self) -> int:
        now = int(datetime.datetime.utcnow().timestamp())
        return now + int(settings.WALLET_TRANSACTION_TTL)

    @property
    def storage_pk(self) -> str:
        return self.get_storage_pk(wallet=self.wallet, nonce=self.nonce)

    @classmethod
    def get_storage_pk(cls, wallet: str, nonce: Optional[str]) -> str:
        if nonce:
            return f"{wallet}_{nonce}{cls.TRANSACTION_KEY_POSTFIX}"
        else:
            return f"{wallet}{cls.TRANSACTION_KEY_POSTFIX}"

    def as_dict(self) -> Dict[str, Any]:
        return {"ttl": self.ttl, "type": self.type.value, "data": self.data}


class Wallet:
    WALLET_KEY_POSTFIX = "#wallet"
    USER_KEY_POSTFIX = "#user"
    USER_WALLET_KEY = "wallet"

    BALANCE_KEY = "balance"
    DEFAULT_BALANCE = 0

    def __init__(
        self,
        aws: Optional[AWSManager] = None,
        table_name: Optional[str] = None,
        pk: Optional[Union[uuid.UUID, str]] = None,
    ):

        if table_name is None:
            table_name = settings.WALLET_TABLE_NAME

        self._table_name: str = table_name
        self._aws: Optional[AWSManager] = aws

        if isinstance(pk, uuid.UUID):
            pk = str(pk)
        self._pk: Optional[str] = pk

    @property
    def pk(self) -> str:
        """Unique wallet identifier"""
        if not self._pk:
            raise exceptions.WalletDoesNotExistsError("Create wallet first")

        return self._pk

    @cached_property
    def storage(self) -> Storage:
        """Inner CRUD storage"""
        if not self._aws:
            raise ValueError("AWS manager was not set")

        return Storage(aws=self._aws, table_name=self._table_name)

    @property
    def storage_pk(self) -> str:
        """Storage representation of the unique wallet identifier"""
        return f"{self.pk}{self.WALLET_KEY_POSTFIX}"

    async def create_wallet(self, user_id: str) -> None:
        """Create wallet for specified user.
        User can have only one wallet at the system.
        """
        self._pk = str(uuid.uuid4())

        transaction = Transaction(
            wallet=self.pk,
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
                    self.storage.item_builder.put_idempotency_item(
                        pk=transaction.storage_pk, data=transaction.as_dict()
                    ),
                    # create wallet
                    self.storage.item_builder.put_idempotency_item(
                        pk=self.storage_pk,
                        data={self.BALANCE_KEY: self.DEFAULT_BALANCE},
                    ),
                    # create link between wallet and user
                    self.storage.item_builder.put_idempotency_item(
                        pk=user_pk, data={self.USER_WALLET_KEY: self.pk}
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
        """Return actually user balance"""
        # todo: support both strong and eventual consistency
        try:
            response = await self.storage.get(pk=self.storage_pk, fields="balance")
        except exceptions.ObjectNotFoundError:
            raise exceptions.WalletDoesNotExistsError(
                f"Wallet with {self.pk} does not exists"
            )

        return int(response["balance"])

    async def atomic_transfer(
        self, amount: int, target_wallet: "Wallet", nonce: str
    ) -> None:
        """Transfer user funds from one wallet to another"""

        if target_wallet == self:
            raise ValueError("Impossible to transfer funds to self")

        transaction = Transaction(
            wallet=self.pk,
            nonce=nonce,
            type=TransactionType.TRANSFER,
            data={"amount": amount, "target_wallet": target_wallet.pk},
        )

        try:
            await self.storage.transaction_write_items(
                items=[
                    self.storage.item_builder.put_idempotency_item(
                        pk=transaction.storage_pk, data=transaction.as_dict()
                    ),
                    self.storage.item_builder.update_atomic_decrement(
                        pk=self.storage_pk, update_key=self.BALANCE_KEY, amount=amount
                    ),
                    self.storage.item_builder.update_atomic_increment(
                        pk=target_wallet.storage_pk,
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

            # todo: check wallet does not exists
            if e.errors[1]:
                raise exceptions.WalletInsufficientFundsError(str(e.errors[1]))

            if e.errors[2]:
                raise exceptions.WalletDoesNotExistsError(str(e.errors[1]))

            raise exceptions.BaseWalletError(str(e))

    async def atomic_deposit(self, amount: int, nonce: str) -> None:
        transaction = Transaction(
            type=TransactionType.DEPOSIT,
            data={"amount": amount},
            nonce=nonce,
            wallet=self.pk,
        )

        try:
            await self.storage.transaction_write_items(
                items=[
                    self.storage.item_builder.put_idempotency_item(
                        pk=transaction.storage_pk,
                        data=transaction.as_dict(),
                    ),
                    self.storage.item_builder.update_atomic_increment(
                        pk=self.storage_pk,
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
                f"Wallet with {self.pk} does not exists"
            )

    def __repr__(self) -> str:
        return f"Wallet(pk={self._pk})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Wallet):
            return False

        return self._pk == other._pk
