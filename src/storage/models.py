import dataclasses
import datetime
import enum
import uuid
from functools import cached_property
from typing import Optional, Union

from core.aws import AWSManager
from core.config import settings
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
    data: dict

    @cached_property
    def ttl(self) -> int:
        return (
            int(datetime.datetime.utcnow().timestamp())
            + settings.WALLET_TRANSACTION_TTL
        )

    @property
    def storage_pk(self):
        if self.nonce:
            return f"{self.wallet}_{self.nonce}{self.TRANSACTION_KEY_POSTFIX}"
        else:
            return f"{self.wallet}{self.TRANSACTION_KEY_POSTFIX}"

    def as_dict(self):
        return {"ttl": self.ttl, "type": self.type.value, "data": self.data}


class Wallet:
    WALLET_KEY_POSTFIX = "#wallet"
    USER_KEY_POSTFIX = "#user"
    USER_WALLET_KEY = "wallet"

    BALANCE_KEY = "balance"
    DEFAULT_BALANCE = 0

    def __init__(
        self,
        aws: AWSManager = None,
        table_name: str = None,
        pk: Union[uuid.UUID, str] = None,
    ):

        if table_name is None:
            table_name = settings.WALLET_TABLE_NAME

        self._table_name = table_name
        self._aws = aws

        if isinstance(pk, uuid.UUID):
            pk = str(pk)
        self._pk = pk

    @property
    def pk(self) -> str:
        """Unique wallet identifier"""
        if not self._pk:
            raise ValueError("Create wallet first")
        return self._pk

    @cached_property
    def storage(self) -> Optional[Storage]:
        if self._aws:
            return Storage(aws=self._aws, table_name=self._table_name)

    @property
    def storage_pk(self) -> str:
        """Storage representation of the unique wallet identifier"""
        return f"{self.pk}{self.WALLET_KEY_POSTFIX}"

    async def create_wallet(self, user_id: uuid.UUID) -> None:
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

        # todo: return custom errors exceptions based, such as WalletExists,
        # UserAlreadyHasWallet, nonce
        await self.storage.transaction_write_items(
            items=[
                # create transaction record
                self.storage.item_builder.put_idempotency_item(
                    pk=transaction.storage_pk, data=transaction.as_dict()
                ),
                # create wallet
                self.storage.item_builder.put_idempotency_item(
                    pk=self.storage_pk, data={self.BALANCE_KEY: self.DEFAULT_BALANCE}
                ),
                # create link between wallet and user
                self.storage.item_builder.put_idempotency_item(
                    pk=user_pk, data={self.USER_WALLET_KEY: self.pk}
                ),
            ]
        )

    async def get_balance(self) -> int:
        """Return actually user balance"""
        # todo: support both strong and eventual consistency
        response = await self.storage.get(pk=self.storage_pk, fields="balance")

        return int(response["balance"])

    async def atomic_transfer(self, nonce: str, amount: int, target_wallet: "Wallet"):
        """Transfer user funds from one wallet to another"""

        if target_wallet == self:
            raise ValueError("Impossible to transfer funds to self")

        transaction = Transaction(
            wallet=self.pk,
            nonce=nonce,
            type=TransactionType.DEPOSIT,
            data={"amount": amount, "target_wallet": target_wallet.pk},
        )

        response = await self.storage.transaction_write_items(
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

        return response

    async def atomic_deposit(self, nonce: str, amount: int) -> None:
        transaction = Transaction(
            type=TransactionType.DEPOSIT,
            data={"amount": amount},
            nonce=nonce,
            wallet=self.pk,
        )

        response = await self.storage.transaction_write_items(
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

        return response

    def __repr__(self):
        return f"Wallet(pk={self.pk})"

    def __eq__(self, other: "Wallet"):
        if not isinstance(other, Wallet):
            return False

        return self.pk == other.pk
