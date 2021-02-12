import uuid

import pydantic


class Wallet(pydantic.BaseModel):
    """Actual information about wallet state."""

    id: pydantic.StrictStr = pydantic.Field(description="Wallet unique identifier")
    balance: str = pydantic.Field(
        ..., min_length=1, regex=r"\d+", strict=True, description="Actual balance"
    )


class WalletCreate(pydantic.BaseModel):
    """Wallet properties to receive via API on creation."""

    user_id: uuid.UUID = pydantic.Field(..., description="User identifier")


class WalletAmountWithNonce(pydantic.BaseModel):
    """Idempotency amount request."""

    amount: str = pydantic.Field(
        ...,
        min_length=1,
        regex=r"\d+",
        strict=True,
        description="Amount of funds in 1/million of the currency unit. USD.",
    )
    nonce: str = pydantic.Field(
        ...,
        min_length=8,
        max_length=16,
        strict=True,
        description=(
            "Arbitrary hex representation of the number used "
            "only once to prevent replay of the same requests"
        ),
    )

    @property
    def amount_int(self) -> int:
        """Funds converted to number."""
        return int(self.amount)


class WalletDeposit(WalletAmountWithNonce):
    """Properties to receive via API on deposit."""


class WalletTransfer(WalletAmountWithNonce):
    """Properties to receive via API on transfer."""
