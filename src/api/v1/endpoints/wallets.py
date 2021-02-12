import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

import storage
from api import dependencies
from api.v1 import schemas

router = APIRouter()


@router.post("/", response_model=schemas.wallet.Wallet)
async def create_wallet(
    *,
    wallet: storage.Wallet = Depends(dependencies.get_wallet),
    wallet_in: schemas.wallet.WalletCreate,
) -> Any:
    """Create a new wallet for a User Account.

    Without an active wallet registered in the system,
    the user is not able to deposit or transfer money.
    Users can have only one wallet per Account.
    """
    await wallet.create_wallet(user_id=str(wallet_in.user_id))

    return schemas.wallet.Wallet(
        id=wallet.wallet_id, balance=str(wallet.DEFAULT_BALANCE)
    )


@router.get("/me", response_model=schemas.wallet.Wallet)
def get_user_wallet() -> Any:  # pragma: no cover
    """Get current user wallet by user id."""
    # todo: not implemented
    raise HTTPException(
        status_code=501, detail="The server does not support this functionality yet"
    )


@router.get("/{wallet_id}/balance", response_model=schemas.wallet.Wallet)
async def get_wallet_balance_by_id(
    wallet: storage.Wallet = Depends(dependencies.get_wallet),
) -> Any:
    """Retrieve a specified wallet balance by wallet id.

    The amount of funds available in a balance that can be sent.
    """
    balance = await wallet.get_balance()
    return schemas.wallet.Wallet(id=wallet.wallet_id, balance=str(balance))


@router.put("/{wallet_id}/deposit", status_code=204)
async def wallet_deposit(
    *,
    wallet: storage.Wallet = Depends(dependencies.get_wallet),
    wallet_in: schemas.wallet.WalletDeposit,
) -> Any:
    """Add money to your Wallet API balance.

    ## Prevent duplicate transfers with idempotency key

    To prevent an operation from being performed more than once,
    The wallet supports passing in a `nonce` with a unique key as the value.

    Multiple PUTs with the same idempotency key won’t result in
    multiple resources being a deposit.

    For example, if a request to initiate a deposit fails due to a network connection
    the issue, you can safely reattempt the request with the same idempotency key
    to ensure that only a single deposit is affected.
    """

    await wallet.atomic_deposit(nonce=wallet_in.nonce, amount=wallet_in.amount_int)


@router.put("/{wallet_id}/transfer/{target_wallet_id}", status_code=204)
async def wallet_transfer(
    *,
    target_wallet_id: uuid.UUID,
    wallet: storage.Wallet = Depends(dependencies.get_wallet),
    wallet_in: schemas.wallet.WalletTransfer,
) -> Any:
    """Initiate a transfer money from one wallet to another inside the system.

    ## Prevent duplicate transfers with idempotency key
    To prevent an operation from being performed more than once,
    The wallet supports passing in a `nonce` with a unique key as the value.

    Multiple PUTs with the same idempotency key won’t result in
    multiple resources being transferred.

    For example, if a request to initiate a transfer fails due to a network connection
    the issue, you can safely reattempt the request with the same idempotency key
    to ensure that only a single transfer is created.
    """

    target_wallet = storage.Wallet(wallet_id=target_wallet_id)

    await wallet.atomic_transfer(
        nonce=wallet_in.nonce, amount=wallet_in.amount_int, target_wallet=target_wallet
    )
