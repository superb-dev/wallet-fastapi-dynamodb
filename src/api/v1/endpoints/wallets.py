import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

import schemas.wallet
from api import dependencies
from storage.exceptions import ConditionalCheckFailedError
from storage.models import Wallet

router = APIRouter()


@router.post("/", response_model=schemas.wallet.Wallet)
async def create_wallet(
    *,
    wallet: Wallet = Depends(dependencies.get_wallet),
    wallet_in: schemas.wallet.WalletCreate,
) -> Any:
    """
    Create new wallet.
    """

    try:
        await wallet.create_wallet(user_id=str(wallet_in.user_id))
    except ConditionalCheckFailedError:
        raise HTTPException(
            status_code=400,
            detail="The wallet with for this user id already exists in the system.",
        )

    return schemas.wallet.Wallet(id=wallet.pk, balance=str(wallet.DEFAULT_BALANCE))


@router.get("/me", response_model=schemas.wallet.Wallet)
def get_user_wallet() -> Any:
    """
    Get a specific wallet by user id.
    """
    raise HTTPException(
        status_code=501, detail="The  server does not support this functionality yet"
    )


@router.get("/{wallet_id}", response_model=schemas.wallet.Wallet)
async def get_wallet_balance_by_id(
    wallet: Wallet = Depends(dependencies.get_wallet),
) -> Any:
    """
    Get a specified wallet balance by wallet id.
    """
    balance = await wallet.get_balance()
    return schemas.wallet.Wallet(id=wallet.pk, balance=str(balance))


@router.put(
    "/{wallet_id}/deposit",
)
async def wallet_deposit(
    *,
    wallet: Wallet = Depends(dependencies.get_wallet),
    wallet_in: schemas.wallet.WalletDeposit,
) -> Any:
    """
    Deposit funds to a wallet.
    """

    try:
        await wallet.atomic_deposit(nonce=wallet_in.nonce, amount=wallet_in.int_amount)
    except ConditionalCheckFailedError:
        raise HTTPException(
            status_code=400,
            detail="The operation with this nonce already exists.",
        )


@router.put(
    "/{wallet_id}/transfer/{target_wallet_id}", response_model=schemas.wallet.Wallet
)
async def wallet_transfer(
    *,
    target_wallet_id: uuid.UUID,
    wallet: Wallet = Depends(dependencies.get_wallet),
    wallet_in: schemas.wallet.WalletTransfer,
) -> Any:
    """
    Transfer money from one wallet to another.
    """
    target_wallet = Wallet(pk=target_wallet_id)

    await wallet.atomic_transfer(
        nonce=wallet_in.nonce, amount=wallet_in.int_amount, target_wallet=target_wallet
    )

    # todo: raise not found error
    if not wallet:
        raise HTTPException(
            status_code=404,
            detail="The wallet with this wallet id does not exist in the system",
        )
