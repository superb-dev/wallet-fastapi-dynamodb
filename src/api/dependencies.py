import uuid
from typing import Optional

import fastapi

import core.aws
import core.storage
import crud.wallet
from core.config import settings


def get_aws_manager() -> core.aws.AWSManager:
    """Get a global instance of AWS services manager."""
    from api.application import aws_manager

    return aws_manager


def get_wallet_storage(
    wallet_id: Optional[uuid.UUID] = None,
    aws: core.aws.AWSManager = fastapi.Depends(get_aws_manager),
) -> crud.wallet.Wallet:
    """Initiates a instance of the wallet storage by wallet identifier.


    If wallet_id is empty, then only create operation is available.

    Args:
        wallet_id: public identifier of payment address
        aws: instance of the AWS services manager.

    Returns:
        an instance of the user wallet to save or read data from persistent storage.
    """

    return crud.wallet.Wallet(
        storage=core.storage.DynamoDB(aws=aws, table_name=settings.WALLET_TABLE_NAME),
        wallet_id=wallet_id,
    )
