import uuid
from typing import Optional

import fastapi

import core.aws
import storage


def get_aws_manager() -> core.aws.AWSManager:
    from api.application import aws_manager

    return aws_manager


def get_wallet(
    wallet_id: Optional[uuid.UUID] = None,
    aws: core.aws.AWSManager = fastapi.Depends(get_aws_manager),
) -> storage.Wallet:
    return storage.Wallet(aws=aws, wallet_id=wallet_id)
