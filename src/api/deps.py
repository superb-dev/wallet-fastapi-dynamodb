import uuid

from fastapi import Depends

from core.aws import AWSManager
from storage.models import Wallet


def get_aws_manager() -> AWSManager:
    from api.application import aws_manager

    return aws_manager


def get_wallet(wallet_id: uuid.UUID = None, aws: AWSManager = Depends(get_aws_manager)):
    return Wallet(aws=aws, pk=wallet_id)
