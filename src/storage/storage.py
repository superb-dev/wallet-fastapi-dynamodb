import functools
import logging
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import aiobotocore.client
import boto3.dynamodb.types
import botocore.exceptions

from core.aws import AWSManager
from core.config import settings
from storage import exceptions

logger = logging.getLogger(__name__)

__all__ = ("ItemBuilder", "Storage")


class ItemBuilder:
    def __init__(self, table_name: str, pk_field: str) -> None:
        self.table_name = table_name
        self.pk_field = pk_field

        self.deserializer = boto3.dynamodb.types.TypeDeserializer()
        self.serializer = boto3.dynamodb.types.TypeSerializer()

    def put_idempotency_item(self, pk: str, data: Dict[str, Any]) -> Dict[str, Any]:
        item = {k: self.serialize(v) for k, v in data.items()}
        item[self.pk_field] = self.serialize(pk)

        return {
            "Put": {
                "TableName": self.table_name,
                "Item": item,
                "ConditionExpression": "attribute_not_exists(#key)",
                "ExpressionAttributeNames": {"#key": self.pk_field},
            }
        }

    def update_atomic_increment(
        self, pk: str, update_key: str, amount: int
    ) -> Dict[str, Any]:
        if amount <= 0:
            raise ValueError("Amount can not be lower than 0")

        return {
            "Update": {
                "TableName": self.table_name,
                "Key": {self.pk_field: self.serialize(pk)},
                "UpdateExpression": "SET #key = #key + :n",
                "ExpressionAttributeValues": {
                    ":n": self.serialize(amount),
                },
                "ExpressionAttributeNames": {"#key": update_key},
            }
        }

    def update_atomic_decrement(
        self, pk: str, update_key: str, amount: int
    ) -> Dict[str, Any]:
        if amount <= 0:
            raise ValueError("Amount can not be lower than 0")

        return {
            "Update": {
                "TableName": self.table_name,
                "Key": {self.pk_field: self.serialize(pk)},
                "UpdateExpression": "SET #key = #key - :n",
                "ConditionExpression": "#key >= :n",
                "ExpressionAttributeValues": {
                    ":n": self.serialize(amount),
                },
                "ExpressionAttributeNames": {"#key": update_key},
            }
        }

    def deserialize(self, attr_value: Dict[str, Any]) -> Any:
        return self.deserializer.deserialize(attr_value)

    def serialize(self, attr_value: Any) -> Dict[str, Any]:
        return cast(Dict[str, Any], self.serializer.serialize(attr_value))


def handle_botocore_exceptions(
    warn: Tuple[str, ...] = ()
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def inner_handle_botocore_exceptions(
        func: Callable[..., Any]
    ) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in warn:
                    logger.warning(
                        "Calling function: %r failed: %s", func.__qualname__, str(e)
                    )
                else:
                    logger.exception(
                        "Calling function: %r failed: %s", func.__qualname__, str(e)
                    )
                    raise exceptions.BaseStorageError.from_boto(e) from e

        return wrapper

    return inner_handle_botocore_exceptions


class Storage:
    """DynamoDB Communication class"""

    PK_FIELD = "pk"

    @cached_property
    def client(self) -> aiobotocore.client.AioBaseClient:
        return self._aws.dynamodb

    def __init__(self, aws: AWSManager, table_name: str):
        self._aws: AWSManager = aws
        self.table_name = table_name

        self.item_builder = ItemBuilder(table_name=table_name, pk_field=self.PK_FIELD)

    @handle_botocore_exceptions()
    async def get(
        self, pk: str, fields: Optional[Union[List[str], str]] = None
    ) -> Dict[str, Any]:
        kwargs = {
            "TableName": self.table_name,
            "Key": {self.PK_FIELD: self.item_builder.serialize(pk)},
        }

        if fields:
            if not isinstance(fields, str):
                fields = ",".join(fields)

            kwargs["ProjectionExpression"] = fields

        response = await self.client.get_item(**kwargs)

        if item := response.get("Item"):
            return {
                k: self.item_builder.deserialize(v)
                for k, v in item.items()
                if k != self.PK_FIELD
            }
        else:
            raise exceptions.ObjectNotFoundError(f"Object with {pk=} was not found")

    @handle_botocore_exceptions()
    async def create(
        self,
        pk: str,
        data: Dict[str, Any],
    ) -> None:
        item = self.item_builder.put_idempotency_item(pk=pk, data=data)
        await self.client.put_item(**item["Put"])

    async def table_exists(self) -> bool:
        existing_tables = (await self.client.list_tables())["TableNames"]
        return self.table_name in existing_tables

    @handle_botocore_exceptions()
    async def create_table(
        self,
        read_capacity: int = 1,
        write_capacity: int = 1,
        ttl_attribute: Optional[str] = None,
    ) -> None:
        await self._create_table(read_capacity, write_capacity)

        logger.info("Waiting for table to be created...")
        waiter = self.client.get_waiter("table_exists")

        await waiter.wait(TableName=self.table_name)
        logger.info(f"{self.table_name=} created")

        if ttl_attribute:
            await self._update_time_to_live(ttl_attribute)

    @handle_botocore_exceptions()
    async def drop_table(self) -> None:
        await self.client.delete_table(TableName=self.table_name)

        logger.info(f"Request for {self.table_name} deletion sent.")

        waiter = self.client.get_waiter("table_not_exists")
        await waiter.wait(TableName=self.table_name)

        logger.info(f"Table {self.table_name} dropped")

    async def delete(self, pk: str) -> None:
        try:
            await self._delete(pk=pk)
        except exceptions.ConditionalCheckFailedError as e:
            raise exceptions.ObjectNotFoundError(
                f"Object with {pk} was not found"
            ) from e

    @handle_botocore_exceptions()
    async def transaction_write_items(self, items: List[Dict[str, Any]]) -> None:
        try:
            await self.client.transact_write_items(TransactItems=items)
        except self.client.exceptions.TransactionCanceledException as e:
            logger.warning(f"Conditions failed: {str(e)}")
            # https://docs.amazonaws.cn/en_us/amazondynamodb/latest/APIReference/API_TransactWriteItems.html

            for error in e.response["CancellationReasons"]:

                if error["Code"] == "ConditionalCheckFailed":
                    raise exceptions.ConditionalCheckFailedError(error["Message"])
                if error["Code"] == "TransactionConflict":
                    raise exceptions.ConditionalCheckFailedError(error["Message"])

            # todo: parse other exception types
            raise exceptions.UnknownStorageError() from e

    @handle_botocore_exceptions(warn=("ResourceInUseException",))
    async def _create_table(self, read_capacity: int, write_capacity: int) -> None:
        await self.client.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": self.PK_FIELD, "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": self.PK_FIELD, "AttributeType": "S"}
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": read_capacity,
                "WriteCapacityUnits": write_capacity,
            },
            Tags=[
                {"Key": "Project", "Value": settings.PROJECT_NAME},
            ],
        )

        logger.info(f"Request for {self.table_name=} creation sent.")

    @handle_botocore_exceptions()
    async def _update_time_to_live(self, ttl_attribute: str) -> None:
        await self.client.update_time_to_live(
            TableName=self.table_name,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": ttl_attribute,
            },
        )

    @handle_botocore_exceptions()
    async def _delete(self, pk: str) -> None:
        await self.client.delete_item(
            TableName=self.table_name,
            Key={self.PK_FIELD: self.item_builder.serialize(pk)},
            ConditionExpression="attribute_exists(#key)",
            ExpressionAttributeNames={"#key": self.PK_FIELD},
        )
