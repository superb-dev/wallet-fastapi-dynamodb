import logging
from functools import cached_property
from typing import Any, List, Union

import boto3.dynamodb.types
import botocore.exceptions

from core.aws import AWSManager
from core.config import settings
from storage.exceptions import ConditionalCheckFailedError, UnknownStorageError

logger = logging.getLogger(__name__)


class ItemBuilder:
    def __init__(self, table_name: str, pk_field: str):
        self.table_name = table_name
        self.pk_field = pk_field

        self.deserializer = boto3.dynamodb.types.TypeDeserializer()
        self.serializer = boto3.dynamodb.types.TypeSerializer()

    def put_idempotency_item(self, pk: str, data: dict) -> dict:
        data[self.pk_field] = pk

        return {
            "Put": {
                "TableName": self.table_name,
                "Item": {k: self.serialize(v) for k, v in data.items()},
                "ConditionExpression": "attribute_not_exists(#key)",
                "ExpressionAttributeNames": {"#key": self.pk_field},
            }
        }

    def update_atomic_increment(self, pk: str, update_key: str, amount: int) -> dict:
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

    def update_atomic_decrement(self, pk, update_key: str, amount: int) -> dict:
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

    def deserialize(self, attr_value: dict) -> Any:
        return self.deserializer.deserialize(attr_value)

    def serialize(self, attr_value: Any) -> dict:
        return self.serializer.serialize(attr_value)


class Storage:
    PK_FIELD = "pk"

    @cached_property
    def client(self):
        return self._aws.dynamodb

    def __init__(
        self,
        aws: AWSManager,
        table_name: str = None,
    ):
        self._aws: AWSManager = aws
        self.table_name = table_name

        self.item_builder = ItemBuilder(table_name=table_name, pk_field=self.PK_FIELD)

    async def get(self, pk: str, fields: Union[List[str], str] = None) -> dict:
        kwargs = {
            "TableName": self.table_name,
            "Key": {self.PK_FIELD: self.item_builder.serialize(pk)},
        }

        if fields:
            if not isinstance(fields, str):
                fields = ",".join(fields)

            kwargs["ProjectionExpression"] = fields

        response = await self.client.get_item(**kwargs)

        return {
            k: self.item_builder.deserialize(v) for k, v in response["Item"].items()
        }

    async def table_exists(self):
        existing_tables = (await self.client.list_tables())["TableNames"]
        return self.table_name in existing_tables

    async def create_table(
        self, read_capacity: int = 1, write_capacity: int = 1, ttl_attribute: str = None
    ) -> None:
        try:
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
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ResourceInUseException":
                logger.warning("Table already exists", exc_info=e)
            else:
                raise
        else:
            logger.info(f"Request for {self.table_name=} creation sent.")

            logger.info("Waiting for table to be created...")
            waiter = self.client.get_waiter("table_exists")
            await waiter.wait(TableName=self.table_name)
            logger.info(f"{self.table_name=} created")

            if ttl_attribute:
                logger.info(f"Enable {self.table_name=} table ttl")

                try:
                    await self.client.update_time_to_live(
                        TableName=self.table_name,
                        TimeToLiveSpecification={
                            "Enabled": True,
                            "AttributeName": ttl_attribute,
                        },
                    )
                except botocore.exceptions.ClientError as e:
                    logger.error("Error setting TTL on table", exc_info=e)
                    raise

    async def delete_table(self) -> None:
        await self.client.delete_table(TableName=self.table_name)

        logger.info(f"Request for {self.table_name} deletion sent.")

        waiter = self.client.get_waiter("table_not_exists")
        await waiter.wait(TableName=self.table_name)

        logger.info(f"Table {self.table_name} dropped")

    async def delete(self, pk: str):
        await self.client.delete_item(
            TableName=self.table_name,
            Key={self.PK_FIELD: self.item_builder.serialize(pk)},
        )

    async def transaction_write_items(self, items):
        try:
            return await self.client.transact_write_items(TransactItems=items)
        except self.client.exceptions.TransactionCanceledException as e:
            logger.warning(f"Conditions failed: {str(e)}")
            # https://docs.amazonaws.cn/en_us/amazondynamodb/latest/APIReference/API_TransactWriteItems.html

            for error in e.response["CancellationReasons"]:
                if error["Code"] == "ConditionalCheckFailed":
                    raise ConditionalCheckFailedError(error["Message"])
                if error["Code"] == "TransactionConflict":
                    raise ConditionalCheckFailedError(error["Message"])

            # todo: parse other exception types
            raise UnknownStorageError()

        except botocore.exceptions.ClientError as e:
            logger.exception(f"Request failed: {str(e)}")
            raise UnknownStorageError()
