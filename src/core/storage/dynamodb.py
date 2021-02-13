import functools
import logging
from functools import cached_property
from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Union, cast

import aiobotocore.client
import boto3.dynamodb.types
import botocore.exceptions

from core.aws import AWSManager
from core.config import settings
from core.storage import exceptions

logger = logging.getLogger(__name__)

__all__ = ("DynamoDBItemFactory", "DynamoDB")


class DynamoDBItemFactory:
    """Helper class to build operations request for low-level
    DynamoDB TransactWriteItems API.

    Factory methods of the class will returns an dictionary, these dictionaries can be
    directly passed to botocore transaction methods.

    See Also:
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html
    """

    def __init__(self, table_name: str, pk_attribute_name: str) -> None:
        """Initiates DynamoDB factory.

        Args:
            table_name: main DynamoDB table on which operations will be performed
            pk_attribute_name: name of the primary(partition) key attribute.
        """
        self.table_name = table_name
        self.pk_field = pk_attribute_name

        self.deserializer = boto3.dynamodb.types.TypeDeserializer()
        self.serializer = boto3.dynamodb.types.TypeSerializer()

    def put_idempotency_item(self, pk: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a PutItem operation to create a new item
        conditionally check that item does not exists.

        Args:
             pk: primary key value that define specific item in the table
             data: python dictionary which contains attributes to be
                set to the DynamoDB table.

        Returns:
            Prepared request for the DynamoDB transactional update.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
        """
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
        """Prepare an UpdateItem operation to edit an existing item's
        attributes ensure the item exists at the table.

        Use this action to increment attributes on an existing
        item conditionally.

        Args:
            pk: primary key value that define specific item in the table
            update_key: name of the item attribute which contains value to be updated
            amount: number by which to increment the numeric value

        Returns:
            Prepared request for the DynamoDB transactional update.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
        """
        if amount <= 0:
            msg = f"Unable to decrement. Amount for the {pk} can not be lower than 0"
            logger.error(msg)
            raise ValueError(msg)

        return {
            "Update": {
                "TableName": self.table_name,
                "Key": {self.pk_field: self.serialize(pk)},
                "UpdateExpression": "SET #key = #key + :n",
                "ConditionExpression": "attribute_exists(#key)",
                "ExpressionAttributeValues": {
                    ":n": self.serialize(amount),
                },
                "ExpressionAttributeNames": {"#key": update_key},
            }
        }

    def update_atomic_decrement(
        self, pk: str, update_key: str, amount: int
    ) -> Dict[str, Any]:
        """Prepare an UpdateItem operation to edit an existing item's
        attributes ensure the item exists at the table,
        and attribute value is greater or equal to the amount.

        Use this action to decrement attributes on an existing
        item conditionally.

        Args:
            pk: primary key value that define specific item in the table
            update_key: name of the item attribute which contains value to be updated
            amount: number by which to increment attributes numeric value

        Returns:
            Prepared request for the DynamoDB transactional update.

        Raises:
            ValueError: if amount is invalid

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
        """
        if amount <= 0:
            msg = f"Unable to decrement. Amount for the {pk} can not be lower than 0"
            logger.error(msg)
            raise ValueError(msg)

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

    def deserialize(self, value: Dict[str, Any]) -> Any:
        """The method to deserialize the DynamoDB data types to the native python

        Args:
            value: A DynamoDB value to be deserialized

        Returns:
            Python deserialized value

        See Also:
            https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes

        """
        return self.deserializer.deserialize(value)

    def serialize(self, value: Any) -> Dict[str, Any]:
        """The method to convert python data types to the DynamoDB data types

        Args:
            value: A python value to be serialized to DynamoDB

        Returns:
            A dictionary that represents a dynamoDB data type.

        See Also:
            https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes
        """
        return cast(Dict[str, Any], self.serializer.serialize(value))


def handle_botocore_exceptions(
    warn: Tuple[str, ...] = ()
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap a function and with a decorator which do a conversion from inner
    botocore exception to the BaseStorageError.

    Args:
        warn: List of the botocore codes that should not produce an error to the caller,
            and can safely be skipped with an warning message.

    Returns:
        A decorator that invokes exception handler with the decorated
        function as the wrapper argument and the arguments to wraps() as the
        remaining arguments.

    """

    def inner_handle_botocore_exception_handler(
        func: Callable[..., Any]
    ) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except botocore.exceptions.ClientError as e:
                exc_args = (
                    "Calling function: %r failed: %s",
                    func.__qualname__,
                    str(e),
                )

                if e.response["Error"]["Code"] in warn:
                    logger.warning(*exc_args)
                else:
                    logger.exception(*exc_args)
                    raise exceptions.BaseStorageError.from_boto(e) from e

        return wrapper

    return inner_handle_botocore_exception_handler


class DynamoDB:
    """High level wrapper around Amazon DynamoDB client, supports for CRUD
    operations.

    With DynamoDB, you can create database tables that can store and retrieve any a
    mount of data, and serve any level of request traffic.
    You can scale up or scale down your tables' throughput
    capacity without downtime or performance degradation.

    See Also:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
    """

    # names of the primary key attribute
    PK_ATTRIBUTE_NAME = "pk"

    # default DynamoDB capacity for the create table request
    DEFAULT_READ_CAPACITY: int = 1
    DEFAULT_WRITE_CAPACITY: int = 1

    # upper limit of the transaction write request
    MAX_TRANSACTION_WRITE_BATCH_SIZE: int = 25

    @cached_property
    def _client(self) -> aiobotocore.client.AioBaseClient:
        """A low-level client representing Amazon DynamoDB."""
        return self._aws.dynamodb

    def __init__(self, aws: AWSManager, table_name: str):
        """Initialize new DynamoDB storage.

        Args:
            aws: instance of the AWS services manager.
            table_name: The name of the table to operate on.
        """
        self._aws: AWSManager = aws
        self.table_name = table_name

        self.item_factory = DynamoDBItemFactory(
            table_name=table_name, pk_attribute_name=self.PK_ATTRIBUTE_NAME
        )

    @handle_botocore_exceptions()
    async def get(
        self, pk: str, fields: Optional[Union[List[str], str]] = None
    ) -> Dict[str, Any]:
        """The GetItem operation returns a set of attributes for the
        item with the given primary key.

        Args:
            pk: primary key value that define specific item in the table
            fields: a string or list that identifies one or more attributes
                to retrieve from the table. If no attribute names are specified,
                then all attributes are returned.
        Returns:
            Represents the data for an specified attribute.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.get_item
        """
        kwargs = {
            "TableName": self.table_name,
            "Key": {self.PK_ATTRIBUTE_NAME: self.item_factory.serialize(pk)},
        }

        if fields:
            if not isinstance(fields, str):
                fields = ",".join(fields)

            kwargs["ProjectionExpression"] = fields

        response = await self._client.get_item(**kwargs)

        # If there is no matching item, GetItem does not return any
        # data and there will be no Item element in the response.
        if item := response.get("Item"):
            return {
                k: self.item_factory.deserialize(v)
                for k, v in item.items()
                if k != self.PK_ATTRIBUTE_NAME
            }
        else:
            raise exceptions.ObjectNotFoundError(f"Object with {pk=} was not found")

    @handle_botocore_exceptions()
    async def create(
        self,
        pk: str,
        data: Dict[str, Any],
    ) -> None:
        """Creates a new item with specified primary at the DynamoDB Table.

        If an item that has the same primary key as the new item already exists in
        the specified table, will raise and error.

        Args:
            pk: primary key value that define specific item in the table
            data: python dictionary which contains attributes to be
                set to the DynamoDB table.

        Raises:
            ConditionalCheckFailedError: if item already exists.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
        """
        item = self.item_factory.put_idempotency_item(pk=pk, data=data)
        await self._client.put_item(**item["Put"])

    async def table_exists(self) -> bool:
        """Check that table is presented at the DynamoDB storage.

        Returns:
            True if table already exists at the the DynamoDB storage,
            otherwise returns False
        """
        existing_tables = (await self._client.list_tables())["TableNames"]
        return self.table_name in existing_tables

    @handle_botocore_exceptions()
    async def create_table(
        self,
        read_capacity: int = DEFAULT_READ_CAPACITY,
        write_capacity: int = DEFAULT_WRITE_CAPACITY,
        ttl_attribute: Optional[str] = None,
    ) -> None:
        """Adds a new table to the DynamoDB, and waits until table become active.

        Table names must be unique at the same AWS Region.

        Args:
            read_capacity: The maximum number of strongly consistent reads
                consumed per second.
            write_capacity: The maximum number of writes consumed per second.
            ttl_attribute: The name of the TTL attribute used to store the expiration
                time for items in the table.
                If presented TTL will be enabled to the table.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
        """
        await self._create_table(read_capacity, write_capacity)

        logger.info("Waiting for table to be created...")
        waiter = self._client.get_waiter("table_exists")

        # CreateTable is an asynchronous operation.
        # Upon receiving a CreateTable request, DynamoDB immediately
        # returns a response with a TableStatus of `CREATING`.
        # After the table is created, DynamoDB sets the TableStatus to `ACTIVE`.
        # You can perform read and write operations only on an ACTIVE table.
        await waiter.wait(TableName=self.table_name)

        logger.info(f"{self.table_name=} was created")

        if ttl_attribute:
            await self._enable_time_to_live(ttl_attribute)

    @handle_botocore_exceptions()
    async def drop_table(self) -> None:
        """This operation deletes a table and all of its items.

        DynamoDB might continue to accept data read and write operations,
        such as GetItem and PutItem,
        on a table in the DELETING state until the table deletion is complete.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.delete_table
        """
        await self._client.delete_table(TableName=self.table_name)

        logger.info(f"Request for {self.table_name} deletion sent.")

        # After a DeleteTable request, the specified table is in the DELETING state
        # until DynamoDB completes the deletion.
        # need to wait until finishing
        waiter = self._client.get_waiter("table_not_exists")
        await waiter.wait(TableName=self.table_name)

        logger.info(f"Table {self.table_name} has been dropped")

    async def delete(self, pk: str) -> None:
        """Deletes a single item in a table by primary key.

        Args:
            pk: primary key value that define specific item in the table

        Raises:
            ObjectNotFoundError: if item with specified pk does not exists.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.delete_item
        """

        try:
            await self._delete(pk=pk)
        except exceptions.ConditionalCheckFailedError as e:
            raise exceptions.ObjectNotFoundError(
                f"Object with {pk} was not found"
            ) from e

    @handle_botocore_exceptions()
    async def transaction_write_items(self, items: Collection[Dict[str, Any]]) -> None:
        """Is a synchronous write operation that groups up to 25 action requests.

        The actions are completed atomically so that either all of them succeed,
        or all of them fail. They are defined by the following objects:

        - Put — Initiates a PutItem operation to write a new item.
        - Update — Initiates an UpdateItem operation to update an existing item.
        - Delete — Initiates a DeleteItem operation to delete an existing item.
        - ConditionCheck — Applies a condition to an item that is not being modified
            by the transaction.

        Args:
            items: An ordered array of up to 25 transaction write items

        Returns:
            Inner response of the botocore transact_write_items

        Raises:
            TransactionMultipleError: on most (except service connection errors
                or table not operable) of the failures will return this error

        """

        if len(items) > self.MAX_TRANSACTION_WRITE_BATCH_SIZE:
            msg = (
                f"There are more than {self.MAX_TRANSACTION_WRITE_BATCH_SIZE} "
                "requests in the batch."
            )
            logger.error(msg)
            raise ValueError(msg)

        await self._client.transact_write_items(TransactItems=items)

    @handle_botocore_exceptions(warn=("ResourceInUseException",))
    async def _create_table(self, read_capacity: int, write_capacity: int) -> None:
        """Adds a new table to the DynamoDB, and will NOT blocks until table
        become active.

        Table names must be unique at the same AWS Region.

        Args:
            read_capacity: The maximum number of strongly consistent reads
                consumed per second.
            write_capacity: The maximum number of writes consumed per second.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
        """
        await self._client.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": self.PK_ATTRIBUTE_NAME, "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": self.PK_ATTRIBUTE_NAME, "AttributeType": "S"}
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
    async def _enable_time_to_live(self, attribute_name: str) -> None:
        """Enables Time to Live (TTL) for the current table.

        Args:
            attribute_name: The name of the TTL attribute used to
                store the expiration time for items in the table.
        """
        await self._client.update_time_to_live(
            TableName=self.table_name,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": attribute_name,
            },
        )

    @handle_botocore_exceptions()
    async def _delete(self, pk: str) -> None:
        """Deletes a single item in a table by primary key.

        Args:
            pk: primary key value that define specific item in the table

        Raises:
            ConditionalCheckFailedError: if item does not exists.

        See Also:
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.delete_item
        """
        await self._client.delete_item(
            TableName=self.table_name,
            Key={self.PK_ATTRIBUTE_NAME: self.item_factory.serialize(pk)},
            ConditionExpression="attribute_exists(#key)",
            ExpressionAttributeNames={"#key": self.PK_ATTRIBUTE_NAME},
        )
