import asyncio
import logging

import core.storage
from core.aws import AWSManager
from core.config import settings

logger = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL.value)
    logger.info("begin")

    async with AWSManager() as aws:
        table_name = settings.WALLET_TABLE_NAME

        storage = core.storage.DynamoDB(aws=aws, table_name=table_name)

        if await storage.table_exists():
            logger.info(f"{table_name=} already exists")
            return

        await storage.create_table(
            read_capacity=settings.AWS_DYNAMODB_READ_CAPACITY,
            write_capacity=settings.AWS_DYNAMODB_WRITE_CAPACITY,
        )
        logger.info("end")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
