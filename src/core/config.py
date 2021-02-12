import enum

import pydantic


class LogLevel(str, enum.Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(pydantic.BaseSettings):
    API_V1_STR: str = "/api/v1"

    PROJECT_NAME: str = "wallet"
    PROJECT_VERSION: str = "1.0.0"

    HOST: str = "127.0.0.1"
    # Main API communication endpoint
    PORT: int = 5000
    # logging messages which have severity level or higher will be emitted
    LOG_LEVEL: LogLevel = LogLevel.INFO

    # The secret access key for your AWS account.
    AWS_ACCESS_KEY_ID: str = "secret_key"
    # # The access key for your AWS account.
    AWS_SECRET_ACCESS_KEY: str = "access_key"
    # The name of the region associated with the AWS client.
    AWS_REGION_NAME: str = "us-west-2"
    # The DYNAMODB endpoint (only for testing) keep it empty for production
    AWS_DYNAMODB_ENDPOINT_URL: pydantic.AnyHttpUrl = pydantic.Field(
        "http://127.0.0.1:8001"
    )  # "http://dynamodb:8000"

    # represents one strongly consistent read per second, or two eventually consistent
    # reads per second, for an item up to 4 KB in size.
    AWS_DYNAMODB_READ_CAPACITY: int = 10

    # A write capacity unit represents one write per second,
    # for an item up to 1 KB in size.
    AWS_DYNAMODB_WRITE_CAPACITY: int = 10

    # name of the main database table
    WALLET_TABLE_NAME: str = "wallet"

    # default transaction ttl
    WALLET_TRANSACTION_TTL: int = 30 * 60  # 30 minutes

    # An integer representing the maximum number of total attempts
    # that will be made on a single request.
    AWS_CLIENT_MAX_ATTEMPTS: int = 1

    # The time in seconds till a timeout exception is thrown when
    # attempting to make a connection
    AWS_CLIENT_CONNECT_TIMEOUT: float = 1
    # The time in seconds till a timeout exception is thrown when
    # attempting to read from a connection.
    AWS_CLIENT_READ_TIMEOUT: float = 0.5
    # The maximum number of connections to keep in a connection pool.
    AWS_CLIENT_MAX_POOL_CONNECTIONS: int = 50
    # Whether parameter validation should occur when serializing requests.
    AWS_CLIENT_PARAMETER_VALIDATION: bool = False

    class Config:
        case_sensitive = True
        env_prefix = "WALLET_"


settings = Settings()
