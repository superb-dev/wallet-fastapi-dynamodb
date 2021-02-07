import secrets

from pydantic import BaseSettings


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8

    PROJECT_NAME: str = "wallet"

    # The secret access key for your AWS account.
    AWS_ACCESS_KEY_ID: str = "secret_key"
    # # The access key for your AWS account.
    AWS_SECRET_ACCESS_KEY: str = "access_key"
    # The name of the region associated with the AWS client.
    AWS_REGION_NAME: str = "us-west-2"
    # The DYNAMODB endpoint (only for testing) keep it empty for production
    AWS_DYNAMODB_ENDPOINT_URL: str = "http://127.0.0.1:8001"  # "http://dynamodb:8000"

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


settings = Settings()
