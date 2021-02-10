from typing import Set

import botocore.exceptions


class BaseStorageError(Exception):
    code = 500
    botocore_code: Set[str] = set()

    @classmethod
    def from_boto(
        cls, exception: botocore.exceptions.ClientError
    ) -> "BaseStorageError":
        code = exception.response["Error"]["Code"]

        for err_cls in cls.__subclasses__():
            if code in err_cls.botocore_code:
                break
        else:
            err_cls = UnknownStorageError

        return err_cls(exception.response["Error"]["Message"])


class UnknownStorageError(BaseStorageError):
    code = 500


class ObjectNotFoundError(BaseStorageError):
    botocore_code = {"ResourceNotFoundException"}
    code = 404


class ConditionalCheckFailedError(BaseStorageError):
    botocore_code = {"ConditionalCheckFailedException"}
    code = 409


class ValidationError(BaseStorageError):
    botocore_code = {"ValidationException"}
    code = 500


class TransactionConflictError(BaseStorageError):
    code = 409
