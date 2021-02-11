from typing import Any, List, Optional, Set

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

        return err_cls(
            exception.response.get("Error", {}).get("Message", "Unknown boto error"),
            inner_error=exception,
        )

    def __init__(
        self, *args: Any, inner_error: Optional[botocore.exceptions.ClientError] = None
    ) -> None:
        self.inner_error = inner_error
        super().__init__(*args)


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
    botocore_code = {"TransactionConflictException"}


class TransactionMultipleError(BaseStorageError):
    # https://docs.amazonaws.cn/en_us/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
    botocore_code = {"TransactionCanceledException"}
    errors: List[Optional["BaseStorageError"]]
    code = 409

    def __init__(
        self, *args: Any, inner_error: Optional[botocore.exceptions.ClientError] = None
    ) -> None:
        self.inner_error = inner_error
        super().__init__(*args)
        self.errors = []

        if inner_error:
            for error in inner_error.response["CancellationReasons"]:
                if error["Code"] == "ConditionalCheckFailed":
                    self.errors.append(ConditionalCheckFailedError(error["Message"]))
                elif error["Code"] == "TransactionConflict":
                    self.errors.append(ConditionalCheckFailedError(error["Message"]))
                elif error["Code"] == "None":
                    # keep tracking to help end user to detect which one
                    # of the transaction item is failed
                    self.errors.append(None)
                else:
                    # todo: parse other exception types
                    self.errors.append(UnknownStorageError(error["Message"]))


class BaseWalletError(Exception):
    code = 500


class WalletAlreadyExistsError(BaseWalletError):
    code = 409


class WalletDoesNotExistsError(BaseWalletError):
    code = 404


class WalletTransactionAlreadyRegisteredError(BaseWalletError):
    code = 409


class WalletInsufficientFundsError(BaseWalletError):
    code = 409
