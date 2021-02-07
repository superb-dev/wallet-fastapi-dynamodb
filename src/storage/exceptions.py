class BaseStorageError(Exception):
    code = 500


class UnknownStorageError(Exception):
    code = 500


class ConditionalCheckFailedError(BaseStorageError):
    code = 409


class TransactionConflictError(BaseStorageError):
    code = 409
