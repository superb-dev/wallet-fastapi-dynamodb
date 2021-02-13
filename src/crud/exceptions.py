class BaseWalletError(Exception):
    """Base class for all wallet exceptions."""

    code = 500


class WalletAlreadyExistsError(BaseWalletError):
    """Wallet is already exists in the storage."""

    code = 409


class WalletNotFoundError(BaseWalletError):
    """Wallet was not found."""

    code = 404


class WalletTransactionAlreadyRegisteredError(BaseWalletError):
    """Transaction is already registered for another operation."""

    code = 409


class WalletInsufficientFundsError(BaseWalletError):
    """An Insufficient Funds error means that the user wallet does not have enough
    funds to complete the operation."""

    code = 409
