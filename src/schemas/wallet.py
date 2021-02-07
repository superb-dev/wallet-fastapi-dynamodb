import uuid

import pydantic

# TODO: Comments


# Shared properties
class Wallet(pydantic.BaseModel):
    id: str
    balance: int


# Properties to receive via API on creation
class WalletCreate(pydantic.BaseModel):
    user_id: uuid.UUID


# Properties to receive via API on update
class WalletTransfer(pydantic.BaseModel):
    amount: pydantic.conint(gt=0)
    nonce: pydantic.constr(min_length=8, max_length=16)


# Additional properties to return via API
class WalletDeposit(pydantic.BaseModel):
    amount: pydantic.conint(gt=0)
    nonce: pydantic.constr(min_length=8, max_length=16)
