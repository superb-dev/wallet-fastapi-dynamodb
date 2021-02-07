import pytest
from httpx import AsyncClient


class TestWalletAPI:
    async def test_create_wallet(self, client: AsyncClient) -> None:
        pass
        # data = {}
        # response = client.post(
        #     f"{settings.API_V1_STR}/wallets/", json=data,
        # )
        # assert response.status_code == 200
        # content = response.json()
        # assert content["title"] == data["title"]

    async def test_create_wallet_not_valid_user(self, client: AsyncClient) -> None:
        pass

    async def test_create_wallet_already_exists(self, client: AsyncClient) -> None:
        pass

    @pytest.mark.skip(reason="Not implemented")
    async def test_get_user_wallet(self, client: AsyncClient) -> None:
        pass

    async def test_get_wallet_balance(self, client: AsyncClient) -> None:
        pass

    async def test_get_wallet_balance_not_exists(self, client: AsyncClient) -> None:
        pass

    async def test_deposit(self, client: AsyncClient) -> None:
        pass

    async def test_deposit_negative_amount(self, client: AsyncClient) -> None:
        pass

    async def test_deposit_invalid_wallet(self, client: AsyncClient) -> None:
        pass

    async def test_deposit_not_existing_wallet(self, client: AsyncClient) -> None:
        pass

    async def test_deposit_idempotency(self, client: AsyncClient) -> None:
        pass

    async def test_transfer(self, client: AsyncClient) -> None:
        pass

    async def test_transfer_not_existing_wallet(self, client: AsyncClient) -> None:
        pass

    async def test_transfer_not_existing_wallet_target_wallet(
        self, client: AsyncClient
    ) -> None:
        pass

    async def test_transfer_negative_amount(self, client: AsyncClient) -> None:
        pass

    async def test_transfer_idempotency(self, client: AsyncClient) -> None:
        pass
