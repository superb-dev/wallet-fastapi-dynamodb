import datetime
import uuid

import pytest
from httpx import AsyncClient

from core.config import settings
from storage.models import Wallet

pytestmark = pytest.mark.asyncio


class TestWalletAPI:
    def generate_nonce(self) -> str:
        return hex(int(datetime.datetime.utcnow().timestamp() * 1000))

    async def test_create_wallet(self, client: AsyncClient) -> None:
        user_id = str(uuid.uuid4())
        response = await client.post(
            f"{settings.API_V1_STR}/wallets/",
            json={"user_id": user_id},
        )
        assert response.status_code == 200
        data = response.json()

        assert data["balance"] == "0"
        assert type(data["id"]) is str

    async def test_create_wallet_already_exists(self, client: AsyncClient) -> None:
        user_id = str(uuid.uuid4())
        response = await client.post(
            f"{settings.API_V1_STR}/wallets/",
            json={"user_id": user_id},
        )
        assert response.status_code == 200
        data = response.json()

        assert data["balance"] == "0"
        assert type(data["id"]) is str

        response = await client.post(
            f"{settings.API_V1_STR}/wallets/",
            json={"user_id": user_id},
        )
        assert response.status_code == 409

    async def test_get_wallet_balance(self, client: AsyncClient, wallet) -> None:
        await wallet.atomic_deposit(500, nonce="test_get_wallet_balance")
        response = await client.get(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/balance"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["balance"] == "500"

    async def test_get_wallet_balance_not_exists(self, client: AsyncClient) -> None:
        wallet_id = Wallet.generate_wallet_id()
        response = await client.get(
            f"{settings.API_V1_STR}/wallets/{wallet_id}/balance"
        )
        assert response.status_code == 404
        data = response.json()

        assert data == {
            "detail": f"Wallet with self.wallet_id='{wallet_id}' does not exists"
        }

    async def test_deposit(self, client: AsyncClient, wallet) -> None:
        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/deposit",
            json={"amount": "1000", "nonce": "test_deposit"},
        )
        assert response.status_code == 204, response.json()
        assert await wallet.get_balance() == 1000

    @pytest.mark.parametrize("invalid_amount", ["-1000", -100, "str", 10 ** 21])
    async def test_deposit_negative_amount(
        self, client: AsyncClient, wallet, invalid_amount
    ) -> None:
        nonce = self.generate_nonce()
        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/deposit",
            json={"amount": invalid_amount, "nonce": nonce},
        )
        assert response.status_code == 422, response.json()

        assert await wallet.get_balance() == 0

    async def test_deposit_invalid_wallet(self, client: AsyncClient) -> None:
        response = await client.put(
            f"{settings.API_V1_STR}/wallets/invalid-wallet/deposit",
            json={"amount": "str", "nonce": "test_deposit"},
        )
        assert response.status_code == 422

    async def test_deposit_not_existing_wallet(self, client: AsyncClient) -> None:
        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{Wallet.generate_wallet_id()}/deposit",
            json={"amount": "1000", "nonce": "not_existing"},
        )
        assert response.status_code == 404, response.json()

    async def test_deposit_idempotency(self, client: AsyncClient, wallet) -> None:
        nonce = self.generate_nonce()
        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/deposit",
            json={"amount": "1000", "nonce": nonce},
        )

        assert response.status_code == 204, response.json()

        assert await wallet.get_balance() == 1000

        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/deposit",
            json={"amount": "1000", "nonce": nonce},
        )

        assert response.status_code == 409

        assert response.json() == {
            "detail": f"Transaction with nonce {nonce} already registered."
        }

    async def test_transfer(self, client: AsyncClient, wallet, wallet_factory) -> None:
        nonce = self.generate_nonce()

        target_wallet = await wallet_factory()

        await wallet.atomic_deposit(1000, "test_api_transfer")
        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/"
            f"transfer/{target_wallet.wallet_id}/",
            json={"amount": "100", "nonce": nonce},
        )

        assert response.status_code == 204, response.json()

        assert await wallet.get_balance() == 900
        assert await target_wallet.get_balance() == 100

    async def test_transfer_from_not_existing_wallet(
        self, client: AsyncClient, wallet
    ) -> None:

        nonce = self.generate_nonce()

        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{Wallet.generate_wallet_id()}/"
            f"transfer/{wallet.wallet_id}/",
            json={"amount": "100", "nonce": nonce},
        )

        assert response.status_code == 409, response.json()

        assert response.json() == {
            "detail": "Wallet has insufficient funds to complete operation: "
            "The conditional request failed"
        }
        assert await wallet.get_balance() == 0

    async def test_transfer_not_existing_wallet_target_wallet(
        self, client: AsyncClient, wallet
    ) -> None:
        nonce = self.generate_nonce()

        await wallet.atomic_deposit(1000, nonce)

        nonce = self.generate_nonce()

        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/"
            f"transfer/{Wallet.generate_wallet_id()}/",
            json={"amount": "100", "nonce": nonce},
        )

        assert response.status_code == 404, response.json()

        assert response.json() == {
            "detail": "Wallet does not exists: The conditional request failed"
        }
        assert await wallet.get_balance() == 1000

    @pytest.mark.parametrize("invalid_amount", ["-1000", -100, "str", 10 ** 21])
    async def test_transfer_invalid_amount(
        self, client: AsyncClient, wallet, wallet_factory, invalid_amount
    ) -> None:
        nonce = self.generate_nonce()

        target_wallet = await wallet_factory()

        await wallet.atomic_deposit(1000, "test_api_transfer")

        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/"
            f"transfer/{target_wallet.wallet_id}/",
            json={"amount": invalid_amount, "nonce": nonce},
        )

        assert response.status_code == 422, response.json()

        assert await wallet.get_balance() == 1000
        assert await target_wallet.get_balance() == 0

    async def test_transfer_idempotency(
        self, client: AsyncClient, wallet, wallet_factory
    ) -> None:
        nonce = self.generate_nonce()

        target_wallet = await wallet_factory()

        await wallet.atomic_deposit(1000, "test_api_transfer")
        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/"
            f"transfer/{target_wallet.wallet_id}/",
            json={"amount": "100", "nonce": nonce},
        )

        assert response.status_code == 204, response.json()

        assert await wallet.get_balance() == 900
        assert await target_wallet.get_balance() == 100

        response = await client.put(
            f"{settings.API_V1_STR}/wallets/{wallet.wallet_id}/"
            f"transfer/{target_wallet.wallet_id}/",
            json={"amount": "100", "nonce": nonce},
        )

        assert response.status_code == 409, response.json()

        assert response.json() == {
            "detail": f"Transaction with nonce {nonce} already registered."
        }

    @pytest.mark.skip(reason="Not implemented")
    async def test_get_user_wallet(self, client: AsyncClient) -> None:
        pass

    @pytest.mark.skip(reason="Not implemented")
    async def test_create_wallet_not_valid_user(self, client: AsyncClient) -> None:
        pass
