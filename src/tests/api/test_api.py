import pytest
from httpx import AsyncClient

from api.application import app

pytestmark = pytest.mark.asyncio


class TestAPI:
    @pytest.mark.parametrize("method", ["get", "post", "patch", "delete"])
    async def test_invalid_url(self, client: AsyncClient, method) -> None:
        response = await getattr(client, method)("/invalid")
        assert response.status_code == 404
        assert response.json() == {"detail": "Not Found"}

    async def test_list_all_routes(self, client: AsyncClient):
        response = await client.get("/")
        assert response.status_code == 200
        routes = response.json()

        names = {route["name"] for route in routes}

        assert "root" not in names
        assert len(app.routes) == len(names) + 1
