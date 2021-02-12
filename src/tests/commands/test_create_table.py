import pytest

pytestmark = pytest.mark.asyncio


async def test_ok():
    assert False


async def test_create_already_exists():
    assert False
