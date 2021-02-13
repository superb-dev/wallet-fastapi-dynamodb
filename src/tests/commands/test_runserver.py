from unittest import mock

import pytest

from commands import runserver

pytestmark = pytest.mark.asyncio


class TestRunServerCommand:
    async def test_ok(self):
        with mock.patch("commands.runserver.uvicorn.run", mock.Mock()) as run:
            runserver.main()

        run.assert_called_once_with(
            "api.application:app", host="127.0.0.1", port=5000, log_level="info"
        )
