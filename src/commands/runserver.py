import uvicorn

from core.config import settings


def main() -> None:
    uvicorn.run(
        "api.application:app",
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
