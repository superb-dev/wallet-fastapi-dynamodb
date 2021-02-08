import uvicorn

from core.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "api.application:app",
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
    )
