from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False

    MONGODB_URI: str
    RABBITMQ_URI: str
    REDIS_URI: str

    BOT_TOKEN: str

    class Config:
        env_file = ".env"


settings = Settings()
