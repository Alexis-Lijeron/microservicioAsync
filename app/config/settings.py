from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # Database
    database_url: str

    # JWT
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    # Authentication
    disable_auth: bool = True  # Set to True to disable authentication globally

    # Environment
    environment: str = "development"
    debug: bool = True

    # CORS
    allowed_hosts: List[str] = ["localhost", "127.0.0.1"]

    # Pagination
    default_page_size: int = 20
    max_page_size: int = 100

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
