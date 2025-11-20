from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    host: str = Field(alias="DB_HOST")
    port: int = Field(alias="DB_PORT")
    db_name: str = Field(alias="DB_NAME")
    user: str = Field(alias="DB_USER")
    password: str = Field(alias="DB_PASSWORD")

    spi_host: str = Field(alias="SPI_KY_HOST")
    spi_user: str = Field(alias="SPI_KY_USER")
    spi_password: str = Field(alias="SPI_KY_PASSWORD")

    spi_asm_user: str = Field(alias="SPI_ASM_USER")
    spi_asm_password: str = Field("SPI_ASM_PASSWORD")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
