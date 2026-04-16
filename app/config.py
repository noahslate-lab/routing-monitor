"""
Configuration settings for Routing Monitor
"""
try:
    from pydantic_settings import BaseSettings
except ImportError:
    from pydantic import BaseSettings


class Settings(BaseSettings):
    # Chili Piper
    chili_piper_api_key: str = ""
    chili_piper_base_url: str = "https://fire.chilipiper.com/api/fire-edge"

    # HubSpot
    hubspot_api_key: str = ""
    hubspot_base_url: str = "https://api.hubapi.com"
    hubspot_portal_id: str = "21261434"

    # Slack
    slack_bot_token: str = ""
    slack_routing_monitor_channel: str = "routing-monitor"

    # Polling
    routing_monitor_interval_minutes: int = 30

    # Application
    environment: str = "development"
    port: int = 8100

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
