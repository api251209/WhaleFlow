"""FinMind API async client with retry, rate limiting, and response validation."""

import asyncio
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError

from whaleflow.config import settings
from whaleflow.fetchers.rate_limiter import get_rate_limiter
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)


class FinMindResponse(BaseModel):
    """Validates the FinMind API response envelope."""

    status: int
    msg: str
    data: list[dict[str, Any]] = []


class FinMindFetcher:
    def __init__(self, token: str | None = None):
        self._token = token or settings.finmind_api_token
        self._base_url = settings.finmind_base_url
        self._limiter = get_rate_limiter()

    async def fetch(self, dataset: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Fetch records from FinMind API.

        Args:
            dataset: FinMind dataset name (e.g. 'TaiwanStockInfo')
            params: Query parameters (date, stock_id, start_date, end_date, etc.)

        Returns:
            List of record dicts from the API response.

        Raises:
            RuntimeError: On daily rate limit exceeded or unrecoverable API error.
        """
        payload = {"dataset": dataset, "token": self._token, **params}

        for attempt in range(1, settings.fetch_retry_times + 1):
            await self._limiter.acquire()
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(self._base_url, params=payload)
                    response.raise_for_status()

                raw = response.json()
                validated = FinMindResponse.model_validate(raw)

                if validated.status != 200:
                    logger.warning(
                        "FinMind non-200 status for %s: %s", dataset, validated.msg
                    )
                    return []

                logger.debug(
                    "Fetched %d records from %s (attempt %d)",
                    len(validated.data),
                    dataset,
                    attempt,
                )
                return validated.data

            except ValidationError as e:
                logger.error("FinMind response schema changed for %s: %s", dataset, e)
                return []

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 402:
                    raise RuntimeError(
                        "FinMind API payment required -- daily limit may be exceeded."
                    ) from e
                logger.warning(
                    "HTTP %d on attempt %d/%d for %s",
                    e.response.status_code,
                    attempt,
                    settings.fetch_retry_times,
                    dataset,
                )

            except httpx.RequestError as e:
                logger.warning(
                    "Request error on attempt %d/%d for %s: %s",
                    attempt,
                    settings.fetch_retry_times,
                    dataset,
                    e,
                )

            if attempt < settings.fetch_retry_times:
                delay = settings.fetch_retry_delay * (2 ** (attempt - 1))
                logger.info("Retrying in %.1f seconds...", delay)
                await asyncio.sleep(delay)

        logger.error("All %d attempts failed for dataset %s", settings.fetch_retry_times, dataset)
        return []
