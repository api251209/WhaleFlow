from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class FetcherProtocol(Protocol):
    async def fetch(self, dataset: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch records for a given FinMind dataset and parameters."""
        ...
