from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field


class EnqueueRequest(BaseModel):
    task: str = Field(..., min_length=1, description="Handler name the worker should run")
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: Literal["high", "normal", "low"] = "normal"
    max_retries: int = Field(default=3, ge=0, le=10)
    idempotency_key: Optional[str] = Field(
        default=None, min_length=1, max_length=255,
        description="Same key within 24h returns the original job instead of creating a duplicate",
    )


class EnqueueResponse(BaseModel):
    job_id: str
    status: str
    priority: str
    deduplicated: bool = False
