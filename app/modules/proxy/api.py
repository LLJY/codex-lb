from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from typing import cast

from fastapi import APIRouter, Body, Depends, File, Form, Request, Response, Security, UploadFile, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import usage as usage_core
from app.core.auth.dependencies import (
    set_openai_error_format,
    validate_codex_usage_identity,
    validate_proxy_api_key,
    validate_proxy_api_key_authorization,
    validate_usage_api_key,
)
from app.core.clients.proxy import ProxyResponseError
from app.core.config.settings import get_settings
from app.core.config.settings_cache import get_settings_cache
from app.core.errors import OpenAIErrorEnvelope, openai_error, response_failed_event
from app.core.exceptions import ProxyAuthError, ProxyRateLimitError
from app.core.metrics.prometheus import PROMETHEUS_AVAILABLE, bridge_public_contract_error_total
from app.core.middleware.api_firewall import _parse_trusted_proxy_networks, resolve_connection_client_ip
from app.core.openai.chat_requests import ChatCompletionsRequest
from app.core.openai.chat_responses import ChatCompletionResult, collect_chat_completion, stream_chat_chunks
from app.core.openai.exceptions import ClientPayloadError
from app.core.openai.model_registry import UpstreamModel, get_model_registry, is_public_model
from app.core.openai.models import (
    CompactResponseResult,
    OpenAIError,
    OpenAIResponsePayload,
    OpenAIResponseResult,
)
from app.core.openai.models import (
    OpenAIErrorEnvelope as OpenAIErrorEnvelopeModel,
)
from app.core.openai.parsing import parse_response_payload
from app.core.openai.requests import ResponsesCompactRequest, ResponsesRequest
from app.core.openai.v1_requests import V1ResponsesCompactRequest, V1ResponsesRequest
from app.core.resilience.overload import is_local_overload_error_code, merge_retry_after_headers
from app.core.runtime_logging import log_error_response
from app.core.types import JsonValue
from app.core.usage.types import UsageWindowRow
from app.core.utils.json_guards import is_json_mapping
from app.core.utils.sse import format_sse_event, inject_sse_keepalives, parse_sse_data_json
from app.db.models import Account, AccountStatus, UsageHistory
from app.db.session import get_background_session
from app.dependencies import ProxyContext, get_proxy_context, get_proxy_websocket_context
from app.modules.api_keys.repository import ApiKeysRepository
from app.modules.api_keys.service import (
    ApiKeyData,
    ApiKeyInvalidError,
    ApiKeyRateLimitExceededError,
    ApiKeySelfLimitData,
    ApiKeySelfUsageData,
    ApiKeysService,
    ApiKeyUsageReservationData,
)
from app.modules.firewall.repository import FirewallRepository
from app.modules.firewall.service import FirewallRepositoryPort, FirewallService
from app.modules.proxy import service as proxy_service_module
from app.modules.proxy.helpers import _rate_limit_details
from app.modules.proxy.http_bridge_forwarding import parse_forwarded_request
from app.modules.proxy.request_policy import (
    apply_api_key_enforcement,
    openai_invalid_payload_error,
    openai_validation_error,
    validate_model_access,
)
from app.modules.proxy.schemas import (
    CodexModelEntry,
    CodexModelsResponse,
    ModelListItem,
    ModelListResponse,
    ModelMetadata,
    RateLimitStatusPayload,
    ReasoningLevelSchema,
    V1UsageLimitResponse,
    V1UsageResponse,
)
from app.modules.proxy.types import (
    CreditStatusDetailsData,
    RateLimitStatusPayloadData,
    RateLimitWindowSnapshotData,
)
from app.modules.usage.repository import UsageRepository

logger = logging.getLogger(__name__)

_PUBLIC_RESPONSE_OUTPUT_ITEM_TYPES = frozenset(
    {
        "message",
        "function_call",
        "function_call_output",
        "reasoning",
        "web_search_call",
        "file_search_call",
        "computer_call",
        "code_interpreter_call",
        "mcp_approval_request",
        "mcp_list_tools",
        "output_image",
    }
)
_PUBLIC_RESPONSE_TEXT_PART_TYPES = frozenset({"output_text", "input_text", "text", "refusal"})

router = APIRouter(
    prefix="/backend-api/codex",
    tags=["proxy"],
    dependencies=[Security(validate_proxy_api_key), Depends(set_openai_error_format)],
)
ws_router = APIRouter(
    prefix="/backend-api/codex",
    tags=["proxy"],
)
v1_router = APIRouter(
    prefix="/v1",
    tags=["proxy"],
    dependencies=[Security(validate_proxy_api_key), Depends(set_openai_error_format)],
)
v1_ws_router = APIRouter(
    prefix="/v1",
    tags=["proxy"],
)
usage_router = APIRouter(
    tags=["proxy"],
    dependencies=[Depends(set_openai_error_format)],
)
transcribe_router = APIRouter(
    prefix="/backend-api",
    tags=["proxy"],
    dependencies=[Security(validate_proxy_api_key), Depends(set_openai_error_format)],
)
internal_router = APIRouter(
    prefix="/internal/bridge",
    tags=["proxy"],
    dependencies=[Depends(set_openai_error_format)],
)

_TRANSCRIPTION_MODEL = "gpt-4o-transcribe"
_UNAVAILABLE_SELECTION_ERROR_CODES = {
    "no_accounts",
    "no_plan_support_for_model",
    "additional_quota_data_unavailable",
    "no_additional_quota_eligible_accounts",
}
_STREAM_STARTUP_ERROR_PROBE_SECONDS = 0.05
_HTTP_BRIDGE_STARTUP_ERROR_PROBE_SECONDS = 0.5


@router.post(
    "/responses",
    responses={
        200: {
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                }
            }
        }
    },
)
async def responses(
    request: Request,
    payload: ResponsesRequest = Body(...),
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> Response:
    return await _stream_responses(
        request,
        payload,
        context,
        api_key,
        codex_session_affinity=True,
        openai_cache_affinity=True,
        prefer_http_bridge=True,
        convert_startup_event_errors=True,
    )


@ws_router.websocket("/responses")
async def responses_websocket(
    websocket: WebSocket,
    context: ProxyContext = Depends(get_proxy_websocket_context),
) -> None:
    api_key, denial = await _validate_proxy_websocket_request(websocket)
    if denial is not None:
        await websocket.send_denial_response(denial)
        return
    turn_state = proxy_service_module.ensure_downstream_turn_state(websocket.headers)
    await websocket.accept(headers=proxy_service_module.build_downstream_turn_state_accept_headers(turn_state))
    forwarded_headers = dict(websocket.headers)
    forwarded_headers.setdefault("x-codex-turn-state", turn_state)
    await context.service.proxy_responses_websocket(
        websocket,
        forwarded_headers,
        codex_session_affinity=True,
        openai_cache_affinity=True,
        api_key=api_key,
    )


@v1_router.post(
    "/responses",
    response_model=OpenAIResponseResult,
    responses={
        200: {
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                }
            }
        }
    },
)
async def v1_responses(
    request: Request,
    payload: V1ResponsesRequest = Body(...),
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> Response:
    try:
        responses_payload = payload.to_responses_request()
    except ClientPayloadError as exc:
        error = openai_invalid_payload_error(exc.param)
        return _logged_error_json_response(request, 400, error)
    except ValidationError as exc:
        error = openai_validation_error(exc)
        return _logged_error_json_response(request, 400, error)
    if responses_payload.stream:
        return await _stream_responses(
            request,
            responses_payload,
            context,
            api_key,
            codex_session_affinity=False,
            openai_cache_affinity=True,
            prefer_http_bridge=True,
            convert_startup_event_errors=True,
        )
    return await _collect_responses(
        request,
        responses_payload,
        context,
        api_key,
        codex_session_affinity=False,
        openai_cache_affinity=True,
        prefer_http_bridge=True,
    )


@internal_router.post(
    "/responses",
    include_in_schema=False,
    responses={
        200: {
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                }
            }
        }
    },
)
async def internal_bridge_responses(
    request: Request,
    payload: ResponsesRequest = Body(...),
    context: ProxyContext = Depends(get_proxy_context),
) -> Response:
    forwarded_request_context, internal_error = parse_forwarded_request(
        request.headers,
        payload=payload,
        current_instance=get_settings().http_responses_session_bridge_instance_id,
    )
    if internal_error is not None or forwarded_request_context is None:
        assert internal_error is not None
        return _logged_error_json_response(request, internal_error.status_code, internal_error.payload)
    api_key, auth_error = await _validate_internal_bridge_api_key(request)
    if auth_error is not None:
        return auth_error
    skip_limit_enforcement = api_key is None or forwarded_request_context.context.reservation is not None
    forwarded_headers = _strip_internal_bridge_headers(request.headers)
    return await _stream_responses(
        request,
        payload,
        context,
        api_key,
        codex_session_affinity=forwarded_request_context.context.codex_session_affinity,
        openai_cache_affinity=True,
        prefer_http_bridge=True,
        skip_limit_enforcement=skip_limit_enforcement,
        api_key_reservation_override=forwarded_request_context.context.reservation,
        include_rate_limit_headers=False,
        forwarded_request=True,
        forwarded_headers=forwarded_headers,
        forwarded_downstream_turn_state=forwarded_request_context.context.downstream_turn_state,
        forwarded_affinity_kind=forwarded_request_context.context.original_affinity_kind,
        forwarded_affinity_key=forwarded_request_context.context.original_affinity_key,
        mask_startup_previous_response_errors=False,
    )


@v1_ws_router.websocket("/responses")
async def v1_responses_websocket(
    websocket: WebSocket,
    context: ProxyContext = Depends(get_proxy_websocket_context),
) -> None:
    api_key, denial = await _validate_proxy_websocket_request(websocket)
    if denial is not None:
        await websocket.send_denial_response(denial)
        return
    turn_state = proxy_service_module.ensure_downstream_turn_state(websocket.headers)
    await websocket.accept(headers=proxy_service_module.build_downstream_turn_state_accept_headers(turn_state))
    forwarded_headers = dict(websocket.headers)
    forwarded_headers.setdefault("x-codex-turn-state", turn_state)
    await context.service.proxy_responses_websocket(
        websocket,
        forwarded_headers,
        codex_session_affinity=False,
        openai_cache_affinity=True,
        api_key=api_key,
    )


@router.get("/models", response_model=CodexModelsResponse)
async def models(
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> Response:
    return await _build_codex_models_response(api_key)


@v1_router.get("/models", response_model=ModelListResponse)
async def v1_models(
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> Response:
    return await _build_models_response(api_key)


@v1_router.get("/usage", response_model=V1UsageResponse)
async def v1_usage(
    api_key: ApiKeyData = Security(validate_usage_api_key),
) -> V1UsageResponse:
    async with get_background_session() as session:
        service = ApiKeysService(ApiKeysRepository(session))
        usage = await service.get_key_usage_summary_for_self(api_key.id)
        aggregate_limits = await _build_aggregate_credit_limits(session)

    if usage is None:
        raise ProxyAuthError("Invalid API key")

    return V1UsageResponse(
        request_count=usage.request_count,
        total_tokens=usage.total_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        total_cost_usd=usage.total_cost_usd,
        limits=_build_v1_usage_limits(usage, aggregate_limits),
    )


def _build_v1_usage_limits(
    usage: ApiKeySelfUsageData,
    aggregate_limits: dict[str, V1UsageLimitResponse],
) -> list[V1UsageLimitResponse]:
    raw_limits = [_to_v1_usage_limit_response(limit) for limit in usage.limits]
    credit_overrides = {
        limit.limit_window: limit
        for limit in usage.limits
        if limit.limit_type == "credits" and limit.model_filter is None
    }

    if aggregate_limits:
        merged: list[V1UsageLimitResponse] = []
        for window in ("5h", "7d"):
            aggregate = aggregate_limits.get(window)
            if aggregate is None:
                continue
            merged.append(_apply_credit_override(aggregate, credit_overrides.get(window)))
        if {item.limit_window for item in merged} == {"5h", "7d"}:
            return merged

    return raw_limits


def _to_v1_usage_limit_response(limit: ApiKeySelfLimitData) -> V1UsageLimitResponse:
    current_value = max(0, min(limit.current_value, limit.max_value))
    return V1UsageLimitResponse(
        limit_type=limit.limit_type,
        limit_window=limit.limit_window,
        max_value=limit.max_value,
        current_value=current_value,
        remaining_value=max(0, limit.max_value - current_value),
        model_filter=limit.model_filter,
        reset_at=limit.reset_at.isoformat() + "Z",
        source=limit.source,
    )


def _apply_credit_override(
    aggregate_limit: V1UsageLimitResponse,
    override_limit: ApiKeySelfLimitData | None,
) -> V1UsageLimitResponse:
    if override_limit is None:
        return aggregate_limit

    override_max = max(0, override_limit.max_value)
    current_value = max(0, min(aggregate_limit.current_value, override_max))
    return V1UsageLimitResponse(
        limit_type="credits",
        limit_window=aggregate_limit.limit_window,
        max_value=override_max,
        current_value=current_value,
        remaining_value=max(0, override_max - current_value),
        model_filter=None,
        reset_at=aggregate_limit.reset_at,
        source="api_key_override",
    )


async def _build_codex_usage_payload_for_api_key(api_key: ApiKeyData) -> RateLimitStatusPayloadData:
    async with get_background_session() as session:
        service = ApiKeysService(ApiKeysRepository(session))
        usage = await service.get_key_usage_summary_for_self(api_key.id)

    if usage is None:
        raise ProxyAuthError("Invalid API key")

    key_limits = [_to_v1_usage_limit_response(limit) for limit in usage.limits]
    primary_credit_limit = _select_codex_usage_limit(key_limits, "5h") or _select_codex_usage_limit(key_limits, "daily")
    secondary_credit_limit = (
        _select_codex_usage_limit(key_limits, "7d")
        or _select_codex_usage_limit(key_limits, "weekly")
        or _select_codex_usage_limit(key_limits, "monthly")
    )

    return RateLimitStatusPayloadData(
        plan_type="api_key",
        rate_limit=_rate_limit_details(
            _codex_usage_window_snapshot(primary_credit_limit),
            _codex_usage_window_snapshot(secondary_credit_limit),
        ),
        credits=_codex_usage_credit_snapshot(primary_credit_limit, secondary_credit_limit),
    )


def _select_codex_usage_limit(
    limits: list[V1UsageLimitResponse],
    window: str,
) -> V1UsageLimitResponse | None:
    candidates = [
        limit
        for limit in limits
        if limit.limit_window == window and limit.model_filter is None and limit.limit_type == "credits"
    ]
    return candidates[0] if candidates else None


def _codex_usage_window_snapshot(limit: V1UsageLimitResponse | None) -> RateLimitWindowSnapshotData | None:
    if limit is None or limit.max_value <= 0:
        return None
    reset_at = datetime.fromisoformat(limit.reset_at.replace("Z", "+00:00"))
    reset_epoch = int(reset_at.timestamp())
    now_epoch = int(time.time())
    used_percent = max(0, min(100, int((limit.current_value / limit.max_value) * 100)))
    window_seconds = {"5h": 18000, "daily": 86400, "7d": 604800, "weekly": 604800, "monthly": 2592000}.get(
        limit.limit_window
    )
    return RateLimitWindowSnapshotData(
        used_percent=used_percent,
        limit_window_seconds=window_seconds,
        reset_after_seconds=max(0, reset_epoch - now_epoch),
        reset_at=reset_epoch,
    )


def _codex_usage_credit_snapshot(
    primary_limit: V1UsageLimitResponse | None,
    secondary_limit: V1UsageLimitResponse | None,
) -> CreditStatusDetailsData | None:
    preferred = secondary_limit or primary_limit
    if preferred is None or preferred.limit_type != "credits":
        return None
    return CreditStatusDetailsData(
        has_credits=preferred.remaining_value > 0,
        unlimited=False,
        balance=str(preferred.remaining_value),
        approx_local_messages=None,
        approx_cloud_messages=None,
    )


async def _build_aggregate_credit_limits(session: AsyncSession) -> dict[str, V1UsageLimitResponse]:
    usage_repository = UsageRepository(session)
    primary_latest = await usage_repository.latest_by_account(window="primary")
    secondary_latest = await usage_repository.latest_by_account(window="secondary")

    primary_rows = [_usage_entry_to_window_row(entry) for entry in primary_latest.values()]
    secondary_rows = [_usage_entry_to_window_row(entry) for entry in secondary_latest.values()]
    primary_rows, secondary_rows = usage_core.normalize_weekly_only_rows(primary_rows, secondary_rows)

    account_ids = {row.account_id for row in primary_rows} | {row.account_id for row in secondary_rows}
    if not account_ids:
        return {}

    account_map = {account.id: account for account in await _load_accounts_by_id(session, account_ids)}
    if not account_map:
        return {}

    active_account_ids = set(account_map)
    primary_rows = [row for row in primary_rows if row.account_id in active_account_ids]
    secondary_rows = [row for row in secondary_rows if row.account_id in active_account_ids]
    limits: dict[str, V1UsageLimitResponse] = {}

    for window_key, rows, label in (("primary", primary_rows, "5h"), ("secondary", secondary_rows, "7d")):
        if not rows:
            continue
        summary = usage_core.summarize_usage_window(rows, account_map, window_key)
        max_value = max(0, int(round(summary.capacity_credits or 0.0)))
        if max_value <= 0:
            continue
        if summary.reset_at is None:
            continue
        current_value = max(0, min(int(round(summary.used_credits or 0.0)), max_value))
        limits[label] = V1UsageLimitResponse(
            limit_type="credits",
            limit_window=label,
            max_value=max_value,
            current_value=current_value,
            remaining_value=max(0, max_value - current_value),
            model_filter=None,
            reset_at=datetime.fromtimestamp(summary.reset_at, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            source="aggregate",
        )

    return limits


async def _load_accounts_by_id(session: AsyncSession, account_ids: set[str]) -> list[Account]:
    if not account_ids:
        return []
    result = await session.execute(
        select(Account).where(
            Account.id.in_(account_ids),
            Account.status.notin_((AccountStatus.DEACTIVATED, AccountStatus.PAUSED)),
        )
    )
    return list(result.scalars().all())


def _usage_entry_to_window_row(entry: UsageHistory) -> UsageWindowRow:
    return UsageWindowRow(
        account_id=entry.account_id,
        used_percent=entry.used_percent,
        reset_at=entry.reset_at,
        window_minutes=entry.window_minutes,
        recorded_at=entry.recorded_at,
    )


@transcribe_router.post("/transcribe")
async def backend_transcribe(
    request: Request,
    file: UploadFile = File(...),
    prompt: str | None = Form(None),
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> JSONResponse:
    return await _transcribe_request(
        request=request,
        file=file,
        prompt=prompt,
        context=context,
        api_key=api_key,
    )


@v1_router.post("/audio/transcriptions")
async def v1_audio_transcriptions(
    request: Request,
    model: str = Form(...),
    file: UploadFile = File(...),
    prompt: str | None = Form(None),
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> JSONResponse:
    if model != _TRANSCRIPTION_MODEL:
        return _logged_error_json_response(
            request,
            status_code=400,
            content=_openai_invalid_transcription_model_error(model),
        )
    return await _transcribe_request(
        request=request,
        file=file,
        prompt=prompt,
        context=context,
        api_key=api_key,
    )


async def _build_codex_models_response(api_key: ApiKeyData | None) -> Response:
    reservation = await _enforce_request_limits(
        api_key,
        request_model=None,
        request_service_tier=None,
    )

    allowed_models = _allowed_models_for_api_key(api_key)

    registry = get_model_registry()
    models = registry.get_models_with_fallback()

    if not models:
        await _release_reservation(reservation)
        return JSONResponse(content=CodexModelsResponse(models=[]).model_dump(mode="json"))

    entries: list[CodexModelEntry] = []
    for slug, model in models.items():
        if not is_public_model(model, allowed_models):
            continue
        entries.append(_to_codex_model_entry(model))
    await _release_reservation(reservation)
    return JSONResponse(content=CodexModelsResponse(models=entries).model_dump(mode="json"))


async def _build_models_response(api_key: ApiKeyData | None) -> Response:
    reservation = await _enforce_request_limits(
        api_key,
        request_model=None,
        request_service_tier=None,
    )

    allowed_models = _allowed_models_for_api_key(api_key)
    created = int(time.time())

    registry = get_model_registry()
    models = registry.get_models_with_fallback()

    if not models:
        await _release_reservation(reservation)
        return JSONResponse(content=ModelListResponse(data=[]).model_dump(mode="json"))

    items: list[ModelListItem] = []
    for slug, model in models.items():
        if not is_public_model(model, allowed_models):
            continue
        items.append(
            ModelListItem(
                id=slug,
                created=created,
                owned_by="codex-lb",
                metadata=_to_model_metadata(model),
            )
        )
    await _release_reservation(reservation)
    return JSONResponse(content=ModelListResponse(data=items).model_dump(mode="json"))


def _allowed_models_for_api_key(api_key: ApiKeyData | None) -> set[str] | None:
    allowed_models = set(api_key.allowed_models) if api_key and api_key.allowed_models else None
    if api_key and api_key.enforced_model:
        forced = {api_key.enforced_model}
        return forced if allowed_models is None else (allowed_models & forced)
    return allowed_models


def _to_codex_model_entry(model: UpstreamModel) -> CodexModelEntry:
    raw = model.raw

    extra: dict[str, JsonValue] = {}
    skip_keys = {
        "slug",
        "display_name",
        "description",
        "base_instructions",
        "default_reasoning_level",
        "supported_reasoning_levels",
        "supported_in_api",
        "priority",
        "minimal_client_version",
        "supports_reasoning_summaries",
        "support_verbosity",
        "default_verbosity",
        "supports_parallel_tool_calls",
        "context_window",
        "input_modalities",
        "available_in_plans",
        "prefer_websockets",
        "visibility",
    }
    for key, value in raw.items():
        if key not in skip_keys and isinstance(value, (bool, int, float, str, type(None), list, Mapping)):
            extra[key] = value

    return CodexModelEntry(
        slug=model.slug,
        display_name=model.display_name,
        description=model.description,
        base_instructions=model.base_instructions,
        default_reasoning_level=model.default_reasoning_level,
        supported_reasoning_levels=[
            ReasoningLevelSchema(effort=rl.effort, description=rl.description)
            for rl in model.supported_reasoning_levels
        ],
        supported_in_api=model.supported_in_api,
        priority=model.priority,
        minimal_client_version=model.minimal_client_version,
        supports_reasoning_summaries=model.supports_reasoning_summaries,
        support_verbosity=model.support_verbosity,
        default_verbosity=model.default_verbosity,
        supports_parallel_tool_calls=model.supports_parallel_tool_calls,
        context_window=_effective_context_window(model),
        input_modalities=list(model.input_modalities),
        available_in_plans=sorted(model.available_in_plans),
        prefer_websockets=model.prefer_websockets,
        visibility=_model_visibility(model),
        **extra,
    )


def _effective_context_window(model: UpstreamModel) -> int:
    overrides = get_settings().model_context_window_overrides
    return overrides.get(model.slug, model.context_window)


def _model_visibility(model: UpstreamModel) -> str:
    visibility = model.raw.get("visibility")
    return visibility if isinstance(visibility, str) else "list"


def _to_model_metadata(model: UpstreamModel) -> ModelMetadata:
    return ModelMetadata(
        display_name=model.display_name,
        description=model.description,
        context_window=_effective_context_window(model),
        input_modalities=list(model.input_modalities),
        supported_reasoning_levels=[
            ReasoningLevelSchema(effort=rl.effort, description=rl.description)
            for rl in model.supported_reasoning_levels
        ],
        default_reasoning_level=model.default_reasoning_level,
        supports_reasoning_summaries=model.supports_reasoning_summaries,
        support_verbosity=model.support_verbosity,
        default_verbosity=model.default_verbosity,
        prefer_websockets=model.prefer_websockets,
        supports_parallel_tool_calls=model.supports_parallel_tool_calls,
        supported_in_api=model.supported_in_api,
        minimal_client_version=model.minimal_client_version,
        priority=model.priority,
    )


@v1_router.post(
    "/chat/completions",
    response_model=ChatCompletionResult,
    responses={
        200: {
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                }
            }
        }
    },
)
async def v1_chat_completions(
    request: Request,
    payload: ChatCompletionsRequest = Body(...),
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> Response:
    effective_model = _effective_model_for_api_key(api_key, payload.model)
    validate_model_access(api_key, effective_model)

    rate_limit_headers = await context.service.rate_limit_headers()
    try:
        responses_payload = payload.to_responses_request()
    except ClientPayloadError as exc:
        error = openai_invalid_payload_error(exc.param)
        return _logged_error_json_response(request, 400, error, headers=rate_limit_headers)
    except ValidationError as exc:
        error = openai_validation_error(exc)
        return _logged_error_json_response(request, 400, error, headers=rate_limit_headers)
    reservation = await _enforce_request_limits(
        api_key,
        request_model=effective_model,
        request_service_tier=responses_payload.service_tier,
    )
    responses_payload.stream = True
    apply_api_key_enforcement(responses_payload, api_key)
    stream = context.service.stream_responses(
        responses_payload,
        request.headers,
        codex_session_affinity=False,
        propagate_http_errors=True,
        openai_cache_affinity=True,
        api_key=api_key,
        api_key_reservation=reservation,
        suppress_text_done_events=True,
    )
    stream, startup_error = await _probe_stream_startup_error(stream, convert_event_errors=True)
    if startup_error is not None:
        return _stream_startup_error_response(request, startup_error, headers=rate_limit_headers)
    if payload.stream:
        stream_options = payload.stream_options
        include_usage = bool(stream_options and stream_options.include_usage)
        return StreamingResponse(
            inject_sse_keepalives(
                stream_chat_chunks(
                    _stream_proxy_errors_as_response_failed(stream),
                    model=responses_payload.model,
                    include_usage=include_usage,
                ),
                get_settings().sse_keepalive_interval_seconds,
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", **rate_limit_headers},
        )

    try:
        first = await stream.__anext__()
    except StopAsyncIteration:
        first = None
    except ProxyResponseError as exc:
        return _logged_error_json_response(request, exc.status_code, exc.payload, headers=rate_limit_headers)

    stream_with_first = _prepend_first(first, stream)
    result = await collect_chat_completion(stream_with_first, model=responses_payload.model)
    if isinstance(result, OpenAIErrorEnvelopeModel):
        error = result.error
        code = error.code if error else None
        status_code = 503 if code in _UNAVAILABLE_SELECTION_ERROR_CODES else 502
        return _logged_error_json_response(
            request,
            status_code,
            content=result.model_dump(mode="json", exclude_none=True),
            headers=rate_limit_headers,
        )
    return JSONResponse(
        content=result.model_dump(mode="json", exclude_none=True),
        status_code=200,
        headers=rate_limit_headers,
    )


async def _stream_responses(
    request: Request,
    payload: ResponsesRequest,
    context: ProxyContext,
    api_key: ApiKeyData | None,
    *,
    codex_session_affinity: bool = False,
    openai_cache_affinity: bool = False,
    suppress_text_done_events: bool = False,
    prefer_http_bridge: bool = False,
    skip_limit_enforcement: bool = False,
    api_key_reservation_override: ApiKeyUsageReservationData | None = None,
    include_rate_limit_headers: bool = True,
    forwarded_request: bool = False,
    forwarded_headers: Mapping[str, str] | None = None,
    forwarded_downstream_turn_state: str | None = None,
    forwarded_affinity_kind: str | None = None,
    forwarded_affinity_key: str | None = None,
    convert_startup_event_errors: bool = False,
    mask_startup_previous_response_errors: bool = True,
) -> Response:
    apply_api_key_enforcement(payload, api_key)
    validate_model_access(api_key, payload.model)
    owns_reservation = api_key_reservation_override is None
    reservation = (
        api_key_reservation_override
        if skip_limit_enforcement
        else await _enforce_request_limits(
            api_key,
            request_model=payload.model,
            request_service_tier=payload.service_tier,
        )
    )

    rate_limit_headers = await context.service.rate_limit_headers() if include_rate_limit_headers else {}
    bridge_active = prefer_http_bridge and proxy_service_module.get_settings().http_responses_session_bridge_enabled
    effective_headers = forwarded_headers or request.headers
    downstream_turn_state = (
        forwarded_downstream_turn_state
        if bridge_active and forwarded_downstream_turn_state is not None
        else proxy_service_module.ensure_http_downstream_turn_state(effective_headers)
        if bridge_active
        else None
    )
    turn_state_headers = (
        proxy_service_module.build_downstream_turn_state_response_headers(downstream_turn_state)
        if downstream_turn_state is not None
        else {}
    )
    payload.stream = True
    if prefer_http_bridge:
        stream = context.service.stream_http_responses(
            payload,
            effective_headers,
            codex_session_affinity=codex_session_affinity,
            propagate_http_errors=True,
            openai_cache_affinity=openai_cache_affinity,
            api_key=api_key,
            api_key_reservation=reservation,
            suppress_text_done_events=suppress_text_done_events,
            downstream_turn_state=downstream_turn_state,
            forwarded_request=forwarded_request,
            forwarded_affinity_kind=forwarded_affinity_kind,
            forwarded_affinity_key=forwarded_affinity_key,
        )
    else:
        stream = context.service.stream_responses(
            payload,
            request.headers,
            codex_session_affinity=codex_session_affinity,
            propagate_http_errors=True,
            openai_cache_affinity=openai_cache_affinity,
            api_key=api_key,
            api_key_reservation=reservation,
            suppress_text_done_events=suppress_text_done_events,
        )
    startup_probe_timeout_seconds = (
        get_settings().proxy_request_budget_seconds
        if forwarded_request
        else _HTTP_BRIDGE_STARTUP_ERROR_PROBE_SECONDS
        if prefer_http_bridge
        else _STREAM_STARTUP_ERROR_PROBE_SECONDS
    )
    stream, startup_error = await _probe_stream_startup_error(
        stream,
        convert_event_errors=convert_startup_event_errors or bridge_active,
        timeout_seconds=startup_probe_timeout_seconds,
    )
    if startup_error is not None:
        if reservation is not None and owns_reservation:
            await _release_reservation(reservation)
        return _stream_startup_error_response(
            request,
            startup_error,
            headers=rate_limit_headers,
            mask_previous_response_not_found=mask_startup_previous_response_errors,
        )
    stream = _normalize_public_responses_stream(_stream_proxy_errors_as_response_failed(stream))
    keepalive_interval_seconds = 0.0 if forwarded_request else get_settings().sse_keepalive_interval_seconds
    return StreamingResponse(
        inject_sse_keepalives(
            stream,
            keepalive_interval_seconds,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", **turn_state_headers, **rate_limit_headers},
    )


def _strip_internal_bridge_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {key: value for key, value in headers.items() if not key.lower().startswith("x-codex-bridge-")}


async def _collect_responses(
    request: Request,
    payload: ResponsesRequest,
    context: ProxyContext,
    api_key: ApiKeyData | None,
    *,
    codex_session_affinity: bool = False,
    openai_cache_affinity: bool = False,
    suppress_text_done_events: bool = False,
    prefer_http_bridge: bool = False,
) -> Response:
    apply_api_key_enforcement(payload, api_key)
    validate_model_access(api_key, payload.model)
    reservation = await _enforce_request_limits(
        api_key,
        request_model=payload.model,
        request_service_tier=payload.service_tier,
    )

    rate_limit_headers = await context.service.rate_limit_headers()
    bridge_active = prefer_http_bridge and proxy_service_module.get_settings().http_responses_session_bridge_enabled
    downstream_turn_state = (
        proxy_service_module.ensure_http_downstream_turn_state(request.headers) if bridge_active else None
    )
    turn_state_headers = (
        proxy_service_module.build_downstream_turn_state_response_headers(downstream_turn_state)
        if downstream_turn_state is not None
        else {}
    )
    payload.stream = True
    if prefer_http_bridge:
        stream = context.service.stream_http_responses(
            payload,
            request.headers,
            codex_session_affinity=codex_session_affinity,
            propagate_http_errors=True,
            openai_cache_affinity=openai_cache_affinity,
            api_key=api_key,
            api_key_reservation=reservation,
            suppress_text_done_events=suppress_text_done_events,
            downstream_turn_state=downstream_turn_state,
        )
    else:
        stream = context.service.stream_responses(
            payload,
            request.headers,
            codex_session_affinity=codex_session_affinity,
            propagate_http_errors=True,
            openai_cache_affinity=openai_cache_affinity,
            api_key=api_key,
            api_key_reservation=reservation,
            suppress_text_done_events=suppress_text_done_events,
        )
    try:
        response_payload = await _collect_responses_payload(stream)
    except ProxyResponseError as exc:
        await _release_reservation(reservation)
        error = _parse_error_envelope(exc.payload)
        return _logged_error_json_response(
            request,
            exc.status_code,
            error.model_dump(mode="json", exclude_none=True),
            headers=rate_limit_headers,
        )
    if isinstance(response_payload, OpenAIResponsePayload):
        if response_payload.status == "failed":
            error_payload = _error_envelope_from_response(response_payload.error)
            status_code = _status_for_error(error_payload.error)
            return _logged_error_json_response(
                request,
                status_code,
                error_payload.model_dump(mode="json", exclude_none=True),
                headers={**turn_state_headers, **rate_limit_headers},
            )
        return JSONResponse(
            content=response_payload.model_dump(mode="json", exclude_none=True),
            headers={**turn_state_headers, **rate_limit_headers},
        )
    status_code = _status_for_error(response_payload.error)
    return _logged_error_json_response(
        request,
        status_code,
        response_payload.model_dump(mode="json", exclude_none=True),
        headers={**turn_state_headers, **rate_limit_headers},
    )


@router.post("/responses/compact", response_model=CompactResponseResult)
async def responses_compact(
    request: Request,
    payload: ResponsesCompactRequest = Body(...),
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> JSONResponse:
    return await _compact_responses(
        request, payload, context, api_key, codex_session_affinity=True, openai_cache_affinity=True
    )


@v1_router.post("/responses/compact", response_model=CompactResponseResult)
async def v1_responses_compact(
    request: Request,
    payload: V1ResponsesCompactRequest = Body(...),
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Security(validate_proxy_api_key),
) -> JSONResponse:
    try:
        compact_payload = payload.to_compact_request()
    except ClientPayloadError as exc:
        error = openai_invalid_payload_error(exc.param)
        return _logged_error_json_response(request, 400, error)
    except ValidationError as exc:
        error = openai_validation_error(exc)
        return _logged_error_json_response(request, 400, error)
    return await _compact_responses(
        request,
        compact_payload,
        context,
        api_key,
        codex_session_affinity=False,
        openai_cache_affinity=True,
    )


async def _compact_responses(
    request: Request,
    payload: ResponsesCompactRequest,
    context: ProxyContext,
    api_key: ApiKeyData | None,
    codex_session_affinity: bool = False,
    openai_cache_affinity: bool = False,
) -> JSONResponse:
    apply_api_key_enforcement(payload, api_key)
    validate_model_access(api_key, payload.model)
    reservation = await _enforce_request_limits(
        api_key,
        request_model=payload.model,
        request_service_tier=_compact_request_service_tier(payload),
    )

    rate_limit_headers = await context.service.rate_limit_headers()
    try:
        result = await context.service.compact_responses(
            payload,
            request.headers,
            codex_session_affinity=codex_session_affinity,
            openai_cache_affinity=openai_cache_affinity,
            api_key=api_key,
            api_key_reservation=reservation,
        )
    except NotImplementedError:
        error = OpenAIErrorEnvelopeModel(
            error=OpenAIError(
                message="responses/compact is not implemented",
                type="server_error",
                code="not_implemented",
            )
        )
        return _logged_error_json_response(
            request,
            501,
            error.model_dump(mode="json", exclude_none=True),
            headers=rate_limit_headers,
        )
    except ProxyResponseError as exc:
        error = _parse_error_envelope(exc.payload)
        return _logged_error_json_response(
            request,
            exc.status_code,
            error.model_dump(mode="json", exclude_none=True),
            headers=rate_limit_headers,
        )
    finally:
        await _release_reservation(reservation)
    return JSONResponse(
        content=result.model_dump(mode="json", exclude_none=True),
        headers=rate_limit_headers,
    )


async def _transcribe_request(
    *,
    request: Request,
    file: UploadFile,
    prompt: str | None,
    context: ProxyContext,
    api_key: ApiKeyData | None,
) -> JSONResponse:
    validate_model_access(api_key, _TRANSCRIPTION_MODEL)
    reservation = await _enforce_request_limits(
        api_key,
        request_model=_TRANSCRIPTION_MODEL,
        request_service_tier=None,
    )
    rate_limit_headers = await context.service.rate_limit_headers()
    try:
        audio_bytes = await file.read()
        result = await context.service.transcribe(
            audio_bytes=audio_bytes,
            filename=file.filename or "audio.wav",
            content_type=file.content_type,
            prompt=prompt,
            headers=request.headers,
            api_key=api_key,
        )
    except ProxyResponseError as exc:
        error = _parse_error_envelope(exc.payload)
        return _logged_error_json_response(
            request,
            exc.status_code,
            error.model_dump(mode="json", exclude_none=True),
            headers=rate_limit_headers,
        )
    finally:
        await _release_reservation(reservation)
    return JSONResponse(content=result, headers=rate_limit_headers)


@usage_router.get("/api/codex/usage", response_model=RateLimitStatusPayload)
@usage_router.get("/api/codex/usage/", response_model=RateLimitStatusPayload, include_in_schema=False)
async def codex_usage(
    context: ProxyContext = Depends(get_proxy_context),
    api_key: ApiKeyData | None = Depends(validate_codex_usage_identity),
) -> RateLimitStatusPayload:
    payload = (
        await _build_codex_usage_payload_for_api_key(api_key)
        if api_key is not None
        else await context.service.get_rate_limit_payload()
    )
    return RateLimitStatusPayload.from_data(payload)


async def _prepend_first(first: str | None, stream: AsyncIterator[str]) -> AsyncIterator[str]:
    try:
        if first is not None:
            yield first
        async for line in stream:
            yield line
    finally:
        await _close_async_iterator(stream)


async def _probe_stream_startup_error(
    stream: AsyncIterator[str],
    *,
    convert_event_errors: bool = False,
    timeout_seconds: float = _STREAM_STARTUP_ERROR_PROBE_SECONDS,
) -> tuple[AsyncIterator[str], ProxyResponseError | OpenAIErrorEnvelopeModel | None]:
    first_task = asyncio.create_task(anext(stream))
    try:
        first = await asyncio.wait_for(asyncio.shield(first_task), timeout=timeout_seconds)
    except TimeoutError as exc:
        if first_task.done():
            try:
                first = await first_task
            except StopAsyncIteration:
                await _close_async_iterator(stream)
                return _prepend_first(None, stream), None
            except ProxyResponseError as proxy_exc:
                await _close_async_iterator(stream)
                return _prepend_first(None, stream), proxy_exc
            except TimeoutError as source_exc:
                await _close_async_iterator(stream)
                return _prepend_first(None, stream), _startup_timeout_error(source_exc)
            return await _stream_startup_probe_result(first, stream, convert_event_errors=convert_event_errors)
        del exc
        return _prepend_first_task(first_task, stream), None
    except asyncio.CancelledError:
        await _cancel_pending_first_task(first_task)
        await _close_async_iterator(stream)
        raise
    except StopAsyncIteration:
        await _close_async_iterator(stream)
        return _prepend_first(None, stream), None
    except ProxyResponseError as exc:
        await _close_async_iterator(stream)
        return _prepend_first(None, stream), exc
    return await _stream_startup_probe_result(first, stream, convert_event_errors=convert_event_errors)


async def _stream_startup_probe_result(
    first: str,
    stream: AsyncIterator[str],
    *,
    convert_event_errors: bool,
) -> tuple[AsyncIterator[str], ProxyResponseError | OpenAIErrorEnvelopeModel | None]:
    if convert_event_errors:
        first_error = _stream_event_error(first)
        if first_error is not None:
            envelope, status_code = first_error
            await _drain_async_iterator(stream)
            await _close_async_iterator(stream)
            if status_code is not None:
                return _prepend_first(None, stream), ProxyResponseError(
                    status_code,
                    cast(OpenAIErrorEnvelope, envelope.model_dump(mode="json", exclude_none=True)),
                )
            return _prepend_first(None, stream), envelope
    return _prepend_first(first, stream), None


def _startup_timeout_error(exc: TimeoutError) -> ProxyResponseError:
    return ProxyResponseError(
        504,
        openai_error("upstream_request_timeout", "Proxy request budget exhausted"),
        failure_exception_type=type(exc).__name__,
    )


def _prepend_first_task(first_task: asyncio.Task[str], stream: AsyncIterator[str]) -> AsyncIterator[str]:
    return _FirstTaskPrefixedStream(first_task, stream)


class _FirstTaskPrefixedStream:
    def __init__(self, first_task: asyncio.Task[str], stream: AsyncIterator[str]) -> None:
        self._first_task = first_task
        self._stream = stream
        self._first_consumed = False
        self._closed = False

    def __aiter__(self) -> "_FirstTaskPrefixedStream":
        return self

    async def __anext__(self) -> str:
        if self._closed:
            raise StopAsyncIteration
        if not self._first_consumed:
            self._first_consumed = True
            try:
                return await self._first_task
            except StopAsyncIteration:
                await self.aclose()
                raise
            except asyncio.CancelledError:
                await self.aclose()
                raise
        try:
            return await self._stream.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except asyncio.CancelledError:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await _cancel_pending_first_task(self._first_task)
        await _close_async_iterator(self._stream)


async def _cancel_pending_first_task(first_task: asyncio.Task[str]) -> None:
    if not first_task.done():
        first_task.cancel()
    try:
        await first_task
    except BaseException:
        pass


async def _close_async_iterator(stream: AsyncIterator[str]) -> None:
    aclose = getattr(stream, "aclose", None)
    if callable(aclose):
        await aclose()


async def _stream_proxy_errors_as_response_failed(stream: AsyncIterator[str]) -> AsyncIterator[str]:
    try:
        try:
            async for line in stream:
                yield line
        except ProxyResponseError as exc:
            envelope = _parse_error_envelope(exc.payload)
            _, envelope = _mask_previous_response_not_found_error(envelope, default_status=exc.status_code)
            error = envelope.error
            yield format_sse_event(_response_failed_event_from_error(error))
    finally:
        await _close_async_iterator(stream)


def _response_failed_event_from_error(error: OpenAIError | None) -> dict[str, JsonValue]:
    event = response_failed_event(
        error.code if error and error.code else "upstream_error",
        error.message if error and error.message else "Upstream error",
        error.type if error and error.type else "server_error",
        error_param=error.param if error else None,
    )
    if error is None:
        return cast(dict[str, JsonValue], event)
    error_payload = cast(dict[str, JsonValue], error.model_dump(mode="json", exclude_none=True))
    error_payload.setdefault("code", "upstream_error")
    error_payload.setdefault("message", "Upstream error")
    error_payload.setdefault("type", "server_error")
    event["response"]["error"] = error_payload
    return cast(dict[str, JsonValue], event)


def _stream_startup_error_response(
    request: Request,
    error: ProxyResponseError | OpenAIErrorEnvelopeModel,
    *,
    headers: Mapping[str, str],
    mask_previous_response_not_found: bool = True,
) -> JSONResponse:
    if isinstance(error, ProxyResponseError):
        envelope = _parse_error_envelope(error.payload)
        if mask_previous_response_not_found:
            status_code, envelope = _mask_previous_response_not_found_error(envelope, default_status=error.status_code)
        else:
            status_code = error.status_code
        return _logged_error_json_response(
            request,
            status_code,
            envelope.model_dump(mode="json", exclude_none=True),
            headers=headers,
        )
    if mask_previous_response_not_found:
        status_code, envelope = _mask_previous_response_not_found_error(error)
    else:
        status_code, envelope = _status_for_error(error.error), error
    return _logged_error_json_response(
        request,
        status_code,
        envelope.model_dump(mode="json", exclude_none=True),
        headers=headers,
    )


def _stream_event_error(event_block: str) -> tuple[OpenAIErrorEnvelopeModel, int | None] | None:
    payload = _parse_sse_payload(event_block)
    if payload is None:
        return None
    return _stream_event_error_from_payload(payload)


def _stream_event_error_from_payload(
    payload: dict[str, JsonValue],
) -> tuple[OpenAIErrorEnvelopeModel, int | None] | None:
    status_code = _stream_event_status(payload)
    event_type = payload.get("type")
    if event_type == "error":
        return _parse_event_error_envelope(payload), status_code
    if event_type != "response.failed":
        return None
    response = payload.get("response")
    if not isinstance(response, dict):
        return _default_error_envelope(), status_code
    error_value = response.get("error")
    if isinstance(error_value, dict):
        try:
            return OpenAIErrorEnvelopeModel.model_validate({"error": error_value}), status_code
        except ValidationError:
            return _default_error_envelope(), status_code
    parsed = parse_response_payload(response)
    if parsed is not None and parsed.error is not None:
        return _error_envelope_from_response(parsed.error), status_code
    return _default_error_envelope(), status_code


def _stream_event_status(payload: Mapping[str, JsonValue]) -> int | None:
    status = payload.get("status")
    if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599:
        return status
    return None


def _parse_sse_payload(line: str) -> dict[str, JsonValue] | None:
    return parse_sse_data_json(line)


def _logged_error_json_response(
    request: Request,
    status_code: int,
    content: Mapping[str, JsonValue] | OpenAIErrorEnvelopeModel | OpenAIErrorEnvelope,
    *,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    code, message = _error_details_from_content(content)
    effective_headers = dict(headers or {})
    if status_code == 429 and is_local_overload_error_code(code):
        effective_headers = merge_retry_after_headers(effective_headers)
    log_error_response(
        logger,
        request,
        status_code,
        code,
        message,
        category="proxy_error_response",
    )
    return JSONResponse(status_code=status_code, content=content, headers=effective_headers or None)


def _error_details_from_content(
    content: Mapping[str, JsonValue] | OpenAIErrorEnvelopeModel | OpenAIErrorEnvelope,
) -> tuple[str | None, str | None]:
    if isinstance(content, OpenAIErrorEnvelopeModel):
        error = content.error
        if error is None:
            return None, None
        return error.code, error.message
    if not isinstance(content, Mapping):
        return None, None
    error = content.get("error")
    if not is_json_mapping(error):
        return None, None
    error_mapping = error
    code = error_mapping.get("code")
    message = error_mapping.get("message")
    return code if isinstance(code, str) else None, message if isinstance(message, str) else None


async def _validate_proxy_websocket_request(
    websocket: WebSocket,
) -> tuple[ApiKeyData | None, JSONResponse | None]:
    denial = await _websocket_firewall_denial_response(websocket)
    if denial is not None:
        return None, denial
    try:
        if "request" in inspect.signature(validate_proxy_api_key_authorization).parameters:
            api_key = await validate_proxy_api_key_authorization(
                websocket.headers.get("authorization"),
                request=websocket,
            )
        else:
            api_key = await validate_proxy_api_key_authorization(websocket.headers.get("authorization"))
    except ProxyAuthError as exc:
        return None, JSONResponse(
            status_code=exc.status_code,
            content=openai_error(exc.code, exc.message, error_type=exc.error_type),
        )
    return api_key, None


async def _validate_internal_bridge_api_key(
    request: Request,
) -> tuple[ApiKeyData | None, JSONResponse | None]:
    dashboard_settings = await get_settings_cache().get()
    if not dashboard_settings.api_key_auth_enabled:
        return None, None
    try:
        api_key = await validate_proxy_api_key_authorization(
            request.headers.get("authorization"),
            request=request,
        )
    except ProxyAuthError as exc:
        return None, JSONResponse(
            status_code=exc.status_code,
            content=openai_error(exc.code, exc.message, error_type=exc.error_type),
        )
    return api_key, None


async def _websocket_firewall_denial_response(websocket: WebSocket) -> JSONResponse | None:
    settings = get_settings()
    client_ip = resolve_connection_client_ip(
        websocket.headers,
        websocket.client.host if websocket.client else None,
        trust_proxy_headers=settings.firewall_trust_proxy_headers,
        trusted_proxy_networks=_parse_trusted_proxy_networks(settings.firewall_trusted_proxy_cidrs),
    )
    async with get_background_session() as session:
        repository = cast(FirewallRepositoryPort, FirewallRepository(session))
        service = FirewallService(repository)
        if await service.is_ip_allowed(client_ip):
            return None
    return JSONResponse(
        status_code=403,
        content=openai_error("ip_forbidden", "Access denied for client IP", error_type="access_error"),
    )


async def _enforce_request_limits(
    api_key: ApiKeyData | None,
    *,
    request_model: str | None,
    request_service_tier: str | None,
) -> ApiKeyUsageReservationData | None:
    if api_key is None:
        return None

    async with get_background_session() as session:
        service = ApiKeysService(ApiKeysRepository(session))
        try:
            return await service.enforce_limits_for_request(
                api_key.id,
                request_model=request_model,
                request_service_tier=request_service_tier,
            )
        except ApiKeyRateLimitExceededError as exc:
            message = f"{exc}. Usage resets at {exc.reset_at.isoformat()}Z."
            raise ProxyRateLimitError(message) from exc
        except ApiKeyInvalidError as exc:
            raise ProxyAuthError(str(exc)) from exc


async def _release_reservation(reservation: ApiKeyUsageReservationData | None) -> None:
    if reservation is None:
        return
    async with get_background_session() as session:
        service = ApiKeysService(ApiKeysRepository(session))
        await service.release_usage_reservation(reservation.reservation_id)


def _effective_model_for_api_key(api_key: ApiKeyData | None, requested_model: str) -> str:
    if api_key is None or api_key.enforced_model is None:
        return requested_model
    return api_key.enforced_model


def _compact_request_service_tier(payload: ResponsesCompactRequest) -> str | None:
    value = payload.service_tier
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


async def _collect_responses_payload(stream: AsyncIterator[str]) -> OpenAIResponseResult:
    output_items: dict[int, dict[str, JsonValue]] = {}
    terminal_result: OpenAIResponseResult | None = None
    contract_violation_kind: str | None = None
    async for line in stream:
        payload = _parse_sse_payload(line)
        if not payload:
            if _looks_like_sse_data_block(line):
                contract_violation_kind = contract_violation_kind or "invalid_json"
            continue
        event_type = payload.get("type")
        _collect_output_item_event(payload, output_items)
        if terminal_result is not None:
            continue
        if event_type == "error":
            terminal_result = _parse_event_error_envelope(payload)
            continue
        if event_type == "response.failed":
            response = payload.get("response")
            if isinstance(response, dict):
                error_value = response.get("error")
                if isinstance(error_value, dict):
                    try:
                        terminal_result = OpenAIErrorEnvelopeModel.model_validate({"error": error_value})
                        continue
                    except ValidationError:
                        terminal_result = _default_error_envelope()
                        continue
                parsed = parse_response_payload(response)
                if parsed is not None and parsed.error is not None:
                    terminal_result = _error_envelope_from_response(parsed.error)
                    continue
            terminal_result = _default_error_envelope()
            continue
        if event_type in ("response.completed", "response.incomplete"):
            response = payload.get("response")
            if is_json_mapping(response):
                normalized_response, violation_kind = _normalize_public_response_mapping(response, output_items)
                if violation_kind is not None:
                    contract_violation_kind = contract_violation_kind or violation_kind
                if normalized_response is not None:
                    parsed = parse_response_payload(normalized_response)
                else:
                    parsed = None
                if parsed is not None:
                    terminal_result = parsed
                    continue
            error_kind = contract_violation_kind or "invalid_json"
            terminal_result = _public_contract_error_envelope(
                error_kind,
                _public_contract_error_message(error_kind),
            )

    if terminal_result is not None:
        return terminal_result
    error_kind = contract_violation_kind or "upstream_stream_truncated"
    return _public_contract_error_envelope(
        error_kind,
        _public_contract_error_message(error_kind),
    )


def _collect_output_item_event(
    payload: dict[str, JsonValue],
    output_items: dict[int, dict[str, JsonValue]],
) -> None:
    event_type = payload.get("type")
    if event_type not in ("response.output_item.added", "response.output_item.done"):
        return
    output_index = payload.get("output_index")
    item = payload.get("item")
    if not isinstance(output_index, int) or not isinstance(item, dict):
        return
    output_items[output_index] = dict(item)


def _merge_collected_output_items(
    response: Mapping[str, JsonValue],
    output_items: dict[int, dict[str, JsonValue]],
) -> dict[str, JsonValue]:
    merged = dict(response)
    if not output_items:
        return merged

    existing_output = response.get("output")
    if isinstance(existing_output, list) and existing_output:
        return merged

    merged["output"] = [item for _, item in sorted(output_items.items())]
    return merged


async def _normalize_public_responses_stream(stream: AsyncIterator[str]) -> AsyncIterator[str]:
    terminal_seen = False
    contract_violation_kind: str | None = None
    seen_text_delta_keys: set[tuple[str | None, int | None]] = set()
    try:
        async for event_block in stream:
            if event_block.strip() == "data: [DONE]":
                if terminal_seen:
                    yield event_block
                continue
            payload = _parse_sse_payload(event_block)
            if payload is None:
                if _looks_like_sse_data_block(event_block):
                    contract_violation_kind = contract_violation_kind or "invalid_json"
                continue
            normalized_payload, violation_kind = _normalize_public_stream_payload(payload)
            if violation_kind is not None:
                contract_violation_kind = contract_violation_kind or violation_kind
            if normalized_payload is None:
                continue
            event_type = normalized_payload.get("type")
            is_terminal_event = isinstance(event_type, str) and event_type in {
                "response.completed",
                "response.incomplete",
                "response.failed",
                "error",
            }
            if is_terminal_event:
                terminal_seen = True
            if event_type == "response.output_text.delta":
                seen_text_delta_keys.add(_text_delta_stream_key(normalized_payload))
            for synthetic_payload in _synthetic_text_delta_events(normalized_payload, seen_text_delta_keys):
                yield format_sse_event(synthetic_payload)
            yield format_sse_event(normalized_payload)
            if is_terminal_event:
                return
        if terminal_seen:
            return
        error_kind = contract_violation_kind or "upstream_stream_truncated"
        yield format_sse_event(
            response_failed_event(
                error_kind,
                _public_contract_error_message(error_kind),
            )
        )
    finally:
        if terminal_seen:
            await _drain_async_iterator(stream)
        await _close_async_iterator(stream)


async def _drain_async_iterator(stream: AsyncIterator[str]) -> None:
    try:
        async for _ in stream:
            pass
    except Exception:
        logger.debug("Failed to drain response stream after terminal event", exc_info=True)


def _normalize_public_stream_payload(
    payload: dict[str, JsonValue],
) -> tuple[dict[str, JsonValue] | None, str | None]:
    event_type = payload.get("type")
    if event_type == "error":
        parsed_error = _parse_event_error_envelope(payload)
        if _is_previous_response_not_found_public_error(parsed_error.error):
            return _stream_incomplete_response_failed_payload(), None
        return payload, None
    if event_type == "response.failed":
        parsed_error = _stream_event_error_from_payload(payload)
        if parsed_error is not None and _is_previous_response_not_found_public_error(parsed_error[0].error):
            return _stream_incomplete_response_failed_payload(), None
        return payload, None
    if event_type in ("response.completed", "response.incomplete"):
        response = payload.get("response")
        if not is_json_mapping(response):
            return (
                cast(
                    dict[str, JsonValue],
                    response_failed_event(
                        "invalid_json",
                        _public_contract_error_message("invalid_json"),
                    ),
                ),
                "invalid_json",
            )
        normalized_response, violation_kind = _normalize_public_response_mapping(response)
        if normalized_response is None:
            error_kind = violation_kind or "invalid_output_item"
            return (
                cast(
                    dict[str, JsonValue],
                    response_failed_event(
                        error_kind,
                        _public_contract_error_message(error_kind),
                    ),
                ),
                error_kind,
            )
        normalized_payload = dict(payload)
        normalized_payload["response"] = normalized_response
        return normalized_payload, violation_kind
    if event_type in ("response.output_item.added", "response.output_item.done"):
        item = payload.get("item")
        if not is_json_mapping(item):
            return None, "invalid_output_item"
        normalized_item = _normalize_public_output_item(item)
        if normalized_item is None:
            return None, "invalid_output_item"
        normalized_payload = dict(payload)
        normalized_payload["item"] = normalized_item
        violation_kind = None
        item_type = item.get("type")
        if isinstance(item_type, str) and not _is_public_passthrough_output_item_type(item_type):
            violation_kind = "invalid_output_item"
        return normalized_payload, violation_kind
    return payload, None


def _stream_incomplete_response_failed_payload() -> dict[str, JsonValue]:
    return cast(
        dict[str, JsonValue],
        response_failed_event(
            "stream_incomplete",
            "Upstream websocket closed before response.completed",
        ),
    )


def _synthetic_text_delta_events(
    payload: Mapping[str, JsonValue],
    seen_text_delta_keys: set[tuple[str | None, int | None]],
) -> list[dict[str, JsonValue]]:
    event_type = payload.get("type")
    if event_type == "response.output_item.done":
        output_index = payload.get("output_index")
        item = payload.get("item")
        if isinstance(output_index, int) and is_json_mapping(item):
            synthetic = _synthetic_text_delta_for_output_item(output_index, item, seen_text_delta_keys)
            return [synthetic] if synthetic is not None else []
    if event_type not in {"response.completed", "response.incomplete"}:
        return []
    response = payload.get("response")
    if not is_json_mapping(response):
        return []
    output = response.get("output")
    if not isinstance(output, list):
        return []

    synthetic_events: list[dict[str, JsonValue]] = []
    for output_index, item in enumerate(output):
        if not is_json_mapping(item):
            continue
        synthetic = _synthetic_text_delta_for_output_item(output_index, item, seen_text_delta_keys)
        if synthetic is not None:
            synthetic_events.append(synthetic)
    return synthetic_events


def _synthetic_text_delta_for_output_item(
    output_index: int,
    item: Mapping[str, JsonValue],
    seen_text_delta_keys: set[tuple[str | None, int | None]],
) -> dict[str, JsonValue] | None:
    normalized_item = _normalize_public_output_item(item)
    if normalized_item is None:
        return None
    if normalized_item.get("type") != "message":
        return None
    text = _extract_message_output_text(normalized_item)
    if text is None:
        return None
    key = _output_item_stream_key(output_index, normalized_item)
    if _seen_text_delta_for_output_item(key, seen_text_delta_keys):
        return None
    seen_text_delta_keys.add(key)

    event: dict[str, JsonValue] = {
        "type": "response.output_text.delta",
        "output_index": output_index,
        "content_index": 0,
        "delta": text,
    }
    item_id = normalized_item.get("id")
    if isinstance(item_id, str) and item_id:
        event["item_id"] = item_id
    return event


def _text_delta_stream_key(payload: Mapping[str, JsonValue]) -> tuple[str | None, int | None]:
    item_id = payload.get("item_id")
    output_index = payload.get("output_index")
    return (
        item_id if isinstance(item_id, str) and item_id else None,
        output_index if isinstance(output_index, int) else None,
    )


def _output_item_stream_key(
    output_index: int,
    item: Mapping[str, JsonValue],
) -> tuple[str | None, int | None]:
    item_id = item.get("id")
    return (item_id if isinstance(item_id, str) and item_id else None, output_index)


def _seen_text_delta_for_output_item(
    key: tuple[str | None, int | None],
    seen_text_delta_keys: set[tuple[str | None, int | None]],
) -> bool:
    item_id, output_index = key
    return any(
        candidate in seen_text_delta_keys
        for candidate in (
            key,
            (item_id, None) if item_id is not None else None,
            (None, output_index) if output_index is not None else None,
            (None, None),
        )
        if candidate is not None
    )


def _normalize_public_response_mapping(
    response: Mapping[str, JsonValue],
    output_items: dict[int, dict[str, JsonValue]] | None = None,
) -> tuple[dict[str, JsonValue] | None, str | None]:
    merged = _merge_collected_output_items(response, output_items or {})
    output = merged.get("output")
    if not isinstance(output, list):
        return merged, None
    normalized_output: list[JsonValue] = []
    dropped_items = 0
    for item in output:
        if not is_json_mapping(item):
            dropped_items += 1
            continue
        normalized_item = _normalize_public_output_item(item)
        if normalized_item is None:
            dropped_items += 1
            continue
        normalized_output.append(normalized_item)
    if output and not normalized_output:
        _record_public_contract_violation("invalid_output_item")
        return None, "invalid_output_item"
    normalized = dict(merged)
    normalized["output"] = normalized_output
    if dropped_items:
        _record_public_contract_violation("invalid_output_item")
        return normalized, "invalid_output_item"
    return normalized, None


def _normalize_public_output_item(item: Mapping[str, JsonValue]) -> dict[str, JsonValue] | None:
    item_type = item.get("type")
    if isinstance(item_type, str) and _is_public_passthrough_output_item_type(item_type):
        return dict(item)
    text_value = _extract_public_output_item_text(item)
    if text_value is None:
        return None
    normalized: dict[str, JsonValue] = {
        "type": "message",
        "role": "assistant",
        "status": item.get("status") if isinstance(item.get("status"), str) else "completed",
        "content": [{"type": "output_text", "text": text_value}],
    }
    item_id = item.get("id")
    if isinstance(item_id, str) and item_id:
        normalized["id"] = item_id
    return normalized


def _is_public_passthrough_output_item_type(item_type: str) -> bool:
    if item_type in _PUBLIC_RESPONSE_OUTPUT_ITEM_TYPES:
        return True
    return item_type.endswith("_call") or item_type.endswith("_call_output")


def _extract_public_output_item_text(item: Mapping[str, JsonValue]) -> str | None:
    direct_text = item.get("text")
    if isinstance(direct_text, str) and direct_text:
        return direct_text
    content = item.get("content")
    if is_json_mapping(content):
        content_parts: list[Mapping[str, JsonValue]] = [content]
    elif isinstance(content, list):
        content_parts = [part for part in content if is_json_mapping(part)]
    else:
        content_parts = []
    parts: list[str] = []
    for part in content_parts:
        part_type = part.get("type")
        if isinstance(part_type, str) and part_type in _PUBLIC_RESPONSE_TEXT_PART_TYPES:
            text = part.get("text")
            if isinstance(text, str) and text:
                parts.append(text)
                continue
        text = part.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    if parts:
        return "".join(parts)
    summary = item.get("summary")
    if isinstance(summary, str) and summary:
        return summary
    return None


def _extract_message_output_text(item: Mapping[str, JsonValue]) -> str | None:
    content = item.get("content")
    if is_json_mapping(content):
        content_parts: list[Mapping[str, JsonValue]] = [content]
    elif isinstance(content, list):
        content_parts = [part for part in content if is_json_mapping(part)]
    else:
        return None
    parts: list[str] = []
    for part in content_parts:
        if part.get("type") != "output_text":
            continue
        text = part.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    return "".join(parts) if parts else None


def _looks_like_sse_data_block(event_block: str) -> bool:
    return "data:" in event_block


def _public_contract_error_message(kind: str) -> str:
    if kind == "invalid_json":
        return "Responses stream produced an invalid JSON payload"
    if kind == "invalid_output_item":
        return "Responses stream produced unsupported output items"
    if kind == "upstream_stream_truncated":
        return "Responses stream ended before a terminal event"
    return "Responses stream violated the public contract"


def _public_contract_error_envelope(kind: str, message: str) -> OpenAIErrorEnvelopeModel:
    _record_public_contract_violation(kind)
    return OpenAIErrorEnvelopeModel(
        error=OpenAIError(
            message=message,
            type="server_error",
            code=kind,
        )
    )


def _record_public_contract_violation(kind: str) -> None:
    logger.warning("bridge_public_contract_violation kind=%s", kind)
    if PROMETHEUS_AVAILABLE and bridge_public_contract_error_total is not None:
        bridge_public_contract_error_total.labels(kind=kind).inc()


def _parse_event_error_envelope(payload: dict[str, JsonValue]) -> OpenAIErrorEnvelopeModel:
    error_value = payload.get("error")
    if isinstance(error_value, dict):
        try:
            return OpenAIErrorEnvelopeModel.model_validate({"error": error_value})
        except ValidationError:
            return _default_error_envelope()
    return _default_error_envelope()


def _default_error_envelope() -> OpenAIErrorEnvelopeModel:
    return OpenAIErrorEnvelopeModel(
        error=OpenAIError(
            message="Upstream error",
            type="server_error",
            code="upstream_error",
        )
    )


def _parse_error_envelope(payload: JsonValue | OpenAIErrorEnvelope) -> OpenAIErrorEnvelopeModel:
    if not isinstance(payload, dict):
        return _default_error_envelope()
    if payload.get("type") == "error":
        return _parse_event_error_envelope(cast(dict[str, JsonValue], payload))
    try:
        return OpenAIErrorEnvelopeModel.model_validate(payload)
    except ValidationError:
        return _default_error_envelope()


def _openai_invalid_transcription_model_error(model: str) -> OpenAIErrorEnvelope:
    error = openai_error(
        "invalid_request_error",
        f"Unsupported transcription model '{model}'. Only '{_TRANSCRIPTION_MODEL}' is supported.",
        error_type="invalid_request_error",
    )
    error["error"]["param"] = "model"
    return error


def _error_envelope_from_response(error_value: OpenAIError | None) -> OpenAIErrorEnvelopeModel:
    if error_value is None:
        return _default_error_envelope()
    return OpenAIErrorEnvelopeModel(error=error_value)


def _is_previous_response_not_found_public_error(error_value: OpenAIError | None) -> bool:
    if error_value is None:
        return False
    code = error_value.code or error_value.type
    if code in {"bridge_previous_response_not_found", "previous_response_not_found"}:
        return True
    message = error_value.message or ""
    return (
        code in {None, "invalid_request_error"}
        and error_value.param == "previous_response_id"
        and "previous response" in message.lower()
        and "not found" in message.lower()
    )


def _mask_previous_response_not_found_error(
    envelope: OpenAIErrorEnvelopeModel,
    *,
    default_status: int | None = None,
) -> tuple[int, OpenAIErrorEnvelopeModel]:
    if not _is_previous_response_not_found_public_error(envelope.error):
        return default_status if default_status is not None else _status_for_error(envelope.error), envelope
    return (
        502,
        OpenAIErrorEnvelopeModel(
            error=OpenAIError(
                message="Upstream websocket closed before response.completed",
                type="server_error",
                code="stream_incomplete",
            )
        ),
    )


def _status_for_error(error_value: OpenAIError | None) -> int:
    if error_value and error_value.code == "upstream_request_timeout":
        return 504
    if error_value and error_value.code == "previous_response_not_found":
        return 502
    if error_value and error_value.code in _UNAVAILABLE_SELECTION_ERROR_CODES:
        return 503
    if error_value and error_value.code in {"rate_limit_exceeded", "usage_limit_reached", "insufficient_quota"}:
        return 429
    if error_value and error_value.code in {"invalid_api_key", "invalid_authentication"}:
        return 401
    if error_value and error_value.code == "invalid_request_error":
        return 400
    if error_value and error_value.type == "authentication_error":
        return 401
    if error_value and error_value.type == "invalid_request_error":
        return 400
    if error_value and error_value.type in {"rate_limit_error", "usage_limit_reached", "insufficient_quota"}:
        return 429
    return 502
