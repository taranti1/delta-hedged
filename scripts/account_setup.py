#!/usr/bin/env python
"""OPERATOR tool: set up the Kalshi subaccount that System 1 (this repo's M1 market maker) trades on.

A human runs it, one step at a time, in the order of docs/ACCOUNT_SETUP.md. It never chains
steps and never moves money on its own.

    python scripts/account_setup.py                      # = status (read-only)
    python scripts/account_setup.py upgrade-tier --to advanced                 [--execute]
    python scripts/account_setup.py create-subaccount --exchange-index 2       [--execute]
    python scripts/account_setup.py create-key --subaccount 1 --name dh-sub1-runner \\
        --pem ~/.kalshi/dh-sub1-runner.pem --env-file ~/.kalshi/dh-sub1.env \\
        --id-var KALSHI_KEY_ID --path-var KALSHI_PRIVATE_KEY_PATH             [--execute]
    python scripts/account_setup.py transfer --from 0 --to 1 --amount-dollars 150 \\
        --exchange-index 2                                                     [--execute]
    python scripts/account_setup.py shard-transfer --from-subaccount 0 --from-shard 0 \\
        --to-subaccount 1 --to-shard 2 --probe-dollars 1                       [--execute]  # FIRST: $1 test
    python scripts/account_setup.py shard-transfer --from-subaccount 0 --from-shard 0 \\
        --to-subaccount 1 --to-shard 2 --amount-dollars 150                    [--execute]
    python scripts/account_setup.py shard-transfer --resume <transfer_id>      # poll only
    python scripts/account_setup.py set-netting --subaccount 1 --off           [--execute]
    python scripts/account_setup.py forget-pending --id <id>                   # local state only

Safety rules
  * Write commands are DRY RUNS unless --execute is given. A dry run uses a read-only REST
    client (any non-GET raises before it is signed or sent). With --execute the tool prints the
    exact request (method, URL, JSON body) and sends it only after you TYPE the confirmation it
    asks for, at an interactive terminal.
  * Subaccount 0 (the primary account, where the user's other live system, "System 2", trades)
    is touched only as the SOURCE of a transfer. Nothing is ever sent TO subaccount 0.
  * transfer / shard-transfer out of subaccount 0 refuse while a System 2 process runs (ps match
    on kalshi_m1 / two_leg_launcher / short_duration_screener) unless --i-stopped-system2. They
    read the unscoped GET /portfolio/balance (the read System 2's cash parity uses) before and
    after, and print the exact System 2 `declare-transfer` command for the withdrawal.
  * transfer saves its client_transfer_id in the PER-USER state file (not per checkout:
    ~/.kalshi/dh_account_setup/state.<env>.json, override DH_ACCOUNT_SETUP_STATE) BEFORE
    sending, together with the subaccount balances and the unscoped balance read just before
    the FIRST attempt. Re-running the same command after an unknown outcome reuses the id, so
    Kalshi applies the transfer at most once. Once a request may have left, the id is never
    dropped by the tool (whatever the retry's answer: timeout, 5xx, 400, 409, no connection):
    "applied / not applied" is decided from evidence, i.e. the subaccount balance deltas since
    the first attempt (exactly -amount / +amount) and GET /portfolio/subaccounts/transfers.
    A transfer that evidence shows applied is completed (with the System 2 declaration,
    bracketed from the first attempt's reading); one that shows no move stays saved until
    you `forget-pending` it. shard-transfer has no idempotency key in the API, so while one is
    unresolved every further transfer is refused; its result must match the amount exactly
    (1 cent tolerance), and a full shard-transfer needs a successful --probe-dollars transfer
    on the same route first.
  * Every write attempt (dry run, declined, sending, outcome, refusal) is appended to
    data/logs/account_setup.jsonl. No key material is logged; API key ids are masked.

Credentials: the admin steps need an UNRESTRICTED key (a subaccount-restricted key cannot
create subaccounts, transfer funds or manage API keys). The config is --config, else
config/kalshi.admin.yaml if it exists, else config/kalshi.yaml (then the committed example);
credentials are resolved by dh.kalshi.config.load_config (auth.env_file supported).

Endpoints (docs/kalshi_specs/openapi.yaml 3.31.0, operationId in the constants below).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shlex
import subprocess
import sys
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from dh.kalshi.envfile import parse_env_text  # noqa: E402
from dh.kalshi.rest import KalshiHTTPError, NotSentError, TransportError, UnknownOutcome  # noqa: E402

# ----------------------------------------------------------------------------- constants
TIERS = ("basic", "advanced", "expert", "premier", "paragon", "prime", "prestige")
BTC_SERIES = ("KXBTCD", "KXBTC", "KXBTC15M")
CRYPTO_SHARD = 2  # all KXBTC* series and markets report exchange_index 2 (2026-09-25)
MAX_SUBACCOUNT = 63
MAX_SHARD = 100  # IntraExchangeInstanceTransferRequest.*_exchange_shard maximum
MAX_TRANSFER_DOLLARS = Decimal("1000")  # typo guard; M1 needs ~$150
M1_WORST_CASE_DOLLARS = Decimal("50")  # config/m1.yaml risk.max_total_worst_loss
M1_DAILY_HALT_DOLLARS = Decimal("25")  # config/m1.yaml risk.daily_loss_halt
M1_SUGGESTED_DOLLARS = Decimal("150")
SYSTEM2_MIN_SHARD_CASH = Decimal("50")  # System 2 launch preflight: SESSION_BUDGET per traded shard
REQUIRED_RESTRICTED_KEYS = 2  # runner + watchdog

SYSTEM2_PATTERNS = ("kalshi_m1", "two_leg_launcher", "short_duration_screener")
SYSTEM2_ROOT = Path("/Users/thomast/Desktop/trading-strategy/Kalshi")
SYSTEM2_LEDGER = Path("data/two_leg_live/cash_transfers.json")  # written by declare-transfer
SYSTEM2_DECLARE = ".venv/bin/python -m kalshi_m1.experiments.two_leg_launcher declare-transfer"

LOG_PATH = REPO / "data" / "logs" / "account_setup.jsonl"
# Unresolved / finished transfers live in ONE per-user file (not per checkout): a second
# checkout or worktree sees the same saved client_transfer_id and cannot silently start over.
STATE_ENV_VAR = "DH_ACCOUNT_SETUP_STATE"
STATE_DIR = Path.home() / ".kalshi" / "dh_account_setup"
LEGACY_STATE_PATH = REPO / "data" / "logs" / "account_setup_state.json"  # before 2026-09-25 (per checkout)
ADMIN_CONFIG = REPO / "config" / "kalshi.admin.yaml"
DELTA_TOL = Decimal("0.01")  # a balance delta "equals" the amount within 1 cent, never more
PROBE_MAX_DOLLARS = Decimal("5")  # shard-transfer --probe-dollars cap
# a listed transfer counts only if created at most this long before the first attempt (clock skew
# between this host and Kalshi; review NEW-4: was 120 s, which let an EARLIER identical transfer match)
TRANSFER_LIST_SKEW_S = 5


def default_state_path(env: str = "prod") -> Path:
    """The per-user state file (``DH_ACCOUNT_SETUP_STATE`` overrides it)."""
    p = os.environ.get(STATE_ENV_VAR, "")
    return Path(p).expanduser() if p else canonical_state_path(env)


def canonical_state_path(env: str = "prod") -> Path:
    """The per-user state file WITHOUT any override (the one idempotency normally rests on)."""
    return STATE_DIR / f"state.{env}.json"


STATE_COMMANDS = ("transfer", "shard-transfer", "forget-pending")  # commands whose idempotency rests on the state


def state_override_problem(chosen: Path, canonical: Path) -> str:
    """Review NEW-6: a --state-file / DH_ACCOUNT_SETUP_STATE override must not bypass idempotency.
    '' when ``chosen`` is the canonical per-user file, or holds every pending and completed record
    of it; else the refusal (the unresolved / finished transfers the override would not see)."""
    try:
        if chosen.expanduser().resolve() == canonical.expanduser().resolve() or not canonical.exists():
            return ""
    except OSError:
        return ""
    try:
        base = State(canonical)
    except (OSError, ValueError, RuntimeError) as exc:
        return f"the per-user state file {canonical} is unreadable ({type(exc).__name__}): fix it before using another one"
    try:
        other = State(chosen) if chosen.exists() else None
    except (OSError, ValueError, RuntimeError) as exc:
        return f"the state file {chosen} is unreadable ({type(exc).__name__})"
    have_p = set(other.pending) if other else set()
    have_c = {str(c.get("id")) for c in other.completed} if other else set()
    miss_p = [i for i in base.pending if i not in have_p]
    miss_c = [str(c.get("id")) for c in base.completed if str(c.get("id")) not in have_c]
    if not miss_p and not miss_c:
        return ""
    parts = []
    if miss_p:
        parts.append(f"UNRESOLVED transfer(s) {miss_p}")
    if miss_c:
        parts.append(f"{len(miss_c)} completed transfer(s)")
    return (f"the state file {chosen} does not hold the {' and '.join(parts)} of the per-user state {canonical}: "
            "with it this tool could re-send a transfer whose client_transfer_id is saved there, or repeat a "
            "completed one without --again. Use the per-user file, or pass --i-know-state-file if this is deliberate")


STATE_PATH = default_state_path()
ENV_VAR_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# API paths relative to /trade-api/v2 (openapi 3.31.0 operationId)
P_LIMITS = "/account/limits"  # GET GetAccountApiLimits
P_UPGRADE = "/account/api_usage_level/upgrade"  # POST UpgradeAccountApiUsageLevel: no body, 201, 30 write tokens
P_BALANCE = "/portfolio/balance"  # GET GetBalance: no subaccount = primary, all shards
P_SUBACCOUNTS = "/portfolio/subaccounts"  # POST CreateSubaccount {exchange_index}
P_SUB_BALANCES = "/portfolio/subaccounts/balances"  # GET GetSubaccountBalances
P_SUB_TRANSFER = "/portfolio/subaccounts/transfer"  # POST ApplySubaccountTransfer
P_SUB_TRANSFERS = "/portfolio/subaccounts/transfers"  # GET GetSubaccountTransfers
P_NETTING = "/portfolio/subaccounts/netting"  # GET GetSubaccountNetting / PUT UpdateSubaccountNetting
P_SHARD_TRANSFER = "/portfolio/intra_exchange_instance_transfer"  # POST IntraExchangeInstanceTransfer
P_SHARD_TRANSFERS = "/portfolio/intra_exchange_instance_transfers"  # GET list; /{transfer_id} GET one
P_API_KEYS = "/api_keys"  # GET GetApiKeys
P_API_KEYS_GENERATE = "/api_keys/generate"  # POST GenerateApiKey
P_SERIES = "/series/{}"  # GET GetSeries

NON_ATOMIC_WARNING = (
    "Kalshi: cross-exchange-index subaccount transfers run in up to three non-atomic steps. If a "
    "later step fails, completed steps are not undone, so funds may remain in the PRIMARY account "
    "(subaccount 0) on the source or destination exchange index."
)

WRITE_COMMANDS = ("upgrade-tier", "create-subaccount", "create-key", "transfer", "shard-transfer", "set-netting")


class UsageError(ValueError):
    """Bad arguments (exit 2)."""


class Abort(Exception):
    """Raised by a pre-send step: nothing is sent (exit 2)."""


# ----------------------------------------------------------------------------- small helpers
def parse_dollars(text: str) -> Decimal:
    """Positive dollar amount with at most 2 decimals (whole cents), at most MAX_TRANSFER_DOLLARS."""
    try:
        d = Decimal(str(text).strip().lstrip("$"))
    except InvalidOperation:
        raise UsageError(f"not a dollar amount: {text!r}") from None
    if not d.is_finite() or d <= 0:
        raise UsageError("the amount must be a positive number of dollars")
    exp = d.as_tuple().exponent
    if isinstance(exp, int) and exp < -2:
        raise UsageError("the amount has more than 2 decimals (whole cents only)")
    if d > MAX_TRANSFER_DOLLARS:
        raise UsageError(f"the amount is above the ${MAX_TRANSFER_DOLLARS} typo guard of this tool")
    return d.quantize(Decimal("0.01"))


def to_cents(d: Decimal) -> int:
    """ApplySubaccountTransferRequest.amount_cents."""
    return int(d * 100)


def to_centicents(d: Decimal) -> int:
    """IntraExchangeInstanceTransferRequest.amount ('The amount to transfer in centicents')."""
    return int(d * 10_000)


def dec(x: Any) -> Decimal:
    d = Decimal(str(x))
    if not d.is_finite():
        raise ValueError(f"not a finite amount: {x!r}")
    return d


def unscoped_dollars(body: dict[str, Any]) -> Decimal:
    """GetBalanceResponse -> dollars, exactly as System 2 reads it (balance_dollars, else cents)."""
    if "balance_dollars" in body:
        return dec(body["balance_dollars"])
    return Decimal(int(body["balance"])) / 100


def usd(d: Decimal, places: int = 2) -> str:
    return f"${d:.{places}f}"


def mask_id(key_id: Any) -> str:
    s = str(key_id or "")
    return ("..." + s[-6:]) if len(s) > 6 else ("..." if s else "?")


def sanitize(obj: Any) -> Any:
    """Drop key material and mask key ids before anything is logged."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if k in ("private_key", "public_key"):
                out[k] = "<redacted>"
            elif k == "api_key_id":
                out[k] = mask_id(v)
            else:
                out[k] = sanitize(v)
        return out
    if isinstance(obj, list):
        return [sanitize(x) for x in obj]
    return obj


def same_amount(typed: str, amount: Decimal) -> bool:
    try:
        return Decimal(typed.strip().lstrip("$")) == amount
    except (InvalidOperation, ValueError):
        return False


def tier_rank(tier: str) -> int:
    t = str(tier or "").strip().lower()
    return TIERS.index(t) if t in TIERS else -1


def ps_lines() -> list[str]:
    """`ps` output, one '<pid> <command>' line per process (BSD and procps syntax)."""
    r = subprocess.run(["ps", "axo", "pid=,command="], capture_output=True, text=True, timeout=10, check=False)
    if r.returncode != 0:
        raise RuntimeError(f"ps exited {r.returncode}")
    return r.stdout.splitlines()


def system2_processes(lines: Sequence[str], own_pid: int) -> list[str]:
    hits: list[str] = []
    for line in lines:
        s = line.strip()
        pid_s, _, cmd = s.partition(" ")
        try:
            pid = int(pid_s)
        except ValueError:
            continue
        if pid == own_pid:
            continue
        if any(p in cmd for p in SYSTEM2_PATTERNS):
            hits.append(f"pid {pid}: {cmd.strip()[:160]}")
    return hits


def _parse_iso(s: Any) -> datetime:
    v = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    if v.tzinfo is None:
        raise ValueError("timestamp without timezone")
    return v


# ----------------------------------------------------------------------------- request bodies
def create_subaccount_body(exchange_index: int) -> dict[str, Any]:
    return {"exchange_index": exchange_index}  # CreateSubaccountRequest


def subaccount_transfer_body(client_transfer_id: str, from_sub: int, to_sub: int, amount: Decimal, exchange_index: int) -> dict[str, Any]:
    return {  # ApplySubaccountTransferRequest
        "client_transfer_id": client_transfer_id,
        "from_subaccount": from_sub,
        "to_subaccount": to_sub,
        "amount_cents": to_cents(amount),
        "exchange_index": exchange_index,
    }


def shard_transfer_body(amount: Decimal, from_shard: int, to_shard: int, from_sub: int, to_sub: int) -> dict[str, Any]:
    return {  # IntraExchangeInstanceTransferRequest
        "source": "event_contract",
        "destination": "event_contract",
        "amount": to_centicents(amount),
        "source_exchange_shard": from_shard,
        "destination_exchange_shard": to_shard,
        "source_subaccount": from_sub,
        "destination_subaccount": to_sub,
    }


def netting_body(subaccount: int, enabled: bool) -> dict[str, Any]:
    return {"subaccount_number": subaccount, "enabled": enabled}  # UpdateSubaccountNettingRequest


def generate_key_body(name: str, subaccount: int) -> dict[str, Any]:
    # GenerateApiKeyRequest. RSA: dh.kalshi.auth signs RSA-PSS only. read + write: a restricted
    # key still cannot transfer funds, manage subaccounts or API keys (getting_started/subaccounts).
    return {"name": name, "key_type": "rsa", "scopes": ["read", "write"], "subaccount": subaccount}


# ----------------------------------------------------------------------------- context, log, state
@dataclass
class Reading:
    """One unscoped GET /portfolio/balance reading (what System 2's cash parity reads)."""

    at: datetime
    dollars: Decimal

    def as_json(self) -> dict[str, str]:
        return {"at": self.at.isoformat(), "dollars": str(self.dollars)}

    @staticmethod
    def from_json(d: dict[str, Any]) -> Reading:
        return Reading(_parse_iso(d["at"]), dec(d["dollars"]))


@dataclass
class Ctx:
    rest: Any
    base_url: str = ""
    env: str = "prod"
    out: Callable[[str], None] = print
    ask: Callable[[str], str] = input
    ps: Callable[[], list[str]] = ps_lines
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    log_path: Path = LOG_PATH
    state_path: Path = STATE_PATH
    legacy_state_path: Path | None = None  # the old per-checkout file (checked when the state is loaded)
    system2_root: Path = SYSTEM2_ROOT
    key_id: str = ""
    config_source: str = ""
    own_pid: int = field(default_factory=os.getpid)


def log_event(ctx: Ctx, **rec: Any) -> None:
    rec = {"ts": ctx.now().isoformat(), "env": ctx.env, **sanitize(rec)}
    ctx.log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(ctx.log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, default=str, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())


class State:
    """The per-user state file (``default_state_path``): unresolved transfers (pending) and
    finished ones."""

    def __init__(self, path: Path) -> None:
        self.path = path
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("version") != 1 or not isinstance(data.get("pending"), dict) or not isinstance(data.get("completed"), list):
                raise RuntimeError(f"unreadable state file {path}: inspect it, then fix or move it aside")
        else:
            data = {"version": 1, "pending": {}, "completed": []}
        self.data = data

    @property
    def pending(self) -> dict[str, dict[str, Any]]:
        return self.data["pending"]

    @property
    def completed(self) -> list[dict[str, Any]]:
        return self.data["completed"]

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        tmp = self.path.with_name(self.path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, sort_keys=True, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)


def load_state(ctx: Ctx) -> State:
    """The per-user state, after checking the old per-checkout file (``legacy_state_path``):
    its entries are moved into the per-user file the first time (the old file is renamed
    ``.migrated-<unix s>``); if both exist and the old one still holds an UNRESOLVED transfer,
    nothing proceeds until it is resolved by hand (a second checkout must never start over)."""
    st = State(ctx.state_path)
    legacy = ctx.legacy_state_path
    if legacy is None or not legacy.exists() or legacy.resolve() == ctx.state_path.resolve():
        return st
    old = State(legacy)
    if not old.pending and not old.completed:
        return st
    if not ctx.state_path.exists():
        st.data = old.data
        st.save()
        dest = legacy.with_name(f"{legacy.name}.migrated-{int(ctx.now().timestamp())}")
        os.replace(legacy, dest)
        ctx.out(f"note: moved the per-checkout state {legacy} to the per-user state {ctx.state_path} (old file: {dest})")
        return st
    if old.pending:
        raise Abort(f"the old per-checkout state file {legacy} still holds unresolved transfer(s) {list(old.pending)} "
                    f"while the per-user state {ctx.state_path} exists: merge its 'pending' entries into the per-user "
                    "file by hand (or resolve them), then move the old file aside")
    return st


@dataclass
class Outcome:
    kind: str  # dry_run | declined | ok | rejected | unknown | not_sent
    body: Any = None
    status: int = 0
    detail: str = ""


EXIT = {"dry_run": 0, "ok": 0, "declined": 3, "aborted": 2, "rejected": 1, "unknown": 1, "not_sent": 1}


def refuse(ctx: Ctx, command: str, reason: str) -> int:
    ctx.out(f"REFUSED: {reason}")
    log_event(ctx, command=command, event="refused", reason=reason)
    return 2


def show_request(ctx: Ctx, method: str, path: str, body: Any, params: Sequence[tuple[str, str]] = ()) -> None:
    q = ("?" + "&".join(f"{k}={v}" for k, v in params)) if params else ""
    ctx.out(f"Request ({ctx.env}): {method} {ctx.base_url}{path}{q}")
    ctx.out("Body: " + (json.dumps(body, indent=2) if body is not None else "(none)"))


async def confirm_and_send(
    ctx: Ctx,
    *,
    command: str,
    method: str,
    path: str,
    body: Any,
    execute: bool,
    prompt: str,
    matches: Callable[[str], bool],
    params: Sequence[tuple[str, str]] = (),
    before_send: Callable[[], Awaitable[None]] | None = None,
    log_extra: dict[str, Any] | None = None,
) -> Outcome:
    """Print the request; dry run unless execute; send only after the typed confirmation.

    ``before_send`` runs after the confirmation and immediately before the request (final
    checks, the balance reading that opens the System 2 bracket, persisting the idempotency
    key); if it raises Abort or a read fails, nothing is sent."""
    extra = dict(log_extra or {})
    show_request(ctx, method, path, body, params)
    if not execute:
        ctx.out("DRY RUN: nothing was sent. Re-run with --execute to send it (you will be asked to type a confirmation).")
        log_event(ctx, command=command, event="dry_run", method=method, path=path, body=body, **extra)
        return Outcome("dry_run")
    typed = ctx.ask(prompt)
    if not matches(typed or ""):
        ctx.out("Confirmation did not match: nothing was sent.")
        log_event(ctx, command=command, event="declined", method=method, path=path, body=body, **extra)
        return Outcome("declined")
    if before_send is not None:
        try:
            await before_send()
        except (Abort, KalshiHTTPError, TransportError, OSError) as exc:
            ctx.out(f"REFUSED just before sending: {exc}. Nothing was sent.")
            log_event(ctx, command=command, event="aborted", method=method, path=path, reason=str(exc), **extra)
            return Outcome("aborted", detail=str(exc))
    log_event(ctx, command=command, event="sending", method=method, path=path, body=body, **extra)
    try:
        res = await ctx.rest.write(method, path, json_body=body, params=list(params), stream="kalshi.rest.account")
    except KalshiHTTPError as exc:
        ctx.out(f"REJECTED by Kalshi: HTTP {exc.status} {exc.code} {exc.message} (this request was refused).")
        log_event(ctx, command=command, event="rejected", method=method, path=path, status=exc.status,
                  error=f"{exc.code} {exc.message}".strip(), **extra)
        return Outcome("rejected", exc.body, exc.status, str(exc))
    except NotSentError as exc:
        ctx.out(f"NOT SENT: the connection was never established ({exc}): this request never left.")
        log_event(ctx, command=command, event="not_sent", method=method, path=path, error=str(exc), **extra)
        return Outcome("not_sent", detail=str(exc))
    if isinstance(res, UnknownOutcome):
        ctx.out(f"OUTCOME UNKNOWN: {res.reason} (it may or may not have been applied).")
        log_event(ctx, command=command, event="unknown", method=method, path=path, status=res.status,
                  reason=res.reason, response=res.body, **extra)
        return Outcome("unknown", res.body, res.status, res.reason)
    log_event(ctx, command=command, event="ok", method=method, path=path, response=res, **extra)
    return Outcome("ok", res, 200)


# ----------------------------------------------------------------------------- reads
async def read_limits(ctx: Ctx) -> dict[str, Any]:
    return await ctx.rest.get(P_LIMITS, (), stream="kalshi.rest.account")


async def read_sub_balances(ctx: Ctx) -> dict[tuple[int, int], Decimal]:
    """GET /portfolio/subaccounts/balances -> {(subaccount, exchange_index): dollars}."""
    body = await ctx.rest.get(P_SUB_BALANCES, (), stream="kalshi.rest.portfolio")
    out: dict[tuple[int, int], Decimal] = {}
    for r in body.get("subaccount_balances") or []:
        key = (int(r["subaccount_number"]), int(r["exchange_index"]))
        out[key] = out.get(key, Decimal(0)) + dec(r["balance"])
    return out


async def read_netting(ctx: Ctx) -> list[dict[str, Any]]:
    body = await ctx.rest.get(P_NETTING, (), stream="kalshi.rest.portfolio")
    return list(body.get("netting_configs") or [])


async def read_unscoped(ctx: Ctx) -> Reading:
    # no subaccount, no exchange_index: the primary account's aggregate over all shards, the
    # exact read System 2's cash parity uses (changelog 2026-08-13; System 2 D-059)
    body = await ctx.rest.get(P_BALANCE, (), stream="kalshi.rest.portfolio")
    return Reading(ctx.now(), unscoped_dollars(body))


async def read_api_keys(ctx: Ctx) -> list[dict[str, Any]]:
    body = await ctx.rest.get(P_API_KEYS, (), stream="kalshi.rest.account")
    return list(body.get("api_keys") or [])


async def read_sub_transfers(ctx: Ctx, pages: int = 5) -> list[dict[str, Any]]:
    """GET /portfolio/subaccounts/transfers (GetSubaccountTransfers: transfer_id, from_subaccount,
    to_subaccount, amount_cents, exchange_index, created_ts), newest pages first."""
    out: list[dict[str, Any]] = []
    cursor = ""
    for _ in range(max(1, pages)):
        params: list[tuple[str, str]] = [("limit", "100")] + ([("cursor", cursor)] if cursor else [])
        body = await ctx.rest.get(P_SUB_TRANSFERS, params, stream="kalshi.rest.portfolio")
        out += [t for t in body.get("transfers") or [] if isinstance(t, dict)]
        cursor = str(body.get("cursor") or "")
        if not cursor:
            break
    return out


def rows_json(table: dict[tuple[int, int], Decimal]) -> dict[str, str]:
    return {f"{s}:{x}": str(v) for (s, x), v in table.items()}


def rows_from_json(d: dict[str, Any] | None) -> dict[tuple[int, int], Decimal] | None:
    if not isinstance(d, dict):
        return None
    out: dict[tuple[int, int], Decimal] = {}
    for k, v in d.items():
        s, x = str(k).split(":")
        out[(int(s), int(x))] = dec(v)
    return out


def near(a: Decimal, b: Decimal) -> bool:
    return abs(a - b) <= DELTA_TOL


@dataclass
class Evidence:
    """What the balances and the transfer list say about one same-shard transfer since its FIRST
    attempt: ``balance`` applied (source -amount AND destination +amount on the transfer's own
    route, within 1 cent) / none (both unchanged) / mixed (anything else) / unreadable; ``listed``
    True / False / None (the list could not be read): a row of GET /portfolio/subaccounts/transfers
    with the same route and amount, created at most TRANSFER_LIST_SKEW_S before the first attempt,
    after every earlier identical completed transfer, and NOT claimed by another record
    (``transfer_id``: the row this record claims; review NEW-4)."""

    balance: str
    listed: bool | None
    d_from: Decimal | None = None
    d_to: Decimal | None = None
    detail: str = ""
    transfer_id: str = ""  # the listed transfer this evidence claims ('' = none / list unreadable)
    candidates: int = 0  # unclaimed listed rows that match (more than one: ambiguous)

    @property
    def verdict(self) -> str:
        """applied | not_seen (nothing moved, nothing listed) | conflict | unresolved.

        ``applied`` needs the EXACT balance deltas on the route (review NEW-4: a listed row plus
        unrelated balance movement is never enough) and, when the list is readable, exactly one
        unclaimed matching row (never a row another record already claimed)."""
        if self.balance == "applied":
            if self.listed is None:
                return "applied"  # the list could not be read: the exact deltas on the route decide
            return "applied" if self.listed and self.candidates == 1 else "unresolved"
        if self.balance == "none":
            return "conflict" if self.listed else "not_seen"
        return "unresolved"


def claimed_transfer_ids(st: State | None) -> set[str]:
    """Listed transfer ids already claimed by a completed record (review NEW-4)."""
    if st is None:
        return set()
    return {str(c.get("claimed_transfer_id")) for c in st.completed if c.get("claimed_transfer_id")}


async def transfer_evidence(ctx: Ctx, e: dict[str, Any], amount: Decimal, st: State | None = None) -> Evidence:
    p = e["params"]
    frm, to, idx = int(p["from_subaccount"]), int(p["to_subaccount"]), int(p["exchange_index"])
    first = rows_from_json(e.get("first_rows"))
    balance, d_from, d_to, detail = "unreadable", None, None, "no subaccount balances saved at the first attempt"
    if first is not None:
        try:
            now = await read_sub_balances(ctx)
        except (KalshiHTTPError, TransportError, KeyError, ValueError, TypeError, InvalidOperation) as exc:
            detail = f"GET /portfolio/subaccounts/balances failed: {type(exc).__name__}"
        else:
            d_from = now.get((frm, idx), Decimal(0)) - first.get((frm, idx), Decimal(0))
            d_to = now.get((to, idx), Decimal(0)) - first.get((to, idx), Decimal(0))
            if near(d_from, -amount) and near(d_to, amount):
                balance = "applied"
            elif near(d_from, Decimal(0)) and near(d_to, Decimal(0)):
                balance = "none"
            else:
                balance = "mixed"
            detail = f"since the first attempt: subaccount {frm} {d_from:+}, subaccount {to} {d_to:+} (shard {idx})"
    listed: bool | None = None
    tid, n_cand = "", 0
    try:
        since = int(_parse_iso(e.get("first_attempt_at") or e.get("created_at")).timestamp()) - TRANSFER_LIST_SKEW_S
        # never a row created before an earlier identical transfer completed (that one's own row)
        for c in (st.completed if st is not None else []):
            if c.get("kind") == e.get("kind") and c.get("params") == e.get("params") and c.get("completed_at"):
                since = max(since, int(_parse_iso(c["completed_at"]).timestamp()) + 1)
        claimed = claimed_transfer_ids(st)
        cents = to_cents(amount)
        rows = await read_sub_transfers(ctx)
        cand = [t for t in rows if int(t.get("from_subaccount", -1)) == frm and int(t.get("to_subaccount", -1)) == to
                and int(t.get("amount_cents", -1)) == cents and int(t.get("exchange_index", 0)) == idx
                and int(t.get("created_ts") or 0) >= since and str(t.get("transfer_id") or "") not in claimed]
        n_cand = len(cand)
        listed = n_cand > 0
        if n_cand == 1:
            tid = str(cand[0].get("transfer_id") or "")
        elif n_cand > 1:
            detail += f"; {n_cand} unclaimed identical transfers listed (ambiguous)"
    except (KalshiHTTPError, TransportError, KeyError, ValueError, TypeError) as exc:
        detail += f"; GET /portfolio/subaccounts/transfers failed: {type(exc).__name__}"
    return Evidence(balance, listed, d_from, d_to, detail, tid, n_cand)


async def own_key_restriction(ctx: Ctx) -> tuple[str, int | None]:
    """('unrestricted' | 'restricted' | 'unknown', subaccount) of the key this tool signs with."""
    try:
        keys = await read_api_keys(ctx)
    except KalshiHTTPError as exc:
        return ("restricted", None) if exc.status == 403 else ("unknown", None)
    for k in keys:
        if ctx.key_id and str(k.get("api_key_id")) == ctx.key_id:
            sub = k.get("subaccount")
            return ("restricted", int(sub)) if sub is not None else ("unrestricted", None)
    return "unknown", None


async def admin_key_problem(ctx: Ctx) -> str | None:
    kind, sub = await own_key_restriction(ctx)
    if kind == "restricted":
        where = f"subaccount {sub}" if sub is not None else "a single subaccount (GET /api_keys is forbidden)"
        return (f"the key this tool signs with ({mask_id(ctx.key_id)}) is restricted to {where}; admin steps need "
                "the UNRESTRICTED key: pass --config config/kalshi.admin.yaml (docs/ACCOUNT_SETUP.md step 0)")
    if kind == "unknown":
        ctx.out(f"note: could not tell whether key {mask_id(ctx.key_id)} is subaccount-restricted (not in GET /api_keys)")
    return None


def numbered_subaccounts(table: dict[tuple[int, int], Decimal], netting: list[dict[str, Any]]) -> dict[int, set[int]]:
    """numbered subaccount -> shards it is listed on (balances rows, netting rows)."""
    out: dict[int, set[int]] = {}
    for sub, shard in table:
        if sub >= 1:
            out.setdefault(sub, set()).add(shard)
    for r in netting:
        try:
            sub, shard = int(r["subaccount_number"]), int(r.get("exchange_index", -1))
        except (KeyError, TypeError, ValueError):
            continue
        if sub >= 1:
            out.setdefault(sub, set())
            if shard >= 0:
                out[sub].add(shard)
    return out


def system2_check(ctx: Ctx, allow: bool) -> tuple[list[str], str | None]:
    """(matching processes, refusal reason or None). A failing ps fails closed."""
    try:
        procs = system2_processes(ctx.ps(), ctx.own_pid)
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        procs = [f"(could not list processes: {exc})"]
    if not procs:
        return procs, None
    ctx.out("System 2 processes found:")
    for p in procs:
        ctx.out(f"  {p}")
    if allow:
        ctx.out("  --i-stopped-system2 given: proceeding (no System 2 SESSION may run during the transfer).")
        return procs, None
    return procs, ("a System 2 process appears to be running. A transfer out of subaccount 0 while a System 2 "
                   "session runs breaks its session cash parity (campaign stop). Stop the watch and let any "
                   "session end (docs/ACCOUNT_SETUP.md step 4), then re-run; --i-stopped-system2 overrides "
                   "(e.g. only the read-only screener is left)")


def system2_recheck(ctx: Ctx, allow: bool) -> None:
    """Last look right before a transfer out of subaccount 0 leaves (a watch may have launched a
    session while the operator was typing)."""
    if allow:
        return
    try:
        procs = system2_processes(ctx.ps(), ctx.own_pid)
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        raise Abort(f"could not list processes ({exc})") from None
    if procs:
        raise Abort("a System 2 process started: " + "; ".join(procs))


# ----------------------------------------------------------------------------- System 2 declaration
def declare_command(root: Path, amount: str, after: str, before: str, note: str) -> str:
    return (f"cd {shlex.quote(str(root))} && {SYSTEM2_DECLARE} --amount={amount} "
            f"--after {after} --before {before} --note {shlex.quote(note)}")


def build_declaration(ctx: Ctx, amount: Decimal, pre: Reading, post: Reading, note: str, *, retried: bool = False) -> dict[str, Any]:
    """The System 2 declaration for a withdrawal of `amount` from subaccount 0, for a transfer
    the evidence shows APPLIED (never call it for one that is not).

    System 2's settlement cash parity compares its unscoped balance read with its own fills and
    settlements; a transfer out of subaccount 0 is invisible to both, so it must be declared as
    a withdrawal bracketed by the last reading WITHOUT the change (`after`) and the first WITH it
    (`before`) (two_leg_launcher declare-transfer, System 2 DECISIONS.md D-059). ``pre`` is the
    reading taken just before the FIRST attempt (a retry never moves the bracket's start: the
    first attempt may be the one that applied it). It is required when System 2's reading moved
    (a move that its unscoped read does not see must not be declared)."""
    expected = -amount
    observed = post.dollars - pre.dollars
    d: dict[str, Any] = {
        "required": True,
        "amount": f"{expected:.2f}",
        "after": pre.at.isoformat(),
        "before": post.at.isoformat(),
        "observed_delta": str(observed),
        "note": note,
        "warning": "",
    }
    if observed == 0:
        d["required"] = False
        d["warning"] = ("the unscoped balance did NOT change, which contradicts the documented semantics "
                        "(omitted subaccount = primary account only). Do NOT declare anything yet: re-check with "
                        "`status` and compare GET /portfolio/balance with the subaccount-0 rows.")
    elif observed != expected:
        d["warning"] = (f"the unscoped balance changed by {observed} instead of {expected}: something else moved it "
                        "between the two readings (a settlement of a held System 2 position?). System 2 counts its "
                        "own settlements itself, so declare the transfer amount; if its next parity check still "
                        "shows a residual, look at what landed in the bracket.")
    if retried:
        d["warning"] = ((d["warning"] + " ") if d["warning"] else "") + (
            "The transfer needed more than one attempt, so the bracket starts at the FIRST attempt's reading; "
            "if a System 2 session ran since then, its anchor lies inside the bracket and System 2 fails closed "
            "(cash_transfer_straddles_anchor): check with its settlement_check.")
    d["command"] = declare_command(ctx.system2_root, d["amount"], d["after"], d["before"], note)
    return d


def print_declaration(ctx: Ctx, decl: dict[str, Any]) -> None:
    ctx.out("")
    if not decl["required"]:
        ctx.out("System 2 declaration: NOT required by the observed balance change. " + decl["warning"])
        return
    ctx.out("=== System 2 must be told about this withdrawal from subaccount 0 ===")
    ctx.out("Its cash parity reads the unscoped GET /portfolio/balance, which this transfer lowered.")
    ctx.out("Precondition: no System 2 session running (keep the watch stopped until this is done).")
    ctx.out("Run (offline, no network; appends a row to data/two_leg_live/cash_transfers.json there):")
    ctx.out(f"  {decl['command']}")
    if decl["warning"]:
        ctx.out(f"CHECK: {decl['warning']}")


def system2_ledger(root: Path) -> tuple[list[dict[str, Any]] | None, str]:
    p = root / SYSTEM2_LEDGER
    if not p.exists():
        return [], f"{p} does not exist (nothing declared yet)"
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return list(data.get("transfers") or []), str(p)
    except (OSError, ValueError) as exc:
        return None, f"cannot read {p}: {type(exc).__name__}"


def is_declared(decl: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    try:
        want = (dec(decl["amount"]), _parse_iso(decl["after"]), _parse_iso(decl["before"]))
    except (KeyError, ValueError, InvalidOperation):
        return False
    for r in rows:
        try:
            if (dec(r["amount"]), _parse_iso(r["after"]), _parse_iso(r["before"])) == want:
                return True
        except (KeyError, ValueError, InvalidOperation):
            continue
    return False


# ----------------------------------------------------------------------------- status
async def _try(coro: Awaitable[Any]) -> tuple[Any, str | None]:
    try:
        return await coro, None
    except (KalshiHTTPError, TransportError, KeyError, ValueError, TypeError, InvalidOperation) as exc:
        return None, f"{type(exc).__name__}: {exc}"


async def cmd_status(ctx: Ctx, args: argparse.Namespace) -> int:
    out = ctx.out
    target = int(args.subaccount)
    out(f"Kalshi account setup status ({ctx.env}: {ctx.base_url}); config {ctx.config_source or '?'}")
    out("Read-only: GET requests only.")

    out("\n[key used by this tool]")
    kind, ksub = await own_key_restriction(ctx)
    out(f"  {mask_id(ctx.key_id)}: {kind}" + (f" to subaccount {ksub}" if ksub is not None else ""))

    out("\n[API tier] GET /account/limits")
    limits, err = await _try(read_limits(ctx))
    tier = ""
    if err:
        out(f"  error: {err}")
    else:
        tier = str(limits.get("usage_tier", "")).lower()
        rd, wr = limits.get("read") or {}, limits.get("write") or {}
        out(f"  usage_tier {tier or '?'}; read {rd.get('refill_rate')}/s (capacity {rd.get('bucket_capacity')}); "
            f"write {wr.get('refill_rate')}/s (capacity {wr.get('bucket_capacity')})")
        for g in limits.get("grants") or []:
            out(f"  grant: {g.get('level')} on {g.get('exchange_instance')} (source {g.get('source')}, "
                f"expires {g.get('expires_ts') or 'never'})")

    out("\n[balances per (subaccount, shard)] GET /portfolio/subaccounts/balances")
    table, err = await _try(read_sub_balances(ctx))
    if err:
        out(f"  error: {err}")
        table = {}
    for (sub, shard), bal in sorted(table.items()):
        out(f"  subaccount {sub:>2}  shard {shard}  {usd(bal, 4)}")
    netting, nerr = await _try(read_netting(ctx))
    netting = netting or []
    subs = numbered_subaccounts(table, netting)
    out(f"  numbered subaccounts: {', '.join(f'{s} (shards {sorted(v)})' for s, v in sorted(subs.items())) or 'none'}")

    out("\n[primary balance as System 2 reads it] GET /portfolio/balance (no parameters)")
    reading, err = await _try(read_unscoped(ctx))
    if err:
        out(f"  error: {err}")
    else:
        sub0 = sum((b for (s, _), b in table.items() if s == 0), Decimal(0))
        same = "equal" if reading.dollars == sub0 else "DIFFERENT (verify: omitted subaccount should mean primary only)"
        out(f"  unscoped {usd(reading.dollars, 4)}; sum of subaccount-0 rows {usd(sub0, 4)}: {same}")

    out("\n[netting] GET /portfolio/subaccounts/netting")
    if nerr:
        out(f"  error: {nerr}")
    for r in sorted(netting, key=lambda r: (r.get("subaccount_number", 0), r.get("exchange_index", 0))):
        out(f"  subaccount {r.get('subaccount_number')}  shard {r.get('exchange_index')}  "
            f"netting {'ON' if r.get('enabled') else 'off'}")

    out("\n[BTC series shards] GET /series/{ticker}")
    shards: dict[str, Any] = {}
    for t in BTC_SERIES:
        body, err = await _try(ctx.rest.get(P_SERIES.format(t), (), stream="kalshi.rest.series"))
        shards[t] = err or (body.get("series") or {}).get("exchange_index")
        out(f"  {t}: exchange_index {shards[t]}")
    btc_on_2 = all(v == CRYPTO_SHARD for v in shards.values())

    out("\n[API keys] GET /api_keys")
    keys, err = await _try(read_api_keys(ctx))
    keys = keys or []
    if err:
        out(f"  error: {err}")
    for k in keys:
        out(f"  {k.get('name')!s:<24} {mask_id(k.get('api_key_id'))}  scopes {k.get('scopes')}  "
            f"subaccount {k.get('subaccount') if k.get('subaccount') is not None else 'unrestricted'}")
    target_keys = [k for k in keys if k.get("subaccount") == target]

    out("\n[recent transfers]")
    body, err = await _try(ctx.rest.get(P_SUB_TRANSFERS, [("limit", "5")], stream="kalshi.rest.portfolio"))
    for t in (body or {}).get("transfers") or []:
        out(f"  subaccount {t.get('from_subaccount')} -> {t.get('to_subaccount')} shard {t.get('exchange_index')} "
            f"{usd(Decimal(int(t.get('amount_cents', 0))) / 100)} at {t.get('created_ts')} (id {t.get('transfer_id')})")
    if err:
        out(f"  subaccount transfers: error {err}")
    body, err = await _try(ctx.rest.get(P_SHARD_TRANSFERS, [("limit", "5")], stream="kalshi.rest.portfolio"))
    for t in (body or {}).get("transfers") or []:
        out(f"  shard {t.get('source_exchange_shard')} -> {t.get('destination_exchange_shard')} "
            f"${t.get('amount')} {t.get('status')} at {t.get('created_ts')} (id {t.get('transfer_id')})")
    if err:
        out(f"  intra-account transfers: error {err}")

    out("\n[System 2 processes] ps match on " + ", ".join(SYSTEM2_PATTERNS))
    try:
        procs = system2_processes(ctx.ps(), ctx.own_pid)
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        procs = [f"(could not list processes: {exc})"]
    for p in procs or ["none"]:
        out(f"  {p}")

    out("\n[local state] " + str(ctx.state_path) + " (per user, shared by every checkout)")
    legacy_problem = ""
    try:
        st = load_state(ctx)
    except Abort as exc:
        legacy_problem = str(exc)
        out(f"  PROBLEM: {exc}")
        st = State(ctx.state_path)
    for pid_, e in st.pending.items():
        ev = e.get("evidence") or {}
        out(f"  UNRESOLVED {e.get('kind')} {pid_}: {e.get('params')} (attempts {e.get('attempts', 0)}, "
            f"transfer_id {e.get('transfer_id', '-')}" + (f"; last evidence: {ev.get('balance')}, listed {ev.get('listed')}, "
                                                          f"{ev.get('detail')}" if ev else "") + ")")
    rows, where = system2_ledger(ctx.system2_root)
    missing: list[dict[str, Any]] = []
    for e in st.completed:
        decl = e.get("declaration") or {}
        if not decl.get("required"):
            continue
        done = rows is not None and is_declared(decl, rows)
        out(f"  transfer {e.get('id')} {decl.get('amount')}: {'declared to System 2' if done else 'NOT declared to System 2'}")
        if not done:
            missing.append(decl)
    out(f"  System 2 ledger: {where}")

    # --------------------------------------------------------------- checklist
    s2 = table.get((target, CRYPTO_SHARD), Decimal(0))
    tnet = [r for r in netting if r.get("subaccount_number") == target]
    items: list[tuple[bool, str, str]] = []
    items.append((tier_rank(tier) >= 1, f"API tier Advanced or above (now {tier or '?'})",
                  "python scripts/account_setup.py upgrade-tier --to advanced --execute"))
    items.append((btc_on_2, f"KXBTCD/KXBTC/KXBTC15M on shard {CRYPTO_SHARD} ({shards})",
                  "the BTC series moved shard: re-plan the subaccount shard before continuing"))
    items.append((CRYPTO_SHARD in subs.get(target, set()), f"subaccount {target} exists on shard {CRYPTO_SHARD}",
                  f"python scripts/account_setup.py create-subaccount --exchange-index {CRYPTO_SHARD} --execute"))
    items.append((len(target_keys) >= REQUIRED_RESTRICTED_KEYS,
                  f"API keys restricted to subaccount {target}: {len(target_keys)} of {REQUIRED_RESTRICTED_KEYS} (runner + watchdog)",
                  f"python scripts/account_setup.py create-key --subaccount {target} ... --execute (docs/ACCOUNT_SETUP.md step 3)"))
    if table.get((0, CRYPTO_SHARD), Decimal(0)) >= M1_SUGGESTED_DOLLARS:
        fund = (f"python scripts/account_setup.py transfer --from 0 --to {target} --amount-dollars {M1_SUGGESTED_DOLLARS} "
                f"--exchange-index {CRYPTO_SHARD} --execute")
    else:
        route = shard_route(0, 0, target, CRYPTO_SHARD)
        probed = any(e.get("kind") == "shard_transfer" and e.get("probe") and e.get("route") == route
                     and e.get("result") == "complete" for e in st.completed)
        size = f"--amount-dollars {M1_SUGGESTED_DOLLARS}" if probed else "--probe-dollars 1 (the mandatory $1 test first)"
        fund = (f"python scripts/account_setup.py shard-transfer --from-subaccount 0 --from-shard 0 --to-subaccount {target} "
                f"--to-shard {CRYPTO_SHARD} {size} --execute")
    items.append((s2 >= M1_WORST_CASE_DOLLARS,
                  f"subaccount {target} funded on shard {CRYPTO_SHARD}: {usd(s2, 4)} (M1 worst case {usd(M1_WORST_CASE_DOLLARS)}, "
                  f"suggested {usd(M1_SUGGESTED_DOLLARS)})", fund))
    items.append((not missing, f"transfers out of subaccount 0 declared to System 2 ({len(missing)} missing)",
                  missing[0]["command"] if missing else ""))
    items.append((bool(tnet) and not any(r.get("enabled") for r in tnet),
                  f"netting OFF on subaccount {target} (listed rows: {len(tnet)}; Kalshi's default is off, set it explicitly)",
                  f"python scripts/account_setup.py set-netting --subaccount {target} --off --execute"))
    items.append((not st.pending and not legacy_problem, "no unresolved transfer in the local state",
                  "python scripts/account_setup.py shard-transfer --resume <transfer_id> | forget-pending --id <id> (see above)"))
    out(f"\nChecklist (System 1 on subaccount {target}, shard {CRYPTO_SHARD}):")
    for i, (done, text, _) in enumerate(items, 1):
        out(f"  [{'x' if done else ' '}] {i}. {text}")
    todo = [cmd for done, _, cmd in items if not done and cmd]
    out("Next: " + (todo[0] if todo else "nothing: the account side is set up (docs/ACCOUNT_SETUP.md 'After the setup')."))
    if procs and not st.pending:
        out("Note: System 2 processes are running; stop them before any transfer out of subaccount 0.")
    return 0


# ----------------------------------------------------------------------------- write commands
async def cmd_upgrade_tier(ctx: Ctx, args: argparse.Namespace) -> int:
    problem = await admin_key_problem(ctx)
    if problem:
        return refuse(ctx, "upgrade-tier", problem)
    limits = await read_limits(ctx)
    tier = str(limits.get("usage_tier", "")).lower()
    ctx.out(f"Current API usage tier: {tier or '?'}")
    if tier_rank(tier) >= TIERS.index("advanced"):
        ctx.out("Already Advanced or above: nothing to send.")
        return 0
    ctx.out("Grants a permanent Advanced usage-level grant to the WHOLE account (System 2 shares it; budgets only go up:\n"
            "Basic read 200 / write 100 tokens/s -> Advanced 300 / 300). Kalshi's criterion: at least 1 of the\n"
            "account's last 100 Predictions orders was created via the API. Costs 30 write tokens.")
    oc = await confirm_and_send(ctx, command="upgrade-tier", method="POST", path=P_UPGRADE, body=None,
                                execute=args.execute, prompt="Type 'advanced' to send the upgrade request: ",
                                matches=lambda s: s.strip() == "advanced")
    if oc.kind == "ok":
        after, err = await _try(read_limits(ctx))
        ctx.out(f"Done. Tier now: {(after or {}).get('usage_tier', err)}")
    elif oc.kind == "rejected" and oc.status == 403:
        ctx.out("403: no API-created order among the account's last 100 Predictions orders (Kalshi's criterion).")
    return EXIT[oc.kind]


async def cmd_create_subaccount(ctx: Ctx, args: argparse.Namespace) -> int:
    idx = int(args.exchange_index)
    if idx < 0:
        raise UsageError("--exchange-index must be >= 0")
    problem = await admin_key_problem(ctx)
    if problem:
        return refuse(ctx, "create-subaccount", problem)
    tier = str((await read_limits(ctx)).get("usage_tier", "")).lower()
    if tier_rank(tier) < TIERS.index("advanced"):
        return refuse(ctx, "create-subaccount", f"API tier is {tier or '?'}: creating subaccounts needs Advanced or above "
                      "(run upgrade-tier first)")
    for t in BTC_SERIES:
        s = (await ctx.rest.get(P_SERIES.format(t), (), stream="kalshi.rest.series")).get("series") or {}
        if s.get("exchange_index") != idx:
            ctx.out(f"WARNING: {t} reports exchange_index {s.get('exchange_index')}, not {idx}.")
    table = await read_sub_balances(ctx)
    subs = numbered_subaccounts(table, await read_netting(ctx))
    if subs:
        listed = ", ".join(f"{s} (shards {sorted(v)})" for s, v in sorted(subs.items()))
        ctx.out(f"Existing numbered subaccounts: {listed}")
        if not args.another:
            return refuse(ctx, "create-subaccount", "a numbered subaccount already exists. POST /portfolio/subaccounts is not "
                          "idempotent: it always creates the NEXT number. Pass --another only if you want one more.")
    ctx.out(f"Creates the next numbered subaccount (1-63) on exchange shard {idx}. Its netting and balance start empty.")
    oc = await confirm_and_send(ctx, command="create-subaccount", method="POST", path=P_SUBACCOUNTS,
                                body=create_subaccount_body(idx), execute=args.execute,
                                prompt=f"Type 'CREATE {idx}' to create it: ", matches=lambda s: s.strip() == f"CREATE {idx}")
    if oc.kind == "ok":
        ctx.out(f"Created subaccount {(oc.body or {}).get('subaccount_number')} on shard {idx}.")
    elif oc.kind == "unknown":
        ctx.out("Run `status` BEFORE retrying: a retry would create another subaccount if this one was created.")
    return EXIT[oc.kind]


def _outside_repo(p: Path) -> bool:
    try:
        p.resolve().relative_to(REPO.resolve())
        return False
    except ValueError:
        return True


def _env_line(name: str, value: str) -> str:
    return f'{name}="{value}"' if (any(c.isspace() for c in value) or "#" in value) else f"{name}={value}"


async def cmd_create_key(ctx: Ctx, args: argparse.Namespace) -> int:
    sub = int(args.subaccount)
    if not 1 <= sub <= MAX_SUBACCOUNT:
        raise UsageError("--subaccount must be 1..63 (this tool never creates keys for subaccount 0)")
    name = str(args.name).strip()
    if not name:
        raise UsageError("--name must not be empty")
    pem = Path(args.pem).expanduser().absolute()
    envf = Path(args.env_file).expanduser().absolute()
    for v in (args.id_var, args.path_var):
        if not ENV_VAR_RE.match(v) or v.upper().startswith("ALLOW_"):
            raise UsageError(f"invalid variable name {v!r}")
    if args.id_var == args.path_var:
        raise UsageError("--id-var and --path-var must differ")
    for p in (pem, envf):
        if not _outside_repo(p):
            return refuse(ctx, "create-key", f"{p} is inside the repository: keys and env files live outside it (e.g. ~/.kalshi)")
    if pem.exists():
        return refuse(ctx, "create-key", f"{pem} already exists: choose a new file (never overwrite a key)")
    if envf.exists():
        names, _ = parse_env_text(envf.read_text(encoding="utf-8"))
        clash = [v for v in (args.id_var, args.path_var) if v in names]
        if clash:
            return refuse(ctx, "create-key", f"{envf} already defines {clash}: use other variable names or another env file")
    problem = await admin_key_problem(ctx)
    if problem:
        return refuse(ctx, "create-key", problem)
    subs = numbered_subaccounts(await read_sub_balances(ctx), await read_netting(ctx))
    if sub not in subs:
        return refuse(ctx, "create-key", f"subaccount {sub} does not exist (create-subaccount first)")
    body = generate_key_body(name, sub)
    ctx.out(f"Generates an RSA key pair restricted to subaccount {sub} (read + trade on it only; it cannot transfer "
            "funds or manage subaccounts or keys). The response carries the private key ONCE:")
    ctx.out(f"  private key -> {pem} (mode 0600, created now)")
    ctx.out(f"  {args.id_var}=<key id> and {args.path_var}={pem} -> appended to {envf} (mode 0600)")
    ctx.out("  neither the private key nor the key id is printed or logged.")
    fd_box: list[int] = []

    async def before_send() -> None:
        pem.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd_box.append(os.open(pem, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))

    oc = await confirm_and_send(ctx, command="create-key", method="POST", path=P_API_KEYS_GENERATE, body=body,
                                execute=args.execute, prompt=f"Type the key name ({name}) to create the key: ",
                                matches=lambda s: s.strip() == name, before_send=before_send,
                                log_extra={"pem": str(pem), "env_file": str(envf)})
    if oc.kind != "ok":
        if fd_box:
            os.close(fd_box[0])
            pem.unlink(missing_ok=True)
        if oc.kind == "unknown":
            ctx.out(f"A key named {name!r} may exist without its private key: check `status` (API keys) and delete "
                    "such an orphan in the Kalshi web app (Account settings, API keys).")
        return EXIT[oc.kind]
    res = oc.body or {}
    priv, kid = res.get("private_key"), res.get("api_key_id")
    if not priv or not kid:
        os.close(fd_box[0])
        pem.unlink(missing_ok=True)
        ctx.out(f"Kalshi answered without a key id or private key: delete key {name!r} in the Kalshi web app.")
        return 1
    with os.fdopen(fd_box[0], "w", encoding="utf-8") as f:
        f.write(priv if priv.endswith("\n") else priv + "\n")
        f.flush()
        os.fsync(f.fileno())
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

        if not isinstance(serialization.load_pem_private_key(priv.encode("utf-8"), password=None), RSAPrivateKey):
            ctx.out("WARNING: the returned key is not RSA; dh.kalshi.auth signs RSA-PSS only.")
    except (ValueError, TypeError):
        ctx.out("WARNING: the returned private key did not parse; it was saved as received.")
    envf.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    prefix = ""
    if envf.exists() and envf.stat().st_size and not envf.read_bytes().endswith(b"\n"):
        prefix = "\n"
    fd = os.open(envf, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as f:
        f.write(prefix + _env_line(args.id_var, str(kid)) + "\n" + _env_line(args.path_var, str(pem)) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.chmod(envf, 0o600)
    ctx.out(f"Created key {name!r} ({mask_id(kid)}) restricted to subaccount {sub}; private key {pem}; "
            f"variables {args.id_var} / {args.path_var} in {envf}.")
    return 0


async def cmd_transfer(ctx: Ctx, args: argparse.Namespace) -> int:
    frm, to, idx = int(args.from_sub), int(args.to), int(args.exchange_index)
    if not 0 <= frm <= MAX_SUBACCOUNT:
        raise UsageError("--from must be 0..63")
    if not 1 <= to <= MAX_SUBACCOUNT:
        raise UsageError("--to must be 1..63: this tool never moves money INTO subaccount 0")
    if frm == to:
        raise UsageError("--from and --to must differ")
    if idx < 0:
        raise UsageError("--exchange-index must be >= 0")
    amount = parse_dollars(args.amount_dollars)
    st = load_state(ctx)
    key = f"subaccount_transfer:{frm}->{to}@{idx}:{to_cents(amount)}"
    retry = next((e for e in st.pending.values() if e.get("key") == key), None)
    others = [i for i, e in st.pending.items() if e.get("key") != key]
    if others:
        return refuse(ctx, "transfer", f"unresolved earlier transfer(s) {others} in {ctx.state_path}: resolve them first "
                      "(`status`; re-run that exact command, `shard-transfer --resume`, or `forget-pending --id`)")
    done = [e for e in st.completed if e.get("kind") == "subaccount_transfer"
            and e.get("params") == transfer_params(frm, to, amount, idx)]
    if retry is None and done and not args.again:
        return refuse(ctx, "transfer", f"the same transfer ({usd(amount)} subaccount {frm} -> {to}, shard {idx}) already "
                      f"completed ({done[-1].get('id')} at {done[-1].get('completed_at')}): it is NOT sent again. Pass "
                      "--again only if you really want a second one")
    if frm == 0:
        _, why = system2_check(ctx, args.i_stopped_system2)
        if why:
            return refuse(ctx, "transfer", why)
    problem = await admin_key_problem(ctx)
    if problem:
        return refuse(ctx, "transfer", problem)
    table = await read_sub_balances(ctx)
    src = table.get((frm, idx), Decimal(0))
    ctx.out(f"Subaccount {frm} on shard {idx}: {usd(src, 4)}; subaccount {to} on shard {idx}: "
            f"{usd(table.get((to, idx), Decimal(0)), 4)}")
    if retry is not None:
        ctx.out(f"RETRY of the unresolved transfer {retry['id']} (attempts so far {retry.get('attempts', 0)}, first at "
                f"{retry.get('first_attempt_at') or retry.get('created_at')}): the same client_transfer_id is sent again, "
                "so Kalshi applies it at most once; whatever it answers, the outcome is decided from the balances since "
                "the FIRST attempt and the transfer list.")
        ev = await transfer_evidence(ctx, retry, amount, st)
        if ev.verdict == "applied":  # nothing to send: an earlier attempt applied it
            ctx.out(f"Evidence: the transfer is ALREADY APPLIED ({ev.detail}; listed: {ev.listed}). Nothing is sent.")
            log_event(ctx, command="transfer", event="evidence", client_transfer_id=retry["id"], outcome="before retry",
                      verdict=ev.verdict, balance=ev.balance, listed=ev.listed, detail=ev.detail)
            return await complete_transfer(ctx, st, retry["id"], amount, ev, answered="evidence")
    elif src < amount:
        return refuse(ctx, "transfer", f"subaccount {frm} holds {usd(src, 4)} on shard {idx}, less than {usd(amount)}: "
                      f"fund shard {idx} first (shard-transfer, docs/ACCOUNT_SETUP.md step 5)")
    if (to, idx) not in table and idx not in numbered_subaccounts(table, await read_netting(ctx)).get(to, set()):
        ctx.out(f"WARNING: subaccount {to} is not listed on shard {idx} (create it with --exchange-index {idx}); "
                "Kalshi will reject the transfer if it does not exist there.")
    if frm == 0 and retry is None and src - amount < SYSTEM2_MIN_SHARD_CASH:
        ctx.out(f"WARNING: subaccount 0 keeps {usd(src - amount, 4)} on shard {idx}; System 2's launch preflight wants "
                f">= {usd(SYSTEM2_MIN_SHARD_CASH)} on every shard it trades.")
    ctx.out(f"M1 limits for scale: worst case {usd(M1_WORST_CASE_DOLLARS)} total, daily halt {usd(M1_DAILY_HALT_DOLLARS)}; "
            f"suggested allocation {usd(M1_SUGGESTED_DOLLARS)}.")
    cid = retry["id"] if retry is not None else str(uuid.uuid4())
    body = subaccount_transfer_body(cid, frm, to, amount, idx)
    params = transfer_params(frm, to, amount, idx)

    async def before_send() -> None:
        pre = None
        if frm == 0:
            system2_recheck(ctx, args.i_stopped_system2)
            pre = await read_unscoped(ctx)  # opens the System 2 bracket: last reading WITHOUT the change
        rows = await read_sub_balances(ctx)  # the evidence bracket (subaccount rows) of this attempt
        e = st.pending.get(cid)
        if e is None:  # the FIRST attempt: its readings open the bracket for every later attempt
            now = ctx.now().isoformat()
            e = {"kind": "subaccount_transfer", "id": cid, "key": key, "params": params, "created_at": now,
                 "first_attempt_at": now, "attempts": 0, "first_pre": pre.as_json() if pre else None,
                 "first_rows": rows_json(rows), "env": ctx.env, "base_url": ctx.base_url}
        e["attempts"] = int(e.get("attempts", 0)) + 1
        e["last_attempt_at"] = ctx.now().isoformat()
        e["last_pre"] = pre.as_json() if pre else None
        st.pending[cid] = e
        st.save()  # the id (and the bracket) is on disk before the request leaves

    oc = await confirm_and_send(ctx, command="transfer", method="POST", path=P_SUB_TRANSFER, body=body,
                                execute=args.execute, prompt=f"Type the amount in dollars ({amount}) to send the transfer: ",
                                matches=lambda s: same_amount(s, amount), before_send=before_send,
                                log_extra={"client_transfer_id": cid})
    if oc.kind in ("dry_run", "declined", "aborted"):
        return EXIT[oc.kind]
    entry = st.pending.get(cid, {})
    attempts = int(entry.get("attempts", 0))
    if attempts <= 1 and oc.kind in ("rejected", "not_sent"):
        # the ONLY attempt was refused (a 4xx other than 408/409) or never left: definitely not applied
        ctx.out(f"The transfer's only attempt was {'refused' if oc.kind == 'rejected' else 'never sent'}: definitely "
                "not applied; its client_transfer_id is dropped.")
        st.pending.pop(cid, None)
        st.save()
        return 1
    # 200 = applied now, or an idempotent replay of an earlier attempt's apply; anything else: a
    # request may have left (unknown outcome, 5xx, 409, or any answer to a RETRY). Either way the
    # record (and the System 2 declaration, whose `before` must be a reading WITH the change)
    # waits for the balances / the transfer list to show the move.
    what = {"ok": "HTTP 200 (applied per Kalshi)", "unknown": f"outcome unknown ({oc.detail})",
            "rejected": f"HTTP {oc.status} on a retry", "not_sent": "the retry never connected"}.get(oc.kind, oc.kind)
    ev = await transfer_evidence(ctx, entry, amount, st)
    for _ in range(2):  # balances (or the transfer list) may trail the transfer by a moment
        if ev.verdict != "not_seen" and not (ev.balance == "applied" and ev.verdict != "applied"):
            break
        await ctx.sleep(1.0)
        ev = await transfer_evidence(ctx, entry, amount, st)
    entry["last_outcome"] = what
    entry["evidence"] = {"balance": ev.balance, "listed": ev.listed, "detail": ev.detail, "at": ctx.now().isoformat()}
    st.save()
    log_event(ctx, command="transfer", event="evidence", client_transfer_id=cid, outcome=what, verdict=ev.verdict,
              balance=ev.balance, listed=ev.listed, detail=ev.detail)
    if ev.verdict == "applied":
        ctx.out(f"{what}; evidence: APPLIED ({ev.detail}; listed in GET /portfolio/subaccounts/transfers: {ev.listed}).")
        return await complete_transfer(ctx, st, cid, amount, ev, answered=oc.kind)
    if oc.kind == "ok":
        ctx.out(f"{what}, but the balances do not show the move yet ({ev.verdict}: {ev.detail}; listed: {ev.listed}). "
                f"The transfer {cid} stays saved in {ctx.state_path}: re-run this exact command in a minute to finish it "
                "(it checks the evidence first and sends nothing twice) and to get the System 2 declaration.")
    elif ev.verdict == "not_seen":
        ctx.out(f"{what}; evidence: NOT applied so far ({ev.detail}; not in the transfer list). Nothing to declare to "
                f"System 2. The client_transfer_id {cid} STAYS saved in {ctx.state_path}: re-run this exact command "
                "later (it cannot apply twice), or, once `status` confirms nothing moved, `forget-pending --id "
                f"{cid}` and run the command again to send a NEW transfer.")
    else:
        ctx.out(f"{what}; evidence INCONCLUSIVE ({ev.verdict}: {ev.detail}; listed: {ev.listed}). The transfer {cid} "
                f"stays UNRESOLVED in {ctx.state_path}: check `status` (balances, recent transfers) before anything else; "
                "nothing is declared to System 2 until the evidence shows the move.")
    return 1


def transfer_params(frm: int, to: int, amount: Decimal, idx: int) -> dict[str, Any]:
    return {"from_subaccount": frm, "to_subaccount": to, "amount_dollars": str(amount), "exchange_index": idx}


async def complete_transfer(ctx: Ctx, st: State, cid: str, amount: Decimal, ev: Evidence, *, answered: str) -> int:
    """Record an APPLIED same-shard transfer; out of subaccount 0 with the System 2 declaration,
    bracketed from the FIRST attempt's unscoped reading (a retry never moves its start)."""
    entry = st.pending[cid]
    p = entry["params"]
    frm, to, idx = int(p["from_subaccount"]), int(p["to_subaccount"]), int(p["exchange_index"])
    attempts = int(entry.get("attempts", 0))
    if ev.verdict != "applied":  # never record (or declare) a move the evidence does not show
        raise RuntimeError(f"complete_transfer without evidence of the move: {ev}")
    after = await read_sub_balances(ctx)
    ctx.out(f"Transfer applied. Subaccount {frm} shard {idx}: {usd(after.get((frm, idx), Decimal(0)), 4)}; subaccount "
            f"{to} shard {idx}: {usd(after.get((to, idx), Decimal(0)), 4)}")
    if ev.balance not in ("applied", "unreadable"):
        ctx.out(f"CHECK: the balances did not move by exactly {usd(amount)} ({ev.detail}).")
    if ev.transfer_id and ev.transfer_id in claimed_transfer_ids(st):  # belt and braces (review NEW-4)
        raise RuntimeError(f"listed transfer {ev.transfer_id} is already claimed by another record")
    record: dict[str, Any] = {**entry, "id": cid, "completed_at": ctx.now().isoformat(),
                              "result": "applied" if attempts <= 1 and answered == "ok" else f"applied_after_retry:{answered}",
                              "claimed_transfer_id": ev.transfer_id or None,
                              "evidence": {"balance": ev.balance, "listed": ev.listed, "detail": ev.detail,
                                           "transfer_id": ev.transfer_id}}
    record.pop("key", None)
    if frm == 0:
        if not entry.get("first_pre"):
            record["declaration"] = {"required": True, "amount": f"{-amount:.2f}", "after": "", "before": "", "note": "",
                                     "observed_delta": "", "command": "(no reading before the first attempt: bracket by hand)",
                                     "warning": "no balance reading before the first attempt was saved: bracket it by hand"}
        else:
            post = await read_unscoped(ctx)
            record["declaration"] = build_declaration(
                ctx, amount, Reading.from_json(entry["first_pre"]), post,
                f"dh account_setup: subaccount {frm}->{to} shard {idx} {usd(amount)} client_transfer_id {cid}",
                retried=attempts > 1)
    st.pending.pop(cid, None)
    st.completed.append(record)
    st.save()
    log_event(ctx, command="transfer", event="completed", client_transfer_id=cid, result=record["result"],
              declaration=record.get("declaration"))
    if "declaration" in record:
        print_declaration(ctx, record["declaration"])
    return 0


async def _poll_shard_transfer(ctx: Ctx, transfer_id: str, timeout_s: float, poll_s: float) -> tuple[str, Decimal | None]:
    """Poll GET /portfolio/intra_exchange_instance_transfers/{id} -> (final status seen, the
    transfer's listed ``amount``: FixedPointDollars in the response, unlike the request's
    centicents; None if absent / unparseable)."""
    waited = 0.0
    while True:
        body = await ctx.rest.get(f"{P_SHARD_TRANSFERS}/{transfer_id}", (), stream="kalshi.rest.portfolio")
        tr = body.get("transfer") or {}
        status = str(tr.get("status", ""))
        try:
            listed = dec(tr["amount"]) if tr.get("amount") not in (None, "") else None
        except (ValueError, InvalidOperation):
            listed = None
        ctx.out(f"  transfer {transfer_id}: status {status or '?'} (after {waited:.0f} s)")
        if status != "pending":
            return status, listed
        if waited >= timeout_s:
            return "pending", listed
        await ctx.sleep(poll_s)
        waited += poll_s


def shard_route(fsub: int, fshard: int, tsub: int, tshard: int) -> str:
    return f"{fsub}:{fshard}->{tsub}:{tshard}"


def exact_move(changes: dict[tuple[int, int], Decimal], src: tuple[int, int], dst: tuple[int, int], amount: Decimal) -> bool:
    """The source lost exactly ``amount`` and the destination gained exactly ``amount`` (1 cent
    tolerance), and nothing else moved by more than that."""
    if not (near(changes.get(src, Decimal(0)), -amount) and near(changes.get(dst, Decimal(0)), amount)):
        return False
    return all(near(d, Decimal(0)) for k, d in changes.items() if k not in (src, dst))


async def _finish_shard_transfer(ctx: Ctx, st: State, local_id: str, timeout_s: float, poll_s: float) -> int:
    e = st.pending[local_id]
    tid, p = e["transfer_id"], e["params"]
    amount = dec(p["amount_dollars"])
    fsub, fshard, tsub, tshard = int(p["from_subaccount"]), int(p["from_shard"]), int(p["to_subaccount"]), int(p["to_shard"])
    status, listed = await _poll_shard_transfer(ctx, tid, timeout_s, poll_s)
    if status == "pending":
        e["status"] = "pending"
        st.save()
        ctx.out(f"Still pending after {timeout_s:.0f} s. It stays UNRESOLVED; check later with:\n"
                f"  python scripts/account_setup.py shard-transfer --resume {tid}")
        return 1
    if status != "complete":
        e["status"] = f"unexpected:{status}"
        st.save()
        ctx.out(f"FAILED or unknown status {status!r} (the API documents only pending/complete). {NON_ATOMIC_WARNING}\n"
                "Run `status` to see where the money is, then finish by hand (see docs/ACCOUNT_SETUP.md step 5).")
        log_event(ctx, command="shard-transfer", event="unexpected_status", transfer_id=tid, status=status)
        return 1
    before = rows_from_json(e.get("rows_before")) or {}
    src, dst = (fsub, fshard), (tsub, tshard)
    changes: dict[tuple[int, int], Decimal] = {}
    for attempt in range(3):  # balances may trail the 'complete' status by a moment
        after = await read_sub_balances(ctx)
        changes = {k: after.get(k, Decimal(0)) - before.get(k, Decimal(0)) for k in set(before) | set(after)}
        if exact_move(changes, src, dst, amount) or attempt == 2:
            break
        await ctx.sleep(poll_s)
    moved = [(k, d) for k, d in sorted(changes.items()) if d != 0]
    ctx.out("Balance changes since the request: " + (", ".join(f"subaccount {s} shard {x}: {d:+}" for (s, x), d in moved) or "none"))
    d_src, d_dst = changes.get(src, Decimal(0)), changes.get(dst, Decimal(0))
    listed_ok = listed is None or near(listed, amount)
    if listed is not None and not listed_ok:
        ctx.out(f"ALARM: Kalshi lists transfer {tid} with amount ${listed} but ${amount} was requested "
                f"({to_centicents(amount)} centicents): the request units are wrong. Stop and check `status`.")
    if exact_move(changes, src, dst, amount) and listed_ok:
        ctx.out(f"Complete: subaccount {tsub} on shard {tshard} received {usd(amount)} (exactly; subaccount {fsub} on "
                f"shard {fshard} {d_src:+}).")
        result = "complete"
    elif near(d_dst, Decimal(0)) and near(d_src, -amount) and listed_ok:
        result = "partial"
        ctx.out(f"PARTIAL: Kalshi reports the transfer complete, but subaccount {tsub} on shard {tshard} did not receive "
                f"{usd(amount)}. {NON_ATOMIC_WARNING}")
        if near(changes.get((0, tshard), Decimal(0)), amount):
            ctx.out(f"The money sits in subaccount 0 on shard {tshard}. Finish with (a same-shard transfer):\n"
                    f"  python scripts/account_setup.py transfer --from 0 --to {tsub} --amount-dollars {amount} "
                    f"--exchange-index {tshard} --execute")
        else:
            ctx.out("Check `status` to see where the money is.")
    elif near(d_dst, Decimal(0)) and near(d_src, Decimal(0)) and listed_ok:
        result = "not_moved"
        ctx.out(f"NOTHING MOVED: Kalshi reports the transfer complete, but no balance changed. {NON_ATOMIC_WARNING} "
                "Check `status` before anything else.")
    else:
        result = "mismatch"
        ctx.out(f"ALARM: the balances did NOT move by the requested {usd(amount)} (subaccount {tsub} shard {tshard} "
                f"{d_dst:+}, subaccount {fsub} shard {fshard} {d_src:+}; listed ${listed}). STOP: check `status` and "
                "the Kalshi transfer history; do not send anything else until it is understood.")
    record: dict[str, Any] = {**e, "id": local_id, "completed_at": ctx.now().isoformat(), "result": result,
                              "changes": rows_json(changes), "listed_amount": None if listed is None else str(listed)}
    record.pop("rows_before", None)
    if fsub == 0 and e.get("pre") and result != "not_moved":
        post = await read_unscoped(ctx)
        sub0 = sum((d for (s, _), d in changes.items() if s == 0), Decimal(0))
        if result == "mismatch":
            # review NEW-6: exactly what left subaccount 0 PER THE TRANSFER RECORD (Kalshi's listed
            # amount, else the requested one), never the subaccount-0 balance change, which also
            # carries unrelated movements (a System 2 settlement). Both are shown.
            decl_amount = listed if listed is not None else amount
            ctx.out(f"Declaration amount: {usd(decl_amount)} per the transfer record ({'listed by Kalshi' if listed is not None else 'requested; Kalshi listed none'}); "
                    f"the observed subaccount-0 balance change was {sub0:+} (all shards; includes unrelated movements, "
                    "NOT used for the declaration).")
            record["declaration_basis"] = {"transfer_record": str(decl_amount), "observed_subaccount0_change": str(sub0)}
        else:
            # the ACTUAL withdrawal from subaccount 0 (all shards) is what System 2's unscoped read saw
            decl_amount = -sub0 if sub0 < 0 else amount
        record["declaration"] = build_declaration(
            ctx, decl_amount, Reading.from_json(e["pre"]), post,
            f"dh account_setup: subaccount {fsub} shard {fshard} -> subaccount {tsub} shard {tshard} {usd(amount)} "
            f"intra transfer {tid}")
    st.pending.pop(local_id, None)
    st.completed.append(record)
    st.save()
    log_event(ctx, command="shard-transfer", event=result, transfer_id=tid, declaration=record.get("declaration"),
              changes=record["changes"], listed_amount=record["listed_amount"])
    if "declaration" in record:
        print_declaration(ctx, record["declaration"])
    return 0 if result == "complete" else 1


async def cmd_shard_transfer(ctx: Ctx, args: argparse.Namespace) -> int:
    st = load_state(ctx)
    if args.resume:
        tid = str(args.resume)
        local = next((i for i, e in st.pending.items() if e.get("kind") == "shard_transfer" and e.get("transfer_id") == tid), None)
        if local is None:
            blind = [i for i, e in st.pending.items() if e.get("kind") == "shard_transfer" and not e.get("transfer_id")]
            if len(blind) == 1:
                local = blind[0]
        if local is None:
            status, _ = await _poll_shard_transfer(ctx, tid, args.timeout_s, args.poll_s)
            ctx.out(f"No local entry for {tid}; final status seen: {status}.")
            return 0 if status == "complete" else 1
        if int(st.pending[local]["params"].get("from_subaccount", -1)) == 0:
            # finishing reads the unscoped balance that closes System 2's bracket: no session may run
            _, why = system2_check(ctx, args.i_stopped_system2)
            if why:
                return refuse(ctx, "shard-transfer", why)
        if not st.pending[local].get("transfer_id"):
            st.pending[local]["transfer_id"] = tid
            st.save()
            ctx.out(f"Attached transfer {tid} to the unresolved local entry {local} (its POST had an unknown outcome).")
        return await _finish_shard_transfer(ctx, st, local, args.timeout_s, args.poll_s)
    if (args.amount_dollars is None) == (args.probe_dollars is None):
        raise UsageError("shard-transfer needs exactly one of --amount-dollars and --probe-dollars (or --resume)")
    missing = [n for n in ("from_subaccount", "from_shard", "to_subaccount", "to_shard") if getattr(args, n) is None]
    if missing:
        raise UsageError("shard-transfer needs --" + ", --".join(m.replace("_", "-") for m in missing) + " (or --resume)")
    fsub, fshard, tsub, tshard = int(args.from_subaccount), int(args.from_shard), int(args.to_subaccount), int(args.to_shard)
    if not 0 <= fsub <= MAX_SUBACCOUNT:
        raise UsageError("--from-subaccount must be 0..63")
    if not 1 <= tsub <= MAX_SUBACCOUNT:
        raise UsageError("--to-subaccount must be 1..63: this tool never moves money INTO subaccount 0")
    if not (0 <= fshard <= MAX_SHARD and 0 <= tshard <= MAX_SHARD):
        raise UsageError(f"shards must be 0..{MAX_SHARD}")
    if fshard == tshard:
        raise UsageError("same shard: use `transfer --exchange-index N` (Kalshi treats a same-shard request as a subaccount transfer)")
    probe = args.probe_dollars is not None
    amount = parse_dollars(args.probe_dollars if probe else args.amount_dollars)
    if probe and amount > PROBE_MAX_DOLLARS:
        raise UsageError(f"--probe-dollars is a small test transfer: at most {usd(PROBE_MAX_DOLLARS)}")
    route = shard_route(fsub, fshard, tsub, tshard)
    if st.pending:
        return refuse(ctx, "shard-transfer", f"unresolved transfer(s) {list(st.pending)} in {ctx.state_path}: the intra-account "
                      "transfer API has no idempotency key, so nothing new is sent until they are resolved "
                      "(`status`, then `shard-transfer --resume <transfer_id>` or `forget-pending --id <id>`)")
    if not probe and not args.again and any(
            e.get("kind") == "shard_transfer" and not e.get("probe") and e.get("route") == route
            and e.get("params", {}).get("amount_dollars") == str(amount) for e in st.completed):
        return refuse(ctx, "shard-transfer", f"a {usd(amount)} shard-transfer on {route} already completed: it is NOT sent "
                      "again. Pass --again only if you really want a second one")
    if fsub == 0:
        _, why = system2_check(ctx, args.i_stopped_system2)
        if why:
            return refuse(ctx, "shard-transfer", why)
    problem = await admin_key_problem(ctx)
    if problem:
        return refuse(ctx, "shard-transfer", problem)
    if not probe and not any(e.get("kind") == "shard_transfer" and e.get("probe") and e.get("route") == route
                             and e.get("result") == "complete" for e in st.completed):
        why = (f"no successful probe on this route ({route}) yet: first send a small test transfer with --probe-dollars 1 "
               "(same --from/--to arguments) and check it arrived exactly; the request amount is in centicents and the "
               "response in dollars, so a unit mistake must cost $1, not the allocation (docs/ACCOUNT_SETUP.md step 5)")
        if args.execute:
            return refuse(ctx, "shard-transfer", why)
        ctx.out(f"WARNING: --execute would be REFUSED: {why}")
    table = await read_sub_balances(ctx)
    src = table.get((fsub, fshard), Decimal(0))
    ctx.out(f"Subaccount {fsub} on shard {fshard}: {usd(src, 4)}; subaccount {tsub} on shard {tshard}: "
            f"{usd(table.get((tsub, tshard), Decimal(0)), 4)}")
    if src < amount:
        return refuse(ctx, "shard-transfer", f"subaccount {fsub} holds {usd(src, 4)} on shard {fshard}, less than {usd(amount)}")
    if tshard not in numbered_subaccounts(table, await read_netting(ctx)).get(tsub, set()):
        ctx.out(f"WARNING: subaccount {tsub} is not listed on shard {tshard} (create-subaccount --exchange-index {tshard} first).")
    if fsub == 0 and src - amount < SYSTEM2_MIN_SHARD_CASH:
        ctx.out(f"WARNING: subaccount 0 keeps {usd(src - amount, 4)} on shard {fshard}; System 2's launch preflight wants "
                f">= {usd(SYSTEM2_MIN_SHARD_CASH)} on every shard it trades.")
    ctx.out(NON_ATOMIC_WARNING)
    if probe:
        ctx.out(f"PROBE: a {usd(amount)} test transfer on {route}. The request amount is in CENTICENTS "
                f"({to_centicents(amount)} here; openapi IntraExchangeInstanceTransferRequest.amount), the listed amount "
                "in dollars: the result must match exactly before the real transfer is allowed.")
    body = shard_transfer_body(amount, fshard, tshard, fsub, tsub)
    local_id = "shard-" + uuid.uuid4().hex[:12]
    params = {"from_subaccount": fsub, "from_shard": fshard, "to_subaccount": tsub, "to_shard": tshard,
              "amount_dollars": str(amount)}

    async def before_send() -> None:
        pre = None
        if fsub == 0:
            system2_recheck(ctx, args.i_stopped_system2)
            pre = await read_unscoped(ctx)  # opens the System 2 bracket
        rows = await read_sub_balances(ctx)  # the balances the result is compared with, just before sending
        st.pending[local_id] = {"kind": "shard_transfer", "id": local_id, "params": params, "status": "sending",
                                "created_at": ctx.now().isoformat(), "attempts": 1, "probe": probe, "route": route,
                                "pre": pre.as_json() if pre else None, "rows_before": rows_json(rows),
                                "request_amount_centicents": to_centicents(amount), "env": ctx.env}
        st.save()

    oc = await confirm_and_send(ctx, command="shard-transfer", method="POST", path=P_SHARD_TRANSFER, body=body,
                                execute=args.execute, prompt=f"Type the amount in dollars ({amount}) to send the transfer: ",
                                matches=lambda s: same_amount(s, amount), before_send=before_send,
                                log_extra={"local_id": local_id})
    if oc.kind in ("dry_run", "declined", "aborted"):
        return EXIT[oc.kind]
    if oc.kind in ("rejected", "not_sent"):
        st.pending.pop(local_id, None)
        st.save()
        return 1
    tid = (oc.body or {}).get("transfer_id") if oc.kind == "ok" else None
    if not tid:
        st.pending[local_id]["status"] = "unknown"
        st.save()
        ctx.out(f"The request's outcome is unknown and it has no idempotency key: it stays UNRESOLVED as {local_id}. "
                "Run `status` (recent intra-account transfers). If a new transfer is listed: "
                "`shard-transfer --resume <its id>`; if none: `forget-pending --id " + local_id + "`.")
        return 1
    st.pending[local_id].update(transfer_id=tid, status="accepted")
    st.save()
    ctx.out(f"Accepted as transfer {tid} (processed asynchronously); polling every {args.poll_s:g} s for up to {args.timeout_s:g} s.")
    return await _finish_shard_transfer(ctx, st, local_id, args.timeout_s, args.poll_s)


async def cmd_set_netting(ctx: Ctx, args: argparse.Namespace) -> int:
    sub = int(args.subaccount)
    if not 1 <= sub <= MAX_SUBACCOUNT:
        raise UsageError("--subaccount must be 1..63: this tool never changes subaccount 0")
    enabled = bool(args.on)
    problem = await admin_key_problem(ctx)
    if problem:
        return refuse(ctx, "set-netting", problem)
    rows = await read_netting(ctx)
    subs = numbered_subaccounts(await read_sub_balances(ctx), rows)
    if sub not in subs:
        return refuse(ctx, "set-netting", f"subaccount {sub} does not exist")
    cur = [r for r in rows if r.get("subaccount_number") == sub]
    ctx.out(f"Subaccount {sub} netting now: " + (", ".join(f"shard {r.get('exchange_index')} {'ON' if r.get('enabled') else 'off'}"
                                                             for r in cur) or "not listed (Kalshi's default: off)"))
    word = "on" if enabled else "off"
    oc = await confirm_and_send(ctx, command="set-netting", method="PUT", path=P_NETTING, body=netting_body(sub, enabled),
                                execute=args.execute, prompt=f"Type 'netting {word} {sub}' to confirm: ",
                                matches=lambda s: s.strip() == f"netting {word} {sub}")
    if oc.kind == "ok":
        now_rows, err = await _try(read_netting(ctx))
        ctx.out("Now: " + (", ".join(f"shard {r.get('exchange_index')} {'ON' if r.get('enabled') else 'off'}"
                                     for r in (now_rows or []) if r.get("subaccount_number") == sub) or str(err or "not listed")))
    return EXIT[oc.kind]


async def cmd_forget_pending(ctx: Ctx, args: argparse.Namespace) -> int:
    st = load_state(ctx)
    e = st.pending.get(args.id)
    if e is None:
        raise UsageError(f"no unresolved entry {args.id!r} in {ctx.state_path}")
    ctx.out(f"Unresolved {e.get('kind')} {args.id}: {e.get('params')} (transfer_id {e.get('transfer_id', '-')})")
    ctx.out("Forget it ONLY after `status` showed what happened (this changes local state only; nothing is sent).")
    if (ctx.ask(f"Type the id ({args.id}) to forget it: ") or "").strip() != args.id:
        ctx.out("Confirmation did not match: nothing changed.")
        return 3
    st.pending.pop(args.id)
    st.data.setdefault("forgotten", []).append({**e, "forgotten_at": ctx.now().isoformat()})
    st.save()
    log_event(ctx, command="forget-pending", event="forgotten", id=args.id, params=e.get("params"))
    ctx.out("Forgotten.")
    return 0


COMMANDS: dict[str, Callable[[Ctx, argparse.Namespace], Awaitable[int]]] = {
    "status": cmd_status,
    "upgrade-tier": cmd_upgrade_tier,
    "create-subaccount": cmd_create_subaccount,
    "create-key": cmd_create_key,
    "transfer": cmd_transfer,
    "shard-transfer": cmd_shard_transfer,
    "set-netting": cmd_set_netting,
    "forget-pending": cmd_forget_pending,
}


# ----------------------------------------------------------------------------- CLI
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="account_setup.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=None, help="Kalshi config (default: config/kalshi.admin.yaml if present, else config/kalshi.yaml)")
    p.add_argument("--demo", action="store_true", help="use the demo environment of the config")
    p.add_argument("--system2-root", default=str(SYSTEM2_ROOT), help="System 2 checkout (for the printed command and the ledger check)")
    p.add_argument("--state-file", default=None,
                   help=f"state file (default: ${STATE_ENV_VAR}, else {STATE_DIR}/state.<env>.json: one per user, "
                        "shared by every checkout). Another file than the per-user one is refused for transfer / "
                        "shard-transfer / forget-pending when it lacks a record of the per-user file")
    p.add_argument("--i-know-state-file", action="store_true",
                   help=f"accept a --state-file / ${STATE_ENV_VAR} that lacks records of the per-user state file")
    sub = p.add_subparsers(dest="command")
    s = sub.add_parser("status", help="read-only overview and checklist (default)")
    s.add_argument("--subaccount", type=int, default=1, help="System 1's subaccount (default 1)")
    u = sub.add_parser("upgrade-tier", help="POST /account/api_usage_level/upgrade")
    u.add_argument("--to", required=True, choices=["advanced"])
    c = sub.add_parser("create-subaccount", help="POST /portfolio/subaccounts")
    c.add_argument("--exchange-index", type=int, required=True)
    c.add_argument("--another", action="store_true", help="allow creating one more when numbered subaccounts exist")
    k = sub.add_parser("create-key", help="POST /api_keys/generate with subaccount (restricted key)")
    k.add_argument("--subaccount", type=int, required=True)
    k.add_argument("--name", required=True)
    k.add_argument("--pem", required=True, help="new private-key file, outside the repo (mode 0600)")
    k.add_argument("--env-file", required=True, help="KEY=VALUE file (outside the repo) the variables are appended to")
    k.add_argument("--id-var", required=True, help="e.g. KALSHI_KEY_ID or KALSHI_WATCHDOG_KEY_ID")
    k.add_argument("--path-var", required=True, help="e.g. KALSHI_PRIVATE_KEY_PATH or KALSHI_WATCHDOG_PRIVATE_KEY_PATH")
    t = sub.add_parser("transfer", help="POST /portfolio/subaccounts/transfer (same shard)")
    t.add_argument("--from", dest="from_sub", type=int, required=True)
    t.add_argument("--to", type=int, required=True)
    t.add_argument("--amount-dollars", required=True)
    t.add_argument("--exchange-index", type=int, required=True)
    t.add_argument("--i-stopped-system2", action="store_true")
    t.add_argument("--again", action="store_true", help="send a transfer identical to one that already completed")
    x = sub.add_parser("shard-transfer", help="POST /portfolio/intra_exchange_instance_transfer (across shards), then poll")
    x.add_argument("--from-subaccount", type=int)
    x.add_argument("--from-shard", type=int)
    x.add_argument("--to-subaccount", type=int)
    x.add_argument("--to-shard", type=int)
    x.add_argument("--amount-dollars")
    x.add_argument("--probe-dollars", help=f"a small test transfer (at most ${PROBE_MAX_DOLLARS}) on the route: MANDATORY "
                                           "before the first --amount-dollars transfer on it (units check)")
    x.add_argument("--again", action="store_true", help="send a transfer identical to one that already completed")
    x.add_argument("--resume", metavar="TRANSFER_ID", help="poll an existing transfer (read-only)")
    x.add_argument("--timeout-s", type=float, default=180.0)
    x.add_argument("--poll-s", type=float, default=2.0)
    x.add_argument("--i-stopped-system2", action="store_true")
    n = sub.add_parser("set-netting", help="PUT /portfolio/subaccounts/netting")
    n.add_argument("--subaccount", type=int, required=True)
    g = n.add_mutually_exclusive_group(required=True)
    g.add_argument("--on", action="store_true")
    g.add_argument("--off", action="store_true")
    f = sub.add_parser("forget-pending", help="drop an unresolved local entry (local state only)")
    f.add_argument("--id", required=True)
    for sp in (u, c, k, t, x, n):
        sp.add_argument("--execute", action="store_true", help="send it (after a typed confirmation); default: dry run")
    return p


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(list(argv))
    if args.command is None:
        args = parser.parse_args([*argv, "status"])
    return args


def sends_writes(args: argparse.Namespace) -> bool:
    """True only for an --execute write command (a dry run, status, --resume and forget-pending never write)."""
    if args.command not in WRITE_COMMANDS or not getattr(args, "execute", False):
        return False
    return not (args.command == "shard-transfer" and args.resume)


def resolve_config_path(arg: str | None) -> Path | None:
    from dh.kalshi import config as kcfg

    if arg:
        p = Path(arg).expanduser()
        return p if p.is_absolute() else REPO / p
    if ADMIN_CONFIG.is_file():
        return ADMIN_CONFIG
    return kcfg.DEFAULT_CONFIG if kcfg.DEFAULT_CONFIG.is_file() else None


def build_rest(args: argparse.Namespace, *, read_only: bool) -> tuple[Any, Any]:
    from dh.kalshi.config import load_config
    from dh.kalshi.rest import KalshiRest

    cfg = load_config(resolve_config_path(args.config), env="demo" if args.demo else None)
    if cfg.env_file_report is not None:
        print(cfg.env_file_report.summary())
    signer = cfg.signer()
    if signer is None:
        raise SystemExit(f"credentials required: {cfg.credentials_hint()} (config {cfg.source})")
    return KalshiRest(cfg.rest_url, signer, cfg.limiter(), read_only=read_only, **cfg.rest_kwargs()), cfg


async def run(
    argv: Sequence[str],
    *,
    rest: Any = None,
    ask: Callable[[str], str] | None = None,
    ps: Callable[[], list[str]] | None = None,
    now: Callable[[], datetime] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    out: Callable[[str], None] | None = None,
    log_path: Path | None = None,
    state_path: Path | None = None,
    legacy_state_path: Path | None = None,
    key_id: str = "",
    canonical_state: Path | None = None,
) -> int:
    args = parse_args(argv)
    own = rest is None
    ctx_kw: dict[str, Any] = {}
    if own:
        if sends_writes(args) and not sys.stdin.isatty():
            print("REFUSED: --execute needs an interactive terminal for the typed confirmation.")
            return 2
        rest, cfg = build_rest(args, read_only=not sends_writes(args))
        ctx_kw.update(base_url=cfg.rest_url, env=cfg.env, key_id=cfg.key_id, config_source=cfg.source)
    else:
        ctx_kw.update(base_url=getattr(rest, "base_url", ""), key_id=key_id, config_source="(injected)")
    ctx = Ctx(rest=rest, system2_root=Path(args.system2_root).expanduser(), **ctx_kw)
    overridden = False
    if args.state_file:
        ctx.state_path = Path(args.state_file).expanduser()
        overridden = True
    elif state_path is None:  # the per-user file of this Kalshi environment; the old per-checkout file is checked
        ctx.state_path = default_state_path(ctx.env)
        ctx.legacy_state_path = LEGACY_STATE_PATH
        overridden = bool(os.environ.get(STATE_ENV_VAR, ""))
    for name, val in (("ask", ask), ("ps", ps), ("now", now), ("sleep", sleep), ("out", out),
                      ("log_path", log_path), ("state_path", state_path), ("legacy_state_path", legacy_state_path)):
        if val is not None:
            setattr(ctx, name, val)
    if overridden and args.command in STATE_COMMANDS and not args.i_know_state_file:
        why = state_override_problem(ctx.state_path, canonical_state or canonical_state_path(ctx.env))
        if why:
            return refuse(ctx, args.command, why)
    try:
        return await COMMANDS[args.command](ctx, args)
    except UsageError as exc:
        ctx.out(f"error: {exc}")
        return 2
    except Abort as exc:
        return refuse(ctx, args.command, str(exc))
    except (KalshiHTTPError, TransportError) as exc:
        ctx.out(f"error: Kalshi request failed: {exc}. Run `status` to see the current state before anything else.")
        log_event(ctx, command=args.command, event="error", error=str(exc))
        return 1
    finally:
        if own:
            await rest.close()


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
