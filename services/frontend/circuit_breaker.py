import asyncio
import time
import logging
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TypeVar


T = TypeVar("T")
FailurePredicate = Callable[[Exception], bool]
logger = logging.getLogger(__name__)

class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when the circuit is open or a half-open probe is already running."""


class AsyncCircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30,
        success_threshold: int = 2,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")
        if success_threshold < 1:
            raise ValueError("success_threshold must be at least 1")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self._clock = clock
        self._lock = asyncio.Lock()

        self.state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._probe_in_flight = False
        self._opened_at = 0.0
        self._generation = 0


    def _transition_to(self, new_state: CircuitState) -> None:
        previous_state = self.state
        if previous_state is new_state:
            return

        self.state = new_state
        logger.info(
            "Circuit breaker state transition: %s -> %s",
            previous_state.value.upper(),
            new_state.value.upper(),
        )

    async def call(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        is_failure: FailurePredicate,
    ) -> T:
        is_probe, generation = await self._admit_call()

        try:
            result = await operation()
        except asyncio.CancelledError:
            await self._abandon_probe(is_probe, generation)
            raise
        except Exception as exc:
            if is_failure(exc):
                await self._record_failure(is_probe, generation)
            else:
                # A non-qualifying response (such as an HTTP 4xx) shows the
                # dependency responded, so it counts as a successful probe.
                await self._record_success(is_probe, generation)
            raise
        else:
            await self._record_success(is_probe, generation)
            return result

    async def _admit_call(self) -> tuple[bool, int]:
        async with self._lock:
            if self.state is CircuitState.OPEN:
                if self._clock() - self._opened_at < self.recovery_timeout:
                    raise CircuitOpenError("Circuit is open")

                self._transition_to(CircuitState.HALF_OPEN)
                self._half_open_successes = 0
                self._probe_in_flight = False
                self._generation += 1

            if self.state is CircuitState.HALF_OPEN:
                if self._probe_in_flight:
                    raise CircuitOpenError("Half-open probe already in progress")

                self._probe_in_flight = True
                return True, self._generation

            return False, self._generation

    async def _record_success(self, is_probe: bool, generation: int) -> None:
        async with self._lock:
            if generation != self._generation:
                return

            if is_probe:
                if self.state is not CircuitState.HALF_OPEN:
                    return

                self._probe_in_flight = False
                self._half_open_successes += 1
                if self._half_open_successes >= self.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    self._consecutive_failures = 0
                    self._half_open_successes = 0
                    self._generation += 1
            elif self.state is CircuitState.CLOSED:
                self._consecutive_failures = 0

    async def _record_failure(self, is_probe: bool, generation: int) -> None:
        async with self._lock:
            if generation != self._generation:
                return

            if is_probe:
                if self.state is CircuitState.HALF_OPEN:
                    self._open()
            elif self.state is CircuitState.CLOSED:
                self._consecutive_failures += 1
                if self._consecutive_failures >= self.failure_threshold:
                    self._open()

    async def _abandon_probe(self, is_probe: bool, generation: int) -> None:
        if not is_probe:
            return

        async with self._lock:
            if (
                generation == self._generation
                and self.state is CircuitState.HALF_OPEN
            ):
                self._probe_in_flight = False

    def _open(self) -> None:
        self._transition_to(CircuitState.OPEN)
        self._opened_at = self._clock()
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._probe_in_flight = False
        self._generation += 1