import asyncio
import unittest

from services.frontend.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitOpenError,
    CircuitState,
)


class QualifyingFailure(Exception):
    pass


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class AsyncCircuitBreakerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.breaker = AsyncCircuitBreaker(clock=self.clock)

    @staticmethod
    def is_failure(exc: Exception) -> bool:
        return isinstance(exc, QualifyingFailure)

    async def succeed(self) -> str:
        return "ok"

    async def fail(self) -> None:
        raise QualifyingFailure("upstream unavailable")

    async def test_opens_after_five_failures_and_closes_after_two_probes(self):
        for _ in range(5):
            with self.assertRaises(QualifyingFailure):
                await self.breaker.call(self.fail, is_failure=self.is_failure)

        self.assertEqual(self.breaker.state, CircuitState.OPEN)
        with self.assertRaises(CircuitOpenError):
            await self.breaker.call(self.succeed, is_failure=self.is_failure)

        self.clock.advance(30)
        self.assertEqual(
            await self.breaker.call(self.succeed, is_failure=self.is_failure),
            "ok",
        )
        self.assertEqual(self.breaker.state, CircuitState.HALF_OPEN)

        await self.breaker.call(self.succeed, is_failure=self.is_failure)
        self.assertEqual(self.breaker.state, CircuitState.CLOSED)

    async def test_concurrent_requests_allow_only_one_half_open_probe(self):
        for _ in range(5):
            with self.assertRaises(QualifyingFailure):
                await self.breaker.call(self.fail, is_failure=self.is_failure)

        self.clock.advance(30)
        probe_entered = asyncio.Event()
        release_probe = asyncio.Event()

        async def slow_probe() -> str:
            probe_entered.set()
            await release_probe.wait()
            return "ok"

        probe_task = asyncio.create_task(
            self.breaker.call(slow_probe, is_failure=self.is_failure)
        )
        await probe_entered.wait()

        with self.assertRaises(CircuitOpenError):
            await self.breaker.call(self.succeed, is_failure=self.is_failure)

        release_probe.set()
        await probe_task
        self.assertEqual(self.breaker.state, CircuitState.HALF_OPEN)

        await self.breaker.call(self.succeed, is_failure=self.is_failure)
        self.assertEqual(self.breaker.state, CircuitState.CLOSED)

    async def test_failed_probe_reopens_circuit_for_another_30_seconds(self):
        for _ in range(5):
            with self.assertRaises(QualifyingFailure):
                await self.breaker.call(self.fail, is_failure=self.is_failure)

        self.clock.advance(30)
        with self.assertRaises(QualifyingFailure):
            await self.breaker.call(self.fail, is_failure=self.is_failure)

        self.assertEqual(self.breaker.state, CircuitState.OPEN)
        with self.assertRaises(CircuitOpenError):
            await self.breaker.call(self.succeed, is_failure=self.is_failure)

        self.clock.advance(30)
        await self.breaker.call(self.succeed, is_failure=self.is_failure)
        await self.breaker.call(self.succeed, is_failure=self.is_failure)
        self.assertEqual(self.breaker.state, CircuitState.CLOSED)

    async def test_non_qualifying_error_resets_consecutive_failure_count(self):
        for _ in range(4):
            with self.assertRaises(QualifyingFailure):
                await self.breaker.call(self.fail, is_failure=self.is_failure)

        async def rejected_request() -> None:
            raise ValueError("simulated non-qualifying response")

        with self.assertRaises(ValueError):
            await self.breaker.call(
                rejected_request,
                is_failure=self.is_failure,
            )

        for _ in range(4):
            with self.assertRaises(QualifyingFailure):
                await self.breaker.call(self.fail, is_failure=self.is_failure)

        self.assertEqual(self.breaker.state, CircuitState.CLOSED)


if __name__ == "__main__":
    unittest.main()