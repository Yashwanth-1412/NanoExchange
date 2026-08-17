#!/usr/bin/env python3
"""Run NanoExchange and drive it with a real OUCH TCP client."""

from __future__ import annotations

import argparse
import dataclasses
import os
import random
import signal
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path


ENTER_ORDER = b"O"
CANCEL_ORDER = b"X"
REPLACE_ORDER = b"U"

ORDER_ACCEPTED = b"A"
ORDER_EXECUTED = b"E"
ORDER_CANCELED = b"C"
ORDER_REPLACED = b"U"
CANCEL_REJECTED = b"J"

ENTER_FORMAT = ">c14scI8sII4sccccc"
CANCEL_FORMAT = ">c14sI"
REPLACE_FORMAT = ">c14s14sIIIcc"

ENTER_SIZE = struct.calcsize(ENTER_FORMAT)
CANCEL_SIZE = struct.calcsize(CANCEL_FORMAT)
REPLACE_SIZE = struct.calcsize(REPLACE_FORMAT)

assert ENTER_SIZE == 45
assert CANCEL_SIZE == 19
assert REPLACE_SIZE == 43


@dataclasses.dataclass(frozen=True)
class Config:
    engine: Path
    orders: int
    rate: int
    seed: int
    warmup: int
    gateway_host: str
    gateway_port: int
    incremental_host: str
    incremental_port: int
    snapshot_port: int
    interface: str
    output_dir: Path
    core_me: int
    core_gateway: int
    core_publisher: int
    core_streamer: int
    core_logger: int
    runner_core: int
    timeout: float
    settle: float
    cancel_ratio: float
    replace_ratio: float


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", type=Path, default=Path("build/NanoExchange"))
    parser.add_argument("--orders", type=int, default=100_000)
    parser.add_argument("--rate", type=int, default=0, help="orders/second; 0 means send as fast as possible")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--warmup", type=int, default=5_000)
    parser.add_argument("--gateway-host", default="127.0.0.1")
    parser.add_argument("--gateway-port", type=int, default=10000)
    parser.add_argument("--incremental-host", default="127.0.0.1")
    parser.add_argument("--incremental-port", type=int, default=20001)
    parser.add_argument("--snapshot-port", type=int, default=20002)
    parser.add_argument("--interface", default="lo")
    parser.add_argument("--output-dir", type=Path, default=Path("perf-runs"))
    parser.add_argument("--core-me", type=int, default=-1)
    parser.add_argument("--core-gateway", type=int, default=-1)
    parser.add_argument("--core-publisher", type=int, default=-1)
    parser.add_argument("--core-streamer", type=int, default=-1)
    parser.add_argument("--core-logger", type=int, default=-1)
    parser.add_argument("--runner-core", type=int, default=-1, help="CPU for this order-generator process")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--settle", type=float, default=2.0, help="seconds to drain responses after sending")
    parser.add_argument("--cancel-ratio", type=float, default=0.20)
    parser.add_argument("--replace-ratio", type=float, default=0.05)
    args = parser.parse_args()

    if args.orders <= 0:
        parser.error("--orders must be positive")
    if args.rate < 0:
        parser.error("--rate cannot be negative")
    if args.warmup < 0:
        parser.error("--warmup cannot be negative")
    if args.settle < 0:
        parser.error("--settle cannot be negative")
    if not 0.0 <= args.cancel_ratio <= 1.0:
        parser.error("--cancel-ratio must be between 0 and 1")
    if not 0.0 <= args.replace_ratio <= 1.0:
        parser.error("--replace-ratio must be between 0 and 1")
    if args.cancel_ratio + args.replace_ratio > 1.0:
        parser.error("cancel and replace ratios cannot sum above 1")

    return Config(
        engine=args.engine,
        orders=args.orders,
        rate=args.rate,
        seed=args.seed,
        warmup=args.warmup,
        gateway_host=args.gateway_host,
        gateway_port=args.gateway_port,
        incremental_host=args.incremental_host,
        incremental_port=args.incremental_port,
        snapshot_port=args.snapshot_port,
        interface=args.interface,
        output_dir=args.output_dir,
        core_me=args.core_me,
        core_gateway=args.core_gateway,
        core_publisher=args.core_publisher,
        core_streamer=args.core_streamer,
        core_logger=args.core_logger,
        runner_core=args.runner_core,
        timeout=args.timeout,
        settle=args.settle,
        cancel_ratio=args.cancel_ratio,
        replace_ratio=args.replace_ratio,
    )


def token(order_id: int) -> bytes:
    """Encode the same 8-byte big-endian token used by AlphaTrader."""
    return struct.pack(">Q", order_id) + b"\0" * 6


def enter_order(order_id: int, ticker: int, side: bytes, price: int, quantity: int, ioc: bool) -> bytes:
    # OUCH 4.2 EnterOrder, matching QuantLink/Lib/protocol/ouch_messages.h.
    return struct.pack(
        ENTER_FORMAT,
        ENTER_ORDER,
        token(order_id),
        side,
        quantity,
        struct.pack(">Q", ticker),
        price,
        0 if ioc else 99998,
        b"PERF",
        b"Y",
        b"A",
        b"N",
        b"N",
        b"R",
    )


def cancel_order(order_id: int, quantity: int = 0) -> bytes:
    return struct.pack(CANCEL_FORMAT, CANCEL_ORDER, token(order_id), quantity)


def replace_order(order_id: int, new_order_id: int, price: int, quantity: int, ioc: bool) -> bytes:
    return struct.pack(
        REPLACE_FORMAT,
        REPLACE_ORDER,
        token(order_id),
        token(new_order_id),
        quantity,
        price,
        0 if ioc else 99998,
        b"Y",
        b"N",
    )


def response_size(message_type: bytes) -> int:
    # OUCH response sizes from QuantLink/Lib/protocol/ouch_messages.h.
    return {
        ORDER_ACCEPTED: 61,
        ORDER_EXECUTED: 39,
        ORDER_CANCELED: 28,
        ORDER_REPLACED: 61,
        CANCEL_REJECTED: 23,
    }.get(message_type, 0)


def drain_responses(sock: socket.socket, buffer: bytearray) -> int:
    received = 0
    while True:
        try:
            data = sock.recv(64 * 1024)
        except BlockingIOError:
            return received
        if not data:
            raise RuntimeError("NanoExchange closed the OUCH connection")
        buffer.extend(data)

        while buffer:
            size = response_size(bytes(buffer[:1]))
            if size == 0:
                del buffer[0]
                continue
            if len(buffer) < size:
                break
            del buffer[:size]
            received += 1


def wait_for_gateway(host: str, port: int, timeout: float) -> socket.socket:
    deadline = time.monotonic() + timeout
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(min(0.25, max(0.01, deadline - time.monotonic())))
        try:
            sock.connect((host, port))
            sock.setblocking(False)
            return sock
        except OSError as error:
            last_error = error
            sock.close()
            time.sleep(0.05)
    raise RuntimeError(f"could not connect to {host}:{port}: {last_error}")


def start_engine(config: Config, run_dir: Path) -> tuple[subprocess.Popen[str], object]:
    engine_path = config.engine.resolve()
    command = [
        str(engine_path),
        config.interface,
        str(config.gateway_port),
        config.incremental_host,
        str(config.incremental_port),
        str(config.snapshot_port),
        config.interface,
        str(config.core_me),
        str(config.core_gateway),
        str(config.core_publisher),
        str(config.core_streamer),
        str(config.core_logger),
    ]
    output = (run_dir / "engine.stdout.log").open("w", encoding="utf-8")
    process = subprocess.Popen(command, cwd=run_dir, stdout=output, stderr=subprocess.STDOUT, text=True)
    return process, output


def pin_runner(core: int) -> None:
    if core < 0:
        return
    os.sched_setaffinity(0, {core})
    print(f"runner_cpu={core}")


def send_orders(config: Config, sock: socket.socket, feed_sock: socket.socket) -> tuple[int, int, int, int, float]:
    rng = random.Random(config.seed)
    response_buffer = bytearray()
    live_orders: list[int] = []
    next_order_id = 1
    sent_new = 0
    sent_cancel = 0
    sent_replace = 0
    received = 0
    interval = 1.0 / config.rate if config.rate else 0.0
    next_send = time.monotonic()
    generation_start = time.monotonic()

    def send(frame: bytes) -> None:
        nonlocal received
        view = memoryview(frame)
        while view:
            try:
                written = sock.send(view)
                view = view[written:]
            except BlockingIOError:
                received += drain_responses(sock, response_buffer)
                drain_feed(feed_sock)
                time.sleep(0)

    total = config.warmup + config.orders
    for sequence in range(total):
        while interval and time.monotonic() < next_send:
            received += drain_responses(sock, response_buffer)
            drain_feed(feed_sock)
            time.sleep(0)
        if interval:
            next_send = max(next_send + interval, time.monotonic())

        if live_orders and rng.random() < config.cancel_ratio:
            index = rng.randrange(len(live_orders))
            order_id = live_orders.pop(index)
            send(cancel_order(order_id))
            if sequence >= config.warmup:
                sent_cancel += 1
            continue

        if live_orders and rng.random() < config.replace_ratio:
            index = rng.randrange(len(live_orders))
            order_id = live_orders[index]
            new_order_id = next_order_id
            next_order_id += 1
            live_orders[index] = new_order_id
            price = 10_000 + rng.randrange(1, 1_000)
            send(replace_order(order_id, new_order_id, price, rng.randrange(1, 500), False))
            if sequence >= config.warmup:
                sent_replace += 1
            continue

        order_id = next_order_id
        next_order_id += 1
        ticker = rng.randrange(4)
        side = b"B" if rng.randrange(2) == 0 else b"S"
        price = 10_000 + rng.randrange(1, 1_000)
        quantity = rng.randrange(1, 500)
        ioc = rng.random() < 0.10
        send(enter_order(order_id, ticker, side, price, quantity, ioc))
        if not ioc:
            live_orders.append(order_id)
        if sequence >= config.warmup:
            sent_new += 1

    generation_elapsed = time.monotonic() - generation_start
    deadline = time.monotonic() + config.settle
    while time.monotonic() < deadline:
        received += drain_responses(sock, response_buffer)
        drain_feed(feed_sock)
        time.sleep(0.001)

    received += drain_responses(sock, response_buffer)
    drain_feed(feed_sock)
    return sent_new, sent_cancel, sent_replace, received, generation_elapsed


def drain_feed(sock: socket.socket) -> int:
    received = 0
    while True:
        try:
            data = sock.recv(64 * 1024)
        except BlockingIOError:
            return received
        if not data:
            return received
        received += len(data)


def subscribe_feed(sock: socket.socket, config: Config) -> None:
    for _ in range(3):
        sock.sendto(b"NEXSUB", (config.incremental_host, config.incremental_port))
        time.sleep(0.05)


def stop_engine(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main() -> int:
    config = parse_args()
    if not config.engine.is_file():
        print(f"engine binary not found: {config.engine}", file=sys.stderr)
        return 2

    run_dir = config.output_dir / time.strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=False)
    engine_log = run_dir / "nanoexchange.log"

    # NanoExchange writes nanoexchange.log relative to its working directory.
    process, engine_stdout = start_engine(config, run_dir)
    pin_runner(config.runner_core)
    feed_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    feed_sock.bind((config.gateway_host, 0))
    feed_sock.setblocking(False)
    try:
        with wait_for_gateway(config.gateway_host, config.gateway_port, config.timeout) as sock:
            subscribe_feed(feed_sock, config)
            sent_new, sent_cancel, sent_replace, received, elapsed = send_orders(config, sock, feed_sock)
            operations = sent_new + sent_cancel + sent_replace
            print(f"sent_new={sent_new}")
            print(f"sent_cancel={sent_cancel}")
            print(f"sent_replace={sent_replace}")
            print(f"responses_received={received}")
            print(f"order_generation_seconds={elapsed:.6f}")
            print(f"order_operations={operations}")
            print(f"order_generation_rate={operations / elapsed:.2f}")
    finally:
        feed_sock.close()
        stop_engine(process)
        engine_stdout.close()
    if not engine_log.exists():
        print(f"warning: engine log not found at {engine_log}", file=sys.stderr)
    print(f"run_dir={run_dir}")
    print(f"engine_exit_code={process.returncode}")
    return 0 if process.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
