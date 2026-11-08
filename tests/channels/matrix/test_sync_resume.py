"""Sync-resume behavior for the Matrix channel.

After a restart the client resumes from the stored sync token, so its first
sync is incremental. Rooms quiet since that token never register, and the
first incremental entry that later registers one carries no member summary,
which makes nio report ``member_count == 0`` and misclassify real groups as
DMs in the channel's group-policy gates.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nanobot.bus.queue import MessageBus
from nanobot.channels.matrix.runtime import MatrixChannel, MatrixConfig

pytest.importorskip("nio")


def _channel_with_client(client) -> MatrixChannel:
    channel = MatrixChannel(MatrixConfig(), MessageBus())
    channel._running = True
    channel.client = client
    return channel


def _fake_sync_forever(collector):
    async def fake_sync_forever(timeout=0, full_state=False):
        collector.append((timeout, full_state))
        raise asyncio.CancelledError()

    return fake_sync_forever


async def test_first_sync_blanks_restored_sync_token():
    collector = []
    client = SimpleNamespace(
        loaded_sync_token="tok-from-previous-boot",
        sync_forever=_fake_sync_forever(collector),
        stopped=False,
    )
    channel = _channel_with_client(client)

    def _require_client():
        return channel.client

    channel._require_client = _require_client

    await channel._sync_loop()

    assert collector == [(30000, True)]
    assert client.loaded_sync_token == ""


async def test_sync_loop_tolerates_missing_loaded_sync_token():
    collector = []
    client = SimpleNamespace(
        sync_forever=_fake_sync_forever(collector),
        stopped=False,
    )
    channel = _channel_with_client(client)

    def _require_client():
        return channel.client

    channel._require_client = _require_client

    await channel._sync_loop()

    assert collector == [(30000, True)]
    assert not hasattr(client, "loaded_sync_token")
