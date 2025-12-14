from __future__ import annotations

import base64

from dickgrowerbot.main import _decode_deeplink_payload


def test_decode_deeplink_payload_roundtrip() -> None:
    code = "TEST_CODE"
    encoded = base64.urlsafe_b64encode(code.encode("utf-8")).decode("ascii").rstrip("=")
    assert _decode_deeplink_payload(encoded) == code

