"""AWS Kinesis Video Streams client for Owlet cameras."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import hashlib
import hmac
import json
import logging
from typing import Any
from urllib.parse import quote

import aiohttp

_LOGGER = logging.getLogger(__name__)


class KinesisVideoClient:
    """Client for AWS Kinesis Video Streams API."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        region: str = "eu-west-1",
    ):
        """Initialize the Kinesis client."""
        self.session = session
        self.region = region
        self.service = "kinesisvideo"
        self.control_endpoint = f"https://kinesisvideo.{region}.amazonaws.com"

    async def get_data_endpoint(
        self,
        stream_name: str,
        access_key: str,
        secret_key: str,
        session_token: str | None = None,
    ) -> str:
        """Get the data endpoint for a Kinesis Video Stream."""
        endpoint = self.control_endpoint
        headers = self._get_signed_headers(
            method="POST",
            uri="/getDataEndpoint",
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            payload={
                "StreamName": stream_name,
                "APIName": "GET_HLS_STREAMING_SESSION_URL",
            },
        )

        try:
            async with asyncio.timeout(10):
                response = await self.session.post(
                    f"{endpoint}/getDataEndpoint",
                    json={
                        "StreamName": stream_name,
                        "APIName": "GET_HLS_STREAMING_SESSION_URL",
                    },
                    headers=headers,
                )
                response.raise_for_status()
                data = await response.json()
                return data.get("DataEndpoint", "")

        except Exception as err:
            _LOGGER.error(f"Error getting data endpoint: {err}")
            raise

    async def get_hls_streaming_url(
        self,
        stream_name: str,
        data_endpoint: str,
        access_key: str,
        secret_key: str,
        session_token: str | None = None,
        expires: int = 43200,  # 12 hours
    ) -> str:
        """Get HLS streaming URL for a Kinesis Video Stream."""
        service = "kinesisvideo"
        
        headers = self._get_signed_headers(
            method="POST",
            uri="/getHLSStreamingSessionURL",
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            endpoint=data_endpoint,
            payload={
                "StreamName": stream_name,
                "PlaybackMode": "LIVE",
                "HLSFragmentSelector": {
                    "FragmentSelectorType": "SERVER_TIMESTAMP"
                },
                "ContainerFormat": "MPEG_TS",
                "DiscontinuityMode": "ALWAYS",
                "DisplayFragmentTimestamp": "NEVER",
                "Expires": expires,
            },
        )

        try:
            async with asyncio.timeout(10):
                response = await self.session.post(
                    f"{data_endpoint}/getHLSStreamingSessionURL",
                    json={
                        "StreamName": stream_name,
                        "PlaybackMode": "LIVE",
                        "HLSFragmentSelector": {
                            "FragmentSelectorType": "SERVER_TIMESTAMP"
                        },
                        "ContainerFormat": "MPEG_TS",
                        "DiscontinuityMode": "ALWAYS",
                        "DisplayFragmentTimestamp": "NEVER",
                        "Expires": expires,
                    },
                    headers=headers,
                )
                response.raise_for_status()
                data = await response.json()
                return data.get("HLSStreamingSessionURL", "")

        except Exception as err:
            _LOGGER.error(f"Error getting HLS URL: {err}")
            raise

    def _get_signed_headers(
        self,
        method: str,
        uri: str,
        access_key: str,
        secret_key: str,
        session_token: str | None = None,
        endpoint: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Generate AWS Signature V4 signed headers."""
        if endpoint is None:
            endpoint = self.control_endpoint
        
        # Parse endpoint to get host
        host = endpoint.replace("https://", "").replace("http://", "")
        
        # Current timestamp
        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        # Payload
        payload_str = json.dumps(payload) if payload else ""
        payload_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

        # Canonical request
        canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"
        signed_headers = "host;x-amz-date"
        
        if session_token:
            canonical_headers += f"x-amz-security-token:{session_token}\n"
            signed_headers += ";x-amz-security-token"

        canonical_request = (
            f"{method}\n"
            f"{uri}\n"
            f"\n"  # Query string (empty)
            f"{canonical_headers}\n"
            f"{signed_headers}\n"
            f"{payload_hash}"
        )

        # String to sign
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.region}/{self.service}/aws4_request"
        string_to_sign = (
            f"{algorithm}\n"
            f"{amz_date}\n"
            f"{credential_scope}\n"
            f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )

        # Signing key
        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        k_date = sign(f"AWS4{secret_key}".encode("utf-8"), date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, self.service)
        signing_key = sign(k_service, "aws4_request")

        # Signature
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Authorization header
        authorization_header = (
            f"{algorithm} "
            f"Credential={access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )

        headers = {
            "Content-Type": "application/json",
            "X-Amz-Date": amz_date,
            "Authorization": authorization_header,
            "Host": host,
        }
        
        if session_token:
            headers["X-Amz-Security-Token"] = session_token

        return headers
