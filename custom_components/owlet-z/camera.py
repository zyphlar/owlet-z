"""Owlet Camera integration."""
from __future__ import annotations

import asyncio
from datetime import timedelta
import logging
from typing import Any

import aiohttp
import async_timeout

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_API_TOKEN, CONF_REGION, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, MANUFACTURER

_LOGGER = logging.getLogger(__name__)

# Camera API endpoints based on region
CAMERA_KMS_ENDPOINTS = {
    "world": "https://camera-kms.owletdata.com/kms/",
    "europe": "https://camera-kms.eu.owletdata.com/kms/",
}

# AWS Kinesis Video endpoint template
AWS_KINESIS_ENDPOINT_TEMPLATE = "https://kinesisvideo.{region}.amazonaws.com"


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Owlet cameras."""
    session = async_get_clientsession(hass)
    region = config_entry.data[CONF_REGION]
    token = config_entry.data[CONF_API_TOKEN]
    
    # Get cameras from Owlet API
    camera_api = OwletCameraAPI(session, region, token)
    
    try:
        cameras = await camera_api.get_cameras()
        _LOGGER.info(f"Found {len(cameras)} Owlet camera(s)")
        
        entities = [
            OwletCamera(hass, camera, camera_api)
            for camera in cameras
        ]
        
        async_add_entities(entities)
        
    except Exception as err:
        _LOGGER.error(f"Error setting up Owlet cameras: {err}")


class OwletCameraAPI:
    """API client for Owlet cameras."""

    def __init__(self, session: aiohttp.ClientSession, region: str, token: str):
        """Initialize the camera API."""
        self.session = session
        self.region = region
        self.token = token
        self.kms_endpoint = CAMERA_KMS_ENDPOINTS.get(
            region, CAMERA_KMS_ENDPOINTS["world"]
        )

    async def get_cameras(self) -> list[dict[str, Any]]:
        """Get list of cameras from Owlet API."""
        # This will need to call the Owlet devices API to get cameras
        # For now, return empty list - needs actual API implementation
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        
        try:
            # TODO: Implement actual camera discovery endpoint
            # This might be part of the existing get_devices call
            # For now, we'll need to check if pyowletapi has camera support
            _LOGGER.warning("Camera discovery not yet implemented")
            return []
            
        except Exception as err:
            _LOGGER.error(f"Error getting cameras: {err}")
            return []

    async def get_stream_credentials(self, camera_id: str) -> dict[str, Any]:
        """Get AWS Kinesis credentials for a camera."""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        
        try:
            async with async_timeout.timeout(10):
                response = await self.session.post(
                    self.kms_endpoint,
                    json={"camera_id": camera_id},
                    headers=headers,
                )
                response.raise_for_status()
                return await response.json()
                
        except asyncio.TimeoutError:
            _LOGGER.error(f"Timeout getting credentials for camera {camera_id}")
            raise
        except aiohttp.ClientError as err:
            _LOGGER.error(f"Error getting credentials for camera {camera_id}: {err}")
            raise

    async def get_hls_streaming_url(
        self, stream_name: str, aws_credentials: dict[str, Any]
    ) -> str:
        """Get HLS streaming URL from AWS Kinesis Video Streams."""
        # This would use AWS SDK to:
        # 1. Get data endpoint for the stream
        # 2. Call GetHLSStreamingSessionURL
        # For now, this is a placeholder
        _LOGGER.warning("HLS streaming URL generation not yet implemented")
        return ""


class OwletCamera(Camera):
    """Representation of an Owlet Camera."""

    _attr_has_entity_name = True
    _attr_supported_features = CameraEntityFeature.STREAM

    def __init__(
        self,
        hass: HomeAssistant,
        camera_data: dict[str, Any],
        api: OwletCameraAPI,
    ) -> None:
        """Initialize the camera."""
        super().__init__()
        self._hass = hass
        self._api = api
        self._camera_data = camera_data
        self._attr_name = camera_data.get("name", "Owlet Camera")
        self._attr_unique_id = camera_data.get("device_id") or camera_data.get("dsn")
        self._stream_url: str | None = None
        self._last_url_refresh = None

    @property
    def device_info(self):
        """Return device information."""
        return {
            "identifiers": {(DOMAIN, self.unique_id)},
            "name": self._attr_name,
            "manufacturer": MANUFACTURER,
            "model": self._camera_data.get("model", "Owlet Cam"),
        }

    @property
    def available(self) -> bool:
        """Return True if camera is available."""
        return True

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Return a still image from the camera."""
        # Get the stream URL and extract a frame
        # This is optional - HLS streams don't provide easy still images
        return None

    async def stream_source(self) -> str | None:
        """Return the stream source URL."""
        # Refresh URL if needed (AWS URLs expire)
        if self._should_refresh_url():
            await self._refresh_stream_url()
        
        return self._stream_url

    def _should_refresh_url(self) -> bool:
        """Check if stream URL needs to be refreshed."""
        if self._stream_url is None:
            return True
        
        # AWS HLS URLs typically expire after a period
        # Refresh every 30 minutes to be safe
        if self._last_url_refresh is None:
            return True
            
        from datetime import datetime
        age = datetime.now() - self._last_url_refresh
        return age > timedelta(minutes=30)

    async def _refresh_stream_url(self) -> None:
        """Refresh the HLS streaming URL."""
        try:
            camera_id = self._camera_data.get("device_id") or self._camera_data.get("dsn")
            
            # Step 1: Get AWS credentials from Owlet KMS API
            credentials = await self._api.get_stream_credentials(camera_id)
            
            # Step 2: Use credentials to get HLS URL from AWS Kinesis
            stream_name = self._camera_data.get("kinesis_stream_name", camera_id)
            self._stream_url = await self._api.get_hls_streaming_url(
                stream_name, credentials
            )
            
            from datetime import datetime
            self._last_url_refresh = datetime.now()
            
            _LOGGER.info(f"Refreshed stream URL for camera {self._attr_name}")
            
        except Exception as err:
            _LOGGER.error(f"Error refreshing stream URL: {err}")
            self._stream_url = None
