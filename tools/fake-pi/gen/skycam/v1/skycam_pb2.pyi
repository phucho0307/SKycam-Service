import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UploadState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UPLOAD_STATE_UNSPECIFIED: _ClassVar[UploadState]
    UPLOAD_STATE_NOT_FOUND: _ClassVar[UploadState]
    UPLOAD_STATE_IN_PROGRESS: _ClassVar[UploadState]
    UPLOAD_STATE_COMPLETE: _ClassVar[UploadState]
UPLOAD_STATE_UNSPECIFIED: UploadState
UPLOAD_STATE_NOT_FOUND: UploadState
UPLOAD_STATE_IN_PROGRESS: UploadState
UPLOAD_STATE_COMPLETE: UploadState

class UploadFrameRequest(_message.Message):
    __slots__ = ("header", "chunk")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    header: FrameHeader
    chunk: FitsChunk
    def __init__(self, header: _Optional[_Union[FrameHeader, _Mapping]] = ..., chunk: _Optional[_Union[FitsChunk, _Mapping]] = ...) -> None: ...

class FrameHeader(_message.Message):
    __slots__ = ("frame_id", "device_id", "captured_at", "temperature_c", "probe_temp_c", "preview_jpeg", "fits")
    FRAME_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURED_AT_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_C_FIELD_NUMBER: _ClassVar[int]
    PROBE_TEMP_C_FIELD_NUMBER: _ClassVar[int]
    PREVIEW_JPEG_FIELD_NUMBER: _ClassVar[int]
    FITS_FIELD_NUMBER: _ClassVar[int]
    frame_id: str
    device_id: str
    captured_at: _timestamp_pb2.Timestamp
    temperature_c: float
    probe_temp_c: float
    preview_jpeg: bytes
    fits: FitsInfo
    def __init__(self, frame_id: _Optional[str] = ..., device_id: _Optional[str] = ..., captured_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., temperature_c: _Optional[float] = ..., probe_temp_c: _Optional[float] = ..., preview_jpeg: _Optional[bytes] = ..., fits: _Optional[_Union[FitsInfo, _Mapping]] = ...) -> None: ...

class FitsInfo(_message.Message):
    __slots__ = ("size_bytes", "sha256")
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    size_bytes: int
    sha256: bytes
    def __init__(self, size_bytes: _Optional[int] = ..., sha256: _Optional[bytes] = ...) -> None: ...

class FitsChunk(_message.Message):
    __slots__ = ("offset", "data")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    offset: int
    data: bytes
    def __init__(self, offset: _Optional[int] = ..., data: _Optional[bytes] = ...) -> None: ...

class UploadFrameResponse(_message.Message):
    __slots__ = ("frame_id", "duplicate")
    FRAME_ID_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    frame_id: str
    duplicate: bool
    def __init__(self, frame_id: _Optional[str] = ..., duplicate: _Optional[bool] = ...) -> None: ...

class GetUploadStatusRequest(_message.Message):
    __slots__ = ("frame_id", "device_id")
    FRAME_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    frame_id: str
    device_id: str
    def __init__(self, frame_id: _Optional[str] = ..., device_id: _Optional[str] = ...) -> None: ...

class GetUploadStatusResponse(_message.Message):
    __slots__ = ("state", "committed_bytes")
    STATE_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_BYTES_FIELD_NUMBER: _ClassVar[int]
    state: UploadState
    committed_bytes: int
    def __init__(self, state: _Optional[_Union[UploadState, str]] = ..., committed_bytes: _Optional[int] = ...) -> None: ...

class DeviceSessionRequest(_message.Message):
    __slots__ = ("hello", "telemetry", "heartbeat", "command_ack")
    HELLO_FIELD_NUMBER: _ClassVar[int]
    TELEMETRY_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ACK_FIELD_NUMBER: _ClassVar[int]
    hello: Hello
    telemetry: Telemetry
    heartbeat: Heartbeat
    command_ack: CommandAck
    def __init__(self, hello: _Optional[_Union[Hello, _Mapping]] = ..., telemetry: _Optional[_Union[Telemetry, _Mapping]] = ..., heartbeat: _Optional[_Union[Heartbeat, _Mapping]] = ..., command_ack: _Optional[_Union[CommandAck, _Mapping]] = ...) -> None: ...

class DeviceSessionResponse(_message.Message):
    __slots__ = ("settings", "command")
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    settings: DeviceSettings
    command: Command
    def __init__(self, settings: _Optional[_Union[DeviceSettings, _Mapping]] = ..., command: _Optional[_Union[Command, _Mapping]] = ...) -> None: ...

class Hello(_message.Message):
    __slots__ = ("device_id", "client_version")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    client_version: str
    def __init__(self, device_id: _Optional[str] = ..., client_version: _Optional[str] = ...) -> None: ...

class Telemetry(_message.Message):
    __slots__ = ("recorded_at", "temperature_c", "humidity_pct", "probe_temp_c")
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_C_FIELD_NUMBER: _ClassVar[int]
    HUMIDITY_PCT_FIELD_NUMBER: _ClassVar[int]
    PROBE_TEMP_C_FIELD_NUMBER: _ClassVar[int]
    recorded_at: _timestamp_pb2.Timestamp
    temperature_c: float
    humidity_pct: float
    probe_temp_c: float
    def __init__(self, recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., temperature_c: _Optional[float] = ..., humidity_pct: _Optional[float] = ..., probe_temp_c: _Optional[float] = ...) -> None: ...

class Heartbeat(_message.Message):
    __slots__ = ("sent_at",)
    SENT_AT_FIELD_NUMBER: _ClassVar[int]
    sent_at: _timestamp_pb2.Timestamp
    def __init__(self, sent_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Command(_message.Message):
    __slots__ = ("command_id", "abort_exposure", "capture_now")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    ABORT_EXPOSURE_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_NOW_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    abort_exposure: AbortExposure
    capture_now: CaptureNow
    def __init__(self, command_id: _Optional[str] = ..., abort_exposure: _Optional[_Union[AbortExposure, _Mapping]] = ..., capture_now: _Optional[_Union[CaptureNow, _Mapping]] = ...) -> None: ...

class AbortExposure(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CaptureNow(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CommandAck(_message.Message):
    __slots__ = ("command_id", "ok", "error")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    ok: bool
    error: str
    def __init__(self, command_id: _Optional[str] = ..., ok: _Optional[bool] = ..., error: _Optional[str] = ...) -> None: ...

class DeviceSettings(_message.Message):
    __slots__ = ("device_id", "exposure_ms", "gain", "preview_gamma", "preview_contrast", "preview_brightness", "updated_at")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPOSURE_MS_FIELD_NUMBER: _ClassVar[int]
    GAIN_FIELD_NUMBER: _ClassVar[int]
    PREVIEW_GAMMA_FIELD_NUMBER: _ClassVar[int]
    PREVIEW_CONTRAST_FIELD_NUMBER: _ClassVar[int]
    PREVIEW_BRIGHTNESS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    exposure_ms: float
    gain: int
    preview_gamma: float
    preview_contrast: float
    preview_brightness: float
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, device_id: _Optional[str] = ..., exposure_ms: _Optional[float] = ..., gain: _Optional[int] = ..., preview_gamma: _Optional[float] = ..., preview_contrast: _Optional[float] = ..., preview_brightness: _Optional[float] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UpdateDeviceSettingsRequest(_message.Message):
    __slots__ = ("settings",)
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    settings: DeviceSettings
    def __init__(self, settings: _Optional[_Union[DeviceSettings, _Mapping]] = ...) -> None: ...

class UpdateDeviceSettingsResponse(_message.Message):
    __slots__ = ("settings", "delivered")
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_FIELD_NUMBER: _ClassVar[int]
    settings: DeviceSettings
    delivered: bool
    def __init__(self, settings: _Optional[_Union[DeviceSettings, _Mapping]] = ..., delivered: _Optional[bool] = ...) -> None: ...

class SendCommandRequest(_message.Message):
    __slots__ = ("device_id", "command", "ack_timeout_ms")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    ACK_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    command: Command
    ack_timeout_ms: int
    def __init__(self, device_id: _Optional[str] = ..., command: _Optional[_Union[Command, _Mapping]] = ..., ack_timeout_ms: _Optional[int] = ...) -> None: ...

class SendCommandResponse(_message.Message):
    __slots__ = ("command_id", "acknowledged", "error")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    acknowledged: bool
    error: str
    def __init__(self, command_id: _Optional[str] = ..., acknowledged: _Optional[bool] = ..., error: _Optional[str] = ...) -> None: ...

class ListConnectedDevicesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListConnectedDevicesResponse(_message.Message):
    __slots__ = ("devices",)
    DEVICES_FIELD_NUMBER: _ClassVar[int]
    devices: _containers.RepeatedCompositeFieldContainer[ConnectedDevice]
    def __init__(self, devices: _Optional[_Iterable[_Union[ConnectedDevice, _Mapping]]] = ...) -> None: ...

class ConnectedDevice(_message.Message):
    __slots__ = ("device_id", "connected_at", "last_message_at")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_MESSAGE_AT_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    connected_at: _timestamp_pb2.Timestamp
    last_message_at: _timestamp_pb2.Timestamp
    def __init__(self, device_id: _Optional[str] = ..., connected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_message_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
