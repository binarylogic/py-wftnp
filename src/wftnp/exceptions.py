"""Public failures; no device-specific error interpretation."""


class WftnpError(Exception):
    """Base class for protocol/client failures."""


class NotConnected(WftnpError):
    """The client is not ready for requests."""


class ConnectionLost(WftnpError):
    """Transport failed. An in-flight write may have reached the device."""


class RequestTimeout(WftnpError, TimeoutError):
    """A request deadline expired; a transmitted write may have taken effect."""


class ProtocolError(WftnpError):
    """Invalid framing or an inconsistent response from the peer."""


class OperationRejected(WftnpError):
    """The peer returned a nonzero WFTNP response code."""

    def __init__(self, operation: int, code: int) -> None:
        self.operation = operation
        self.code = code
        super().__init__(f"WFTNP operation {operation} rejected with response code {code}")


class SubscriptionError(WftnpError):
    """A notification subscription has terminated with an error."""


class SubscriptionOverflow(SubscriptionError):
    """Consumer fell behind; the subscription is closed rather than silently dropping data."""


class CallbackError(SubscriptionError):
    """A subscription callback raised an exception."""
