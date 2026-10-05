"""Async WFTNP protocol client. Fitness profile decoding is deliberately external."""

from .client import WftnpClient
from .exceptions import (
    CallbackError,
    ConnectionLost,
    NotConnected,
    OperationRejected,
    ProtocolError,
    RequestTimeout,
    SubscriptionError,
    SubscriptionOverflow,
    WftnpError,
)
from .models import (
    Advertisement,
    Characteristic,
    CharacteristicProperties,
    ClientState,
    Endpoint,
    Notification,
    Service,
)
from .subscription import Subscription

__all__ = [
    "Advertisement",
    "CallbackError",
    "Characteristic",
    "CharacteristicProperties",
    "ClientState",
    "ConnectionLost",
    "Endpoint",
    "NotConnected",
    "Notification",
    "OperationRejected",
    "ProtocolError",
    "RequestTimeout",
    "Service",
    "Subscription",
    "SubscriptionError",
    "SubscriptionOverflow",
    "WftnpClient",
    "WftnpError",
]
