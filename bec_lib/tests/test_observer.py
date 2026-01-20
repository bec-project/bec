import os
from unittest import mock

import pytest
from typeguard import TypeCheckError

import bec_lib
from bec_messages import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.observer import Observer, ObserverManager

dir_path = os.path.dirname(bec_lib.__file__)


@pytest.mark.parametrize(
    "kwargs,raised_error",
    [
        (
            {
                "name": "stop scan if ring current drops",
                "device": "ring_current",
                "on_trigger": "pause",
                "on_resume": "restart",
            },
            AttributeError,
        ),
        (
            {
                "name": "stop scan if ring current drops",
                "device": "ring_current",
                "on_trigger": "pause",
                "on_resume": "whatever",
            },
            ValueError,
        ),
        (
            {
                "name": "stop scan if ring current drops",
                "device": "ring_current",
                "on_trigger": "pause",
                "on_resume": "restart",
                "limits": [380, 420],
            },
            None,
        ),
        (
            {
                "name": "stop scan if ring current drops",
                "device": "ring_current",
                "on_trigger": "pause",
                "on_resume": "restart",
                "limits": [380, 420],
                "target_value": 20,
            },
            AttributeError,
        ),
        (
            {
                "name": "stop scan if ring current drops",
                "device": "ring_current",
                "on_trigger": "pause",
                "on_resume": "restart",
                "limits": 20,
            },
            TypeCheckError,
        ),
        (
            {
                "name": "stop scan if ring current drops",
                "device": "ring_current",
                "on_trigger": "pause",
                "on_resume": "restart",
                "limits": [380, 420],
                "low_limit": 20,
            },
            AttributeError,
        ),
    ],
)
def test_observer(kwargs, raised_error):
    if not raised_error:
        observer = Observer(**kwargs)
        return
    with pytest.raises(raised_error):
        observer = Observer(**kwargs)


@pytest.fixture()
def device_manager(dm_with_devices):
    dm = dm_with_devices
    with mock.patch.object(dm, "_get_config"):
        dm.initialize("")
    return dm


def test_observer_manager_None(device_manager):
    with mock.patch.object(device_manager.connector, "get", return_value=None) as connector_get:
        observer_manager = ObserverManager(device_manager=device_manager)
        connector_get.assert_called_once_with(MessageEndpoints.observer())
        assert len(observer_manager._observer) == 0


def test_observer_manager_msg(device_manager):
    msg = messages.ObserverMessage(
        observer=[
            {
                "name": "test_observer",
                "device": "samx",
                "on_trigger": "pause",
                "on_resume": "restart",
                "limits": [380, None],
            }
        ]
    )
    with mock.patch.object(device_manager.connector, "get", return_value=msg) as connector_get:
        observer_manager = ObserverManager(device_manager=device_manager)
        connector_get.assert_called_once_with(MessageEndpoints.observer())
        assert len(observer_manager._observer) == 1


@pytest.mark.parametrize(
    "observer,raises_error",
    [
        (
            Observer.from_dict(
                {
                    "name": "test_observer",
                    "device": "samx",
                    "on_trigger": "pause",
                    "on_resume": "restart",
                    "limits": [380, None],
                }
            ),
            False,
        )
    ],
)
def test_add_observer(device_manager, observer, raises_error):
    with mock.patch.object(device_manager.connector, "get", return_value=None) as connector_get:
        observer_manager = ObserverManager(device_manager=device_manager)
        observer_manager.add_observer(observer)
        with pytest.raises(AttributeError):
            observer_manager.add_observer(observer)


@pytest.mark.parametrize(
    "observer,raises_error",
    [
        (
            Observer.from_dict(
                {
                    "name": "test_observer",
                    "device": "samx",
                    "on_trigger": "pause",
                    "on_resume": "restart",
                    "limits": [380, None],
                }
            ),
            True,
        ),
        (
            Observer.from_dict(
                {
                    "name": "test_observer",
                    "device": "samy",
                    "on_trigger": "pause",
                    "on_resume": "restart",
                    "limits": [380, None],
                }
            ),
            False,
        ),
    ],
)
def test_add_observer_existing_device(device_manager, observer, raises_error):
    default_observer = Observer.from_dict(
        {
            "name": "test_observer",
            "device": "samx",
            "on_trigger": "pause",
            "on_resume": "restart",
            "limits": [380, None],
        }
    )
    with mock.patch.object(device_manager.connector, "get", return_value=None) as connector_get:
        observer_manager = ObserverManager(device_manager=device_manager)
        observer_manager.add_observer(default_observer)
        if raises_error:
            with pytest.raises(AttributeError):
                observer_manager.add_observer(observer)
        else:
            observer_manager.add_observer(observer)
