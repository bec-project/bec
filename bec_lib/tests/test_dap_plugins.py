from math import inf
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.dap_plugin_objects import DAPPluginObject, LmfitService1D
from bec_lib.dap_plugins import DAPPlugins
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.scan_data_container import ScanDataContainer
from bec_lib.scan_items import ScanItem
from bec_lib.scan_report import ScanReport


@pytest.fixture
def dap_plugin_message():
    msg = messages.AvailableResourceMessage(
        **{
            "resource": {
                "GaussianModel": {
                    "class": "LmfitService1D",
                    "user_friendly_name": "GaussianModel",
                    "class_doc": "A model based on a Gaussian or normal distribution lineshape.\n\n    The model has three Parameters: `amplitude`, `center`, and `sigma`.\n    In addition, parameters `fwhm` and `height` are included as\n    constraints to report full width at half maximum and maximum peak\n    height, respectively.\n\n    .. math::\n\n        f(x; A, \\mu, \\sigma) = \\frac{A}{\\sigma\\sqrt{2\\pi}} e^{[{-{(x-\\mu)^2}/{{2\\sigma}^2}}]}\n\n    where the parameter `amplitude` corresponds to :math:`A`, `center` to\n    :math:`\\mu`, and `sigma` to :math:`\\sigma`. The full width at half\n    maximum is :math:`2\\sigma\\sqrt{2\\ln{2}}`, approximately\n    :math:`2.3548\\sigma`.\n\n    For more information, see: https://en.wikipedia.org/wiki/Normal_distribution\n\n    ",
                    "run_doc": "A model based on a Gaussian or normal distribution lineshape.\n\n    The model has three Parameters: `amplitude`, `center`, and `sigma`.\n    In addition, parameters `fwhm` and `height` are included as\n    constraints to report full width at half maximum and maximum peak\n    height, respectively.\n\n    .. math::\n\n        f(x; A, \\mu, \\sigma) = \\frac{A}{\\sigma\\sqrt{2\\pi}} e^{[{-{(x-\\mu)^2}/{{2\\sigma}^2}}]}\n\n    where the parameter `amplitude` corresponds to :math:`A`, `center` to\n    :math:`\\mu`, and `sigma` to :math:`\\sigma`. The full width at half\n    maximum is :math:`2\\sigma\\sqrt{2\\ln{2}}`, approximately\n    :math:`2.3548\\sigma`.\n\n    For more information, see: https://en.wikipedia.org/wiki/Normal_distribution\n\n    \n        Args:\n            scan_item (ScanItem): Scan item or scan ID\n            device_x (DeviceBase | str): Device name for x\n            signal_x (DeviceBase | str): Signal name for x\n            device_y (DeviceBase | str): Device name for y\n            signal_y (DeviceBase | str): Signal name for y\n            parameters (dict): Fit parameters\n        ",
                    "run_name": "fit",
                    "signature": [
                        {
                            "name": "args",
                            "kind": "VAR_POSITIONAL",
                            "default": "_empty",
                            "annotation": "_empty",
                        },
                        {
                            "name": "scan_item",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "ScanItem | str",
                        },
                        {
                            "name": "device_x",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "signal_x",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "device_y",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "signal_y",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "parameters",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "dict",
                        },
                        {
                            "name": "kwargs",
                            "kind": "VAR_KEYWORD",
                            "default": "_empty",
                            "annotation": "_empty",
                        },
                    ],
                    "auto_fit_supported": True,
                    "params": {
                        "amplitude": {
                            "name": "amplitude",
                            "value": 1.0,
                            "vary": True,
                            "min": -inf,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "center": {
                            "name": "center",
                            "value": 0.0,
                            "vary": True,
                            "min": -inf,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "sigma": {
                            "name": "sigma",
                            "value": 1.0,
                            "vary": True,
                            "min": 0,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "fwhm": {
                            "name": "fwhm",
                            "value": 2.35482,
                            "vary": False,
                            "min": -inf,
                            "max": inf,
                            "expr": "2.3548200*sigma",
                            "brute_step": None,
                            "user_data": None,
                        },
                        "height": {
                            "name": "height",
                            "value": 0.3989423,
                            "vary": False,
                            "min": -inf,
                            "max": inf,
                            "expr": "0.3989423*amplitude/max(1e-15, sigma)",
                            "brute_step": None,
                            "user_data": None,
                        },
                    },
                    "class_args": [],
                    "class_kwargs": {"model": "GaussianModel"},
                },
                "StepModel": {
                    "class": "LmfitService1D",
                    "user_friendly_name": "StepModel",
                    "class_doc": "A model based on a Step function.\n\n    The model has three Parameters: `amplitude` (:math:`A`), `center`\n    (:math:`\\mu`), and `sigma` (:math:`\\sigma`).\n\n    There are four choices for `form`:\n\n    - `'linear'` (default)\n    - `'atan'` or `'arctan'` for an arc-tangent function\n    - `'erf'` for an error function\n    - `'logistic'` for a logistic function (for more information, see:\n      https://en.wikipedia.org/wiki/Logistic_function)\n\n    The step function starts with a value 0 and ends with a value of\n    :math:`A` rising to :math:`A/2` at :math:`\\mu`, with :math:`\\sigma`\n    setting the characteristic width. The functional forms are defined as:\n\n    .. math::\n        :nowrap:\n\n        \\begin{eqnarray*}\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'linear{}'}})  & = A \\min{[1, \\max{(0, \\alpha + 1/2)}]} \\\\\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'arctan{}'}})  & = A [1/2 + \\arctan{(\\alpha)}/{\\pi}] \\\\\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'erf{}'}})     & = A [1 + {\\operatorname{erf}}(\\alpha)]/2 \\\\\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'logistic{}'}})& = A \\left[1 - \\frac{1}{1 + e^{\\alpha}} \\right]\n        \\end{eqnarray*}\n\n    where :math:`\\alpha = (x - \\mu)/{\\sigma}`.\n\n    ",
                    "run_doc": "A model based on a Step function.\n\n    The model has three Parameters: `amplitude` (:math:`A`), `center`\n    (:math:`\\mu`), and `sigma` (:math:`\\sigma`).\n\n    There are four choices for `form`:\n\n    - `'linear'` (default)\n    - `'atan'` or `'arctan'` for an arc-tangent function\n    - `'erf'` for an error function\n    - `'logistic'` for a logistic function (for more information, see:\n      https://en.wikipedia.org/wiki/Logistic_function)\n\n    The step function starts with a value 0 and ends with a value of\n    :math:`A` rising to :math:`A/2` at :math:`\\mu`, with :math:`\\sigma`\n    setting the characteristic width. The functional forms are defined as:\n\n    .. math::\n        :nowrap:\n\n        \\begin{eqnarray*}\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'linear{}'}})  & = A \\min{[1, \\max{(0, \\alpha + 1/2)}]} \\\\\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'arctan{}'}})  & = A [1/2 + \\arctan{(\\alpha)}/{\\pi}] \\\\\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'erf{}'}})     & = A [1 + {\\operatorname{erf}}(\\alpha)]/2 \\\\\n        & f(x; A, \\mu, \\sigma, {\\mathrm{form={}'logistic{}'}})& = A \\left[1 - \\frac{1}{1 + e^{\\alpha}} \\right]\n        \\end{eqnarray*}\n\n    where :math:`\\alpha = (x - \\mu)/{\\sigma}`.\n\n    \n        Args:\n            scan_item (ScanItem): Scan item or scan ID\n            device_x (DeviceBase | str): Device name for x\n            signal_x (DeviceBase | str): Signal name for x\n            device_y (DeviceBase | str): Device name for y\n            signal_y (DeviceBase | str): Signal name for y\n            parameters (dict): Fit parameters\n        ",
                    "run_name": "fit",
                    "signature": [
                        {
                            "name": "args",
                            "kind": "VAR_POSITIONAL",
                            "default": "_empty",
                            "annotation": "_empty",
                        },
                        {
                            "name": "scan_item",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "ScanItem | str",
                        },
                        {
                            "name": "device_x",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "signal_x",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "device_y",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "signal_y",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "parameters",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "dict",
                        },
                        {
                            "name": "kwargs",
                            "kind": "VAR_KEYWORD",
                            "default": "_empty",
                            "annotation": "_empty",
                        },
                    ],
                    "auto_fit_supported": True,
                    "params": {
                        "amplitude": {
                            "name": "amplitude",
                            "value": 1.0,
                            "vary": True,
                            "min": -inf,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "center": {
                            "name": "center",
                            "value": 0.0,
                            "vary": True,
                            "min": -inf,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "sigma": {
                            "name": "sigma",
                            "value": 1.0,
                            "vary": True,
                            "min": -inf,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                    },
                    "class_args": [],
                    "class_kwargs": {"model": "StepModel"},
                },
                "Ptychography": {
                    "class": "PtychographyDAP",
                    "user_friendly_name": "ptycho",
                    "class_doc": "A model based on a Gaussian or normal distribution lineshape.\n\n    The model has three Parameters: `amplitude`, `center`, and `sigma`.\n    In addition, parameters `fwhm` and `height` are included as\n    constraints to report full width at half maximum and maximum peak\n    height, respectively.\n\n    .. math::\n\n        f(x; A, \\mu, \\sigma) = \\frac{A}{\\sigma\\sqrt{2\\pi}} e^{[{-{(x-\\mu)^2}/{{2\\sigma}^2}}]}\n\n    where the parameter `amplitude` corresponds to :math:`A`, `center` to\n    :math:`\\mu`, and `sigma` to :math:`\\sigma`. The full width at half\n    maximum is :math:`2\\sigma\\sqrt{2\\ln{2}}`, approximately\n    :math:`2.3548\\sigma`.\n\n    For more information, see: https://en.wikipedia.org/wiki/Normal_distribution\n\n    ",
                    "run_doc": "A model based on a Gaussian or normal distribution lineshape.\n\n    The model has three Parameters: `amplitude`, `center`, and `sigma`.\n    In addition, parameters `fwhm` and `height` are included as\n    constraints to report full width at half maximum and maximum peak\n    height, respectively.\n\n    .. math::\n\n        f(x; A, \\mu, \\sigma) = \\frac{A}{\\sigma\\sqrt{2\\pi}} e^{[{-{(x-\\mu)^2}/{{2\\sigma}^2}}]}\n\n    where the parameter `amplitude` corresponds to :math:`A`, `center` to\n    :math:`\\mu`, and `sigma` to :math:`\\sigma`. The full width at half\n    maximum is :math:`2\\sigma\\sqrt{2\\ln{2}}`, approximately\n    :math:`2.3548\\sigma`.\n\n    For more information, see: https://en.wikipedia.org/wiki/Normal_distribution\n\n    \n        Args:\n            scan_item (ScanItem): Scan item or scan ID\n            device_x (DeviceBase | str): Device name for x\n            signal_x (DeviceBase | str): Signal name for x\n            device_y (DeviceBase | str): Device name for y\n            signal_y (DeviceBase | str): Signal name for y\n            parameters (dict): Fit parameters\n        ",
                    "run_name": "fit",
                    "signature": [
                        {
                            "name": "args",
                            "kind": "VAR_POSITIONAL",
                            "default": "_empty",
                            "annotation": "_empty",
                        },
                        {
                            "name": "scan_item",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "ScanItem | str",
                        },
                        {
                            "name": "device_x",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "signal_x",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "device_y",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "signal_y",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "DeviceBase | str",
                        },
                        {
                            "name": "parameters",
                            "kind": "KEYWORD_ONLY",
                            "default": None,
                            "annotation": "dict",
                        },
                        {
                            "name": "kwargs",
                            "kind": "VAR_KEYWORD",
                            "default": "_empty",
                            "annotation": "_empty",
                        },
                    ],
                    "auto_fit_supported": True,
                    "params": {
                        "amplitude": {
                            "name": "amplitude",
                            "value": 1.0,
                            "vary": True,
                            "min": -inf,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "center": {
                            "name": "center",
                            "value": 0.0,
                            "vary": True,
                            "min": -inf,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "sigma": {
                            "name": "sigma",
                            "value": 1.0,
                            "vary": True,
                            "min": 0,
                            "max": inf,
                            "expr": None,
                            "brute_step": None,
                            "user_data": None,
                        },
                        "fwhm": {
                            "name": "fwhm",
                            "value": 2.35482,
                            "vary": False,
                            "min": -inf,
                            "max": inf,
                            "expr": "2.3548200*sigma",
                            "brute_step": None,
                            "user_data": None,
                        },
                        "height": {
                            "name": "height",
                            "value": 0.3989423,
                            "vary": False,
                            "min": -inf,
                            "max": inf,
                            "expr": "0.3989423*amplitude/max(1e-15, sigma)",
                            "brute_step": None,
                            "user_data": None,
                        },
                    },
                    "class_args": [],
                    "class_kwargs": {"model": "GaussianModel"},
                },
            }
        }
    )
    yield msg


@pytest.fixture
def dap(dap_plugin_message):
    dap_services = {
        "BECClient": messages.StatusMessage(name="BECClient", status=1, info={}),
        "DAPServer/LmfitService1D": messages.StatusMessage(
            name="LmfitService1D", status=1, info={}
        ),
        "DAPServer/PtychographyDAP": messages.StatusMessage(
            name="PtychographyDAP", status=1, info={}
        ),
    }
    client = mock.MagicMock()
    client.service_status = dap_services
    client.connector.get.return_value = dap_plugin_message
    dap_plugins = DAPPlugins(client)
    yield dap_plugins


def test_dap_plugins_construction(dap):
    assert hasattr(dap, "GaussianModel")
    assert hasattr(dap, "StepModel")
    assert hasattr(dap, "ptycho")
    # pylint: disable=no-member
    assert isinstance(dap.GaussianModel, LmfitService1D)
    assert isinstance(dap.StepModel, DAPPluginObject)
    assert isinstance(dap.ptycho, DAPPluginObject)


def test_dap_plugin_fit(dap):
    with mock.patch.object(dap.GaussianModel, "_wait_for_dap_response") as mock_wait:
        dap.GaussianModel.fit()
        dap._parent.connector.set_and_publish.assert_called_once()
        mock_wait.assert_called_once()


class ScanReportMock(ScanReport):
    def __init__(self, scan_id: str) -> None:
        super().__init__()
        self.request = mock.MagicMock()
        self.request.scan.scan_id = scan_id


@pytest.mark.parametrize(
    "input",
    [
        "scan_id_1",
        ScanItem(
            scan_manager=mock.MagicMock(),
            queue_id="queue_id",
            scan_id="scan_id_1",
            scan_number=1,
            status="closed",
        ),
        ScanReportMock("scan_id_1"),
        "scan_data_container",
    ],
)
def test_dap_plugin_fit_input(dap, input, file_history_messages, mock_file):
    if isinstance(input, str) and input == "scan_data_container":
        input = ScanDataContainer(file_path=mock_file, msg=file_history_messages[0])
    with mock.patch.object(dap.GaussianModel, "_wait_for_dap_response") as mock_wait:
        dap.GaussianModel.fit(input)
        request_id = dap._parent.connector.set_and_publish.call_args[0][1].metadata["RID"]
        dap._parent.connector.set_and_publish.assert_called_once_with(
            MessageEndpoints.dap_request(),
            messages.DAPRequestMessage(
                dap_cls="LmfitService1D",
                dap_type="on_demand",
                config={
                    "args": ["scan_id_1"],
                    "kwargs": {},
                    "class_args": [],
                    "class_kwargs": {"model": "GaussianModel"},
                },
                metadata={"RID": request_id},
            ),
        )
        mock_wait.assert_called_once()


def test_dap_auto_run(dap):
    with mock.patch.object(dap.GaussianModel, "_update_dap_config") as mock_update_dap_config:
        dap.GaussianModel.auto_run == False
        dap.GaussianModel.auto_run = True
        mock_update_dap_config.assert_called_once()
        dap.GaussianModel.auto_run = True


def test_dap_wait_for_dap_response_waits_for_RID(dap):
    dap._parent.connector.get.return_value = messages.DAPResponseMessage(
        success=True, data=({}, None), metadata={"RID": "wrong_ID"}
    )
    with pytest.raises(TimeoutError):
        dap.GaussianModel._wait_for_dap_response(request_id="1234", timeout=0.1)


def test_dap_wait_for_dap_respnse_returns(dap):
    dap._parent.connector.get.return_value = messages.DAPResponseMessage(
        success=True, data=({}, None), metadata={"RID": "1234"}
    )
    val = dap.GaussianModel._wait_for_dap_response(request_id="1234", timeout=0.1)
    assert val == messages.DAPResponseMessage(
        success=True, data=({}, None), metadata={"RID": "1234"}
    )


def test_dap_select(dap):
    with mock.patch.object(dap.GaussianModel, "_update_dap_config") as mock_update_dap_config:
        obj = DeviceBase(name="samx", info={"device_info": {"hints": {"fields": ["samx"]}}})
        dap._parent.device_manager.devices.get.return_value = obj
        dap.GaussianModel.select("samx")
        mock_update_dap_config.assert_called_once()
        assert dap.GaussianModel._plugin_config["selected_device"] == ["samx", "samx"]


def test_dap_select_raises_with_too_many_hints(dap):
    with pytest.raises(AttributeError):
        with mock.patch.object(dap.GaussianModel, "_update_dap_config") as mock_update_dap_config:
            obj = DeviceBase(
                name="samx", info={"device_info": {"hints": {"fields": ["samx", "samx2"]}}}
            )
            dap._parent.device_manager.devices.get.return_value = obj
            dap.GaussianModel.select("samx")
            mock_update_dap_config.assert_called_once()
            assert dap.GaussianModel._plugin_config["selected_device"] == ["samx", "samx"]


def test_dap_select_raises_on_device_without_hints(dap):
    with pytest.raises(AttributeError):
        dap._parent.device_manager.devices.get.return_value = DeviceBase(name="samx", info={})
        dap.GaussianModel.select("samx")


def test_dap_select_raises_on_wrong_device(dap):
    dap._parent.device_manager.devices.get.return_value = None
    with pytest.raises(AttributeError):
        dap.GaussianModel.select("samx")


def test_dap_get_data(dap):
    dap._parent.connector.get_last.return_value = {
        "data": messages.ProcessedDataMessage(
            data={"x": [1, 2, 3], "y": [4, 5, 6]}, metadata={"fit_parameters": {"amplitude": 1}}
        )
    }
    data = dap.GaussianModel.get_data()
    dap._parent.connector.get_last.assert_called_once_with(
        MessageEndpoints.processed_data("GaussianModel")
    )

    assert data.data == {"x": [1, 2, 3], "y": [4, 5, 6]}
    assert data.amplitude == 1


def test_dap_update_dap_config_not_called_without_device(dap):
    dap.GaussianModel._update_dap_config(request_id="1234")
    dap._parent.connector.set_and_publish.assert_not_called()


def test_dap_update_dap_config(dap):
    dap.GaussianModel._plugin_config["selected_device"] = ["samx", "samx"]
    dap.GaussianModel._update_dap_config(request_id="1234")
    dap._parent.connector.set_and_publish.assert_called_with(
        MessageEndpoints.dap_request(),
        messages.DAPRequestMessage(
            dap_cls="LmfitService1D",
            dap_type="continuous",
            config=dap.GaussianModel._plugin_config,
            metadata={"RID": "1234"},
        ),
    )
