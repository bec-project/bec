import os
import subprocess
import sys
from types import SimpleNamespace

from bec_lib.utils import import_utils


class DummyBase:
    pass


class DummyChild(DummyBase):
    pass


def _clean_pythonpath() -> str:
    return os.pathsep.join(str(path) for path in sys.path if path)


def test_isinstance_based_on_class_name():
    obj = DummyChild()
    assert import_utils.isinstance_based_on_class_name(
        obj, f"{DummyBase.__module__}.{DummyBase.__name__}"
    )
    assert import_utils.isinstance_based_on_class_name(
        obj, f"{DummyChild.__module__}.{DummyChild.__name__}"
    )
    assert not import_utils.isinstance_based_on_class_name(obj, "builtins.dict")


def test_lazy_import_from_accepts_string_input():
    json_decoder = import_utils.lazy_import_from("json", "JSONDecoder")
    assert json_decoder.__name__ == "JSONDecoder"


def test_lazy_import_from_single_tuple_returns_single_proxy():
    json_decoder = import_utils.lazy_import_from("json", ("JSONDecoder",))
    assert json_decoder.__name__ == "JSONDecoder"


def test_lazy_import_from_multiple_names_returns_tuple():
    proxies = import_utils.lazy_import_from("json", ("JSONDecoder", "JSONEncoder"))
    assert isinstance(proxies, tuple)
    assert [proxy.__name__ for proxy in proxies] == ["JSONDecoder", "JSONEncoder"]


def test_lazy_import_from_materializes_once(monkeypatch):
    calls = []

    def fake_import(module_name):
        calls.append(module_name)
        return SimpleNamespace(DemoClass=type("DemoClass", (), {}))

    monkeypatch.setattr(import_utils, "import_module", fake_import)

    demo_class = import_utils.lazy_import_from("demo.module", "DemoClass")
    assert demo_class.__name__ == "DemoClass"
    assert demo_class.__name__ == "DemoClass"
    assert calls == ["demo.module"]


def test_lazy_import_does_not_import_module_until_use(tmp_path, monkeypatch):
    module_name = "lazy_target_module"
    module_path = tmp_path / "lazy_target_module.py"
    module_path.write_text("VALUE = 123\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop(module_name, None)

    mod = import_utils.lazy_import(module_name)

    assert module_name not in sys.modules
    assert mod.VALUE == 123
    assert module_name in sys.modules


def test_lazy_import_from_does_not_import_module_until_use(tmp_path, monkeypatch):
    module_name = "lazy_from_target_module"
    module_path = tmp_path / "lazy_from_target_module.py"
    module_path.write_text("class DemoClass:\n" "    VALUE = 456\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop(module_name, None)

    demo_cls = import_utils.lazy_import_from(module_name, "DemoClass")

    assert module_name not in sys.modules
    assert demo_cls.VALUE == 456
    assert module_name in sys.modules


def test_importing_import_utils_does_not_import_scan_utils():
    # This needs a clean interpreter because sys.modules is shared by the test process.
    env = os.environ | {"PYTHONPATH": _clean_pythonpath()}
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from bec_lib.utils.import_utils import lazy_import_from; import sys; "
            "print('bec_lib.utils.scan_utils' in sys.modules)",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert proc.stdout.strip() == "False"
