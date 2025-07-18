from bec_server.scan_server.procedures.container_utils import _build_args_from_dict


def test_build_args_from_dict():
    assert _build_args_from_dict({"a": "b", "c": "d"}) == [
        "--build-arg",
        "a=b",
        "--build-arg",
        "c=d",
    ]
