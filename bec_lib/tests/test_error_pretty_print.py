from bec_lib import messages
from bec_lib.utils.error_pretty_print import ErrorInfoPrettyPrinter


def test_error_info_pretty_printer_outputs_summary_and_details(capsys):
    error_info = messages.ErrorInfo(
        error_message='Traceback (most recent call last):\n  File "<stdin>", line 1, in <module>\nException: boom',
        compact_error_message="Short summary",
        exception_type="TestError",
        device="samx",
        context="during scan",
    )
    printer = ErrorInfoPrettyPrinter(error_info)

    printer.pretty_print()
    printer.print_details()

    captured = capsys.readouterr()

    assert "TestError" in captured.out
    assert "during scan" in captured.out
    assert "Device samx" in captured.out
    assert "Short summary" in captured.out
    assert "Error Occurred" in captured.out
    assert "Type: TestError" in captured.out
    assert "Context: during scan" in captured.out
    assert "Device: samx" in captured.out
    assert "Traceback (most recent call last):" in captured.out
