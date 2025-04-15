import sys

import numpy as np
from bec_widgets.utils.colors import set_theme
from bec_widgets.widgets.plots.waveform.waveform import Waveform
from qtpy.QtCore import Qt
from qtpy.QtWidgets import QApplication, QHBoxLayout, QLabel, QSlider, QVBoxLayout, QWidget

from bec_server.data_processing.dap_framework_refactoring.dap_blocks import (
    DAPBlockMessage,
    DAPSchema,
    GradientBlock,
    SmoothBlock,
)
from bec_server.data_processing.dap_framework_refactoring.dap_worker import DAPWorker


def _create_gaussian(size: int, std: float = 2) -> np.ndarray:
    # Parameters
    x = np.linspace(-10, 10, size)
    mean = 0
    noise_level = 0.1

    # Gaussian
    gaussian = 20 * np.exp(-((x - mean) ** 2) / (2 * std**2))

    # Add noise
    return x, gaussian + np.random.normal(0, noise_level, size=x.shape)


if __name__ == "__main__":

    # Initial pipeline definition
    wf = [{"block": SmoothBlock, "kwargs": {"sigma": 20}}, {"block": GradientBlock, "kwargs": {}}]
    x_data, y_data = _create_gaussian(8000, 2)
    initial_msg = DAPBlockMessage(
        data=y_data,
        data_x=x_data,
        schema=DAPSchema(ndim=1, max_shape=(1,), async_update="replace", x_axis=True),
    )

    app = QApplication(sys.argv)
    set_theme("dark")

    widget = QWidget()
    layout = QVBoxLayout(widget)

    waveform = Waveform()
    waveform.bec_dispatcher.client._reset_singleton()
    layout.addWidget(waveform)
    waveform.setWindowTitle("Waveform Demo")
    waveform.resize(1000, 800)
    worker = DAPWorker()

    def update_plot(sigma_value: int):
        worker.clear()
        wf[0]["kwargs"]["sigma"] = sigma_value
        label.setText(f"Sigma: {sigma_value}")
        worker.add_blocks(wf)
        result = worker.run(initial_msg)
        waveform.clear_all()
        waveform.plot(x=x_data, y=y_data, symbol=None)
        waveform.plot(x=result.data_x, y=result.data, symbol=None)

    slider = QSlider(Qt.Horizontal)
    slider_layout = QVBoxLayout()
    slider.setMinimum(1)
    slider.setMaximum(100)
    slider.setValue(50)
    label = QLabel("Sigma: 80")
    slider.setTickInterval(1)
    slider.setTickPosition(QSlider.TicksRight)
    slider.valueChanged.connect(update_plot)
    slider_layout.addWidget(label)
    slider_layout.addWidget(slider)
    layout.addLayout(slider_layout)

    widget.setLayout(layout)
    widget.show()
    widget.resize(1000, 800)

    update_plot(slider.value())  # Initial plot

    sys.exit(app.exec_())
