from bec_ipython_client import BECIPythonClient
from bec_lib.logger import bec_logger

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.SUCCESS

# CONFIG_PATH = "../bec_config.yaml"


# config = ServiceConfig(CONFIG_PATH)

bec = BECIPythonClient()
# bec.initialize(config, RedisConnector)
bec.start()
bec.load_high_level_interface("spec_hli")

dev = bec.device_manager.devices
scans = bec.scans

# dev.samx.readback.read()

# bec._ip.prompts.status = 1


logger.success("Started BECClient")
scans.umv(dev.samx, 5, dev.samy, 20, relative=False)

# with scans.interactive_scan() as scan:
#     for ii in range(10):
#         scans.umv(dev.samx, ii, relative=False)
#         scan.trigger()
#         scan.read_monitored_devices()


# scans.line_scan(dev.samx, -5, 5, dev.samy, -5, 5, steps=10, exp_time=0.1, relative=True)

# scans.round_scan_fly(dev.flyer_sim, 0, 50, 20, 3, exp_time=0.1, relative=True)
# scans.monitor_scan(dev.samx, -100, 100, relative=False)

# from bec_ipython_client.plotting import GrumpyConnector

# bec.plotter = GrumpyConnector()
# bec.plotter.connect()


# def basic_plot(data, metadata):
#     plot_name = f"Scan {metadata['scan_number']}"
#     scan_motors = metadata.get("scan_report_devices")
#     if len(scan_motors) == 2:
#         x = data["data"][scan_motors[0]][scan_motors[0]]["value"]
#         y = data["data"][scan_motors[1]][scan_motors[1]]["value"]
#     elif len(scan_motors) == 1:
#         x = data["data"][scan_motors[0]][scan_motors[0]]["value"]
#         y = data["data"]["bpm4i"]["bpm4i"]["value"]
#     if bec.plotter.current_plot != plot_name:
#         bec.plotter.new_plot(plot_name, {})
#     bec.plotter.append_data([x, y])

# import time

# import numpy as np
# from matplotlib import pyplot as plt

# plt.ion()
# # fig = plt.figure("1")
# fig, ax = plt.subplots(figsize=(10, 8))
# line, = ax.plot([], [])
# # plt.plot([])
# x_data = []
# y_data = []

# def basic_plot(data, metadata):
#     scan_motors = metadata.get("scan_report_devices")
#     x = data["data"][scan_motors[0]][scan_motors[0]]["value"]
#     y = data["data"]["bpm4i"]["bpm4i"]["value"]
#     x_data.append(x)
#     y_data.append(y)
#     plt.plot(x_data, y_data)
#     fig.canvas.draw()
#     fig.canvas.flush_events()
#     time.sleep(0.01)

# from bec_ipython_client.plugins.XTreme.live_analysis import OTFLiveAnalysis

# cb = OTFLiveAnalysis()
# scans.line_scan(
#     dev.samy,
#     -5,
#     5,
#     steps=100,
#     exp_time=0.1,
#     relative=False,
#     callback=cb,
#     md={"polarization": "plus"},
# )
# scans.line_scan(
#     dev.samy,
#     5,
#     -5,
#     steps=100,
#     exp_time=0.1,
#     relative=False,
#     callback=cb,
#     md={"polarization": "plus"},
# )
# scans.line_scan(
#     dev.samy,
#     -5,
#     5,
#     steps=100,
#     exp_time=0.1,
#     relative=False,
#     callback=cb,
#     md={"polarization": "minus"},
# )
# scans.line_scan(
#     dev.samy,
#     5,
#     -5,
#     steps=100,
#     exp_time=0.1,
#     relative=False,
#     callback=cb,
#     md={"polarization": "minus"},
# )
# scans.line_scan(dev.samy, -5, 5, steps=100, exp_time=0.1, relative=False, callback=basic_plot)

# tomo_scan_sim()

# status = scans.fermat_scan(
#     dev.samx, -5, 5, dev.samy, -5, 5, step=1, exp_time=0.02, relative=False, hide_report=True
# )
# time.sleep(2)
# status.subscribe()

# dev.samx.low_limit = -20
# scans.round_scan_fly(dev.samx, dev.samy, 0, 50, 20, 3, exp_time=0.1, relative=True)
# def plotfunc():
#     dp = PlotAxis(bk.device_manager.connector)
#     dp.start()

# scans.umv(dev.samx, 10, relative=True)

# scans.mv(dev.samx, 20, dev.samy, -20)
# s = scans.line_scan(dev.samy, -5, 40, steps=10, exp_time=0.1)
# s = scans.round_roi_scan(dev.samx, 50, dev.samy, 20, dr=2, nth=3, exp_time=0.1)

# scan_def_id = str(uuid.uuid4())
# scans.open_interactive_scan(dev.samx, dev.samy, exp_time=0.1, md={"scan_def_id": scan_def_id})
# for ii in range(5):
#     scans.mv(dev.samx, ii, dev.samy, ii + 3, md={"scan_def_id": scan_def_id})
#     scans.interactive_scan_trigger(dev.samx, dev.samy, md={"scan_def_id": scan_def_id})
# scans.close_interactive_scan(md={"scan_def_id": scan_def_id})


# @scan_def
# def new_scan():
#     for ii in range(10):
#         scans.umv(dev.samx, ii * 10)
#         scans.fermat_scan(dev.samx, -5, 5, dev.samy, -5, 5, step=1, exp_time=0.02, relative=True)


# for ii in range(10):
#     scans.umv(dev.samx, ii * 10)
#     # scans.grid_scan(dev.samx, -5, 5, 5, dev.samy, -5, 5, 10, exp_time=0.02, relative=True)
#     scans.fermat_scan(dev.samx, -5, 5, dev.samy, -5, 5, step=1, exp_time=0.02, relative=True)

# scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02)
# s = scans.grid_scan(dev.samx, -5, 5, 100, dev.samy, -5, 5, 100, exp_time=0.0, hide_report=True)
# scans.umv(dev.samx, 0)
# scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02)


# @scans.scan_group
# def scan_with_decorator():
#     scans.umv(dev.samx, 5, relative=False)
#     scans.line_scan(dev.samx, -5, 5, steps=100, exp_time=0.1, relative=True)
#     scans.umv(dev.samx, 5, relative=False)
#     scans.line_scan(dev.samx, -5, 5, steps=100, exp_time=0.1, relative=True)
#     # scans.line_scan(dev.samx, -8, 8, steps=200, exp_time=0.1, relative=True)


# with scans.scan_def:
#     scan_with_decorator()
# with scans.dataset_id_on_hold:
#     scans.line_scan(dev.samx, -5, 5, steps=10, exp_time=0.1, relative=True)
#     scans.line_scan(dev.samx, -8, 8, steps=10, exp_time=0.1, relative=True)

# scan_def_id = str(uuid.uuid4())
# scans.open_scan_def(md={"scan_def_id": scan_def_id})
# scans.line_scan(dev.samx, -5, 5, steps=10, exp_time=0.1, md={"scan_def_id": scan_def_id})
# scans.line_scan(dev.samx, -8, 8, steps=10, exp_time=0.1, md={"scan_def_id": scan_def_id})
# scans.close_scan_def(md={"scan_def_id": scan_def_id})
# for ii in range(10):
#     scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.01)

# res = dev.samx.read()
# dev.samx.summary()
# res = dev.samx.read(cached=True, use_readback=True).get("value")
# scans.umv(dev.samx, 500)
# print(dev.samx.read(cached=True, use_readback=True))
# scans.umv(dev.samx, 1000)


# @scans.scan_group
# def alignment(*args, **kwargs):
#     scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02)
#     scans.umv(dev.samx, 10)
#     scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02)
#     scans.umv(dev.samx, 10)
#     scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02)


# with scans.scan_group:
#     scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02, relative=True)
#     scans.umv(dev.samx, 10, relative=True)

# alignment()


# scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02, md={"queue_group": queue_group})
# scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.02, md={"queue_group": queue_group})

# scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=1)

# event = threading.Event()
# event.wait()
print("eos")
bec.shutdown()
# p.join()
