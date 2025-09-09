from time import sleep

from bec_server.scan_server.procedures.constants import PROCEDURE, ProcedureWorkerError
from bec_server.scan_server.procedures.container_utils import PodmanCliUtils

image_name = (
    f"ghcr.io/bec-project/{PROCEDURE.CONTAINER.REQUIREMENTS_IMAGE_NAME}:v{PROCEDURE.BEC_VERSION}"
)
podman = PodmanCliUtils()

for i in range(1, 4):
    try:
        output = podman._run_and_capture_error("podman", "pull", image_name)
        print("successfully pulled requirements image for current version")
        exit(0)
    except ProcedureWorkerError as e:
        print(e)
        print("retrying in 2 minutes...")
        sleep(120)
print(f"No more retries. Check if {image_name} actually exists!")
exit(1)
