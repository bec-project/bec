from time import sleep

from bec_server.procedures.constants import PROCEDURE, ProcedureWorkerError
from bec_server.procedures.container_utils import _run_and_capture_error

image_name = (
    f"ghcr.io/bec-project/{PROCEDURE.CONTAINER.REQUIREMENTS_IMAGE_NAME}:v{PROCEDURE.BEC_VERSION}"
)

for i in range(1, 6):
    try:
        output = _run_and_capture_error("podman", "pull", image_name)
        print("successfully pulled requirements image for current version")
        exit(0)
    except ProcedureWorkerError as e:
        print(e)
        print("retrying in 5 minutes...")
        sleep(5 * 60)
print(f"No more retries. Check if {image_name} actually exists!")
exit(1)
