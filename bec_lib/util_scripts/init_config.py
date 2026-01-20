import argparse

import yaml

from bec_messages import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "--config", default="./bec_lib/bec_lib/configs/demo_config.yaml", help="path to the config file"
)
parser.add_argument("--redis", default="localhost:6379", help="redis host and port")

clargs = parser.parse_args()
connector = RedisConnector(clargs.redis)

with open(clargs.config, "r", encoding="utf-8") as stream:
    data = yaml.safe_load(stream)
for name, device in data.items():
    device["name"] = name
config_data = list(data.values())
msg = messages.AvailableResourceMessage(resource=config_data)
connector.set(MessageEndpoints.device_config(), msg)
