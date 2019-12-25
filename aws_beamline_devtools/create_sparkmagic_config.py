import re
import json
import requests
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

url = "https://raw.githubusercontent.com/jupyter-incubator/sparkmagic/master/sparkmagic/example_config.json"

class CreateSparkMagicConfig():
    """
    Create sparkmagic config file from the private IP of an EMR cluster

    Returns:
        Boolean -- Config file created?
    """
    def __init__(self):
        self.config_template = requests.get(url).text

    def generate_config(self, master_private_ip: str):
        logging.info("Generating sparkmagic configuration")
        config_file = open("/home/ec2-user/.sparkmagic/config.json", "w+")
        config_file.write(re.sub("localhost", master_private_ip, self.config_template))
        config_file.close()
        return True
