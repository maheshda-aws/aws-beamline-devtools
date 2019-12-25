#!/usr/bin/env python
import logging
import boto3
import time
from optparse import OptionParser
from aws_beamline_devtools.emr_client import EMR
from aws_beamline_devtools.emr_config import EMRConfig
from aws_beamline_devtools.compute_manager import ComputeManager
from aws_beamline_devtools.create_sparkmagic_config import CreateSparkMagicConfig

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

def main():
    """
    This comand line utility attaches a SageMaker Notebook to either an existing EMR cluster or 
    attaches the notebook to an EMR cluster after creating it. 
       Arguments:
        --emrClusterId, -e: The EMR cluster JobFlowID if the cluster is already running. It ignores all other parameters.(Required for connecting to pre-existing cluster.)
        --clusterSize, -s : T-shirt size of the cluster configured in config file. See emr.yaml.(Required for creating new cluster)
        --configFile, -e  : Externally provided YAML config file to set up EMR cluster. Default value: emr.yaml (Optional for creating new cluster)
        --paraSetName, -p : Parameter set name defined in the YAML file. Default value: default (Optional for creating new cluster)
    """
    parser = OptionParser(usage="usage: %prog [options] filename",
                          version="%prog 1.0")
    parser.add_option("-e", "--emrClusterId",
                      dest="cluster_id",
                      default="UNKNOWN",
                      help="Already existing EMR cluster to attach. When not provided it creates a new cluster.")
    parser.add_option("-s", "--clusterSize",
                      dest="cluster_size",
                      default="UNKNOWN",
                      help="Size of the cluster if needs to be created. See: emr.yaml")
    parser.add_option("-c", "--configFile",
                      dest="config_file",
                      default="emr.yaml",
                      help="Config file provided externally, Default: emr.yaml")
    parser.add_option("-p", "--paramSetName",
                      dest="param_set_name",
                      default="default",
                      help="Parameter set name, See: emr.yaml")

    (options, args) = parser.parse_args()

    logging.info("Options provided: {}".format(options))
    logging.info("Arguments provided: {}".format(args))


    if not options.cluster_id == "UNKNOWN":
        logging.info("Cluster id (--cluster_id) input is provided. Ignoring options --clusterSize, --configFile and --paramSetName")
        logging.info("Attaching Jupyter notebook to cluster id: {}".format(options.cluster_id))
        emr = EMR()
        sparkmagic = CreateSparkMagicConfig()
        master_private_ip = emr.get_cluster_instances(options.cluster_id).get("Instances")[0].get("PrivateIpAddress")
        response = sparkmagic.generate_config(master_private_ip)
        if response:
            logging.info("Connection set up completed. Please test connectivity using shell command:`curl {}:8998/sessions`".format(master_private_ip))

    elif not options.cluster_size == "UNKNOWN":
        logging.info("Parameters: Cluster Size={}, Param set name={}, Config_file={}".format(options.cluster_size, options.param_set_name, options.config_file))
        compute_manager = ComputeManager(cluster_size=options.cluster_size, param_set_name=options.param_set_name, emr_config_path=options.config_file)
        emr = EMR()
        sparkmagic = CreateSparkMagicConfig()
        logging.info("Config file at path: {} shall be used.".format(options.config_file))
        cluster_id = compute_manager.start_compute().get("JobFlowId")
        logging.info("Cluster Id: {}".format(cluster_id))
        cluster_state = emr.get_cluster_state(cluster_id)
        while cluster_state not in ["WAITING"]:
            logging.info("EMR Cluster is not ready yet. Current state={}. WIll check back in 15 secs.".format(cluster_state))
            time.sleep(15)
            cluster_state = emr.get_cluster_state(cluster_id)
        master_private_ip = emr.get_cluster_instances(cluster_id).get("Instances")[0].get("PrivateIpAddress")
        response = sparkmagic.generate_config(master_private_ip)
        if response:
            logging.info("Connection set up completed. Please test connectivity using shell command:`curl {}:8998/sessions`".format(master_private_ip))
    else:
        parser.error("Either provide a valid --emrClusterId or --clusterSize parameter.")

if __name__ == "__main__":
    main()