import os
import json
import boto3
import logging
import importlib
from botocore.config import Config
from aws_beamline_devtools.emr_client import EMR
from aws_beamline_devtools.emr_config import EMRConfig

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

compute_engine = "Spark"


class ComputeManager():
    """
    Creates EMR clusters based on a YAML formatted config file 
    
    Returns:
        Dictionary -- Response to run_job_flow API
    """
    def __init__(self, cluster_size: str, param_set_name: str, emr_config_path: str):
        """
        Creates a new compute based on size, parameter set and configuration path provided.

        Arguments:
            cluster_size {str} -- Size of the cluster to be created
            param_set_name {str} -- Parameters set name
            emr_config_path {str} -- Path where the emr configuration file is stored.
        """
        self._cluster_size = cluster_size
        self._param_set_name = param_set_name
        self._emr_config_path = emr_config_path
        self.compute_client = EMR()
        self.compute_config = EMRConfig(cluster_size=self._cluster_size,
                                    param_set_name=self._param_set_name,
                                    emr_config_path=self._emr_config_path
                                )

    def start_compute(self):
        logging.info("Creating a new EMR cluster: cluster_size = {}, parameter_set_name = {}".format(self._cluster_size, self._param_set_name ))
        response = self.compute_client.create_cluster(
            cluster_name = compute_engine+"-"+self._param_set_name+"-Size-"+self._cluster_size,
            logging_s3_path = self.compute_config.logging_s3_path,
            emr_release = self.compute_config.emr_release_label,
            subnet_id = self.compute_config.subnet_id,
            emr_ec2_role = self.compute_config.emr_ec2_role,
            emr_role=self.compute_config.emr_role,
            num_concurrent_steps = self.compute_config.num_concurrent_steps,
            ebs_root_volume_size = self.compute_config.ebs_root_volume_size,
            instance_type_master = self.compute_config.instance_type_master,
            instance_type_core = self.compute_config.instance_type_core,
            instance_type_task = self.compute_config.instance_type_task,
            instance_ebs_size_master = self.compute_config.instance_ebs_size_master,
            instance_ebs_size_core = self.compute_config.instance_ebs_size_core,
            instance_num_on_demand_master = self.compute_config.instance_num_on_demand_master,
            instance_ebs_size_task = self.compute_config.instance_ebs_size_task,
            instance_num_on_demand_core = self.compute_config.instance_num_on_demand_core,
            instance_num_on_demand_task = self.compute_config.instance_num_on_demand_task,
            instance_num_spot_master = self.compute_config.instance_num_spot_master,
            instance_num_spot_core = self.compute_config.instance_num_spot_core,
            instance_num_spot_task = self.compute_config.instance_num_spot_task,
            spot_bid_percentage_of_on_demand_master  = self.compute_config.spot_bid_percentage_of_on_demand_master,
            spot_bid_percentage_of_on_demand_core = self.compute_config.spot_bid_percentage_of_on_demand_core,
            spot_bid_percentage_of_on_demand_task = self.compute_config.spot_bid_percentage_of_on_demand_task,
            spot_provisioning_timeout_master = self.compute_config.spot_provisioning_timeout_master,
            spot_provisioning_timeout_core= self.compute_config.spot_provisioning_timeout_core,
            spot_provisioning_timeout_task= self.compute_config.spot_provisioning_timeout_task,
            spot_timeout_to_on_demand_master= self.compute_config.spot_timeout_to_on_demand_master,
            spot_timeout_to_on_demand_core= self.compute_config.spot_timeout_to_on_demand_core,
            spot_timeout_to_on_demand_task= self.compute_config.spot_timeout_to_on_demand_task,
            python3= self.compute_config.python3,
            spark_glue_catalog= self.compute_config.spark_glue_catalog,
            hive_glue_catalog= self.compute_config.hive_glue_catalog,
            presto_glue_catalog= self.compute_config.presto_glue_catalog,
            bootstraps_paths= self.compute_config.bootstraps_paths,
            debugging= self.compute_config.debugging,
            applications= self.compute_config.applications,
            visible_to_all_users= self.compute_config.visible_to_all_users,
            key_pair_name= self.compute_config.key_pair_name,
            security_group_master= self.compute_config.security_group_master,
            security_groups_master_additional= self.compute_config.security_groups_master_additional,
            security_group_slave=self.compute_config.security_group_slave,
            security_groups_slave_additional= self.compute_config.security_groups_slave_additional,
            security_group_service_access= self.compute_config.security_group_service_access,
            spark_log_level = "INFO",
            spark_jars_path = self.compute_config.spark_jars_path,
            spark_defaults = self.compute_config.spark_defaults,
            maximize_resource_allocation = self.compute_config.maximize_resource_allocation,
            steps= None,
            keep_cluster_alive_when_no_steps= self.compute_config.keep_cluster_alive_when_no_steps,
            termination_protected= self.compute_config.termination_protected,
            tags= self.compute_config.tags
        )
        logging.info(f"response: \n{json.dumps(response, default=str, indent=4)}")
        return (response)
