import yaml
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
emr_release_label = "emr-5.28.0"
app_name = "spark"
app_version = "2.4.4"

class EMRConfig:
    """
    Module that parses a YAML formatted config file to be used as an input to create EMR cluster.

    Returns:
        Object -- Parameter value
    """

    def __init__(self, cluster_size: str, param_set_name: str, emr_config_path: str="emr.yaml"):
        self._emr_config = yaml.load(open(emr_config_path), Loader=yaml.SafeLoader).get("spec")
        self._cluster_size = cluster_size
        self._app_name = app_name
        self._app_version = app_version
        self._param_set_name = param_set_name
        self._cluster_size_config = self._emr_config["clusterSize"].get(self._param_set_name).get(self._cluster_size)
        self._emr_release_label = emr_release_label
        self._cluster_parameters = self._emr_config["clusterParamSet"].get(self._param_set_name)

    @property
    def  instance_type_master(self):
        return self._cluster_size_config.get("instance_type_master")

    @property
    def  instance_num_on_demand_master(self):
        return self._cluster_size_config.get("instance_num_on_demand_master")

    @property
    def  instance_num_spot_master(self):
        return self._cluster_size_config.get("instance_num_spot_master")

    @property
    def  instance_ebs_size_master(self):
        return self._cluster_size_config.get("instance_ebs_size_master")

    @property
    def  spot_bid_percentage_of_on_demand_master(self):
        return self._cluster_size_config.get("spot_bid_percentage_of_on_demand_master")

    @property
    def  instance_type_core(self):
        return self._cluster_size_config.get("instance_type_core")

    @property
    def  instance_num_on_demand_core(self):
        return self._cluster_size_config.get("instance_num_on_demand_core")

    @property
    def  instance_num_spot_core(self):
        return self._cluster_size_config.get("instance_num_spot_core")

    @property
    def  instance_ebs_size_core(self):
        return self._cluster_size_config.get("instance_ebs_size_core")

    @property
    def  spot_bid_percentage_of_on_demand_core(self):
        return self._cluster_size_config.get("spot_bid_percentage_of_on_demand_core")

    @property
    def  instance_type_task(self):
        return self._cluster_size_config.get("instance_type_task")

    @property
    def  instance_num_on_demand_task(self):
        return self._cluster_size_config.get("instance_num_on_demand_task")

    @property
    def  instance_num_spot_task(self):
        return self._cluster_size_config.get("instance_num_spot_task")

    @property
    def  instance_ebs_size_task(self):
        return self._cluster_size_config.get("instance_ebs_size_task")

    @property
    def  spot_bid_percentage_of_on_demand_task(self):
        return self._cluster_size_config.get("spot_bid_percentage_of_on_demand_task")

    @property
    def spot_provisioning_timeout_master(self):
        return self._cluster_size_config.get("spot_provisioning_timeout_master")

    @property
    def spot_provisioning_timeout_core(self):
        return self._cluster_size_config.get("spot_provisioning_timeout_core")

    @property
    def spot_provisioning_timeout_task(self):
        return self._cluster_size_config.get("spot_provisioning_timeout_task")

    @property
    def spot_timeout_to_on_demand_master(self):
        return self._cluster_size_config.get("spot_timeout_to_on_demand_master")

    @property
    def spot_timeout_to_on_demand_core(self):
        return self._cluster_size_config.get("spot_timeout_to_on_demand_core")

    @property
    def spot_timeout_to_on_demand_task(self):
        return self._cluster_size_config.get("spot_timeout_to_on_demand_task")

    @property
    def emr_release_label(self):
        return self._emr_release_label

    @property
    def logging_s3_path(self):
        return self._cluster_parameters.get("logging_s3_path")

    @property
    def subnet_id(self):
        return self._cluster_parameters.get("subnet_id")

    @property
    def emr_ec2_role(self):
        return self._cluster_parameters.get("emr_ec2_role")

    @property
    def emr_role(self):
        return self._cluster_parameters.get("emr_role")

    @property
    def spark_glue_catalog(self):
        return self._cluster_parameters.get("spark_glue_catalog")

    @property
    def hive_glue_catalog(self):
        return self._cluster_parameters.get("hive_glue_catalog")

    @property
    def presto_glue_catalog(self):
        return self._cluster_parameters.get("presto_glue_catalog")

    @property
    def debugging(self):
        return self._cluster_parameters.get("debugging")

    @property
    def applications(self):
        return self._cluster_parameters.get("applications")

    @property
    def visible_to_all_users(self):
        return self._cluster_parameters.get("visible_to_all_users")

    @property
    def key_pair_name(self):
        return self._cluster_parameters.get("key_pair_name")

    @property
    def security_group_master(self):
        return self._cluster_parameters.get("security_group_master")

    @property
    def security_groups_master_additional(self):
        return self._cluster_parameters.get("security_groups_master_additional")

    @property
    def security_group_slave(self):
        return self._cluster_parameters.get("security_group_slave")

    @property
    def security_groups_slave_additional(self):
        return self._cluster_parameters.get("security_groups_slave_additional")

    @property
    def security_group_service_access(self):
        return self._cluster_parameters.get("security_group_service_access")

    @property
    def spark_log_level(self):
        return self._cluster_parameters.get("spark_log_level")

    @property
    def spark_jars_path(self):
        return self._cluster_parameters.get("spark_jars_path")

    @property
    def spark_defaults(self):
        return self._cluster_parameters.get("spark_defaults")

    @property
    def maximize_resource_allocation(self):
        return self._cluster_parameters.get("maximize_resource_allocation")

    @property
    def keep_cluster_alive_when_no_steps(self):
        return self._cluster_parameters.get("keep_cluster_alive_when_no_steps")

    @property
    def termination_protected(self):
        return self._cluster_parameters.get("termination_protected")

    @property
    def tags(self):
        return self._cluster_parameters.get("tags")

    @property
    def python3(self):
        return self._cluster_parameters.get("python3")

    @property
    def bootstraps_paths(self):
        return self._cluster_parameters.get("bootstraps_paths")

    @property
    def ebs_root_volume_size(self):
        if self._cluster_parameters.get("ebs_root_volume_size") is None:
            return 15
        else:
            return self._cluster_parameters.get("ebs_root_volume_size")

    @property
    def num_concurrent_steps(self):
        if self._cluster_parameters.get("num_concurrent_steps") is None:
            return 5
        else:
            return self._cluster_parameters.get("num_concurrent_steps")


