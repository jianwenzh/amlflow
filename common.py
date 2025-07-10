from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Optional, Union

from logging import Logger
import re
from typing import Any, Dict
from azure.ai.ml import MLClient, PyTorchDistribution
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.entities import Data, JobResourceConfiguration
from azure.ai.ml.entities import CommandComponent, ParallelComponent, PipelineComponent
from azure.identity import DefaultAzureCredential
from azure.ai.ml.dsl import pipeline

# region: common args for aml pipelines #########################
@dataclass
class FlowStepConfig:
    step_name: str
    component_name: str
    kwargs: Dict[str, Any]
    no_input: Optional[bool]=False

    # resource kwargs used to fill JobResourceConfiguration, e.g., shm_size, instance_count, etc.
    # https://learn.microsoft.com/en-us/python/api/azure-ai-ml/azure.ai.ml.entities.jobresourceconfiguration?view=azure-python
    resources_kwargs: Optional[Dict[str, Any]]=None

    # PyTorch, Mpi, TensorFlow, Ray
    distribution: Optional[str]=None
    # distribution_kwargs used to fill distribution configs
    # e.g., for PyTorchDistribution: process_count_per_instance
    distribution_kwargs: Optional[Dict[str, Any]]=None


@dataclass
class DAGFlowStepInputConfig:
    sink_input_name: Optional[str] # name of the input of this component to recive input, if None, default "input_dir"
    source_name: Optional[str] # name of the source input data asset or another step name, if None, default last step
    source_output_name: Optional[str] # name of the output name if the source is another step
    no_input: Optional[bool]=False # if True, no input data asset is required, i.e., the component does not have input data asset, e.g., download hf data/model component that does not require input data asset, but only parameters

@dataclass
class DAGFlowStepConfig:
    component_name: str

    # if None, =component_name, but make sure 
    # (1) it is unique among the job; 
    # (2) valid node name in AML [a-z0-9_], and starting with [a-z]
    step_name: Optional[str]=None 
    kwargs: Optional[Dict[str, Any]]=None

    # default None means: 
    # If the flowstep is the 1st step: the 1st input_asset
    # Else: take last step, i.e., fall back and comptiable to the uniflow mode
    inputs: Optional[List[DAGFlowStepInputConfig]]=None 
    compute: Optional[str]=None
    no_input: Optional[bool]=False # if True, no input data asset is required, i.e., the component does not have input data asset, e.g., a model training component that does not require input data asset, but only parameters
    
    # resource kwargs used to fill JobResourceConfiguration, e.g., shm_size, instance_count, etc.
    # https://learn.microsoft.com/en-us/python/api/azure-ai-ml/azure.ai.ml.entities.jobresourceconfiguration?view=azure-python
    resources_kwargs: Optional[Dict[str, Any]]=None

    # PyTorch, Mpi, TensorFlow, Ray
    distribution: Optional[str]=None
    # distribution_kwargs used to fill distribution configs
    # e.g., for PyTorchDistribution: process_count_per_instance
    distribution_kwargs: Optional[Dict[str, Any]]=None

    # parallel component config
    n_splits: Optional[int]=None # if not None, the step will be parallelized with n_splits, and the component should be a CommandComponent or ParallelComponent
    input_name_to_split: Optional[str]=None # if not None, the input to be split into n_splits, e.g., "input_dir", "input_data", etc.
    spliter_component_name: Optional[str]=None # the component to split the input into n_splits, e.g., "spliter_component"
    spliter_kwargs: Optional[Dict[str, Any]]=None # the kwargs to pass to the spliter component, e.g., {"glob_patterns": "*.jpg|*.png"} for file globbing
    spliter_compute: Optional[str]=None # the compute to run the spliter component, e.g., "cpu-cluster", "gpu-cluster", etc.
    merger_component_name: Optional[str]=None # the component to merge the outputs of the worker component, e.g., "merger_component"
    merger_kwargs: Optional[Dict[str, Any]]=None # the kwargs to pass to the merger component, e.g., {"output_dir": "/path/to/output"}
    merger_compute: Optional[str]=None # the compute to run the merger component, e.g., "cpu-cluster", "gpu-cluster", etc.


@dataclass
class AMLComponentConfig:
    component_name: str
    
    source: Optional[str]="workspace" # workspace | registry

    # from registry
    registry_name: Optional[str]=None
    
    version: Optional[str]=None
    
    kwargs_default: Optional[Dict[str, Any]]=None

# common args
@dataclass
class DatasetArgs:
    name: str = str()
    version: str = str()
    create_from_path: Optional[str] = None # an url path from local, datastore, azure storage, etc. see https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-data-assets?view=azureml-api-2&tabs=cli#create-data-assets


@dataclass
class AmlWorkspaceConfig:
    workspace_name: str = str()
    resource_group: str = str()
    subscription_id: str = str()


@dataclass
class AmlRegistryConfig:
    registry_name: str = str()
    registry_location: Optional[str] = None

# endregion: common args for aml pipelines #########################


# region: aml utils ################################################


def create_workspace_ml_clients(workspace_subscription_id, workspace_resource_group, workspace_name):
    return MLClient(
        credential=DefaultAzureCredential(),         
        subscription_id=workspace_subscription_id,
        resource_group_name=workspace_resource_group,
        workspace_name=workspace_name,
    )


def create_workspace_ml_client_ws(config: AmlWorkspaceConfig):
    return create_workspace_ml_clients(
        workspace_subscription_id=config.subscription_id,
        workspace_resource_group=config.resource_group,
        workspace_name=config.workspace_name,
    )


def create_registry_ml_client(config: AmlRegistryConfig):
    return MLClient(
        credential=DefaultAzureCredential(),  
        registry_name=config.registry_name,
        registry_location=config.get('registry_location', None)  # Optional, if not provided, will use the default location of the registry
    )


def load_or_create_data_asset(ml_client: MLClient, name: str, version: str, create_from_path: str, logger: Logger):
    try:
        ds = ml_client.data.get(name, version=version)
        logger.info(f"Existing data asset found for {name}:{version}")
        if create_from_path is not None:
            logger.warning(f"[!WARNING!] Existing data asset found for {name}:{version}. model_ckpt.create_from_path is not None but ignored! Please update version number if want to overwrite!")
    except:    
        logger.info(f"Creating data asset from given path: {create_from_path}")
        ds = Data(
            path=create_from_path,
            type=AssetTypes.URI_FOLDER,
            name=name,
            version=version,
        )
        
        ml_client.data.create_or_update(ds)
        ds = ml_client.data.get(name, version=version)

    return ds

def load_or_create_data_asset_(ml_client: MLClient, data_asset_config: DatasetArgs, logger: Logger):
    return load_or_create_data_asset(
        ml_client=ml_client,
        name=data_asset_config.name,
        version=data_asset_config.version,
        create_from_path=data_asset_config.create_from_path,
        logger=logger
    )


def valid_node_name(s):
    return re.sub(r"\W+", "_", s)


def override_kwargs(kwargs_default: Dict[str,Any], kwargs_override: Dict[str,Any]) -> Dict[str,Any]:
    if kwargs_default is None or kwargs_override is None:
        return {**(kwargs_default or kwargs_override or {})}
    
    kwargs = {**kwargs_default}
    for k, v in kwargs_override.items():
        kwargs[k] = v

    return kwargs

# endregion: aml utils ################################################

# region: common utils ################################################

class ColorFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning / errors"""

    grey   = "\x1b[90m"
    green  = "\x1b[92m"
    yellow = "\x1b[93m"
    red    = "\x1b[91m"
    reset  = "\x1b[0m"
    format = "[%(asctime)s | %(levelname)-5.5s] %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: red + format + reset
    }

    def format(self, record):
        record.levelname = 'WARN' if record.levelname == 'WARNING' else record.levelname
        record.levelname = 'ERROR' if record.levelname == 'CRITICAL' else record.levelname
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def color_logger(logger: logging.Logger):
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(ColorFormatter())
    logger.addHandler(ch)
    return logger


# endregion: common utils ################################################

# region: aml flow utils ################################################

def set_step_ext_configs(step_config, step_func):
    """
    Set the step's resource and distribution configs if provided.
    """
    if step_config.get("compute", None):
        step_func.compute = step_config.compute

    if step_config.get("resources_kwargs", None):
        step_func.resources = JobResourceConfiguration(**step_config.resources_kwargs)

    if step_config.get("distribution", None):
        if step_config.distribution.lower() == "pytorch":
            step_func.distribution = PyTorchDistribution(**step_config.get("distribution_kwargs", {}))
        else:
            raise ValueError(f"Unsupported distribution type: {step_config.distribution}. Only 'PyTorch' is supported.")

    return step_func

# endregion: aml flow utils ################################################


# region: parallel pipeline component utils ################################################
def create_parallel_pipeline_component(
    # name: str,
    worker_component: Union[CommandComponent, ParallelComponent, PipelineComponent],
    worker_component_kwargs: Dict[str, Any],
    worker_compute: str,
    name_of_input_to_split: str,
    n_splits: int,
    spliter_component: Union[CommandComponent, ParallelComponent, PipelineComponent],
    spliter_kwargs: Dict[str, Any],
    spliter_compute: str,
    merger_component: Union[CommandComponent, ParallelComponent, PipelineComponent],
    merger_kwargs: Dict[str, Any],
    merger_compute: str,
):
    """
    Create a pipeline component to parallelize the execution of a given component. Three steps:
    1. Split the input data into `n_splits` parts.
    2. Execute the component in parallel on each split.
    3. Merge the outputs of the component into a single output.

    Args:
        name (str): The name of the pipeline component.
        worker_component (Union[CommandComponent, ParallelComponent, PipelineComponent]): The component to be executed in parallel.
        worker_component_kwargs (Dict[str, Any]): The keyword arguments for the worker component.
        worker_compute (str): The compute target for the worker component.
        name_of_input_to_split (str): The name of the input to be split into multiple parts.
        n_splits (int): The number of splits to create.
        spliter_component (Union[CommandComponent, ParallelComponent, PipelineComponent]): The component used to split the input data.
        spliter_kwargs (Dict[str, Any]): The keyword arguments for the spliter component.
        spliter_compute (str): The compute target for the spliter component.
        merger_component (Union[CommandComponent, ParallelComponent, PipelineComponent]): The component used to merge the outputs.
        merger_kwargs (Dict[str, Any]): The keyword arguments for the merger component.
        merger_compute (str): The compute target for the merger component.
    """
    spliter_kwargs = {**spliter_kwargs, 'n_splits': n_splits}  # Ensure spliter_kwargs is a copy to avoid modifying the original
    
    # @pipeline(name=name)
    def pipeline_comp_func(
    ):
        spliter_func = spliter_component(
            input_dir=worker_component_kwargs[name_of_input_to_split], 
            **spliter_kwargs)
        spliter_func.compute = spliter_compute

        merger_inputs = {}
        for i in range(n_splits):
            split_output = spliter_func.outputs[f"output_dir_{i}"]
            worker_component_kwargs[name_of_input_to_split] = split_output
            worker_func = worker_component(**worker_component_kwargs)
            worker_func.compute = worker_compute
            merger_inputs[f"input_dir_{i}"] = worker_func.outputs[f"output_dir"]

        if merger_kwargs:
            merger_inputs = {**merger_inputs, **merger_kwargs}

        merger_func = merger_component(**merger_inputs)
        merger_func.compute = merger_compute
        return merger_func

    return pipeline_comp_func
# endregion: parallel pipeline component utils ################################################