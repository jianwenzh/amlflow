
import argparse
from dataclasses import dataclass
from datetime import datetime
import logging
import os
from pathlib import Path
import sys
from typing import Dict, List, Union

from hydra import compose, initialize

from azure.ai.ml.entities import CommandComponent, ParallelComponent, PipelineComponent
from azure.ai.ml import Input, MLClient, load_component
from azure.ai.ml.dsl import pipeline

from logging import Logger

from azure.ai.ml import Output

sys.path.append(r'.')

from common import (
    AMLComponentConfig, 
    AmlRegistryConfig, 
    AmlWorkspaceConfig, 
    FlowStepConfig, 
    DatasetArgs,
    create_registry_ml_client, 
    create_workspace_ml_client_ws, 
    load_or_create_data_asset_, 
    valid_node_name,
    color_logger
)


@dataclass
class ResourceConfigs:
    workspace: AmlWorkspaceConfig
    registries: List[AmlRegistryConfig]
    cpu_target: str
    components_config: List[AMLComponentConfig]
    
@dataclass
class PipelineConfigs:
    experiment: str
    output_path: str
    output_mode: str
    input_data: DatasetArgs
    flow_steps_config: List[FlowStepConfig]
    resource_config: ResourceConfigs


@dataclass
class AmlResources(ResourceConfigs):
    ml_client_ws: MLClient
    ml_client_registries: Dict[str, MLClient] # registry_name -> MLClients
    components: Dict[str, Union[CommandComponent, ParallelComponent, PipelineComponent]] # component_name -> component
    

def _create_aml_resources(resource_config: ResourceConfigs) -> AmlResources:
    ml_client_ws = create_workspace_ml_client_ws(resource_config.workspace)
    registries = resource_config.get('registries', None)
    ml_client_registries = {registry.registry_name: create_registry_ml_client(registry) for registry in registries} if registries else None
    components = {}
    for component_config in resource_config.components_config:
        mlclient = ml_client_registries[component_config.registry_name] if component_config.get('source', '') == 'registry' else ml_client_ws
        components[component_config.component_name] = load_component(
            client=mlclient, 
            name=component_config.component_name, 
            version=component_config.version
            )
        
    return AmlResources(
        workspace=resource_config.workspace,
        registries=registries,
        cpu_target=resource_config.cpu_target,
        components_config=resource_config.components_config,
        ml_client_ws=ml_client_ws,
        ml_client_registries=ml_client_registries,
        components=components,
    )


def create_pipeline(
    name: str,
    step_name_prefix: str,
    pipeline_configs: PipelineConfigs,
    aml_resources: AmlResources,
):
    step_name_prefix = step_name_prefix or ''
    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline(input_data_asset: Input):
        step_input = input_data_asset
        step_idx = 0
        for step in pipeline_configs.flow_steps_config:
            step_component = aml_resources.components[step.component_name]
            step_output = step_component(
                input_dir=step_input,
                **step.kwargs
            )
            valid_name = valid_node_name(f"s-{step_name_prefix or ''}-{step_idx}-{step.component_name}")
            step_output.name = valid_name
            step_input = step_output.outputs.output_dir
            step_idx += 1

        return {"output_dir": step_output.outputs.output_dir}
    
    return _pipeline


def run(
        timestamp: str,
        job_name: str,
        pipeline_cfg: PipelineConfigs,
        logger: logging.Logger,
        output_subfolder: str,
        experiment_name: str = None,
        ):
    orig_loglevel = logger.level
    logger.setLevel(logging.WARN) # AML submit dump out too many details, explicit set WARN to avoid

    aml_resources = _create_aml_resources(pipeline_cfg.resource_config)
    pipeline_fn = create_pipeline(
        name=job_name,
        step_name_prefix=timestamp,
        pipeline_configs=pipeline_cfg,
        aml_resources=aml_resources
    )

    input_data_asset = load_or_create_data_asset_(
        ml_client=aml_resources.ml_client_ws,
        data_asset_config=pipeline_cfg.input_data,
        logger=logger
    )

    pipeline_job = pipeline_fn(input_data_asset=input_data_asset)
    
    # recording the cfgs in job properties
    pipeline_job.properties["pipeline_config"] = pipeline_cfg

    if 'output_path' in pipeline_cfg and pipeline_cfg.output_path:
        pipeline_output_path = pipeline_cfg.output_path.rstrip("/") + "/" + output_subfolder.rstrip("/")
        output_mode = 'upload'
        if 'output_mode' in pipeline_cfg and pipeline_cfg.output_mode:
            output_mode = pipeline_cfg.output_mode

        for k in pipeline_job.outputs:

            pipeline_job.outputs[k] = Output(
                type="uri_folder", mode=output_mode, path=f"{pipeline_output_path}/{timestamp}/{k}/" + r"${{name}}" )

    pipeline_run = aml_resources.ml_client_ws.jobs.create_or_update(
        pipeline_job,
        experiment_name=experiment_name if experiment_name else job_name,
    )

    logger.setLevel(orig_loglevel)
    logger.info(f"\n\nJob name: \n{job_name}")
    logger.info(f"\n\nWorkspace Job link: \n{pipeline_run.studio_url}")


def main(
    experiment_name: str=None,
    job_name_prefix: str=None, # add a customized job name prefix to easily identify your job
    config_file: str=None,
    logger: Logger = None,
):
    config_file_path = Path(os.path.abspath(config_file))
    config_dir = str(config_file_path.parent)
    current_file_dir = str(Path(os.path.abspath(__file__)).parent)
    config_dir = os.path.relpath(path=config_dir, start=current_file_dir)
    config_name = config_file_path.stem

    print(f"config_file_path={config_file_path}")
    print(f"config_dir={config_dir}")
    print(f"config_name={config_name}")

    initialize(version_base=None, config_path=config_dir) 
    pipeline_cfg = compose(config_name=config_name)
    
    if 'experiment' in pipeline_cfg:
        experiment_name = experiment_name or pipeline_cfg.experiment

    experiment_name = experiment_name or config_name

    job_name_prefix = job_name_prefix if job_name_prefix is not None else ""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    job_name = f"[UF][{job_name_prefix}][{timestamp}]"
    
    logger.info(f"job_name={job_name}")

    run(timestamp=timestamp, experiment_name=experiment_name, job_name=job_name, pipeline_cfg=pipeline_cfg, logger=logger, output_subfolder=config_name)


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    color_logger(logger=logger)
    
    parser = argparse.ArgumentParser(description=f'Uniflow pipeline run', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-e', '--experiment_name', '--expname', type=str, default=None, help='experiment name')
    parser.add_argument('-n', '--jobname_prefix', '--jobname', type=str, default=None, help='jobname prefix')
    parser.add_argument('-f', '--config_file', '--config', type=str, default=None, help='config_file full/relative path')
    args = parser.parse_args()

    logger.info(f"--experiment_name  ={args.experiment_name}")
    logger.info(f"--jobname_prefix   ={args.jobname_prefix}")
    logger.info(f"--config_file      ={args.config_file}")
    
    config_file_path = Path(args.config_file)
    config_name = config_file_path.stem
    
    main(
        experiment_name=args.experiment_name,
        job_name_prefix=args.jobname_prefix or config_name,
        config_file=args.config_file,
        logger=logger
    )
