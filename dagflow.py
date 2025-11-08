
import argparse
from dataclasses import dataclass
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Union

from hydra import compose, initialize

from azure.ai.ml.entities import CommandComponent, ParallelComponent, PipelineComponent, Data
from azure.ai.ml import Input, MLClient, load_component
from azure.ai.ml.dsl import pipeline

from logging import Logger

from azure.ai.ml import Output

from common import (
    AMLComponentConfig, 
    AmlRegistryConfig, 
    AmlWorkspaceConfig, 
    DAGFlowStepConfig, 
    DatasetArgs,
    create_parallel_pipeline_component,
    create_registry_ml_client, 
    create_workspace_ml_client_ws, 
    load_or_create_data_asset_, 
    valid_node_name,
    color_logger,
    set_step_ext_configs
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
    share_output: bool # Careful! All outputs will be shared to the same path, so make sure no conflict. Only used in the case when parallel jobs running and writting to the same output root path.
    input_data_assets: List[DatasetArgs]
    flow_steps_config: List[DAGFlowStepConfig]
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


def _create_data_assets(mlclient: MLClient, data_assets_config: List[DatasetArgs], logger: Logger) ->List[Data]:
    ret = []
    for cfg in data_assets_config:
        ds = load_or_create_data_asset_(ml_client=mlclient, data_asset_config=cfg, logger=logger)
        ret.append(ds)

    return ret

def _get_step_output_name(step_name: str, output_name: str) -> str:
    return valid_node_name(f"{step_name}:{output_name}")


def create_pipeline(
    name: str,
    step_name_prefix: str,
    pipeline_configs: PipelineConfigs,
    aml_resources: AmlResources,
):

    def _pipeline_func(input_data_assets: List[Data]):
        data_asset_dict = {ds._original_data.name: ds for ds in input_data_assets}
        step_idx = 0
        step_outputs = {} # {step_name}:{output_name}: <output>
        last_step_default_output = None
        for step_config in pipeline_configs.flow_steps_config:
            step_component_kwargs = {**step_config.kwargs} if step_config.kwargs else {}
            if not step_config.get('no_input', False):
                # get inputs
                if step_config.inputs is None:
                    step_component_kwargs["input_dir"] = input_data_assets[0] if step_idx == 0 else last_step_default_output
                else:
                    for input_config in step_config.inputs:
                        source_data = None
                        if input_config.get('source_output_name', None) is None:
                            source_data = data_asset_dict[input_config.source_name]
                        else:
                            source_step_output_name = _get_step_output_name(step_name=input_config.source_name, output_name=input_config.source_output_name)
                            source_data = step_outputs[source_step_output_name]

                        step_component_kwargs[input_config.sink_input_name] = source_data

            step_component = aml_resources.components[step_config.component_name]
            step_name = step_config.step_name or step_config.component_name # _get_step_name(step_config=step_config)
            if step_config.get('n_splits', None):
                # parallel component
                spliter_component = aml_resources.components[step_config.spliter_component_name]
                spliter_kwargs = step_config.get('spliter_kwargs', None)
                merger_component = aml_resources.components[step_config.merger_component_name]
                step_par_func = create_parallel_pipeline_component(
                    # name='parallel_step',
                    worker_component=step_component,
                    worker_component_kwargs=step_component_kwargs,
                    worker_compute=step_config.get("compute", None),
                    n_splits=step_config.n_splits,
                    name_of_input_to_split=step_config.input_name_to_split,
                    spliter_component=spliter_component,
                    spliter_kwargs=spliter_kwargs,
                    spliter_compute=step_config.spliter_compute,
                    merger_component=merger_component,
                    merger_kwargs=step_config.get('merger_kwargs', None),
                    merger_compute=step_config.merger_compute
                )
                step_output = step_par_func()
            else:
                
                step_output = step_component(**step_component_kwargs)
                step_output.name = valid_node_name(f"s-{step_name_prefix or ''}-{step_name}") # for friendly of aml metrics plot naming
                step_output.description = f"Pipeline job: {name}" # add pipeline job info to each step description, for convenience of dashboard metadata view in columns
                
                set_step_ext_configs(
                    step_config=step_config,
                    step_func=step_output,
                )

            for output_name, output_data in step_output.outputs.items():
                step_outputs[_get_step_output_name(step_name=step_name, output_name=output_name)] = output_data

            last_step_default_output = step_output.outputs.get("output_dir", None)

            step_idx += 1

        return step_outputs
    
    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_1_input(input_data_asset: Input):
        return _pipeline_func(input_data_assets=[input_data_asset])
    
    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_2_inputs(input_data_asset_1: Input, input_data_asset_2: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2])
    
    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_3_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3])
    
    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_4_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4])
    
    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_5_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5])

    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_6_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input, input_data_asset_6: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5, input_data_asset_6])

    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_7_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input, input_data_asset_6: Input, input_data_asset_7: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5, input_data_asset_6, input_data_asset_7])

    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_8_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input, input_data_asset_6: Input, input_data_asset_7: Input, input_data_asset_8: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5, input_data_asset_6, input_data_asset_7, input_data_asset_8])

    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_9_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input, input_data_asset_6: Input, input_data_asset_7: Input, input_data_asset_8: Input, input_data_asset_9: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5, input_data_asset_6, input_data_asset_7, input_data_asset_8, input_data_asset_9])

    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_10_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input, input_data_asset_6: Input, input_data_asset_7: Input, input_data_asset_8: Input, input_data_asset_9: Input, input_data_asset_10: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5, input_data_asset_6, input_data_asset_7, input_data_asset_8, input_data_asset_9, input_data_asset_10])

    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_11_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input, input_data_asset_6: Input, input_data_asset_7: Input, input_data_asset_8: Input, input_data_asset_9: Input, input_data_asset_10: Input, input_data_asset_11: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5, input_data_asset_6, input_data_asset_7, input_data_asset_8, input_data_asset_9, input_data_asset_10, input_data_asset_11])

    @pipeline(name=name, compute=aml_resources.cpu_target)
    def _pipeline_12_inputs(input_data_asset_1: Input, input_data_asset_2: Input, input_data_asset_3: Input, input_data_asset_4: Input, input_data_asset_5: Input, input_data_asset_6: Input, input_data_asset_7: Input, input_data_asset_8: Input, input_data_asset_9: Input, input_data_asset_10: Input, input_data_asset_11: Input, input_data_asset_12: Input):
        return _pipeline_func(input_data_assets=[input_data_asset_1, input_data_asset_2, input_data_asset_3, input_data_asset_4, input_data_asset_5, input_data_asset_6, input_data_asset_7, input_data_asset_8, input_data_asset_9, input_data_asset_10, input_data_asset_11, input_data_asset_12])

    pipeline_map = {
        1: _pipeline_1_input,
        2: _pipeline_2_inputs,
        3: _pipeline_3_inputs,
        4: _pipeline_4_inputs,
        5: _pipeline_5_inputs,
        6: _pipeline_6_inputs,
        7: _pipeline_7_inputs,
        8: _pipeline_8_inputs,
        9: _pipeline_9_inputs,
        10: _pipeline_10_inputs,
        11: _pipeline_11_inputs,
        12: _pipeline_12_inputs,
    }

    return pipeline_map[len(pipeline_configs.input_data_assets)]


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
        step_name_prefix=timestamp,
        name=job_name,
        pipeline_configs=pipeline_cfg,
        aml_resources=aml_resources
    )

    input_data_assets = _create_data_assets(
        mlclient=aml_resources.ml_client_ws,
        data_assets_config=pipeline_cfg.input_data_assets,
        logger=logger
    )

    pipeline_job = pipeline_fn(*input_data_assets)

    # recording the cfgs in job properties
    pipeline_job.properties["pipeline_config"] = pipeline_cfg

    output_path = pipeline_cfg.get('output_path', None)
    if output_path:
        pipeline_output_path = pipeline_cfg.output_path.rstrip("/") # + "/" + output_subfolder.rstrip("/")
        output_mode = 'upload'
        if 'output_mode' in pipeline_cfg and pipeline_cfg.output_mode:
            output_mode = pipeline_cfg.output_mode
            
        for k in pipeline_job.outputs:
            path = pipeline_output_path if pipeline_cfg.get('share_output', False) else f"{pipeline_output_path}/{timestamp}/{k}/" + r"${{name}}"
            pipeline_job.outputs[k] = Output(
                type="uri_folder", mode=output_mode, path=path)

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
    # Put mtbench info at beginning to remind people attention whether set correctly
    job_name = f"[DF][{job_name_prefix}][{timestamp}]"
    
    logger.info(f"job_name={job_name}")

    run(timestamp=timestamp, experiment_name=experiment_name, job_name=job_name, pipeline_cfg=pipeline_cfg, logger=logger, output_subfolder=config_name)


if __name__ == "__main__":

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    color_logger(logger=logger)
    
    parser = argparse.ArgumentParser(description=f'DagflowPipeline run', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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
