from __future__ import annotations
import json
import os
import time
import multiprocessing
import shutil
import functools
import yaml
import datetime

import cue.run_handler as run_handler
from cue._context import ContextHelper
from cue._block import BlockHelper
from cue._executable import Executable
from cue._pipes import FilePipe
from cue._pipeline import Pipeline

###############################################################
# Print wrapper for verbose mode
def vprint(*args, **kwargs):
    print(str(datetime.datetime.now()), ">", *args, **kwargs)


###############################################################
# Execute a pipeline and orchestrate all component processes in a safe
# fashion.
#
# Attributes are:
#   tmp_directory:          path to directory which can store temporary files
#   max_processes:          max number of process workers to spin up
#   pipeline:               pipeline which should be run
#   executable_list:        list of all executables, roughly in order, which should
#                               be run as part of the pipeline  
#   data:                   the json representation of the pipeline
#
class Orchestrator():
    tmp_directory = "./.cue/"
    default_n_times_before_timeout = 10
    default_wait_time_between_tries = .05
    default_max_processes = 4
   
    # Initialze the orchestrator
    def __init__(self, 
        using_directory : str = None,
        verbose : bool = False,
            ) -> None:

        self.tmp_directory = Orchestrator.tmp_directory if using_directory is None else using_directory
        self.verbose = verbose

        if not os.path.exists(self.tmp_directory):
            os.mkdir(self.tmp_directory)  
        self.executable_list = []

        if self.verbose:
            vprint("STATUS:", "Started Pipeline")

    # Returns True if [string] encodes a definition
    def _is_definition(self, 
        key : str,              # some key in a context dict
        val : str               # the value associated with [key] 
            ) -> bool:

        return val == "$see definitions"

    def _unpack_existing_definition(self, 
        input_context : dict,   # the context containing the definiton
        key : str,              # the key defining the definition 
        definitions : dict      # the dict of definitions to lookup the key in
            ) -> None:

        del input_context[key]
        if key not in definitions:
            print(f"exception: definition {key} referenced but not supplied in definitions")
            exit(1)
        
        input_context[key] = definitions[key]

    # Unpack all definitions in some [input_context] by searching the [definitions] dict
    # for the key supplied
    def _unpack_all_definitions_in(self, 
        input_context : dict,   # the context to unpack definitions of
        definitions : dict      # the dict of definitions used for lookup
            ) -> None:

        keys_to_unpack = []
        for key, val in input_context.items():
            if self._is_definition(key, val):
                keys_to_unpack.append(key)

        for key in keys_to_unpack:
            self._unpack_existing_definition(input_context, key, definitions)

    # Recursively unpack all definitions which may be contained in a json dict
    def _unpack_definitions_in_json_dict(self,
        json_object : dict,     # the json object representation
        definitions : dict      # the dict of definitions used for lookup
            ) -> None:

        self._unpack_all_definitions_in(
            json_object.get('context', ContextHelper.empty_context), definitions)

        for elem in json_object.values():
            if isinstance(elem, dict):
                self._unpack_definitions_in_json_dict(elem, definitions)
            elif isinstance(elem, list):
                self._unpack_definitions_in_json_list(elem, definitions)

    # Recursively unpack all definitions which may be contained in a json list
    def _unpack_definitions_in_json_list(self,
        json_object : list,     # the json object representation
        definitions : dict      # the dict of definitions used for lookup
            ) -> None:

        for elem in json_object:
            if isinstance(elem, dict):
                self._unpack_definitions_in_json_dict(elem, definitions)
            elif isinstance(elem, list):
                self._unpack_definitions_in_json_list(elem, definitions)

    # Unpack all definitions contained in [input_json]
    def _preprocess_definitions(self,
        input_json : dict       # the json representing the pipeline
            ) -> None:

        self._unpack_definitions_in_json_dict(input_json, input_json.get('definitions', {}))
            
    # Parse [data] a json representation of a pipeline, or use [self.data] a json
    # has already been read
    def parse(self) -> Orchestrator:

        self._preprocess_definitions(self.data)

        self.pipeline = Pipeline(
            name=self.data['name'], 
            version=self.data['version'],
            script_directory=self.data['script directory'],
            context_json=self.data.get('context', ContextHelper.empty_context),
            blocks_json=self.data.get('blocks', BlockHelper.no_blocks),
            plaintext=self.plaintext)

        errors = self.pipeline.validate()
        if not errors.empty():
            for error in errors:
                print(error)
            exit(1)

        return self
    
    # Read from [path_to_file] to load a json representing a pipeline
    def read(self, 
        yaml_file : str = "",       # yaml file to be read
        json_file : str = "",       # json file to be read
            ) -> Orchestrator:

        if json_file:
            load_method = json.load
            path_to_file = json_file
        elif yaml_file:
            load_method = yaml.safe_load
            path_to_file = yaml_file
        else:
            print("exception: read method incorrect usage; use either one of json/yaml")
            exit(2)

        with open(path_to_file, 'r') as f:
            self.data = load_method(f)

        with open(path_to_file, 'r') as f:
            self.plaintext = f.read()

        self.parse()

        return self 

    # TODO: refactor into smaller methods
    # Add all executables to [self.executable_list] with the correct merged
    # contexts. Also connect all pipes as specified.
    def queue_tasks(self) -> Orchestrator:
        if self.verbose:
            vprint("STATUS:", "Queueing Tasks")
        pipeline_level_executable_index = {}

        for block in self.pipeline.blocks:
            block_level_flattened_contexts = \
                ContextHelper.merge(self.pipeline.flattened_contexts, block.flattened_contexts)

            for block_level_context_instance in block_level_flattened_contexts:
                block_level_executable_index = {}

                for script in block.scripts:
                    script_level_flattened_contexts = \
                        ContextHelper.merge([block_level_context_instance], script.flattened_contexts)

                    for context_instance in script_level_flattened_contexts:
                        executable = Executable(
                            context_instance,
                            script.guid,
                            script.path,
                            block.name,
                            block.serial,
                            self.pipeline.version,
                            self.pipeline.name,
                            self.tmp_directory,
                        )
                        
                        if executable not in self.executable_list:
                            self.executable_list.append(executable)
                            if pipeline_level_executable_index.get(script.returns, None) is None:
                                pipeline_level_executable_index[script.returns] = []
                            pipeline_level_executable_index[script.returns].append(executable)                    
                            
                            if block_level_executable_index.get(script.returns, None) is None:
                                block_level_executable_index[script.returns] = []
                            block_level_executable_index[script.returns].append(executable)

                            if script.takes:
                                if script.takes in block_level_executable_index:
                                    for upstream_executable in block_level_executable_index[script.takes]:
                                        upstream_executable.connect_upstream_of(executable)
                                else:
                                    for upstream_executable in pipeline_level_executable_index[script.takes]:
                                        upstream_executable.connect_upstream_of(executable)
        
        if self.verbose:
            vprint("STATUS:", "Tasks Queued Successfully", end="")

        return self

    # Run all executables by serial order using multiple worker processes
    def run(self,
        given : dict = {}
            ) -> Orchestrator:

        n_times_before_timeout = \
            given.get('n_times_before_timeout', self.default_n_times_before_timeout)
        wait_time_between_tries = \
            given.get('wait_time_between_tries', self.default_wait_time_between_tries)
        max_processes = \
            given.get('max_processes', self.default_max_processes)
        from_serial = \
            given.get('from', self.pipeline.serials[0])

        start = time.time()
        my_xrun = functools.partial(
            self._run, 
            self.tmp_directory, 
            self.pipeline.script_directory,
            n_times_before_timeout,
            wait_time_between_tries,
            self.verbose)

        with multiprocessing.Pool(max_processes) as p:
            serials_to_run = [s for s in self.pipeline.serials if s >= from_serial]
            for serial in serials_to_run:
                same_level_executables = [exec \
                    for exec in self.executable_list if exec.block_serial == serial]
                if self.verbose: 
                    print()
                    blocks = set([exec.block_name for exec in same_level_executables])
                    for block_name in list(blocks):
                        vprint("STATUS:", block_name)

                p.map(my_xrun, same_level_executables)

        if self.verbose:
            print()
            vprint(f"STATUS: Finished in {round(time.time() - start, 3)}")
        return self

    # Remove some temporary files and directories used by the pipeline
    def clean(self) -> None:
        if self.verbose:
            vprint("STATUS:", "Cleaning")
        for executable in self.executable_list:
            executable.clean()
        
        if self.verbose:
            vprint("STATUS:", "Finished Cleaning")

    # Remove full temporary directory
    def purge(self) -> None:
        shutil.rmtree(self.tmp_directory)

    # to run a script
    def _run(self, 
        tmp_directory : str, 
        pipeline_script_directory : str, 
        n_times_before_timeout : int,
        wait_time_between_tries : int,
        verbose : bool,
        executable : Executable
            ) -> Orchestrator:

        # write data to pipeline
        def write_data_pipe_file(executable, pipe_name, pipeline_script_directory):
            received_packages = []
            for filename in os.listdir(executable.data_ingest_directory):
                with open(executable.data_ingest_directory + filename, 'r') as f:
                    received_packages.append(f.read())

            pipe_json = {
                "script_directory": pipeline_script_directory,
                "script_path": executable.script_path,
                "params": executable.context_instance,
                "data": received_packages
            }

            if received_packages or os.path.getsize(pipe_name) == 0:
                with open(pipe_name, 'w') as f:
                    f.write(json.dumps(pipe_json))
            else:
                with open(pipe_name, 'r') as f:
                    pipe_json = json.load(f)

            return pipe_json

        file_pipe = FilePipe(tmp_directory, executable)
        i = 0
        while i < n_times_before_timeout:
            if executable.is_waiting_for_upstream(file_pipe):
                time.sleep(wait_time_between_tries)
                i += 1
            else:
                break

        if i == n_times_before_timeout:
            print("failed script:")
            print(str(executable))
            return

        input_json = write_data_pipe_file(executable, file_pipe.into(), pipeline_script_directory)

        run_handler.run(input_json, file_pipe.out())

        if verbose:
            print('.', end="", flush=True)
            
        with open(file_pipe.out(), 'r') as f:
            data = f.read()

        executable.send(data)
