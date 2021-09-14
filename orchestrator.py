from __future__ import annotations
import json
import sys
import os
import hashlib
import time
import multiprocessing
import queue
import random
import shutil
import run_handler
import functools
import typing

###############################################################
# Wrapper for methods related to contexts.
#
# A context represents a collection of key:value pairs which
# will be passed to the script as parameters. A context may also contain
# multiple values for each key (as a list of values) and will be expanded
# automatically into a list of flat contexts (one value per key) that
# encompasses all possible combinations of key:value pairs. 
class ContextHelper():
    # The empty context; for situations where no context is supplied by the json.
    empty_context = {}

    # Parses and flattens a [context] into list of dicts with key:value pairs
    # that represent the parameters of the flattened context
    @classmethod
    def parse(cls, 
        context : dict          # the json defining the context
            ) -> list:          # returns alist containing simple dicts 
                                #   (vals are str/int/float...)

        flattened_contexts = [{}]
        for key, val in context.items():
            if isinstance(val, list):
                flattened_contexts = cls.expand_parameter_list_into(flattened_contexts, key, val)
            elif isinstance(val, dict):
                flattened_contexts = cls.expand_parameter_range_into(flattened_contexts, key, val)
            else:
                for instance in flattened_contexts:
                    instance.update(ContextHelper.unpair(key, val))

        return flattened_contexts

    # Character which deliminates components of a paired key/val
    # and represents a reserved symbol that cannot appear inside a 
    # key/val
    paired_delimiter = ','

    # True if key represents a pair of parameters
    @classmethod
    def key_is_paired(cls, 
        key : str               # the key of an entry in some context json
            ) -> bool:

        return cls.paired_delimiter in key

    # Expands a paired key/value string by splitting along the pair deliminator
    @classmethod
    def paired_parameter_expansion_for(cls, 
        paired_str : str,       # the string which contains pairs of keys/values
            ) -> list:          # returns a list where each component represents one a 
                                #   single key/value pair

        return map(lambda x: x.strip(), paired_str.split(','))

    # Breaks apart paired keys/values into a simple dict with individual keys/values
    # and merely returns the key/value wrapped inside a dict if they are unpaired
    @classmethod
    def unpair(cls, 
        key : str,              # the key, either paired or unpaired
        val : str               # the val, either paired or unpaired
            ) -> dict:          # returns a simple dict containing all single key:value pairs

        if ContextHelper.key_is_paired(key):
            keys = ContextHelper.paired_parameter_expansion_for(key)
            vals = ContextHelper.paired_parameter_expansion_for(val)
            return {k : v for k, v in zip(keys, vals)}
        else:
            return {key : val}
    
    # Flattens a key:value pair where the value supplied is a list so that
    # the expanded_flattened_contexts contain all combinations of the existing
    # [flattened_contexts] with all values of the [key] as supplied by the [lst]
    @classmethod
    def expand_parameter_list_into(cls, 
        flattened_contexts : list,      # list of flat contexts (only single key:val pairs) 
        key : str,                      # name of the key
        lst : list,                     # list of values which can be assigned to [key]
            ) -> list:                  # returns a list of flattened_contexts now containing
                                        #   one of the values of [key] as a parameter
        expanded_flattened_contexts = []
        for val in lst:
            for instance in flattened_contexts:
                new_instance = {**instance, **ContextHelper.unpair(key, val)}
                expanded_flattened_contexts.append(new_instance)
        
        return expanded_flattened_contexts

    # Flattens a key:value pair where the value supplied represents a 
    # range of possible values for the [key]. Similar to expand_parameter_list_into
    #
    # A valid dict is required which contains the following keys and numeric values
    #   "start": integer to start range at, inclusive.
    #   "end":   integer to end range before, exclusive.
    #   "step":  integer to increment by, default is 1.
    #
    # Equivalent to the python code `range(start, end, step)`
    @classmethod
    def expand_parameter_range_into(cls, 
        flattened_contexts : list,      # list of flat contexts (only single key:val pairs)
        key : str,                      # name of the key 
        dct : dict                      # dict representing the range, see above
            ) -> list:                  # returns a list of flattened_contexts now containing
                                        #   one of the values of [key] as a parameter

        expanded_flattened_contexts = []
        for i in range(dct['start'], dct['end'], dct.get('step', 1)):
            for instance in flattened_contexts:
                new_instance = instance.copy()
                new_instance[key] = i
                expanded_flattened_contexts.append(new_instance)
        return expanded_flattened_contexts

    # Merge two lists of flattened_contexts to create all combinations of 
    # flat contexts with keys from flattened_contexts from both lists.
    # Any key conflicts are resolved by overriding the key supplied from
    # [flattened_contexts1] by those from [flattened_contexts2]
    @classmethod
    def merge(cls, 
        flattened_contexts1 : list,     # first list of flat contexts
        flattened_contexts2 : list      # second list of flat contexts
            ) -> list :                 # list of flat contexts all combinations of 
                                        #   keys taken from the flat contexts from each list
        flattened_contexts = []
        for instance1 in flattened_contexts1:
            for instance2 in flattened_contexts2:
                flattened_contexts.append({**instance1, **instance2})

        return flattened_contexts

###############################################################
# Wrapper for methods related to blocks. 
class BlockHelper():
    # Defines an empty block; used for parsing where no block is defined.
    no_blocks = []

    # Parse through a json object which defines a list of blocks and return
    # a list of Block objects with corresponding data
    #
    # The json should have the following properties
    #   "name":         required <str> name of block
    #   "serial":       required <int> representation of the order in which
    #                       a block should be run. All blocks with a given
    #                       serial can be run in parallel, lower serials are
    #                       run before higher serials
    #   "description":  required <str> describing the action of the block
    #   "context":      optional, context
    #   "scripts":      optional, scripts to be run inside this block
    @classmethod
    def parse(self, 
        blocks_json : list          # list of dicts containing required block info
            ) -> list:              # returns list of block objects

        blocks = []
        for block_json in blocks_json:
            blocks.append(Block(
                block_json['name'],
                block_json['serial'],
                block_json['description'],
                block_json.get('context', ContextHelper.empty_context),
                block_json.get('scripts', ScriptHelper.no_scripts)
            ))
        
        return blocks

###############################################################
# Wrapper for methods relating to scripts, i.e. parsing
class ScriptHelper():
    # Value which should be assigned if there are no scripts
    no_scripts = []

    # Parse a json consisting of a list of dicts where each dict 
    # contains the information required to define a script, and return
    # a list of the script objects
    #
    # The json should have the following attributes
    #   "name":         required <str>, the name of the script
    #   "guid":         required <str>, a globally unique identifier
    #   "path":         required <int/str>, the import path which can be 
    #                       used with the `import` or `__import__` 
    #                       commands to load the script as a module
    #   "pipe_from":    required <int/str>, the [guid] of the (upstream) script from
    #                       where data should be piped from
    @classmethod
    def parse(self, 
        scripts_json : list         # list of dicts with key:value pairs 
                                    # specifying script properties
            ) -> list:              # returns a list of script objects
        scripts = []
        for script_json in scripts_json:
            scripts.append(Script(
                script_json['name'],
                script_json['guid'],
                script_json['path'],
                script_json['pipe_from'],
                script_json.get('context', ContextHelper.empty_context)
            ))
        
        return scripts

###############################################################
# Representation of a executable script; contains all required information
# to load the script, all parameters to provide the script, and all data 
# sources to feed into the script.
#
# Attributes are:
#   context_instance:   dict of parameters to supply the script
#   script_name:        name of script
#   script_guid:        guid of script
#   script_path:        path required to import script
#   block_name:         name of block containing script
#   block_serial:       serial order of block
#   iteration:          iteration of the pipeline
#   tmp_directory:      path to the directory to store temp files
class Executable():
    # Initialize instance attributes of the Executable (see above) 
    def __init__(self, 
            context_instance : dict, 
            script_name : str, 
            script_guid : str, 
            script_path : str, 
            block_name : str, 
            block_serial : int, 
            iteration : str, 
            pipeline_name : str, 
            tmp_directory : str
                ) -> None:

        self.context_instance = context_instance
        self.script_name = script_name
        self.script_guid = script_guid
        self.script_path = script_path
        self.block_name = block_name
        self.block_serial = block_serial
        self.iteration = iteration
        self.pipeline_name = pipeline_name
        self.hash = self.calculate_hash()
        self.data_ingest_directory = tmp_directory + "/" + self.hash + "/"

        if not os.path.exists(self.data_ingest_directory):
            os.mkdir(self.data_ingest_directory)

        self.n_pipes_in = 0
        self.outgoing_pipes = []

    # True if [self] executable is still waiting for upstream data to
    # be propogated to it
    def is_waiting_for_upstream(self
        ) -> bool: 
        return self.n_pipes_in > len(os.listdir(self.data_ingest_directory))

    # Get the run-invariant hash code which uniquely refers to this
    # executable
    def calculate_hash(self
        ) -> str:       # <str> representing the hash of [self]
        
        hasher = hashlib.sha1()
        hasher.update(bytes(str(self), encoding='utf-8'))
        hashcode = hasher.hexdigest()
        
        return hashcode

    # Connect [self] executable as upstream of [executable]. This latter
    # executable will receive the data pipelined out of [self]
    def connect_upstream_of(self, 
        executable : Executable         # [executable] which will be upstream
            ) -> None:

        pipe = Pipe(self, executable)
        self.outgoing_pipes.append(pipe)

    # Send [data] to all downstream executables
    def send(self, 
        data : str       # string returned by executing the script
            ) -> None:

        for pipe in self.outgoing_pipes:
            pipe.transfer(data)

    # Remove all temporary files and folders created by [self] executable
    # and by executables which pipe into [self]
    def clean(self
        ) -> None:
        
        shutil.rmtree(self.data_ingest_directory)

    # Return a string representation of [self]
    def __str__(self
        ) -> str:       # returns <str> representation of [self]
        
        return f"{self.pipeline_name}({self.iteration})/{self.block_name}/{self.script_name}({self.script_guid}):{self.script_path}\n" + \
            json.dumps(self.context_instance, indent=2)

    # Evaluate equality of executables
    def __eq__(self, 
        other : Executable      # [other] executable to compare to [self]
            ) -> bool:          # bool encoding equality as defined below

        if not isinstance(other, Executable):
            return False

        return \
            self.context_instance == other.context_instance and \
            self.block_name == other.block_name and \
            self.script_name == other.script_name and \
            self.script_guid == other.script_guid

###############################################################
# Represents a direct pipe between two executables that can transfer data
# unidirectionally
#
# Attributes are:
#   pipe_from_obj:      the upstream executable connected by the pipe
#   to_obj:             the downstream executable connected by the pipe 
class Pipe():
    def __init__(self,
        pipe_from_obj : Executable,     # upstream executable
        to_obj : Executable             # downstream executable
            ) -> None:                  

        self.pipe_from_obj = pipe_from_obj
        self.to_obj = to_obj
        self.to_obj.n_pipes_in += 1 

    # Transfer [data] to the intake folder of the [to_obj] executable
    def transfer(self, 
        data : str      # data to be transfered
            ) -> None:
        with open(self.to_obj.data_ingest_directory + self.pipe_from_obj.hash, 'w') as f:
            f.write(data)

###############################################################
# Encapsulates the entire script pipeline
#
# Attributes are:
#   name:               name of pipeline
#   iterastion:         iteration of the current pipeline run
#   script_directory:   root directory of the scripts which should be run
#   context_json:       json representing global context for the pipeline
#   blocks_json:        json list representing all blocks of the pipeline
#   flattened_contexts: list of flattened global pipeline contexts
#   blocks:             list of <Block> entities
#   serials:            ascending ordered list of all serial values
class Pipeline():
    def __init__(self, 
        name : str,                 # name of pipeline
        iteration : str,            # iteration of the current pipeline run
        script_directory : str,     # root directory to find scripts
        context_json : dict,        # global context of the pipeline, json specified
        blocks_json : list          # list of all blocks, json specified
            ) -> None:

        self.name = name
        self.iteration = iteration
        self.script_directory = script_directory
        self.context_json = context_json
        self.flattened_contexts = ContextHelper.parse(context_json)
        self.blocks_json = blocks_json
        self.blocks = BlockHelper.parse(blocks_json)
        self.serials = self._get_block_serials()

    # Returns an ascending ordered list of all serial numbers which appear
    # in the pipeline
    def _get_block_serials(self
        ) -> list:
        serials = []
        for block in self.blocks:
            if block.serial not in serials:
                serials.append(block.serial)
        serials.sort()

        return serials

###############################################################
# Encapsulates all information required in a block
#
# A Block is a group of scripts which all share a common context
# and which can all be safely parallelized with little to no
# performance impact
#
# Attributes are:
#   name:           the name of the block
#   serial:         integer expressing the relative position of a block in the
#                       pipeline, with lower numbers representing earlier 
#                       processes
#   description:    descripton of the Block's operation and purpose
#   context_json:   json encoding the block level context
#   scripts_json:   json list containing all scripts to process in the block 
class Block():
    def __init__(self, 
        name : str,             # the name of the block
        serial : int,           # the serial ordering of the block 
        description : str,      # description of actions performed by block 
        context_json : dict,    # json encoding the block level context 
        scripts_json : list,    # json listing all scripts comprising block 
            ) -> None:

        self.name = name
        self.serial = serial
        self.description = description
        self.context_json = context_json
        self.flattened_contexts = ContextHelper.parse(context_json)
        self.scripts_json = scripts_json
        self.scripts = ScriptHelper.parse(scripts_json)

###############################################################
# Encapsulates all information required in a script
#
# A Script is a wrapper around an external executable which should
# be run as part of the pipeline process. It can be defined with a 
# script level context that can be flattened to provide the script
# with necessary parameters.
#
# External script parameters should have a `run` method which takes
# two parameters, `params` and `data`, and should have a list of 
# required parameters `parameters`.
#
# For instance, inside `sample.py`
#
#   import <modules> 
#   
#   parameters = ['root_path', 'duration', 'image_collection']
#
#   def run(params, data):
#       ...
#       ...
#
# The script will be run by calling the `run` method where:
#   params:     a dict which contains all parametes as specified by the
#                   context of a script. This context inherits block and
#                   pipeline level contexts
#
#   data:       a list of strings which were produced by all scripts
#                   upstream of [self] as defined by the [pipe_from] attribute
#                   which will be empty if there are no upstream scripts.
#
# Attributes are:
#   name:               name of script
#   guid:               globally unique identifier
#   path:               path to import script via `import` or `__import__`
#   pipe_from:          guid of script which is upstream of [self] and which will
#                           send data to [self]
#   context_json:       json specifing the script level context
#   flattened_contexts: list of all flat contexts which this script should be
#                           run using
class Script():
    def __init__(self,
        name : str,             # name of script 
        guid : str,             # globally unique identifier 
        path : str,             # path to import script via `__import__` 
        pipe_from : str,        # guid of upstream script 
        context_json : dict     # dict of key:value parameters for script
            ) -> None:
            
        self.name = name
        self.guid = guid
        self.path = path
        self.pipe_from = pipe_from
        self.context_json = context_json
        self.flattened_contexts = ContextHelper.parse(context_json)

###############################################################
# Retains information for data flowing into/out of a given script, as opposed to
# <Pipes> which represent information flowing between two different scripts.
#
# Attributes are:
#   tmp_directory:      directory to store this temporary data
#   name:               hash name of executable which should only be used through
#                           `self.into` and `self.out` methods
class FilePipe():
    def __init__(self, 
        tmp_directory : str,        # directory to store data
        executable : Executable     # executable script which is wrapped
            ) -> None:

        self.name = tmp_directory + "pipe" + executable.hash
        with open(self.into(), 'w') as f:
            f.write("")

        with open(self.out(), 'w') as f:
            f.write("")

    # Return the name of the file storing incoming data
    def into(self) -> str:
        return self.name + ".in"
    
    # Return the name of the file storing outgoing data
    def out(self) -> str:
        return self.name + ".out"

    # Remove all files used by [self]
    def clean(self) -> None:
        os.remove(self.into())
        os.remove(self.out())

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
class ScriptOrchestrator():
    tmp_directory = "./.scriptorchestrator_tmp/"
    max_processes = 8
   
    # Initialze the orchestrator by creating the temporary directory
    def __init__(self):
        if not os.path.exists(self.tmp_directory):
            os.mkdir(self.tmp_directory)  
        self.executable_list = []

    # Parse [data] a json representation of a pipeline, or use [self.data] a json
    # has already been read
    def parse(self, 
        data : dict = None
            ) -> ScriptOrchestrator:

        data = self.data if data is None else data
        self.pipeline = Pipeline(
            name=data['name'], 
            iteration=data['iteration'],
            script_directory=data['script_directory'],
            context_json=data.get('context', ContextHelper.empty_context),
            blocks_json=data.get('blocks', BlockHelper.no_blocks))

        return self
    
    # Read from [path_to_file] to load a json representing a pipeline
    def read(self, 
        path_to_file : str
            ) -> ScriptOrchestrator:
        
        with open(path_to_file) as f:
            self.data = json.load(f)
        self.parse(self.data)
        return self 

    # TODO: refactor into smaller methods
    # Add all executables to [self.executable_list] with the correct merged
    # contexts. Also connect all pipes as specified.
    def queue_tasks(self) -> None:
        pipeline_level_executable_index = {}

        for block in self.pipeline.blocks:
            block_level_flattened_contexts = ContextHelper.merge(self.pipeline.flattened_contexts, block.flattened_contexts)

            for block_level_context_instance in block_level_flattened_contexts:
                block_level_executable_index = {}

                for script in block.scripts:
                    script_level_flattened_contexts = ContextHelper.merge([block_level_context_instance], script.flattened_contexts)

                    for context_instance in script_level_flattened_contexts:
                        executable = Executable(
                            context_instance,
                            script.name,
                            script.guid,
                            script.path,
                            block.name,
                            block.serial,
                            self.pipeline.iteration,
                            self.pipeline.name,
                            self.tmp_directory,
                        )
                        
                        if executable not in self.executable_list:
                            self.executable_list.append(executable)
                            if pipeline_level_executable_index.get(script.guid, None) is None:
                                pipeline_level_executable_index[script.guid] = []
                            pipeline_level_executable_index[script.guid].append(executable)                    
                            
                            if block_level_executable_index.get(script.guid, None) is None:
                                block_level_executable_index[script.guid] = []
                            block_level_executable_index[script.guid].append(executable)

                            if script.pipe_from != -1:
                                if script.pipe_from in block_level_executable_index:
                                    for upstream_executable in block_level_executable_index[script.pipe_from]:
                                        upstream_executable.connect_upstream_of(executable)
                                else:
                                    for upstream_executable in pipeline_level_executable_index[script.pipe_from]:
                                        upstream_executable.connect_upstream_of(executable)

        return self

    # Run all executables by serial order using multiple worker processes
    def run(self) -> None:
        start = time.time()
        my_xrun = functools.partial(xrun, self.tmp_directory, self.pipeline.script_directory)
        with multiprocessing.Pool(self.max_processes) as p:
            for serial in self.pipeline.serials:
                same_level_executables = [exec for exec in self.executable_list if exec.block_serial == serial]
                p.map(my_xrun, same_level_executables)

        print(time.time() - start)

    # Remove all temporary files and directories used by the pipeline
    def clean(self) -> None:
        for executable in self.executable_list:
            executable.clean()

        os.removedirs(self.tmp_directory)


n_times_before_timeout = 100
waittime_after_timeout = .05

# to run a script
def xrun(tmp_directory, pipeline_script_directory, executable):
    i = 0
    while i < n_times_before_timeout:
        if executable.is_waiting_for_upstream():
            time.sleep(waittime_after_timeout)
            i += 1
        else:
            break

    if i == n_times_before_timeout:
        print("failed script:")
        print(str(executable))
        exit(1)

    file_pipe = FilePipe(tmp_directory, executable)
    input_json = write_data_pipe_file(executable, file_pipe.into(), pipeline_script_directory)

    run_handler.run(input_json, file_pipe.out())

    with open(file_pipe.out(), 'r') as f:
        data = f.read()

    file_pipe.clean()
    executable.send(data)

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

    with open(pipe_name, 'w') as f:
        f.write(json.dumps(pipe_json))

    return pipe_json
