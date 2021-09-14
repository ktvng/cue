import json
import sys
import os
import subprocess
import hashlib
import time
import threading
import queue
import random
import shutil
import run_handler

class ContextHelper():
    @classmethod
    def parse(cls, context):
        context_instances = [{}]
        for key, val in context.items():
            if isinstance(val, list):
                context_instances = cls.expand_parameter_list_into(context_instances, key, val)
            elif isinstance(val, dict):
                context_instances = cls.expand_parameter_range_into(context_instances, key, val)
            else:
                for instance in context_instances:
                    instance.update(ContextHelper.flatten(key, val))

        return context_instances

    @classmethod
    def key_is_paired(cls, key):
        return ',' in key

    @classmethod
    def paired_parameter_expansion_for(cls, paired_str):
        return map(lambda x: x.strip(), paired_str.split(','))

    @classmethod
    def flatten(cls, key, val):
        if ContextHelper.key_is_paired(key):
            keys = ContextHelper.paired_parameter_expansion_for(key)
            vals = ContextHelper.paired_parameter_expansion_for(val)
            return {k : v for k, v in zip(keys, vals)}
        else:
            return {key : val}
    
    @classmethod
    def expand_parameter_list_into(cls, context_instances, key, lst):
        expanded_context_instances = []
        for val in lst:
            for instance in context_instances:
                new_instance = {**instance, **ContextHelper.flatten(key, val)}
                expanded_context_instances.append(new_instance)
        
        return expanded_context_instances
        
    @classmethod
    def expand_parameter_range_into(cls, context_instances, key, dct):
        expanded_context_instances = []
        for i in range(dct['start'], dct['end'], dct.get('step', 1)):
            for instance in context_instances:
                new_instance = instance.copy()
                new_instance[key] = i
                expanded_context_instances.append(new_instance)
        return expanded_context_instances

    @classmethod
    def merge(cls, context_instances1, context_instances2):
        context_instances = []
        for instance1 in context_instances1:
            for instance2 in context_instances2:
                context_instances.append({**instance1, **instance2})

        return context_instances

    empty_context = {}

class BlockHelper():
    no_blocks = []

    @classmethod
    def parse(self, blocks_json):
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

class ScriptHelper():
    no_scripts = []

    @classmethod
    def parse(self, scripts_json):
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

class Executable():
    def __init__(self, context_instance, script_name, script_guid, script_path, block_name, iteration, pipeline_name, tmp_directory):
        self.context_instance = context_instance
        self.script_name = script_name
        self.script_guid = script_guid
        self.script_path = script_path
        self.block_name = block_name
        self.iteration = iteration
        self.pipeline_name = pipeline_name
        self.hash = self.calculate_hash()
        self.data_ingest_directory = tmp_directory + "/" + self.hash + "/"
        if not os.path.exists(self.data_ingest_directory):
            os.mkdir(self.data_ingest_directory)

        self.n_pipes_in = 0
        self.outgoing_pipes = []

    def is_waiting_for_upstream(self):
        return self.n_pipes_in > len(os.listdir(self.data_ingest_directory))

    def calculate_hash(self):
        hasher = hashlib.sha1()
        hasher.update(bytes(str(self), encoding='utf-8'))
        hashcode = hasher.hexdigest()
        
        return hashcode

    def connect_upstream_of(self, executable):
        pipe = Pipe(self, executable)
        self.outgoing_pipes.append(pipe)

    def send(self, data):
        for pipe in self.outgoing_pipes:
            pipe.transfer(data)

    def clean(self):
        shutil.rmtree(self.data_ingest_directory)

    def __str__(self):
        return f"{self.pipeline_name}({self.iteration})/{self.block_name}/{self.script_name}({self.script_guid}):{self.script_path}\n" + \
            json.dumps(self.context_instance, indent=2)

    def __eq__(self, other):
        if not isinstance(other, Executable):
            return False

        return \
            self.context_instance == other.context_instance and \
            self.block_name == other.block_name and \
            self.script_name == other.script_name and \
            self.script_guid == other.script_guid

class Pipe():
    def __init__(self, pipe_from_obj, to_obj):
        self.pipe_from_obj = pipe_from_obj
        self.to_obj = to_obj
        self.to_obj.n_pipes_in += 1 
        self.cached_data = "null"

    def transfer(self, data):
        self.cached_data = data
        with open(self.to_obj.data_ingest_directory + self.pipe_from_obj.hash, 'w') as f:
            f.write(data)

class Pipeline():
    def __init__(self, name, iteration, script_directory, context_json, blocks_json):
        self.name = name
        self.iteration = iteration
        self.script_directory = script_directory
        self.context_json = context_json
        self.context_instances = ContextHelper.parse(context_json)
        self.blocks_json = blocks_json
        self.blocks = BlockHelper.parse(blocks_json)

class Block():
    def __init__(self, name, serial, description, context_json, scripts_json):
        self.name = name
        self.serial = serial
        self.description = description
        self.context_json = context_json
        self.context_instances = ContextHelper.parse(context_json)
        self.scripts_json = scripts_json
        self.scripts = ScriptHelper.parse(scripts_json)

class Script():
    def __init__(self, name, guid, path, pipe_from, context_json):
        self.name = name
        self.guid = guid
        self.path = path
        self.pipe_from = pipe_from
        self.context_json = context_json
        self.context_instances = ContextHelper.parse(context_json)

class FilePipe():
    def __init__(self, tmp_directory, executable):
        self.name = tmp_directory + "pipe" + executable.hash
        with open(self.into(), 'w') as f:
            f.write("")

        with open(self.out(), 'w') as f:
            f.write("")

    def into(self):
        return self.name + ".in"
    
    def out(self):
        return self.name + ".out"

    def clean(self):
        os.remove(self.into())
        os.remove(self.out())

class Executor(threading.Thread):
    tmp_directory = "./.scriptorchestrator_tmp/"
    n_times_before_timeout = 20
    waittime_after_timeout = .1

    def __init__(self, id, task_q, pipeline_script_directory):
        threading.Thread.__init__(self)
        self.id = id
        self.task_q = task_q
        self.pipeline_script_directory = pipeline_script_directory

    def run(self):
        while not self.task_q.empty():
            try:
                executable = self.task_q.get()
            except:
                return

            i = 0
            while i < self.n_times_before_timeout:
                if executable.is_waiting_for_upstream():
                    time.sleep(self.waittime_after_timeout)
                    i += 1
                else:
                    break

            if i == self.n_times_before_timeout:
                print("failed script:")
                print(str(executable))
                exit(1)

            file_pipe = FilePipe(self.tmp_directory, executable)
            input_json = self.write_data_pipe_file(executable, file_pipe.into())

            # bash_cmd = f"python3 run_handler.py -i {file_pipe.into()} -o {file_pipe.out()}"
            # subprocess.run(bash_cmd.split())
            run_handler.run(input_json, file_pipe.out())

            with open(file_pipe.out(), 'r') as f:
                data = f.read()

            file_pipe.clean()
            executable.send(data)

    def write_data_pipe_file(self, executable, pipe_name):
        received_packages = []
        for filename in os.listdir(executable.data_ingest_directory):
            with open(executable.data_ingest_directory + filename, 'r') as f:
                received_packages.append(f.read())

        pipe_json = {
            "script_directory": self.pipeline_script_directory,
            "script_path": executable.script_path,
            "params": executable.context_instance,
            "data": received_packages
        }
    
        with open(pipe_name, 'w') as f:
            f.write(json.dumps(pipe_json))

        return pipe_json

class ScriptOrchestrator():
    tmp_directory = "./.scriptorchestrator_tmp/"
    max_threads = 16
    
    def __init__(self):
        if not os.path.exists(self.tmp_directory):
            os.mkdir(self.tmp_directory)  
        self.executable_q = queue.Queue()
        self.executable_list = []

    def parse(self, data=None):
        data = self.data if data is None else data
        self.pipeline = Pipeline(
            name=data['name'], 
            iteration=data['iteration'],
            script_directory=data['script_directory'],
            context_json=data.get('context', ContextHelper.empty_context),
            blocks_json=data.get('blocks', BlockHelper.no_blocks))

        return self
         
    def read(self, path_to_file):
        with open(path_to_file) as f:
            self.data = json.load(f)
        self.parse(self.data)
        return self 

    def queue_tasks(self):
        pipeline_level_executable_index = {}

        for block in self.pipeline.blocks:
            block_level_context_instances = ContextHelper.merge(self.pipeline.context_instances, block.context_instances)

            for block_level_context_instance in block_level_context_instances:
                block_level_executable_index = {}

                for script in block.scripts:
                    script_level_context_instances = ContextHelper.merge([block_level_context_instance], script.context_instances)

                    for context_instance in script_level_context_instances:
                        executable = Executable(
                            context_instance,
                            script.name,
                            script.guid,
                            script.path,
                            block.name,
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

    def run(self):
        start = time.time()

        for task in self.executable_list:
            self.executable_q.put(task)

        task_threads = [Executor(i, self.executable_q, self.pipeline.script_directory) for i in range(self.max_threads)]
        for t in task_threads:
            t.start()
        
        for t in task_threads:
            t.join()

        print(time.time() - start)

    def clean(self):
        for executable in self.executable_list:
            executable.clean()

        os.removedirs(self.tmp_directory)
