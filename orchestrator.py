import json
import sys

class ContextHelper():
    @classmethod
    def parse(cls, context):
        context_instances = [{}]
        for key, val in context.items():
            if isinstance(val, list):
                context_instances = cls.expand_parameter_list_into(context_instances, key, val)
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
    def __init__(self, context_instance, script_name, script_guid, script_path, block_name, iteration, pipeline_name):
        self.context_instance = context_instance
        self.script_name = script_name
        self.script_guid = script_guid
        self.script_path = script_path
        self.block_name = block_name
        self.iteration = iteration
        self.pipeline_name = pipeline_name

        self.received_packages = []
        self.outgoing_pipes = []

    def connect_upstream_of(self, executable):
        pipe = Pipe(self, executable)
        self.outgoing_pipes.append(pipe)

    def receive(self, data, from_object):
        self.received_packages.append(data)

    def send(self, data):
        for pipe in self.outgoing_pipes:
            pipe.transfer(data)

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
        self.cached_data = "null"

    def transfer(self, data):
        self.cached_data = data
        self.to_obj.receive(data, from_object=self.pipe_from_obj)

class Pipeline():
    def __init__(self, name, iteration, root_directory, context_json, blocks_json):
        self.name = name
        self.iteration = iteration
        self.root_directory = root_directory
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


class ScriptOrchestrator():
    def __init__(self):
        self.executable_q = []

    def parse(self, data=None):
        data = self.data if data is None else data
        self.pipeline = Pipeline(
            name=data['name'], 
            iteration=data['iteration'],
            root_directory=data['root_directory'],
            context_json=data.get('context', ContextHelper.empty_context),
            blocks_json=data.get('blocks', BlockHelper.no_blocks))

        return self
         
    def read(self, path_to_file):
        with open(path_to_file) as f:
            self.data = json.load(f)
        self.parse(self.data)
        return self 

    def queue_tasks(self):
        executables_index = {}

        for block in self.pipeline.blocks:
            for script in block.scripts:
                context_instances = ContextHelper.merge(self.pipeline.context_instances, block.context_instances)
                context_instances = ContextHelper.merge(context_instances, script.context_instances)

                for context_instance in context_instances:
                    executable = Executable(
                        context_instance,
                        script.name,
                        script.guid,
                        script.path,
                        block.name,
                        self.pipeline.iteration,
                        self.pipeline.name
                    )
                    
                    if executable not in self.executable_q:
                        self.executable_q.append(executable)
                        if executables_index.get(script.guid, None) is None:
                            executables_index[script.guid] = []
                        executables_index[script.guid].append(executable)                    

                        if script.pipe_from != -1:
                            for upstream_executable in executables_index[script.pipe_from]:
                                upstream_executable.connect_upstream_of(executable)

        return self

    def run(self):
        for executable in self.executable_q:
            sys.path.insert(1, self.pipeline.root_directory)
            script_exe = __import__(executable.script_path)
            data = script_exe.run(executable.context_instance, executable.received_packages)
            executable.send(data)
