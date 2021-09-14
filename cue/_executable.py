from __future__ import annotations
import json
import os
import hashlib
import shutil

from cue._pipes import Pipe, FilePipe


###############################################################
# Representation of a executable script; contains all required information
# to load the script, all parameters to provide the script, and all data 
# sources to feed into the script.
#
# Attributes are:
#   context_instance:   dict of parameters to supply the script
#   script_guid:        guid of script
#   script_path:        path required to import script
#   block_name:         name of block containing script
#   block_serial:       serial order of block
#   version:            version of the pipeline
#   tmp_directory:      path to the directory to store temp files
#
class Executable():
    # Initialize instance attributes of the Executable (see above) 
    def __init__(self, 
        context_instance : dict, 
        script_guid : str, 
        script_path : str, 
        block_name : str, 
        block_serial : int, 
        version : str, 
        pipeline_name : str, 
        tmp_directory : str
            ) -> None:

        self.context_instance = context_instance
        self.script_guid = script_guid
        self.script_path = script_path
        self.block_name = block_name
        self.block_serial = block_serial
        self.version = version
        self.pipeline_name = pipeline_name
        self.hash = self.calculate_hash()
        self.data_ingest_directory = tmp_directory + "/" + self.hash + "/"

        if not os.path.exists(self.data_ingest_directory):
            os.mkdir(self.data_ingest_directory)

        self.n_pipes_in = 0
        self.outgoing_pipes = []

    # True if [self] executable is still waiting for upstream data to
    # be propogated to it
    def is_waiting_for_upstream(self,
        file_pipe : FilePipe        # file_pipe 
            ) -> bool: 

        if os.path.getsize(file_pipe.into()) != 0:
            return False

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
        
        return f"{self.pipeline_name}({self.version})/{self.block_name}" \
            f"/{self.script_guid}:{self.script_path}\n" + \
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
            self.script_guid == other.script_guid

