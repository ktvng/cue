from __future__ import annotations
import cue._executable as exe
import os

###############################################################
# Represents a direct pipe between two executables that can transfer data
# unidirectionally
#
# Attributes are:
#   pipe_from_obj:      the upstream executable connected by the pipe
#   to_obj:             the downstream executable connected by the pipe
#
class Pipe():
    def __init__(self,
        pipe_from_obj : exe.Executable,     # upstream executable
        to_obj : exe.Executable             # downstream executable
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
        executable : exe.Executable     # executable script which is wrapped
            ) -> None:

        self.name = tmp_directory + "cache" + executable.hash
        
        if not os.path.exists(self.into()):
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
    def clean(self,
        ) -> None:

        os.remove(self.into())
        os.remove(self.out())