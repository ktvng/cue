from __future__ import annotations
from cue._context import ContextHelper

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
    #   "takes":        optional <str>, the description of the return value for the 
    #                       (upstream) script from where data should be piped from
    #   "returns":      required <str>, description of the return value for this
    #                       script
    @classmethod
    def parse(self, 
        scripts_json : list         # list of dicts with key:value pairs 
                                    # specifying script properties
            ) -> list:              # returns a list of script objects
        scripts = []
        for script_json in scripts_json:
            scripts.append(Script(
                script_json['script'],
                script_json['path'],
                script_json.get('context', ContextHelper.empty_context),
                script_json['returns'],
                script_json.get('takes', None)
            ))
        
        return scripts


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
#   guid:               globally unique identifier
#   path:               path to import script via `import` or `__import__`
#   takes:              description of return value of script which is upstream of 
#                           [self] and which will send data to [self]
#   returns:            description of the return value of [self]
#   context_json:       json specifing the script level context
#   flattened_contexts: list of all flat contexts which this script should be
#                           run using
#
class Script():
    def __init__(self,
        guid : str,             # globally unique identifier 
        path : str,             # path to import script via `__import__` 
        context_json : dict,    # dict of key:value parameters for script
        returns : str,          # description of data returned by script
        takes : str = None      # description of data taken by script
            ) -> None:
            
        self.name = "name"
        self.guid = guid
        self.path = path
        self.context_json = context_json
        self.flattened_contexts = ContextHelper.parse(context_json)
        self.returns = returns
        self.takes = takes
