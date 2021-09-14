from __future__ import annotations

import difflib
import sys

from cue._context import ContextHelper
from cue._block import BlockHelper
from cue._errors import Errors, PipelineErrorList

###############################################################
# Encapsulates the entire script pipeline
#
# Attributes are:
#   name:               name of pipeline
#   version:            version of the current pipeline run
#   script_directory:   root directory of the scripts which should be run
#   context_json:       json representing global context for the pipeline
#   blocks_json:        json list representing all blocks of the pipeline
#   flattened_contexts: list of flattened global pipeline contexts
#   blocks:             list of <Block> entities
#   serials:            ascending ordered list of all serial values
#   plaintext:          plaintext of the json
#
class Pipeline():
    def __init__(self, 
        name : str,                 # name of pipeline
        version : str,              # version of the current pipeline run
        script_directory : str,     # root directory to find scripts
        context_json : dict,        # global context of the pipeline, json specified
        blocks_json : list,         # list of all blocks, json specified
        plaintext : str             # plaintext of the json
            ) -> None:

        self.name = name
        self.version = version
        self.script_directory = script_directory
        self.context_json = context_json
        self.flattened_contexts = ContextHelper.parse(context_json)
        self.blocks_json = blocks_json
        self.blocks = BlockHelper.parse(blocks_json)
        self.serials = self._get_block_serials()
        self.plaintext = plaintext

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

    # Validate a parsed pipeline file is formatted correctly
    def validate(self) -> PipelineErrorList:
        errors = PipelineErrorList() 
        self._validate_takes_keyword(errors)
        self._validate_script_name_uniqueness(errors)

        self._validate_module_load(errors)

        return errors

    def apply_f_over_pipeline(self, f):
        self._apply_f_over_obj(f, "", self.blocks_json)

    def _apply_f_over_obj(self, f, key, obj):
        if isinstance(obj, dict):
            for key, val in obj.items():
                self._apply_f_over_obj(f, key, val)
        elif isinstance(obj, list):
            for child_obj in obj:
                self._apply_f_over_obj(f, "", child_obj)
        elif isinstance(obj, str):
            f(key, obj)

    def _validate_script_name_uniqueness(self, errors : PipelineErrorList) -> None:
        script_names = {}
        def hash_script_names(key, val):
            if key == 'script':
                if script_names.get(val, 0) == 0:
                    script_names[val] = 1
                else:
                    script_names[val] += 1
            
        self.apply_f_over_pipeline(hash_script_names)
        for key, val in script_names.items():
            if val > 1:
                errors.add(Errors.ScriptNameCollision(
                    offending_input=key,
                    plaintext=self.plaintext))

    def _closest_keyword(self, key, other_keys):
        return difflib.get_close_matches(key, other_keys, n=1)[0]

    def _validate_takes_keyword(self, errors : PipelineErrorList) -> None:
        returns_values = {}
        def hash_returns_values(key, val):
            if key == 'returns':
                returns_values[val] = True

        takes_values = {}
        def hash_takes_values(key, val):
            if key == 'takes':
                takes_values[val] = True

        self.apply_f_over_pipeline(hash_returns_values)
        self.apply_f_over_pipeline(hash_takes_values)

        for takes_value in takes_values:
            if takes_value not in returns_values.keys():
                errors.add(Errors.ScriptInput(
                    offending_input=takes_value,
                    suggestion=self._closest_keyword(takes_value, returns_values.keys()),
                    plaintext=self.plaintext))

    def _validate_module_load(self, errors : PipelineErrorList) -> None:
        script_modules = []

        def get_script_modules(key, val):
            if key == 'path':
                script_modules.append(val)

        self.apply_f_over_pipeline(get_script_modules)

        sys.path.insert(1, self.script_directory)
        for script_path in script_modules:
            try:
                script_exe = __import__(script_path, fromlist=[None])
                if not hasattr(script_exe, 'parameters') or \
                   not (hasattr(script_exe, 'run') and callable(getattr(script_exe, 'run'))):
                        errors.add(Errors.ModuleFormatException(
                        offending_input=script_path,
                        plaintext=self.plaintext))

            except:
                errors.add(Errors.ModuleLoadException(
                    offending_input=script_path,
                    suggestion=self.script_directory,
                    plaintext=self.plaintext))
