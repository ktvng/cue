from __future__ import annotations

from cue._context import ContextHelper
from cue._script import ScriptHelper

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
    #
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
                block_json.get('runs', ScriptHelper.no_scripts)
            ))
        
        return blocks


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
#
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
