from __future__ import annotations

###############################################################
# Wrapper for methods related to contexts.
#
# A context represents a collection of key:value pairs which
# will be passed to the script as parameters. A context may also contain
# multiple values for each key (as a list of values) and will be expanded
# automatically into a list of flat contexts (one value per key) that
# encompasses all possible combinations of key:value pairs. 
#
class ContextHelper():
    # The empty context; for situations where no context is supplied by the json.
    empty_context = {}

    # Parses and flattens a [context] into list of dicts with key:value pairs
    # that represent the parameters of the flattened context
    @classmethod
    def parse(cls, 
        context : dict,         # the json defining the context
            ) -> list:          # returns alist containing simple dicts 
                                #   (vals are str/int/float...)

        flattened_contexts = [{}]
        for key, val in context.items():
            if isinstance(val, list):
                flattened_contexts = cls. \
                    expand_parameter_list_into(flattened_contexts, key, val)
            elif isinstance(val, dict):
                flattened_contexts = cls. \
                    expand_parameter_range_into(flattened_contexts, key, val)
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