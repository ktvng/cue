# Script Orchestrator
The Script Orchestrator is a platform which streamlines the parallelization and 
orchestration of python script files into an integrated data analysis pipeline.

A json document is used to define the structure of the pipeline, where data should be
piped to, and what parameters are required for execution of each script. In addition,
built in flattening enables scripts to be simplified into single-instance algorithms,
which can then be mapped onto all combinations of input parameters in a parallelized 
fashion to both decrease the time-to-run for a script as well as the logical complexity.

The json format allows easy reuse, and at-location execution that can start the pipeline 
at a specific entrypoint to directly rerun any individual script and to propogate the
data to downstream scripts without needing to rerun the entire pipeline from the start.

# Documentation

The Script Orchestrator is currently still in early beta, and this section will be 
updated in time.