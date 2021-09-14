import argparse
from cue.orchestrator import Orchestrator 

def run():
    parser = argparse.ArgumentParser(description="Load and run an automated data analysis pipeline")
    parser.add_argument('-i', '--input', 
        nargs='?',   
        type=str, 
        required=True,
        help='input path to a pipeline file')

    parser.add_argument('-n', '--n_times_before_timeout',
        nargs='?', 
        type=int, 
        help='number of times before timeout')

    parser.add_argument('-w', '--waits',
        nargs='?',
        type=float,
        help='time to wait between retries')

    parser.add_argument('-p', '--cpus',
        nargs='?',
        type=int,
        help='number of processors to use')

    parser.add_argument('-f', '--from', 
        nargs='?',
        type=int,
        help='block serial number to being pipeline at')

    parser.add_argument('--clean',
        action='store_true',
        default=False,
        help='cleans cache and removes all temporary files/directories')

    parser.add_argument('-v', '--verbose',
        action='store_true',
        default=False,
        help='run verbose with status messages printed')

    parser.add_argument('-d', '--dir',
        nargs=1,
        type=str,
        help='use named path instead of default for temporary directory')

    args = parser.parse_args()
    vargs = vars(args)
    is_verbose = vargs['verbose']
    should_purge = vargs['clean']
    path_to_pipeline_json = vargs['input']
    tmp_dir = vargs['dir']

    options = { k : v for k, v in vargs.items() if v is not None }

    if should_purge:
        Orchestrator(using_directory=tmp_dir, verbose=is_verbose).purge()
        exit(0)

    so = Orchestrator(using_directory=tmp_dir, verbose=is_verbose).read(yaml_file=path_to_pipeline_json).queue_tasks().run(given=options)
    so.clean()
    print(vars(args))
