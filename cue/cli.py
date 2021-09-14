import getopt
import sys
from cue.orchestrator import Orchestrator 

def run():
    main(sys.argv[1:])

def main(argv):
    # -r <root> -i [pipeline] -c 
    try:
        opts, args = getopt.getopt(argv, "i:cn:w:p:f:d:vh", 
            [
                'input=', 
                'n_times_before_timeout=', 
                'wait_time_between_tries=', 
                'max_processes=', 
                'from=', 
                'clean', 
                'dir=', 
                'verbose',
                'help'
            ])

    except:
        exit(2)

    options = {}
    path_to_pipeline_json = None
    purge = False
    tmp_dir = None
    verbose = False

    for opt, arg in opts:
        if opt in ['-i', '--input'] :
            path_to_pipeline_json = arg
        elif opt in ['-n', '--n_times_before_timeout']:
            options['n_times_before_timeout'] = int(arg)
        elif opt in ['-w', '--wait_time_between_tries']:
            options['wait_time_between_tries'] = float(arg)
        elif opt in ['-p', '--max_processes']:
            options['max_processes'] = int(arg)
        elif opt in ['-f', '--from']:
            options['from'] = int(arg)
        elif opt in ['--clean']:
            purge = True
        elif opt in ['-d', '--dir']:
            tmp_dir = arg + "/"
        elif opt in ['-v', '--verbose']:
            verbose = True
        elif opt in ['-h', '--help']:
            # TODO: implement
            exit(0)

    if path_to_pipeline_json is None:
        print("exception: expected json filename with [-i]")
        exit(2)

    if purge:
        Orchestrator(using_directory=tmp_dir, verbose=verbose).purge()
        exit(0)

    so = Orchestrator(using_directory=tmp_dir, verbose=verbose).read(yaml_file=path_to_pipeline_json).queue_tasks().run(given=options)
    so.clean()