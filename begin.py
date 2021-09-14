import getopt
import sys
from orchestrator import ScriptOrchestrator

def main(argv):
    # -r <root> -i [pipeline] -c 
    try:
        opts, args = getopt.getopt(argv, "i:cn:w:p:f:", ['input=', 'n_times_before_timeout=', 'wait_time_between_tries=', 'max_processes=', 'from=', 'clean'])

    except:
        exit(2)

    options = {}
    path_to_pipeline_json = None
    purge = False

    for opt, arg in opts:
        if opt in ['-i', '--input'] :
            path_to_pipeline_json = arg
        elif opt in ['-n', '--n_times_before_timeout']:
            options['n_times_before_timeout'] = int(arg)
        elif opt in ['-w', '--wait_time_between_tries']:
            options['wait_time_between_tries'] = int(arg)
        elif opt in ['-p', '--max_processes']:
            options['max_processes'] = int(arg)
        elif opt in ['-f', '--from']:
            options['from'] = int(arg)
        elif opt in ['--clean']:
            purge = True

    if path_to_pipeline_json is None:
        print("exception: expected json filename with [-i]")
        exit(2)

    if purge:
        ScriptOrchestrator().purge()
        exit(0)

    so = ScriptOrchestrator().read(path_to_pipeline_json).queue_tasks().run(given=options)
    so.clean()
    
if __name__ == "__main__":
    main(sys.argv[1:])
