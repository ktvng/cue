import sys
import getopt
import json

def run(input_json, output_filename):
    sys.path.insert(1, input_json['script_directory'])
    script_exe = __import__(input_json['script_path'])
    data = script_exe.run(input_json['params'], input_json['data'])

    if data is None:
        data = ""

    with open(output_filename, 'w') as f:
        f.write(data)

def main(argv):
    # -r <root> -p <path> -i <input> -o <output>
    try:
        opts, args = getopt.getopt(argv, "i:o:")

    except:
        exit(2)

    for opt, arg in opts:
        if opt == '-i':
            with open(arg, 'r') as f:
                input_json = json.load(f)
        elif opt == '-o':
            output_filename = arg

    run(input_json, output_filename)

if __name__ == "__main__":
    main(sys.argv[1:])
