from pathlib import Path
import re


def main():

    # these files have ASF license
    exclude_list = [
        'rust/ballista/src/memory_stream.rs',
        'rust/benchmarks/tpch/src/main.rs'
    ]

    pattern = "Licensed under the Apache License, Version 2.0"

    count = 0
    count += check('rust/**/src/**/*.rs', pattern, exclude_list)
    count += check('rust/**/examples/**/*.rs', pattern, exclude_list)
    count += check('rust/cudf-rs/**/*.hpp', pattern, exclude_list)
    count += check('rust/cudf-rs/**/*.cpp', pattern, exclude_list)
    count += check('**/*.java', pattern, exclude_list)
    count += check('**/*.scala', pattern, exclude_list)
    count += check('python/**/*.py', pattern, exclude_list)
    count += check('python/**/*.pyi', pattern, exclude_list)

    if count > 0:
        print("Found {} files missing ASL header".format(count))
        exit(-1)


def check(files, pattern, exclude_list):
    print("Checking files for ASL license: {}".format(files))
    count = 0
    for path in Path('.').rglob(files):
        if str(path) in exclude_list:
            print("Skipping ASL check for excluded file {}".format(path))
            continue

        found = False
        file = open(path, "r")
        for line in file:
            if re.search(pattern, line):
                found = True
                break
        if not found:
            print("File at {} does not contain ASL license header".format(path))
            count += 1
    return count


if __name__ == "__main__":
    main()
