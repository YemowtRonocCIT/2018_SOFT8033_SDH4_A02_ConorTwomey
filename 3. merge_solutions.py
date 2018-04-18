
# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import codecs
import random
import sys
import os

# ------------------------------------------
# FUNCTION merge_files
# ------------------------------------------
def merge_files(sol_name, dir_name):
    # 1. We create the variable to output
    res = False

    # 2. We create the solutionRDD
    content = []

    # 3. We collect all the files of the dir
    files = os.listdir(dir_name)

    # 4. We collect the content of each file
    for file_name in files:
        # 4.1. We open the file
        my_input_file = codecs.open(dir_name + file_name, "r", encoding='utf-8')

        # 4.2. We read it line by line
        for line in my_input_file:
            content.append(line)

        # 4.3. We close the input file
        my_input_file.close()

    # 5. We avoid empty time_steps: If any actual line has been read at all
    if (len(content) > 0):
        # 5.1. We open the file for writing
        my_output_file = codecs.open(dir_name + sol_name, "w", encoding='utf-8')

        # 5.2. We write the content to the file
        for line in content:
            my_output_file.write(line)

        # 5.3. We close the output file
        my_output_file.close()

        # 5.4. We increase res
        res = True

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sol_name, my_result_dir):
    # 1. We create a list with the directories we want to rename
    directories_to_rename = []

    # 2. We collect all the sub-directories names (there is one sub-directory per time_interval)
    directories = os.listdir(my_result_dir)

    # 3. We traverse each sub-directory (time_interval)
    for dir in directories:
        # 3.1. We get the name of the sub-directory
        dir_name = my_result_dir + "\\" + dir + "\\"

        # 3.2. We merge the parts of such sub-directory
        valid = merge_files(sol_name, dir_name)

        # # 3.3. If the directory is valid (contains some results), we append its name to res
        if valid:
            directories_to_rename.append(str(my_result_dir) + "\\" + str(dir))

    # 4. We traverse the directories so as to rename them
    time_step = 1
    for item in directories_to_rename:
        # 4.1. We rename the directory name
        os.rename(item, item + " (time_step " + str(time_step) + ")")

        #4.2. We increase the time step for renaming next directory
        time_step = time_step + 1


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. If we run it from command line
    if len(sys.argv) > 1:
        my_main(sys.argv[1], sys.argv[2])
    # 2. If we run it via Pycharm
    else:
        sol_name = "solution.txt"
        my_result_dir = os.getcwd() + "\\my_result"
        my_main(sol_name, my_result_dir)
