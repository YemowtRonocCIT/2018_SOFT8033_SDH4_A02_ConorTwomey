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

import json


def set_key_value(dictionary):
    CUISINE_KEY = 'cuisine'
    POINTS_KEY = 'points'
    EVALUATION_KEY = 'evaluation'

    cuisine = dictionary[CUISINE_KEY]
    evaluation = dictionary[EVALUATION_KEY]
    points = dictionary[POINTS_KEY]
    result = (cuisine, (evaluation, points))

    return result


def initialise_accumulator(tupl):
    EVALUATION_INDEX = 0
    POINTS_INDEX = 1
    NEGATIVE_REVIEW = 'Negative'

    review_count = 1
    negative_review_count = 0

    if tupl[EVALUATION_INDEX] == NEGATIVE_REVIEW:
        negative_review_count += 1

    points = tupl[POINTS_INDEX]

    points_tuple = (review_count, negative_review_count, points)
    return points_tuple


def add_new_tuple_to_accumulator(accumulator, new_tuple):
    REVIEW_INDEX = 0
    NEGATIVE_REVIEW_INDEX = 1
    POINTS_INDEX = 2
    NEW_EVALUATION_INDEX = 0
    NEW_POINTS_INDEX = 1
    NEGATIVE_REVIEW = 'Negative'

    review_count = accumulator[REVIEW_INDEX]
    negative_review_count = accumulator[NEGATIVE_REVIEW_INDEX]
    points = accumulator[POINTS_INDEX]

    review_count += 1
    if new_tuple[NEW_EVALUATION_INDEX] == NEGATIVE_REVIEW:
        negative_review_count += 1
        points -= new_tuple[NEW_POINTS_INDEX]
    else:
        points += new_tuple[NEW_POINTS_INDEX]

    result = (review_count, negative_review_count, points)
    return result


def merge_accumulators(accumulator, merging_accumulator):
    zipped_tuples = zip(accumulator, merging_accumulator)
    mapped_tuples = map(sum, zipped_tuples)
    final_tuple = tuple(mapped_tuples)

    return final_tuple

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
    inputRDD = sc.textFile("%s/*.json" % (dataset_dir, ))
    dictRDD = inputRDD.map(lambda line: json.loads(line))
    key_valueRDD = dictRDD.map(set_key_value)

    combinedRDD = key_valueRDD.combineByKey(initialise_accumulator,
                                            add_new_tuple_to_accumulator,
                                            merge_accumulators)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)
