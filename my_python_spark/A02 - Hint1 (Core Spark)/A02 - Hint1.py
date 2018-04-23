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


def extract_review_count(tupl):
    VALUE_INDEX = 1
    REVIEW_COUNT_INDEX = 0

    tuple_value = tupl[VALUE_INDEX]
    view_count = tuple_value[REVIEW_COUNT_INDEX]

    return view_count

def extract_average_views_per_cuisine(rdd):
    number_of_cuisines = rdd.count()
    review_countRDD = rdd.map(extract_review_count)
    review_count = review_countRDD.reduce(lambda x, y: x + y)
    average_reviews_per_cuisine = float(review_count) / float(number_of_cuisines)

    return average_reviews_per_cuisine


def check_enough_reviews(tupl, minimum_review_count):
    VALUE_INDEX = 1
    REVIEW_COUNT_INDEX = 0

    value_tuple = tupl[VALUE_INDEX]
    review_count = value_tuple[REVIEW_COUNT_INDEX]

    return (review_count >= minimum_review_count)


def check_threshold_bad_reviews(tupl, threshold_percentage):
    VALUE_INDEX = 1
    REVIEW_COUNT_INDEX = 0
    BAD_REVIEW_COUNT_INDEX = 1

    value_tuple = tupl[VALUE_INDEX]
    review_count = value_tuple[REVIEW_COUNT_INDEX]
    bad_review_count = value_tuple[BAD_REVIEW_COUNT_INDEX]

    percentage_bad_reviews = (float(bad_review_count) / float(review_count)) * 100

    return (percentage_bad_reviews < threshold_percentage)


def append_average_points_per_review(tupl):
    VALUE_INDEX = 1
    REVIEW_COUNT_INDEX = 0
    NEGATIVE_REVIEW_INDEX = 1
    TOTAL_SCORE_INDEX = 2

    value_tuple = tupl[VALUE_INDEX]
    review_count = value_tuple[REVIEW_COUNT_INDEX]
    negative_review_count = value_tuple[NEGATIVE_REVIEW_INDEX]
    total_score = value_tuple[TOTAL_SCORE_INDEX]

    average_score = float(total_score) / float(review_count)

    final_tuple = (review_count, negative_review_count, total_score, average_score)

    result = (tupl[0], final_tuple)

    return result

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

    combinedRDD.persist()

    average_reviews_per_cuisine = extract_average_views_per_cuisine(combinedRDD)

    filteredRDD = combinedRDD.filter(lambda tupl: check_enough_reviews(tupl, average_reviews_per_cuisine))
    filteredRDD = filteredRDD.filter(lambda tupl: check_threshold_bad_reviews(tupl, percentage_f))

    average_scoreRDD = filteredRDD.map(append_average_points_per_review)

    sortedRDD = average_scoreRDD.sortBy(lambda tupl: tupl[1][3], False)
    sortedRDD.saveAsTextFile(result_dir)



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
