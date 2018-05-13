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

import time
from pyspark.streaming import StreamingContext
import json

DUMMY_KEY = 'dummy'


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
    negative_review_count = int(accumulator[NEGATIVE_REVIEW_INDEX])
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


def extract_average_views_per_cuisine(stream):
    number_of_cuisines = stream.count()
    review_count_stream = stream.map(extract_review_count)
    review_count = review_count_stream.reduce(lambda x, y: x + y)
    average_reviews_per_cuisine = review_count / number_of_cuisines

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


def to_review_count_tuple(tupl):
    VALUE_INDEX = 1
    REVIEW_COUNT_INDEX = 0

    tuple_value = tupl[VALUE_INDEX]
    review_count = tuple_value[REVIEW_COUNT_INDEX]

    return (DUMMY_KEY, review_count)


def reformat_joined_rdd(tupl):
    # Sample: ('dummy', ( (u'Thai', (1, 0, 9)), (37, 253) ))
    working_tuple = tupl[1]

    ORIGINAL_TUPLE_INDEX = 0
    AVERAGE_COUNT_INDEX = 1

    original_tuple = working_tuple[ORIGINAL_TUPLE_INDEX]
    total_count_tuple = working_tuple[AVERAGE_COUNT_INDEX]
    average_review_count = float(total_count_tuple[1]) / float(total_count_tuple[0])

    CUISINE_INDEX = 0
    VALUES_INDEX = 1
    cuisine = original_tuple[CUISINE_INDEX]
    cuisine_values = original_tuple[VALUES_INDEX]

    REVIEW_COUNT_INDEX = 0
    NEGATIVE_REVIEW_COUNT_INDEX = 1
    TOTAL_SCORE_INDEX = 2
    cuisines_review_count = cuisine_values[REVIEW_COUNT_INDEX]
    cuisines_negative_review_count = cuisine_values[NEGATIVE_REVIEW_COUNT_INDEX]
    cuisines_total_score = cuisine_values[TOTAL_SCORE_INDEX]

    points_per_review = float(cuisines_total_score) / float(cuisines_review_count)
    values_tuple = (cuisines_review_count, cuisines_negative_review_count, cuisines_total_score, points_per_review,
                    average_review_count)

    return (cuisine, values_tuple)


def remove_unwanted_cuisines(tupl, percentage_f):
    VALUES_INDEX = 1
    value_tuple = tupl[VALUES_INDEX]

    REVIEW_COUNT_INDEX = 0
    NEGATIVE_REVIEW_COUNT_INDEX = 1
    AVERAGE_REVIEW_COUNT_INDEX = 4

    review_count = value_tuple[REVIEW_COUNT_INDEX]
    negative_review_count = value_tuple[NEGATIVE_REVIEW_COUNT_INDEX]
    average_review_count = value_tuple[AVERAGE_REVIEW_COUNT_INDEX]

    if review_count < average_review_count:
        return False

    percent_bad_reviews = (float(negative_review_count) / float(review_count)) * 100
    if percent_bad_reviews > percentage_f:
        return False

    return True


def cleanup_tuple(tupl):
    CUISINE_INDEX = 0
    VALUES_INDEX = 1
    cuisine = tupl[CUISINE_INDEX]
    value_tuple = tupl[VALUES_INDEX]

    REVIEW_COUNT_INDEX = 0
    NEGATIVE_REVIEW_COUNT_INDEX = 1
    SCORE_INDEX = 2
    AVERAGE_POINTS_PER_REVIEW_INDEX = 3

    review_count = value_tuple[REVIEW_COUNT_INDEX]
    negative_review_count = value_tuple[NEGATIVE_REVIEW_COUNT_INDEX]
    score = value_tuple[SCORE_INDEX]
    average_points_per_review = value_tuple[AVERAGE_POINTS_PER_REVIEW_INDEX]

    final_value_tuple = (review_count, negative_review_count, score, average_points_per_review)

    return (cuisine, final_value_tuple)


# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc, monitoring_dir, result_dir, percentage_f, window_duration, sliding_duration):
    input_stream = ssc.textFileStream(monitoring_dir)
    input_stream = input_stream.window(window_duration * time_step_interval,
                                       sliding_duration * time_step_interval)

    mapped_stream = input_stream.map(lambda line: json.loads(line))
    mapped_stream = mapped_stream.map(set_key_value)

    combined_stream = mapped_stream.combineByKey(initialise_accumulator,
                                                 add_new_tuple_to_accumulator,
                                                 merge_accumulators)

    total_reviews_stream = combined_stream.map(to_review_count_tuple)
    total_reviews = total_reviews_stream.combineByKey(lambda x: x,
                                                      lambda x, y: x + y,
                                                      lambda x, y: x + y)

    number_of_cuisines = combined_stream.countByWindow(window_duration * time_step_interval,
                                                       sliding_duration * time_step_interval)

    number_of_cuisines_stream = number_of_cuisines.map(lambda x: (DUMMY_KEY, x))
    combined_cuisines_reviews_stream = number_of_cuisines_stream.join(total_reviews)

    combined_stream = combined_stream.map(lambda x: (DUMMY_KEY, x))
    joined_stream = combined_stream.join(combined_cuisines_reviews_stream)

    formatted_stream = joined_stream.map(reformat_joined_rdd)

    formatted_stream = formatted_stream.filter(lambda tupl: remove_unwanted_cuisines(tupl, percentage_f))
    formatted_stream = formatted_stream.map(cleanup_tuple)

    formatted_stream.saveAsTextFiles(result_dir)
    
# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(monitoring_dir,
               result_dir,
               max_micro_batches,
               time_step_interval,
               percentage_f,
               window_duration,
               sliding_duration):
    # 1. We create the new Spark Streaming context.
    # This is the main entry point for streaming functionality. It requires two parameters:
    # (*) The underlying SparkContext that it will use to process the data.
    # (**) A batch interval, specifying how often it will check for the arrival of new data,
    # so as to process it.
    ssc = StreamingContext(sc, time_step_interval)

    # 2. We configure the maximum amount of time the data is retained.
    # Think of it: If you have a SparkStreaming operating 24/7, the amount of data it is processing will
    # only grow. This is simply unaffordable!
    # Thus, this parameter sets maximum time duration past arrived data is still retained for:
    # Either being processed for first time.
    # Being processed again, for aggregation with new data.
    # After the timeout, the data is just released for garbage collection.

    # We set this to the maximum amount of micro-batches we allow before considering data
    # old and dumping it times the time_step_interval (in which each of these micro-batches will arrive).
    ssc.remember(max_micro_batches * time_step_interval)

    # 3. We model the ssc.
    # This is the main function of the Spark application:
    # On it we specify what do we want the SparkStreaming context to do once it receives data
    # (i.e., the full set of transformations and ouptut operations we want it to perform).
    my_model(ssc, monitoring_dir, result_dir, percentage_f, window_duration, sliding_duration)

    # 4. We return the ssc configured and modelled.
    return ssc


# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(source_dir, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)
        if verbose == True:
            print(file_name)

        # 3.2. We look for the pattern name= to remove all useless info from the start
        lb_index = file_name.index("name=u'")
        file_name = file_name[(lb_index + 7):]

        # 3.3. We look for the pattern ') to remove all useless info from the end
        ub_index = file_name.index("',")
        file_name = file_name[:ub_index]

        # 3.4. We append the name to the list
        res.append(file_name)
        if verbose == True:
            print(file_name)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose):
    # 1. We get the names of the files on source_dir
    files = get_source_dir_file_names(source_dir, verbose)

    # 2. We simulate the dynamic arriving of such these files from source_dir to dataset_dir
    # (i.e, the files are moved one by one for each time period, simulating their generation).
    for file in files:
        # 2.1. We copy the file from source_dir to dataset_dir
        dbutils.fs.cp(source_dir + file, monitoring_dir + file, False)

        # 2.2. We wait the desired transfer_interval
        time.sleep(time_step_interval)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay
            ):
    # 1. We setup the Spark Streaming context
    # This sets up the computation that will be done when the system receives data.
    ssc = StreamingContext.getActiveOrCreate(checkpoint_dir,
                                             lambda: create_ssc(monitoring_dir,
                                                                result_dir,
                                                                max_micro_batches,
                                                                time_step_interval,
                                                                percentage_f,
                                                                window_duration,
                                                                sliding_duration
                                                                )
                                             )

    # 2. We start the Spark Streaming Context in the background to start receiving data.
    # Spark Streaming will start scheduling Spark jobs in a separate thread.

    # Very important: Please note a Streaming context can be started only once.
    # Moreover, it must be started only once we have fully specified what do we want it to do
    # when it receives data (i.e., the full set of transformations and ouptut operations we want it
    # to perform).
    ssc.start()

    # 3. As the jobs are done in a separate thread, to keep our application (this thread) from exiting,
    # we need to call awaitTermination to wait for the streaming computation to finish.
    ssc.awaitTerminationOrTimeout(time_step_interval)

    if (race_conditions_extra_delay == True):
        time.sleep((sliding_duration - 1) * time_step_interval)

        # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose)

    # 5. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the Spark Streaming Context
    ssc.stop(stopSparkContext=False)

    # 6. Extra security stop command: It acts directly over the Java Virtual Machine,
    # in case the Spark Streaming context was not fully stopped.

    # This is crucial to avoid a Spark application working on the background.
    # For example, Databricks, on its private version, charges per cluster nodes (virtual machines)
    # and hours of computation. If we, unintentionally, leave a Spark application working, we can
    # end up with an unexpected high bill.
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input source folder (static dataset),
    # monitoring folder (dynamic dataset simulation) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    monitoring_dir = "/FileStore/tables/A02/my_monitoring/"
    checkpoint_dir = "/FileStore/tables/A02/my_checkpoint/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 16

    # 3. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 10

    # 4. We specify the maximum amount of micro-batches that we want to allow before considering data
    # old and dumping it.
    max_micro_batches = dataset_micro_batches + 1

    # 5. We configure verbosity during the program run
    verbose = False

    # 6. Extra input arguments
    percentage_f = 10
    window_duration = 4
    sliding_duration = 4

    # 6.4. RACE Conditions: Discussed above. Basically, in which moment of the sliding_window do I want to start.
    # This performs an extra delay at the start of the file transferred to sync SparkContext with file transferrence.
    race_conditions_extra_delay = True

    # 7. We remove the monitoring and output directories
    dbutils.fs.rm(monitoring_dir, True)
    dbutils.fs.rm(result_dir, True)
    dbutils.fs.rm(checkpoint_dir, True)

    # 8. We re-create them again
    dbutils.fs.mkdirs(monitoring_dir)
    dbutils.fs.mkdirs(result_dir)
    dbutils.fs.mkdirs(checkpoint_dir)

    # 9. We call to my_main
    my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay
            )
