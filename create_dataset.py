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

import pymongo
import json

# ------------------------------------------
# FUNCTION mongo_db_get_reviews_in_order
# ------------------------------------------
def mongo_db_get_reviews_in_order(db):
    # 1. We create the variable to output
    res = []

    # 2. We create the pipeline for triggering the query
    pipeline = []

    # 3 Command 1: We unwind based on the grades
    pipeline.append({"$unwind" : "$grades" } )

    # 4. Command 2: We filter just the ones with grade 'A', 'B' or 'Not Yet Graded'
    pipeline.append({"$match": { "$or": [{"grades.grade": 'A'}, {"grades.grade": 'B'}] } } )

    # 5. Command 3: We re-format the documents
    pipeline.append({"$project": {"_id": 0,
                                   "restaurant_id": 1,
                                   "name": 1,
                                   "borough": 1,
                                   "cuisine": 1,
                                   "date": "$grades.date",
                                   "evaluation": "$grades.grade",
                                   "points": "$grades.score"}})

    # 6. We sort the documents by date
    pipeline.append({ "$sort" : { "date" : 1 } })

    # 7. We trigger the query
    res = list(db.restaurants.aggregate(pipeline))

    # 8. We post-process the document
    for review in res:
        # We modify the date, so as to be of type timestamp
        review["date"] = int(review["date"].timestamp())

        # 8.2. We modify the value of the field type, so as to be positive or negative
        if review["evaluation"] == "A":
            review["evaluation"] = "Positive"
        else:
            review["evaluation"] = "Negative"

    # 9. We return res
    return res

# ------------------------------------------
# FUNCTION find_interval_index
# ------------------------------------------
def find_interval_index(value, intervals, size):
    # 1. We create the variable to output
    res = size

    # 2. We compute the size of intervals
    index = 0

    # 3. We traverse the intervals, looking for the right one
    while (index < size):
        # 3.1. If value is smaller than the interval_lb
        if (value < intervals[index]):
            # 3.1.1. We assign the result to the previous index
            res = index - 1
            # 3.1.2. We close the loop
            index = size
        # 3.2. Otherwise, we try with the next index
        else:
            index = index + 1

    # 4. We update res in case it was not in any interval
    if res == size:
        res = -1

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION classify_reviews
# ------------------------------------------
def classify_reviews(reviews, intervals):
    # 1. We create the variable to output
    res = []

    # 2. We initialise res with no documents so far
    size = len(intervals)
    for index in range(size-1):
        res.append([])

    # 3. We classify the reviews
    for document in reviews:
        # 3.1. We get the index the document belongs to
        index = find_interval_index(document["date"], intervals, size)

        # 3.2. If the document is in a valid interval
        if index != -1:
            res[index].append(document)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION num_digits_for_file_name
# ------------------------------------------
def num_digits_for_file_name(size):
    # 1. We create the variable to output
    res = 0

    # 2. We compute the amount of digits
    while size > 0:
        size = size // 10
        res = res + 1

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION get_file_name
# ------------------------------------------
def get_file_name(name, num_digits, file_index, ext):
    # 1. We create the output variable
    res = ""

    # 2. We turn the file_index into a String
    sub_index = str(file_index)

    # 3. We create as many 0's as needed
    str_pad = "" + ("0" * num_digits)
    str_pad = str_pad[0:num_digits - len(sub_index)]

    # 4. We compute the name of the file
    res = name + "_" + str_pad + sub_index + "." + ext

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION print_documents
# ------------------------------------------
def print_documents(documents, file_name):
    # 1. We open the file for writing
    my_file = open(file_name, 'w')

    # 2. We traverse the documents, to dump them to the file
    for doc in documents:
        # 2.1. We print the file
        json.dump(doc, my_file)

        # 2.2. We print the end of line character
        my_file.write('\n')

    # 3. We close the file
    my_file.close()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------/
def my_main(date_intervals, name, ext):
    # 0. We set up the connection to the cluster
    client = pymongo.MongoClient()
    db = client.test

    # 1. We query the MongoDB cluster so as to get the review documents in the format we want
    reviews = mongo_db_get_reviews_in_order(db)

    # 2. We classify the documents
    reviews_per_dates_interval = classify_reviews(reviews, date_intervals)

    # 3. We print these documents to files

    # 3.1. We compute the number of digits we need for file name
    size = len(reviews_per_dates_interval)
    num_digits = num_digits_for_file_name(size)

    for index in range(size):
        # 3.1. We get the interval
        interval_documents = reviews_per_dates_interval[index]

        # 3.2. We get the name of the file
        file_name = get_file_name(name, num_digits, index, ext)

        # 3.3. We print the documents to the file
        print_documents(interval_documents, file_name)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We work with years 2011, 2012, 2013, 2014
    # For each year we consider each quarter (3 months).
    # Thus this list represents the timestamps [01/01/2011, 01/04/2011, 01/07/2011, ..., 01/10/2014, 01/01/2015]
    date_intervals = [1293840000, 1301616000, 1309478400, 1317427200,
                      1325376000, 1333238400, 1341100800, 1349049600,
                      1356998400, 1364774400, 1372636800, 1380585600,
                      1388534400, 1396310400, 1404172800, 1412121600,
                      1420070400 # This last value is the ub, so as to filter any date bigger than this
                     ]

    # 2. We also get the name of the files and their extension
    name = "my_file"
    ext = "json"

    # 3. We call to my_main to create the dataset
    my_main(date_intervals, name, ext)
