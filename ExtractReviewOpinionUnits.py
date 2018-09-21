import sys
import configparser
import MySQLdb
from monkeylearn import MonkeyLearn


# Pass in all config using ConfigParser file
if len(sys.argv) != 2:
    print "\nError: wrong number of arguments.\n"
    print "Usage: python ExtractReviewOpinionUnits.py <config_filename>\n"
    sys.exit(2)

configfile = sys.argv[1]
config = configparser.ConfigParser()
config.read(configfile)

# Get MonkeyLearn config section
monkeylearn_config = config['MONKEY_LEARN']
ml_api_key = monkeylearn_config['api_key']
ml_ou_module_id = monkeylearn_config['opinion_unit_module_id']

# Create the MonkeyLearn object
ml = MonkeyLearn(ml_api_key)

def get_opinion_units(ml, ml_ou_module_id, data):
    #print("INSIDE")
    res = ml.extractors.extract(ml_ou_module_id, data)
    #print(type(res))
    #print(res)
    return res.body
    #print(type(res))

database_config = config['DATABASE']

#Open db connection
db = MySQLdb.connect(host=database_config['host'], user=database_config['user'], passwd=database_config['passwd'], db=database_config['db'])

# Pull a list of existing review_ids from the opinion_units table so we can skip sending those to MonkeyLearn to be
# more resource efficient and not try to insert duplicates
select_unique_review_ids = "SELECT distinct(review_id) from opinion_units"


# build SQL select to pull reviews
select_reviews = "SELECT " + database_config['review_id_col'] + ", " + database_config['review_content_col'] + " FROM " + database_config['reviews_table']


ml_out = []
## prepare a cursor
cursor = db.cursor()
cursor.execute(select_unique_review_ids)
# This fetch returns a tuple of tuples in the form ((4171L,), (4173L,), (4175L,), (4179L,))
# Convert to list for easier use later on [4171L, 4173L, 4175L, 4179L]
ou_review_ids = []
for rev_tuple in cursor.fetchall():
    ou_review_ids.append(rev_tuple[0])
#print(ou_review_ids)
#exit(2)

try:
    # Select the review content from the database
    cursor.execute(select_reviews)
    # MonkeyLearn can only take 500 data elements at a time to process. So chunk it up.
    #for i in range(2):
    i = 0
    while True:
        rows = cursor.fetchmany(100)
        if not rows:
            break
        else:
            i += len(rows)
        print("Range:  {}\n".format(i))
        # build out ML data elements into list
        ml_data = []
        for row in rows:
            if long(row[0]) not in ou_review_ids:
                ml_data.append({"external_id":str(row[0]), "text": row[1]})

        if len(ml_data) > 0:
            print("Fetching opinion units from MonkeyLearn")
            ou_out = get_opinion_units(ml, ml_ou_module_id, ml_data)
            ml_out.append(ou_out)
        #print(type(ou_out))

    #print(ml_data)
    db.commit()
except MySQLdb.OperationalError as oe:
    print "OperationalError ({0}: {1})".format(oe[0], oe[1])
    db.rollback()
except:
   # Rollback in case there is any error
   print "Unexpected error:", sys.exc_info()[0]
   db.rollback()


# Prepare SQL query to insert a record into the Database
insert_ou = "INSERT INTO opinion_units (ou_ordinal, review_id, opinion_unit) VALUES ( %d, %d, '%s' ) "

for ou_out in ml_out:
    for rev in ou_out:
        print(rev['text'])
        ou_ordinal = 0
        for x in rev['extractions']:
            ou_ordinal += 1
            #print("{} for {} ".format(x['tag_name'], rev['external_id']))
            #print("Parsed Value: " + x['parsed_value'])
            #print("Offset Span: {} - {}".format(x['offset_span'][0], x['offset_span'][1]))
            #print("\n\n")
            #print(insert_ou % (int(ou_ordinal), int(rev['external_id']), x['parsed_value']))
            #exit(2)
            filled_sql = insert_ou % (int(ou_ordinal), int(rev['external_id']), db.escape_string(unicode(x['parsed_value']).encode('utf-8')))
            try:
               # Execute the SQL command
               cursor.execute(filled_sql)

               # Commit your changes in the database
               db.commit()
            except MySQLdb.OperationalError as oe:
                print "OperationalError ({0}: {1})".format(oe[0], oe[1])
                db.rollback()
            except:
               # Rollback in case there is any error
               print(filled_sql)
               print "Unexpected error:", sys.exc_info()[0]
               db.rollback()

# disconnect from server
db.close()

# SQL to create Opinion Unit Table
# DROP TABLE IF EXISTS `opinion_units`;
# CREATE TABLE `opinion_units` (
#     `ou_ordinal` bigint(38) NOT NULL,
#     `review_id` bigint(38) NOT NULL,
#     `opinion_unit` varchar(1024) NOT NULL,
#     PRIMARY KEY ( ou_ordinal,  review_id),
#     FOREIGN KEY (review_id) REFERENCES `productreviews`(`review_id`) ON DELETE CASCADE
# ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
#
