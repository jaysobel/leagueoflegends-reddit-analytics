import pickle
import praw
import math
import MySQLdb
import unicodedata
import time

# Loading pickles
search_list = pickle.load(open('pkls/champ_list.pkl', 'rb'))
alias_list = pickle.load(open('pkls/champ_aliases.pkl', 'rb'))
rdt_secrets = pickle.load(open("secret_pickle/sec_dict.pkl", "rb"))
db_secrets = pickle.load(open("secret_pickle/db_secrets.pkl", "rb"))

# character encodings
def asciify(st):
    if type(st) == unicode:
        st = unicodedata.normalize('NFKD', st).encode('ascii', 'ignore').strip()
    else:
        st = unicode(st, 'ascii', 'ignore')
        st = unicodedata.normalize('NFKD', st).encode('ascii', 'ignore').strip()
    return st


# Set up the Reddit API
print "Setting up Reddit API!"
reddit = praw.Reddit(client_id=rdt_secrets['client_id'],
                     client_secret=rdt_secrets['secret'],
                     user_agent=rdt_secrets['agent'])
subreddit = reddit.subreddit('leagueoflegends')

# Setting up the Database
print "Initializing the Database Connection"

db = MySQLdb.connect(host=db_secrets['host'],
                     user=db_secrets['user'],
                     passwd=db_secrets['passwd'],
                     db=db_secrets['db'],
                     port=db_secrets['port'])
cur = db.cursor()

key_label = 'champion'
key_limit = 50

# SQL table creation meta info
meta_table = {key_label:
                  {'type': 'varchar', 'limit': key_limit},
              'post_title':
                  {'api_alias': 'title',            'type': 'varchar', 'limit': 301},
              'post_name':
                  {'api_alias': 'name',             'type': 'varchar', 'limit': 15},
              'domain':
                  {'api_alias': 'domain',           'type': 'varchar', 'limit': 50},
              'flair':
                  {'api_alias': 'link_flair_text',  'type': 'varchar', 'limit': 50},
              'score':
                  {'api_alias': 'score',            'type': 'int'},
              'created_utc':
                  {'api_alias': 'created_utc',      'type': 'int'},
              'date':
                  {'type': 'datetime'}}

INIT_TABLE = False
# Chunk of code to delete the table and re-create it
if INIT_TABLE:
    print "DROPPING AND REMAKING THE TABLE"
    cur.execute("DROP TABLE IF EXISTS LEAGUE")
    # construct the CREATE TABLE query
    sql = """CREATE TABLE LEAGUE (ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,"""
    # unpack the meta table
    for key in meta_table.keys():
        # Create the SQL create table
        typo = meta_table[key]['type']
        col_name = key.upper()
        if typo == 'varchar':
            limit = meta_table[key]['limit']
            sql = sql + " " + col_name + " " + typo + "(" + str(limit) + ")" + ","
        else:
            sql = sql + " " + col_name + " " + typo + ","
    sql = sql[:-1] + ")"
    cur.execute(sql)


# Setting up the querying, unpacking and sql insertion loop
print "Entering the Hyperloop"

# Epoch calculations to set up the sliding-window API query
start, end = 1356998400, 1498184396
year_len = 31622400
frames_per_year = 4.0
frame_width = int(math.ceil(year_len / frames_per_year))
interval_count = int(math.ceil((end - start) / float(frame_width)))

# Iterate the loaded search terms
for term in search_list:
    # initialize the sliding window, and cloud-search extra_query
    frame_end = start - 1
    term = term.lower()

    # Set the query to use additional alias term if they exist for the term
    if not(alias_list.get(term, None) is None):
        title_query = "title:'" + term + "|"
        for alias in alias_list[term]:
            title_query = title_query + alias + "|"
        title_query = title_query[0:-1] + "'"
    else:
        title_query = "title:" + "'" + term + "'"

    print "Search Parameter: " + str(title_query)

    # Iterate the sliding windows
    for i in range(interval_count):
        frame_start = frame_end + 1
        frame_end = frame_end + frame_width
        # Iterate the API responses
        for submission in subreddit.submissions(start=frame_start, end=frame_end, extra_query=title_query):
            if submission != 0:
                # copy over the dictionary structure (keys kept, values overwritten)
                row = meta_table.copy()
                row[key_label] = term
                # For each of the column names in the SQL Table
                for field in row.keys():
                    # if the column comes straight from the API
                    api_field = meta_table[field].get('api_alias', None)
                    if not(api_field is None):
                        # Treat ints for SQL
                        if meta_table[field]['type'] == 'int':
                            val = getattr(submission, api_field)
                            if val is None:
                                val = 0
                            val = int(val)
                        # Treat strings for SQL
                        if meta_table[field]['type'] == 'varchar':
                            val = getattr(submission, api_field)
                            if val is None:
                                val = "NA"
                            else:
                                val = asciify(val)
                                lim = meta_table[field]['limit'] - 1
                                val = val[0:lim]
                        # set the calculated SQL formatted date
                        if api_field == 'created_utc':
                            sql_date = time.gmtime(val)
                            sql_date = time.strftime('%Y-%m-%d %H:%M:%S', sql_date)
                            row['date'] = sql_date
                        row[field] = val

                # create the SQL insert statement
                sql = ("INSERT INTO LEAGUE " + str(tuple(row.keys())) + " VALUES ").replace("'", "")
                # format the string "(%s, %s ... %s, %s)"
                item_count = len(row.values())
                val_str = ("(" + ("%s, " * item_count))[0:-2] + ")"
                sql = sql + val_str
                # perform the insert
                cur.execute(sql, tuple(row.values()))
                db.commit()
            # no posts in API response
            else:
                continue
    print "Finished " + str(term)

# disconnect from the database
db.close()
