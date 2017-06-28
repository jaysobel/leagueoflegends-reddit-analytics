from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import MySQLdb
from nltk.corpus import stopwords
import pickle

db_secrets = pickle.load(open("secret_pickle/db_secrets.pkl", "rb"))

# Setting up the Database
print "Initializing the Database Connection"

db = MySQLdb.connect(host=db_secrets['host'],
                     user=db_secrets['user'],
                     passwd=db_secrets['passwd'],
                     db=db_secrets['db'],
                     port=db_secrets['port'])
cur = db.cursor()

print "Building the text"
# Put all the titles into a big string
sql = "select distinct(post_title) from league where id between 0 and 5000;"
nrow = int(cur.execute(sql))
title_list = []
for i in range(0, nrow):
    title = cur.fetchone()[0]
    if title is None:
        continue
    else:
        title_list.append(title)

stops = set(stopwords.words("english"))
filtered_text = [word for word in title_list if word not in stops]

text = ' '.join(filtered_text)

print "Creating the wordcloud!"
# read the mask image
# taken from
mask = np.array(Image.open('mask.png'))

# use the mask image and a matplotlib colormap
# https://matplotlib.org/examples/color/colormaps_reference.html
wc = WordCloud(background_color="white", max_words=2000, mask=mask, colormap='inferno')
# generate word cloud
wc.generate(text)

# store to file
wc.to_file('visuals/wordcloud.png')

# show
plt.imshow(wc, interpolation="bilinear")
plt.axis("off")
plt.show()

db.close()