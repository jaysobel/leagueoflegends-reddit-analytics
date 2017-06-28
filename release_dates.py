import pandas as pd
import pickle

release_dates = pickle.load(open('pkls/release_dates.pkl', 'rb'))

df = pd.DataFrame(data=None, columns=['champion', 'date'])

# dictionary -> data frame
for key in release_dates.keys():
    value = release_dates[key]
    row = [key, value]

    df.loc[-1] = row
    df.index = df.index + 1

# convert to a datetime format
df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
# sort and re-index in order of release (increment for 1-indexing)
df = df.sort_values(by='date', ascending=True)
df = df.reset_index(drop=True)
df.index = df.index + 1

print df
