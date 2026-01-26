# The Zenless Zone Zero (ZZZ) Character Popularity & Meta Shift Tracker

### Introduction
The Zenless Zone Zero (ZZZ) Character Popularity & Meta Shift Tracker is a data analytics project that analyzes YouTube content to quantify character popularity and detect meta shifts within Zenless Zone Zero. By leveraging the YouTube Data API v3, the project uses video metadata and engagement signals as a proxy for community interest and in-game relevance.

The system ingests YouTube data on a scheduled basis, transforms it into an analytics-ready schema, and produces time-series insights that highlight emerging trends, declining characters, and the impact of banners or patches. The project demonstrates end-to-end data pipeline design, metric modeling, and analytical reasoning using real-world, high-volume API data.

# agent aliases
why mannual entries @src/data/aliases.py?
first name is written after last name for some character
if "word1 word2" broken and searched some char share common surname (demara), would give wrong results
some known by nickname like rina and lucy
some written differently like yi xuan and yixuan

# quota refernce
https://developers.google.com/youtube/v3/determine_quota_cost
search cost 100, 10k/100 can only search 100 times a day, so discovery would run once manually
video list is cheap cost 1 credit only, will run daily for each video

### TODO
1. manage api exhaustion logic
2. error handing and checks at various places
3. need better data modelling
