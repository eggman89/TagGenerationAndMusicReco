
#Tag Generation and Music Recommendation 
##Using Apache Spark 

This project attempts to answer the following 2 questions :

**1. Given the listetning habit of a user, how can we reccomend him the best songs suiable to his profile? This project uses a hybrid of collabarative filtering, popularity based model and content based filtering to achieve this. ** (RecoEngine.scala)

The project assumes that we have user listening habits of all songs till 2008 and the songs post 2009 have not been listened enough. (Although thatt's not the case).

  1. collabarative filtering : This project loads the data of 120k users (48 million plays with play count) and trains the MLlib collabarative filtering model using this. Using this model, we predict the songs user might like more.
Dataset:http://labrosa.ee.columbia.edu/millionsong/tasteprofile

  2. popularity based model: We find the most similar songs (built by last.fm) and reccomend the closest songs to to the user listened songs.
Dataset: http://labrosa.ee.columbia.edu/millionsong/lastfm
(direct: http://labrosa.ee.columbia.edu/millionsong/sites/default/files/lastfm/lastfm_tags.db)

The first two methods are good for songs pre 2008, but this model won't predict newly released songs (post 2009).

  3. content based filtering : to overcome this restriction, we use use the attributes of the top songs in the first two model, train logistic regression model and gives the reccomendation.

For all the above three model, we assign a score to the songs. We then sort the songs by the score and give the final output.
The architecture is as follows: http://imgur.com/a/0adNo

**Q2. Given a set of songs and it's attributes (i.e tempo, energy, dancablity, time signature,key and 12 timbre features(avg timbre and var timbre) ) , can we predict the tag for new songs(e.g. Pop_Rock, Electronica etc..)? **(tagGenerator.scala)

Currently we are able to achieve an accuracy of 70% with Random Forest.

Dataset:
http://labrosa.ee.columbia.edu/millionsong/blog/11-2-28-deriving-genre-dataset
The cleaned and parsed data required for this project can be downloaded from here: https://drive.google.com/a/ncsu.edu/folderview?id=0B5_HzOkbztHuMkptSzJidzl1c1k&usp=sharing

Run the following two files:

tagGenerator.scala : For testing of Genre recognisation

RecoEngine.scala : For prediction of a user (out of 120k)

Team Members: Snehasis Ghosh

Nikhil Raina
