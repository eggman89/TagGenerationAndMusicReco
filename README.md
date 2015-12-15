#Tag Generation and Music Recommendation Using Spark

This project attempts to answer the following 2 questions :

1) Given the listetning habit of a user, how can we reccomend him the best songs suiable to his profile? We use a hybrid of collabarative filtering, popularity based model and content based filtering to achieve this. (RecoEngine.scala)

2)Given a set of songs and it's attributes (i.e tempo, energy, dancablity, time signature,key and 12 timbre features(avg timbre and var timbre) ) , can we predict the tag for new songs(e.g. Pop_Rock, Electronica etc..)? (tagGenerator.scala)

Currently we are able to achieve an accuracy of 70%.

The dataset has beenn acquired from MillionSongDataset, LastFM and echonest APIs.

More details would be added soon.

->Implemented hybrid of Collaborative filtering and Content-based recommendation using Mllib(logistic regression) for a detailed personalised reccomendation .

->Implemented automatic tag generation(using logistic regression, Random Forest, Naive Bayes and Decision tree) for new songs based on it's attributes.

->Provides the prediction accuracy and time taken by all the four algorithms.

The cleaned and parsed data required for this project can be downloaded from here: https://drive.google.com/a/ncsu.edu/folderview?id=0B5_HzOkbztHuMkptSzJidzl1c1k&usp=sharing

The architecture is as follows: http://imgur.com/a/0adNo

Run the following two files:

tagGenerator.scala : For testing of Genre recognisation

RecoEngine.scala : For prediction of a user (out of 120k)

Team Members: Snehasis Ghosh

Nikhil Raina
