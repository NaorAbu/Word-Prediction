# Word-Prediction
Predication of upcoming word (third in this case) in sentence based on Google N-Gram data base.
The program work with AWS with 4 jobs steps as follow:

ngramsJob - CountWord execute on 1gram,2gram,3gram.
splitJob - divides the triples keys to all possible variations: w1,w2,w3,w1w2,w2w3 with value as original triple and maps the triples tuples and one word with the number as they are. the reducer then after the secondary sort saves each key's value and writes to the context the triple as key with the value.
mergeJob - concatenates all the triple's keys values together.
sortJob - just for the secondary sort of values.

The job Launcher runs the job flow over AWS services.

This project made to know better AWS system and get familiar with big data programing.
