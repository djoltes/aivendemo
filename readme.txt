This is Dick Joltes' Aiven demo, using Kafka and Postgres. 

File descriptions
rssMonitorAndStore.py -- the consumer, which waits in a loop for a predetermined period then takes
                         messages off the list, formats them, and inserts them into Postgres
rssProducer.py -- the producer (obviously)

I've included the certificate and key files, though obviousy they'd only work for my instance.

I decided to be a bit more elaborate (and was also having fun) so what the producer does is to
monitor two RSS feeds, one from the NYT and another from BBC, polling each one every hour (it's 
settable via the sleepTime variable) to grab element 0 off the page. From this, we get the 
time the article was inserted along with its summary; these are then packaged into a single string
along with the source (NYT or BBC) using the | character as a delimiter and sent off to the 
Kafka instance.

The consumer also listens in a loop, polling occasionally and traversing the message list. For
each message, the content is extracted, split, then the elements are used for a SQL insert against
the Postgres DB. For completeness, each entry gets its own UUID as a primary key.

I didn't go to the bother of checking for duplicates, but obviously this could be done as well.

