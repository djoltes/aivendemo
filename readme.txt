This is Dick Joltes' Aiven demo, using Kafka and Postgres. 

File descriptions
rssMonitorAndStore.py -- the consumer, which waits in a loop for a predetermined period then takes
                         messages off the list, formats them, and inserts them into Postgres
rssProducer.py -- the producer (obviously)

The Postgres instance uses a separate database called, cleverly enough, "Demo." This contains
a table, "rssdemo," containing the following columns

date (text)
rssentry (text)
entry_date (timestamp with time zone)
id (uuid)
source (text)

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

Running it

All files can go in a single folder. 
1) Create the Kafka instance, then create a topic called RSS
2) On the Postgres side, create the Demo database and the rssdemo table using the layout noted
above. Note: when you copy the connection string for Postgres from the Aiven console, change
the name of the database from the default "defaultdb" to "Demo"
3) Replace the various key and certificate files with your own
4) Update the rssProducer.py and rssMonitorAndStore.py files with the connection strings for
your Kafka and Postgres instances
5) open two shell windows
    In one, execute 'python3 rssMonitorAndStore.py' -- it should start up and sit idly
    In the other, execute 'python rssProducer.py' -- it should start, poll for the first time,
    and note the two articles sent to the Kafka instance

Within a few seconds of the producer sending entries, the info about them should be printed in 
the consumer (monitor) shell; at the same time these entries should appear in the Postgres table.

The scripts will continue running, polling at whatever interval you've selected.
