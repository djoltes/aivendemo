from time import sleep
from json import dumps
from kafka import KafkaProducer
import feedparser


## This uses an RSS feedparser to grab the first event from the BBC and NYT feed. It then sleeps for
## a configured time (currently 4 hours) then polls again, and pushes the next event up to Kafka
## Just for fun, we extract two elements from the feed (published datestamp and summary), then
## concatenate them together as a single message before sending them. The client parses the
## result, extracts the two elements, and writes them to a Postgres DB (database name "Demo").

## More info in the client code.

serverName = 'kafka-236879e7-djoltes-7e49.aivencloud.com:17713'
NewsFeed = feedparser.parse("http://feeds.bbci.co.uk/news/rss.xml") ## our feed...
NewsFeed2 = feedparser.parse("https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml")
sleepTime = 1 # value in hours

producer = KafkaProducer(
    bootstrap_servers=serverName,
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="acc_cert.cert",
    ssl_keyfile="acc_key.key",
)

while True:
  entryBBC = NewsFeed.entries[0]
  entryNYT = NewsFeed2.entries[0]

  ## Get our BBC event elements
  myDateBBC = entryBBC.published
  mySummaryBBC = entryBBC.summary

  ## Same for the NYT
  myDateNYT = entryNYT.published
  mySummaryNYT = entryNYT.summary

  ## For the demo, we'll concatenate the date & string together. They can be split at the monitor end
  eventBBC = "BBC|%s|%s" % (myDateBBC, mySummaryBBC)
  eventNYT = "NYT|%s|%s" % (myDateNYT, mySummaryNYT)
  fmtEventBBC = eventBBC.encode("utf-8") 
  fmtEventNYT = eventNYT.encode("utf-8") 
  producer.send("RSS", fmtEventBBC)
  producer.send("RSS", fmtEventNYT)
  ## Sleep for the configured time in hours, then check the feed again
  sleep(3600 * sleepTime) 



