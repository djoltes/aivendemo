from time import sleep 
from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2
import uuid

## Consumer for the RSS demo feed. This connects to the Kafka instance as well as the 'rssdemo' DB 
## on Postgres, then goes into an infinite loop sleeping and waiting for new events. When one
## shows up, it splits the inbound string on the | character, then uses the two elements from 
## the result to populate the 'rssdemo' table.

## Note that since this is a demo I'm not doing any sanitizing of strings or other significant
## error checking.

## Some initial connection-based strings and other setup
serverName='kafka-236879e7-djoltes-7e49.aivencloud.com:17713'
PGuri = "postgres://avnadmin:ldevpwhpk0i8uwn2@pg-3df90951-djoltes-7e49.aivencloud.com:17711/Demo?sslmode=require"
pollInterval = 10 # poll interval in seconds

## start our consumer
consumer = KafkaConsumer(
    "RSS",
    auto_offset_reset="earliest",
    bootstrap_servers=serverName,
    client_id="demo-client-dj",
    group_id="demo-group",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="acc_cert.cert",
    ssl_keyfile="acc_key.key",
)

## Initialize a DB connection
db_conn = psycopg2.connect(PGuri)
c = db_conn.cursor()

## start polling in an infinite loop
while True:
  
  ## Get our messages and extract the payload
  messages = consumer.poll(timeout_ms=1000)
  for tp, msgs in messages.items():
    for message in msgs:
      ## Next, we need to insert the entry into our Postgres instance. The database is called Demo
      ## and the table is cleverly named "rssdemo". First we'll split the message on the "|" character
      ## so we have the elements isolated...
      messagetext = message.value.decode("utf-8")
      myList = messagetext.split("|")
      source = myList[0]
      date = myList[1]
      entry = myList[2]
      print ("Message: source %s, date %s, entry %s" % (source, date, entry))

      ## create a uuid for a key...
      myUUID = str(uuid.uuid1())
      ## Now set up the insert and execute it.
      sql = """INSERT INTO public.rssdemo (date, rssentry, entry_date, id, source)
               VALUES(%s, %s, now(), %s, %s);"""
      try: 
        c.execute(sql, (date, entry, myUUID, source))
        db_conn.commit()
      except psycopg2.DatabaseError as error:
        print(error)

  sleep(pollInterval)

