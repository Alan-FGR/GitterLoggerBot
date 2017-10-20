import requests, json, Queue, dateutil.parser, pprint
import MySQLdb as mdb

# CONFIGS
GITTER_USER_TOKEN = open('token.txt').readline() #could just paste it here in a string
GITTER_ROOM_NAME = "gitterHQ/sandbox"#"Matt Benic"
# GITTER_ROOM_NAME = "Matt Benic"
MESSAGE_FETCH_BATCH = 3 # batch size, max=100

MYSQL_DATABASE_HOST = 'localhost'
MYSQL_DATABASE_NAME = 'logdb'
MYSQL_DATABASE_USER = 'root'
MYSQL_DATABASE_PASS = ''

class LoggerObject(object):

    # STATICS - is this pythonic? =/
    @staticmethod
    def parseMessage(message):
        parsed = {
            "id": message['id'],
            "date": LoggerObject.dateStrToInt(message['sent']),
            #dateutil.parser.parse(message['sent']),
            "text": message['text'],
            "html": message['html'],
            "user_id": message['fromUser']['id'],
            "user_name": message['fromUser']['username'],
            "user_display": message['fromUser']['displayName'],
            "urls": [x[1] for y in message['urls'] for x in y.iteritems()]
        }
        return parsed

    @staticmethod
    def dateStrToInt(d):
        return int(d[:4] + d[5:7] + d[8:10] + d[11:13] + d[14:16] + d[17:19])

    # CTOR AND OTHER STUFF
    def __init__(self, token, room_name):

        try:
            self.last_stored_message_date = int(open('last_date.txt').readline())
        except:
            print "Impossible to determine last stored message, to dump" \
                  "all the logs please do an 'echo \"0\" > last_date' before" \
                  "running the program. This is a safety measure."
            quit()

        self.room_name = room_name

        self.header = {
            "Content-Type":  "application/json; charset=utf-8",
            "Accept":        "application/json",
            "Authorization": "Bearer " + token
        }

        self.room_id = None

        #get room id
        rooms = self.getRestData("rooms")
        for room in rooms:
            if room["name"] == room_name:
                self.room_id = room["id"]
                break
        if self.room_id is None:
            print "Couldn't find room id, please make sure user is joined to the room."
            quit()

        #currently we just don't write immediately
        #TODO: write immediately but also check edits and update if necessary
        self.buffer = Queue.Queue()

        self._initDB()


    #TURNKEY METHOD
    def StartLogging(self):
        print "resyncing DB"
        #self.updateDB()
        print "starting streaming"
        self.streamToDB()

    # POPULAR FUNCTIONS
    def updateDB(self): #updates db with past messages
        missed_messages = self.getMessagesAfter(self.last_stored_message_date)
        #TODO optimize this, currently it doesn't run much on rem server so whatever
        for message in missed_messages:
            self.storeMessage(message)

    def streamToDB(self): #keeps streaming and updating db
        stream = self.getRoomStreamer()
        for blob in stream.iter_lines():
            print blob
            print '---'
            data = json.loads(blob, 'utf-8')
            print 'data', data
            # message = self.parseMessage(data)
            # pprint.pprint(message)


    #END POPULAR

    def reqStream(self, path):
        return requests.get("https://stream.gitter.im/v1/" + path, headers=self.header, stream=True)

    def reqRest(self, path):
        return requests.get("https://api.gitter.im/v1/" + path, headers=self.header)

    def getRestData(self, path):
        return json.loads(self.reqRest(path).content)

    def getRoomStreamer(self): # rets obj to  iterate
        return self.reqStream("rooms/"+self.room_id+"/chatMessages")

    def getLastMessages(self, before_id = ''):
        path = "rooms/"+self.room_id+"/chatMessages?limit="+str(MESSAGE_FETCH_BATCH)
        if before_id != '':
            path += "&beforeId=" + before_id

        data = self.getRestData(path)

        parsed_messages = []
        for message in data:
            parsed_messages.append(self.parseMessage(message))

        return parsed_messages

    def getMessagesAfter(self, date_int): #can't use msg id - could be deleted
        all_messages = []
        current_backtrack = ''
        while True:
            batch = self.getLastMessages(current_backtrack)
            batch.reverse()
            for message in batch:
                if message['date'] > date_int:
                    #print message['date'], message['text']
                    print message
                    all_messages.append(message)
                else:
                    all_messages.reverse()
                    return all_messages
            if len(batch) <= 0:
                all_messages.reverse()
                return all_messages #message not found
            current_backtrack = batch[-1]['id']

    #DB OPS
    def _initDB(self):
        try:
            self.con = mdb.connect(MYSQL_DATABASE_HOST, MYSQL_DATABASE_USER,
                                   MYSQL_DATABASE_PASS, MYSQL_DATABASE_NAME);
            self.cur = self.con.cursor()
            self.cur.execute("CREATE TABLE IF NOT EXISTS logs("
                        "date BIGINT PRIMARY KEY,"
                        "id VARCHAR(256),"
                        "text TEXT,"
                        "html TEXT,"
                        "user_id VARCHAR(256),"
                        "user_name VARCHAR(256),"
                        "user_display VARCHAR(256),"
                        "urls TEXT"
                        ")")
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            quit()

    def storeMessage(self, message): #TODO variant that stores many at once

        print "STORING============="
        pprint.pprint(message)

        self.cur.execute(
                "INSERT IGNORE  INTO logs(date,id,text,html,user_id,user_name,user_display,urls)"
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                (message['date'], message['id'], message['text'], message['html'], message['user_id'],
                 message['user_name'], message['user_display'], str(message['urls'])))
        self.con.commit()
        self.last_stored_message_date = message['date']
        #open('last_date.txt', 'w').write(str(self.last_stored_message_date)) #this sucks


    #stop
    def stopSystems(self):
        self.con.close()  # DB con
        #TODO end stream


gitter = LoggerObject(GITTER_USER_TOKEN, GITTER_ROOM_NAME).StartLogging()
# gitter.getMessagesAfter(20171019184513)

#pprint.pprint(gitter.getLastMessages())

quit()

print 'finished'
