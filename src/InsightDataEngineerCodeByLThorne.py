# Insight Data Engineer coding challenge: PayMo
# Larisa Thorne
# 2016-11-08

import os

class Person(object):
    def __init__(self, me, friends = None):
        self.me = me
        if friends == None:
            self.friends = []

    def __repr__(self):
        return self.me

# Organize information in batch_payment.txt
def ingest_batch_payment(): 
    path = os.path.dirname(os.path.abspath(__file__))
    batchPath = getBatchPath(path)
    listOfPersons = []
    f = open(batchPath, "r")
    ignoreFirstEntry = 0
    for line in iter(f):
        if ignoreFirstEntry != 0:
            payer = line.split(",")[1]
            recipient = line.split(",")[2]
            if not checkIfInList(payer, listOfPersons):
                newPayer = Person(payer)
                listOfPersons.append(newPayer)
                newPayer.friends.append(recipient)
            else:
                oldPerson = findExistingPerson(payer, listOfPersons)
                if not checkIfInList(recipient, listOfPersons):
                    oldPerson.friends.append(recipient)
            if not checkIfInList(recipient, listOfPersons):
                newRecipient = Person(recipient)
                listOfPersons.append(newRecipient)
                newRecipient.friends.append(payer)
            else:
                oldPerson = findExistingPerson(recipient, listOfPersons)
                if not checkIfInList(payer, listOfPersons):
                    oldPerson.friends.append(payer)
        ignoreFirstEntry = 1

    #for i in range(len(listOfPersons)):
        #print(listOfPersons[i], listOfPersons[i].friends)

    return listOfPersons

def getBatchPath(path):
    upOneDir = ""
    batchFilename = "batch_payment.txt"
    for dir in path.split("/"):
        if "src" != dir:
            upOneDir += dir + "/" 
    return upOneDir + "payment_input/" + batchFilename 

def checkIfInList(person, personList):
    for i in range(len(personList)):
        if (person == str(personList[i])):
            return True
    return False    

def findExistingPerson(person, personList):
    for i in range(len(personList)):
        if (person == str(personList[i])):
            return personList[i]


def ingest_stream_payment():
    listOfPersons = ingest_batch_payment()
    path = os.path.dirname(os.path.abspath(__file__))
    streamPath = getStreamPath(path)
    #print(streamPath)
    f = open(streamPath, "r")
    ignoreFirstEntry = 0
    for line in iter(f):
        if ignoreFirstEntry != 0:
            payer = line.split(",")[1]
            recipient = line.split(",")[2]
            print("Payer: ", payer, "\tRecipient: ", recipient)
            feature1(payer, recipient, listOfPersons)
            feature2(payer, recipient, listOfPersons)
            feature3(payer, recipient, listOfPersons)
        ignoreFirstEntry = 1


def getStreamPath(path):
    upOneDir = ""
    streamFilename = "stream_payment.txt"
    for dir in path.split("/"):
        if "src" != dir:
            upOneDir += dir + "/" 
    return upOneDir + "payment_input/" + streamFilename

# Feature 1: flag if new transaction
def feature1(payer, recipient, personList):
    print("Do feature 1.")
    try:
        friendsList = (findExistingPerson(payer, personList)).friends
        print(friendsList)
        if (recipientAmongFriends(recipient, friendsList)):
            print("Verified.")
        else:
            print("Unverified.")
    except:
        print("No friends.")
        print("Unverified")


def recipientAmongFriends(recipient, friendsList):
    return

# Feature 2: flag if recipient > 2nd degree friend
def feature2(payer, recipient, personList):
    print("Do feature 2.")

# Feature 3: flag if recipient > 4th degree friend
def feature3(payer, recipient, personList):
    print("Do feature 3.")

ingest_stream_payment()