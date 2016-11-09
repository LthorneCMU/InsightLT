# Insight Data Engineer coding challenge: PayMo
# Larisa Thorne
# 2016-11-08

import os


# ------------------------------------------------------------------------
class Person(object):
    def __init__(self, me, friends = None):
        self.me = me
        if friends == None:
            self.friends = []

    def __repr__(self):
        return self.me


# ------------------------------------------------------------------------
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

def checkIfInList(person, personList): # Checks personList of class Person instances for person
    for i in range(len(personList)):
        if (person == str(personList[i])):
            return True
    return False    

def findExistingPerson(person, personList): # Returns instance of class Person
    for i in range(len(personList)):
        if (person == str(personList[i])):
            return personList[i]


# ------------------------------------------------------------------------
# Read in stream_payment.txt and apply features
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
            feature1(payer, recipient, listOfPersons, path)
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


# ------------------------------------------------------------------------
# Feature 1: flag if new transaction, based on batch_payment.txt contents
def feature1(payer, recipient, personList, path):
    print("Do feature 1.")
    try:
        friendsList = findExistingPerson(payer, personList).friends
        print("\tHas friends: ", friendsList)
        if isRecipientAmongFriends(recipient, friendsList):
            print("\tRecipient is friend.")
            print("\tVerified.")
            flag1 = "Verified"
        else:
            print("\tRecipient not friend.")
            print("\tUnverified.")
            flag1 = "Unverified"
    except:
        print("\tNo friends.")
        print("\tUnverified")
        flag1 = "Unverified"
    updateOutput(1, flag1, path)


def isRecipientAmongFriends(recipient, friendsList): # Similar to checkIfInList, but list is just a regular array!
    for i in range(len(friendsList)):
        if (recipient == friendsList[i]):
            return True
    return False

def getOutput(n, path):
    upOneDir = ""
    if (n == 1):
        filename = "output1.txt"
    elif (n == 2):
        filename = "output2.txt"
    else:
        filename = "output3.txt"
    for dir in path.split("/"):
        if "src" != dir:
            upOneDir += dir + "/"
    return upOneDir + "payment_output/" + filename

def updateOutput(n, flag, path):
    pathToOutputFile = getOutput(n, path)
    f = open(pathToOutputFile, "a") # Open in append mode
    if os.path.getsize(pathToOutputFile) == 0:
        toWrite = flag
    else:
        toWrite = "\n" + flag
    print(repr(toWrite))
    f.write(toWrite)

# ------------------------------------------------------------------------
# Feature 2: flag if recipient > 2nd degree friend
def feature2(payer, recipient, personList):
    print("Do feature 2.")
    


# ------------------------------------------------------------------------
# Feature 3: flag if recipient > 4th degree friend
def feature3(payer, recipient, personList):
    print("Do feature 3.")

ingest_stream_payment()