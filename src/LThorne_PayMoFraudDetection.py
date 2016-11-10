# Insight Data Engineer coding challenge: PayMo anti-fraud detection
# Larisa Thorne
# 2016-11-08

import os

# ------------------------------------------------------------------------
# Define class Person as container of friend networks, by level (1-4 and a 
#   flattened version for quick lookup)
# ------------------------------------------------------------------------

class Person(object):
    def __init__(self, me, firstFriends = None):
        self.me = me
        if firstFriends == None:
            self.firstFriends = []
            self.secondFriends = []
            self.thirdFriends = []
            self.fourthFriends = []
            self.allFriendsCompressed = []

    def __repr__(self): # For debugging
        return self.me

    def __eq__(self, other):
        return str(self.me) == str(other.me)


# ------------------------------------------------------------------------
# Organize input information in batch_payment.txt:
# ------------------------------------------------------------------------

def ingest_batch_payment(): 

    path = os.path.dirname(os.path.abspath(__file__))
    batchPath = getBatchPath(path)
    listOfPersons = []
    f = open(batchPath, "r")
    ignoreFirstEntry = 0 # Don't need header data
    for line in iter(f): # Read input file line by line
        if ignoreFirstEntry != 0:
            payer = line.split(",")[1]
            recipient = line.split(",")[2]
            # Add new entries to listOfPersons:
            if not checkIfInList(payer, listOfPersons):
                person1 = Person(payer)
                listOfPersons.append(person1)
            if not checkIfInList(recipient, listOfPersons):
                person2 = Person(recipient)
                listOfPersons.append(person2)
            # Ensure same instance of Person used:
            person1new = findExistingPerson(payer, listOfPersons)
            person2new = findExistingPerson(recipient, listOfPersons)
            # Update first friend connections:
            if not checkIfInList(person1new, person2new.allFriendsCompressed):
                (person2new.firstFriends).append(person1new)
                (person2new.allFriendsCompressed).append(person1new)
            if not checkIfInList(person2new, person1new.allFriendsCompressed):
                (person1new.firstFriends).append(person2new)
                (person1new.allFriendsCompressed).append(person2new)
        ignoreFirstEntry = 1
    updateNthFriends(listOfPersons) # Organizes friend networks

    return listOfPersons


# --- Helper functions: ---  

def getBatchPath(path):
    upOneDir = ""
    batchFilename = "batch_payment.txt"
    for dir in path.split("/"):
        if "src" != dir:
            upOneDir += dir + "/" 
    return upOneDir + "payment_input/" + batchFilename 

def updateFirstFriends(person1, person2):
    if (not checkIfInList(person2, person1.allFriendsCompressed)
        and not checkIfInList(person1, person2.allFriendsCompressed)
        and person1 != person2):
        person1.firstFriends.append(person2)
        person1.allFriendsCompressed.append(person2)
        person2.firstFriends.append(person1) # Reciprocate
        person2.allFriendsCompressed.append(person1)

def updateSecondFriends(person1, person2):
    if (not checkIfInList(person2, person1.allFriendsCompressed)
        and not checkIfInList(person1, person2.allFriendsCompressed)
        and person1 != person2):
        person1.secondFriends.append(person2)
        person1.allFriendsCompressed.append(person2)
        person2.secondFriends.append(person1) # Reciprocate
        person2.allFriendsCompressed.append(person1)

def updateThirdFriends(person1, person2):
    if (not checkIfInList(person2, person1.allFriendsCompressed)
        and not checkIfInList(person1, person2.allFriendsCompressed)
        and person1 != person2): 
        person1.thirdFriends.append(person2)
        person1.allFriendsCompressed.append(person2)
        person2.thirdFriends.append(person1) # Reciprocate
        person2.allFriendsCompressed.append(person1)

def updateFourthFriends(person1, person2):
    if (not checkIfInList(person2, person1.allFriendsCompressed)
        and not checkIfInList(person1, person2.allFriendsCompressed)
        and person1 != person2): 
        person1.fourthFriends.append(person2)
        person1.allFriendsCompressed.append(person2)
        person2.fourthFriends.append(person1) # Reciprocate
        person2.allFriendsCompressed.append(person1)

def updateNthFriends(personList):
    # Loop through 'perspective' of each person in personList
    for i in range(len(personList)):
        currPerson = personList[i] # Me
        # Loop through my friends (my 1D friends)
        for j in range(len(currPerson.firstFriends)):
            firstFriend = currPerson.firstFriends[j]
            updateFirstFriends(firstFriend, currPerson)
            # Loop through my 1D friend's friends (my 2D friends)
            for k in range(len(firstFriend.firstFriends)):
                secondFriend = firstFriend.firstFriends[k]
                updateFirstFriends(secondFriend, firstFriend)
                updateSecondFriends(secondFriend, currPerson)
                # Loop through my 2D friend's friends (my 3D friends)
                for l in range(len(secondFriend.firstFriends)):
                    thirdFriend = secondFriend.firstFriends[l]
                    updateFirstFriends(thirdFriend, secondFriend)
                    updateSecondFriends(thirdFriend, firstFriend)
                    updateThirdFriends(thirdFriend, currPerson)
                    # Loop through my 3D friend's friends (my 4D friends)
                    for m in range(len(thirdFriend.firstFriends)):
                        fourthFriend = thirdFriend.firstFriends[m]
                        updateFirstFriends(fourthFriend, thirdFriend)
                        updateSecondFriends(fourthFriend, secondFriend)
                        updateThirdFriends(fourthFriend, firstFriend)
                        updateFourthFriends(fourthFriend, currPerson)

def checkIfInList(person, personList):
    for i in range(len(personList)):
        if (str(person) == str(personList[i])):
            return True
    return False    

def findExistingPerson(person, personList):
    for i in range(len(personList)):
        if (person == str(personList[i])):
            return personList[i]
    return None


# ------------------------------------------------------------------------
# Read in stream_payment.txt and apply features
# ------------------------------------------------------------------------

def ingest_stream_payment():

    listOfPersons = ingest_batch_payment()
    path = os.path.dirname(os.path.abspath(__file__))
    streamPath = getStreamPath(path)
    f = open(streamPath, "r", encoding = "utf8") # encoding to deal with emoji
    ignoreFirstEntry = 0 # Ignore header
    for line in iter(f):
        if ignoreFirstEntry != 0:
            payer = line.split(",")[1]
            recipient = line.split(",")[2]
            feature1(payer, recipient, listOfPersons, path)
            feature2(payer, recipient, listOfPersons, path)
            feature3(payer, recipient, listOfPersons, path)
        ignoreFirstEntry = 1


# ------------------------------------------------------------------------
# Feature 1: flag if new transaction, based on batch_payment.txt contents
# ------------------------------------------------------------------------

def feature1(payer, recipient, personList, path):

    try:
        firstFriendsList = findExistingPerson(payer, personList).firstFriends
        if checkFirstFriends(payer, recipient, personList):
            flag1 = "Trusted"
        else:
            flag1 = "Unverified"
    except:
        flag1 = "Unverified"
    updateOutput(1, flag1, path)


# ------------------------------------------------------------------------
# Feature 2: flag if recipient > 2nd degree friend
# ------------------------------------------------------------------------

def feature2(payer, recipient, personList, path):

    try:
        if (checkFirstFriends(payer, recipient, personList)
            or checkSecondFriends(payer, recipient, personList)):
            flag2 = "Trusted"
        else:
            flag2 = "Unverified"
    except:
        flag2 = "Unverified"
    updateOutput(2, flag2, path)


# ------------------------------------------------------------------------
# Feature 3: flag if recipient > 4th degree friend
# ------------------------------------------------------------------------

def feature3(payer, recipient, personList, path):

    try:
        if (checkFirstFriends(payer, recipient, personList)
            or checkSecondFriends(payer, recipient, personList)
            or checkThirdFriends(payer, recipient, personList)
            or checkFourthFriends(payer, recipient, personList)):
            flag3 = "Trusted"
        else:
            flag3 = "Unverified"
    except:
        flag3 = "Unverified"
    updateOutput(3, flag3, path)


# --- Helper functions: ---   

def getStreamPath(path):
    upOneDir = ""
    streamFilename = "stream_payment.txt"
    for dir in path.split("/"):
        if "src" != dir:
            upOneDir += dir + "/" 
    return upOneDir + "payment_input/" + streamFilename

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
    f.write(toWrite)

def checkFirstFriends(person1, person2, personList):
    payerPerson = findExistingPerson(person1, personList)
    if checkIfInList(person2, payerPerson.firstFriends):
        return True
    return False

def checkSecondFriends(person1, person2, personList):
    payerPerson = findExistingPerson(person1, personList)
    if checkIfInList(person2, payerPerson.secondFriends):
        return True
    return False

def checkThirdFriends(person1, person2, personList):
    payerPerson = findExistingPerson(person1, personList)
    if checkIfInList(person2, payerPerson.thirdFriends):
        return True
    return False

def checkFourthFriends(person1, person2, personList):
    payerPerson = findExistingPerson(person1, personList)
    if checkIfInList(person2, payerPerson.fourthFriends):
        return True
    return False



# ------------------------------------------------------------------------
# Run the anti-fraud suite:
# ------------------------------------------------------------------------

ingest_stream_payment()
