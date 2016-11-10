# Insight Data Engineer coding challenge: PayMo
# Larisa Thorne
# 2016-11-08

import os


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
# Organize information in batch_payment.txt
def ingest_batch_payment(): 
    path = os.path.dirname(os.path.abspath(__file__))
    batchPath = getBatchPath(path)
    listOfPersons = []
    f = open(batchPath, "r")
    ignoreFirstEntry = 0
    for line in iter(f): # Read line by line
        if ignoreFirstEntry != 0:
            payer = line.split(",")[1]
            recipient = line.split(",")[2]
            #person1 = Person(payer)
            #person2 = Person(recipient)

            if not checkIfInList(payer, listOfPersons):
                person1 = Person(payer)
                listOfPersons.append(person1)
            if not checkIfInList(recipient, listOfPersons):
                person2 = Person(recipient)
                listOfPersons.append(person2)

            person1new = findExistingPerson(payer, listOfPersons)
            person2new = findExistingPerson(recipient, listOfPersons)

            if not checkIfInList(person1new, person2new.allFriendsCompressed):
                print("Add ", person1new, "to ", person2new)
                (person2new.firstFriends).append(person1new)
                (person2new.allFriendsCompressed).append(person1new)
            if not checkIfInList(person2new, person1new.allFriendsCompressed):
                print("Add ", person2new, "to ", person1new)
                (person1new.firstFriends).append(person2new)
                (person1new.allFriendsCompressed).append(person2new)

            print(person1new, person1new.allFriendsCompressed, "\t", person2new, person2new.allFriendsCompressed)
            print(person1new.firstFriends)


        ignoreFirstEntry = 1
    updateNthFriends(listOfPersons)

    for i in range(len(listOfPersons)):
        print(listOfPersons[i], 
                "\n\tFirst friends: ", listOfPersons[i].firstFriends,
                "\n\tSecond friends: ", listOfPersons[i].secondFriends,
                "\n\tThird friends: ", listOfPersons[i].thirdFriends,
                "\n\tFourth friends: ", listOfPersons[i].fourthFriends)

    return listOfPersons

# -------------------------------    

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
        print("1st friend update!")
        person1.firstFriends.append(person2)
        person2.firstFriends.append(person1) # Reciprocate
        person1.allFriendsCompressed.append(person2)
        person2.allFriendsCompressed.append(person1) # Reciprocate

def updateSecondFriends(person1, person2):
    if (not checkIfInList(person2, person1.allFriendsCompressed)
        and not checkIfInList(person1, person2.allFriendsCompressed)
        and person1 != person2): 
        print("2nd friend update!")
        person1.secondFriends.append(person2)
        person2.secondFriends.append(person1) # Reciprocate
        person1.allFriendsCompressed.append(person2)
        person2.allFriendsCompressed.append(person1) # Reciprocate

def updateThirdFriends(person1, person2):
    if (not checkIfInList(person2, person1.allFriendsCompressed)
        and not checkIfInList(person1, person2.allFriendsCompressed)
        and person1 != person2): 
        print("3rd friend update!")
        person1.thirdFriends.append(person2)
        person2.thirdFriends.append(person1) # Reciprocate
        person1.allFriendsCompressed.append(person2)
        person2.allFriendsCompressed.append(person1) # Reciprocate

def updateFourthFriends(person1, person2):
    if (not checkIfInList(person2, person1.allFriendsCompressed)
        and not checkIfInList(person1, person2.allFriendsCompressed)
        and person1 != person2): 
        print("4th friend update!")
        person1.fourthFriends.append(person2)
        person2.fourthFriends.append(person1) # Reciprocate
        person1.allFriendsCompressed.append(person2)
        person2.allFriendsCompressed.append(person1) # Reciprocate


def updateNthFriends(personList):
    #print("listOfPersons: ", personList)

    # Let "me" be each person in personList
    for i in range(len(personList)):
        currPerson = personList[i] # me
        print("\ncurrPerson ", currPerson, "had direct friend transactions with: ", currPerson.firstFriends)

        # Loop through my friends (my 1D friends)
        for j in range(len(currPerson.firstFriends)):
            firstFriend = currPerson.firstFriends[j]
            print("\tFriend of ", currPerson, " is ", firstFriend)
            updateFirstFriends(firstFriend, currPerson)

            # Loop through my 1D friend's friends (my 2D friends)
            for k in range(len(firstFriend.firstFriends)):
                secondFriend = firstFriend.firstFriends[k] # 1D list of strings
                print("\t\tFriend of ", firstFriend, " is ", secondFriend)
                updateFirstFriends(secondFriend, firstFriend)
                updateSecondFriends(secondFriend, currPerson)

                # Loop through my 2D friend's friends (my 3D friends)
                for l in range(len(secondFriend.firstFriends)):
                    thirdFriend = secondFriend.firstFriends[l]
                    print("\t\t\tFriend of ", secondFriend, " is ", thirdFriend)
                    updateFirstFriends(thirdFriend, secondFriend)
                    updateSecondFriends(thirdFriend, firstFriend)
                    updateThirdFriends(thirdFriend, currPerson)

                    # Loop through my 3D friend's friends (my 4D friends)
                    for m in range(len(thirdFriend.firstFriends)):
                        fourthFriend = thirdFriend.firstFriends[m]
                        print("\t\t\t\tFriend of ", thirdFriend, " is ", fourthFriend)
                        updateFirstFriends(fourthFriend, thirdFriend)
                        updateSecondFriends(fourthFriend, secondFriend)
                        updateThirdFriends(fourthFriend, firstFriend)
                        updateFourthFriends(fourthFriend, currPerson)




def checkIfInList(person, personList): # Checks personList of class Person instances of person
    for i in range(len(personList)):
        if (str(person) == str(personList[i])):
            return True
    return False    

def findExistingPerson(person, personList): # Returns instance of class Person
    for i in range(len(personList)):
        if (person == str(personList[i])):
            return personList[i]
    return None



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
            feature2(payer, recipient, listOfPersons, path)
            feature3(payer, recipient, listOfPersons, path)
        ignoreFirstEntry = 1

# -------------------------------   

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
        firstFriendsList = findExistingPerson(payer, personList).firstFriends
        if checkFirstFriends(payer, recipient, personList):
            flag1 = "Trusted"
        else:
            flag1 = "Unverified"
    except:
        flag1 = "Unverified"
    updateOutput(1, flag1, path)

# -------------------------------   

def checkFirstFriends(person1, person2, personList):
    payerPerson = findExistingPerson(person1, personList)
    if checkIfInList(person2, payerPerson.firstFriends):
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
    #f.write(toWrite)

# ------------------------------------------------------------------------
# Feature 2: flag if recipient > 2nd degree friend
def feature2(payer, recipient, personList, path):
    print("Do feature 2.")
    try:
        if (checkFirstFriends(payer, recipient, personList)
            or checkSecondFriends(payer, recipient, personList)):
            flag2 = "Trusted"
        else:
            flag2 = "Unverified"
    except:
        flag2 = "Unverified"
    updateOutput(2, flag2, path)

def checkSecondFriends(person1, person2, personList):
    payerPerson = findExistingPerson(person1, personList)
    if checkIfInList(person2, payerPerson.secondFriends):
        return True
    return False

# ------------------------------------------------------------------------
# Feature 3: flag if recipient > 4th degree friend
def feature3(payer, recipient, personList, path):
    print("Do feature 3.")
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

ingest_stream_payment()