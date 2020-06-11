import sys
import time
import os
import csv

# FUNCTIONS

# Try to find the file
# Exit out if the file can't be found
def fileCheck(srcFileLoc):
    if not os.path.isfile(srcFileLoc):
        print("File path {} does not exist. Exiting...".format(srcFileLoc))
        sys.exit()
    print("Finding files {}".format(srcFileLoc))

    tmpList = []

    with open(srcFileLoc) as srcFile:
        reader = csv.reader(srcFile)  # create the csv reader

        for row in reader:
            tmpList.append(row)

    return tmpList


# Compare each line and make sure the line has the correct column length
# RETURNS: Acceptable list and Fix list
def fLenComp(origlist, headerlist):
    # Comparable Values
    headerLen = len(headerlist)

    # Lists
    acceptList = []
    fixItList = []

    acceptList.append(headerlist)
    fixItList.append(headerlist)

    for row in origlist:
        t = len(row)
        #print(t)
        if t == headerLen:
            acceptList.append(row)
        else:
            fixItList.append(row)

    return acceptList, fixItList

# IO PRINT OUTPUT FILE
# Write out the results
def printOutput(outputFile, dataset):
    try:
        if os.name == "nt":
            with open(outputFile, "w", newline='') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerows(dataset)
                print("Wrote out to: %s" % outputFile)
        else:
            with open(outputFile, "w") as csvFile:
                writer = csv.writer(csvFile)
                writer.writerows(dataset)
                print("Wrote out to: %s" % outputFile)
    except Exception as exc:
        print("Exception occurred on output: %s" % exc)