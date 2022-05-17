import time
"""
The goal of this task is to prepare statistical analysis of set of data from disks.

Each entry of the data set consists of following fields separated by ;
character:

    datacenter
    hostname
    disk serial
    disk age (in s)
    total reads
    total writes
    average IO latency from 5 minutes (in ms)
    total uncorrected read errors
    total uncorrected write errors

The proper solution (a script in Python) should output following
information:

    How many disks are in total and in each DC
    Which disk is the youngest/oldest one and what is its age (in days)
    What's the average disk age per DC (in days)
    How many read/write IO/s disks processes on average
    Find top 5 disks with lowest/highest average IO/s (reads+writes, print disks and their avg IO/s)
    Find disks which are most probably broken, i.e. have non-zero uncorrected errors (print disks and error counter)

There should also be tests that verify if parts of the script are processing data properly.
"""


def secondsToDays(number):
    return number/86400


def getAge(number):
    return int(number[3])


def GetIo(number):
    return int(number[-4]) + int(number[-5])


def GetErrors(number):
    return int(number[-1]) + int(number[-2])


def PrintErrors(table):
    [print(f"{x[2]} has {GetErrors(x)} errors") for x in table]


def PrintAvgIOs(table):
    [print(f"{x[2]} has {GetIo(x)} IOs") for x in table]


def gather_Data(filename, printData):
    with open(filename) as dataset:
        types = {}
        data = [x.split(';') for x in dataset]
        data.sort(key=getAge)
        newest, oldest = data[0], data[-1]
        size = len(data)
        avgage = secondsToDays(sum([int(x[3]) for x in data])) / size
        avgios = sum([GetIo(x) for x in data]) / size
        data.sort(key=GetIo)
        lowest, highest = data[:5], reversed(data[-5:])
        errors = [x for x in data if GetErrors(x) != 0]
        for row in data:
            if row[0] not in types:
                types[row[0]] = 1
            else:
                types[row[0]] += 1
        if printData:
            PrintErrors(errors)
            print("\nTop 5 IO/s:")
            PrintAvgIOs(highest)
            print("\nTop 5 IO/s in reverse:")
            PrintAvgIOs(lowest)
            print(f"\n{newest[2]} is the newest disk and has {round(secondsToDays(int(newest[3])), 2)} days")
            print(f"\n{oldest[2]} is the oldest disk and has {round(secondsToDays(int(oldest[3])), 2)} days")
            print("\nAverage days: {:0.1f}.\n".format(avgage))
            print("Average IOs: {:0.1f}.\n".format(avgios))
            print(f"Discs of each DC: {types}")
            print(f"\nDataset contains {size} rows")
        return {'top': highest, 'last': lowest, 'sizebygroup': types,
                'size': size, 'oldest': oldest, 'newest': newest, 'errors': errors}


if __name__ == '__main__':
    start = time.time()
    metadata = gather_Data('data.raw', True)
    print(f"\nall operations lasted: {round(time.time() - start, 4)} seconds")
