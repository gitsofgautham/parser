from pystdf.Importer import STDF2DataFrame
import os
import time
from multiprocessing import Queue


def process(args):
    stdfFilename = args[0]
    outputPath = args[1]
    msgQueue: Queue = args[2]
    convertedCSV = []
    newFolder = os.path.basename(stdfFilename).split(".")[0]
    newPath = outputPath+"/"+newFolder

    msgQueue.put({"STATUS": "RUNNING", "TYPE": "INFO", "PROGRESS":True,"MSG": [
                 f"Start to convert '{stdfFilename}' into CSV..."]})
    try:
        startTime = time.time()
        dfs = STDF2DataFrame(stdfFilename)
        msgQueue.put({"STATUS": "RUNNING", "TYPE": "INFO", "PROGRESS":True,"MSG": [
                     f"'{stdfFilename}' is converted into DataFrame, start to write CSV..."]})
        if not os.path.exists(newPath):
            os.makedirs(newPath)
        for key in dfs.keys():
            fn = f"{newPath}/{key}.csv"
            convertedCSV.append(f"{key}.csv")
            dfs[key].to_csv(path_or_buf=fn)
        totalTime = time.time() - startTime
    except Exception as e:
        msgQueue.put({"STATUS": "EXCEPT", "PROGRESS":True, "TYPE": "ERROR", "MSG": [
                     f">An exception was caught while converting '{stdfFilename}'", ">"+str(e), ">Conversion failed..."]})

    msgQueue.put({"STATUS": "FINISH", "PROGRESS":True,"DATA": {"CSV": convertedCSV, "FOLDER": newFolder}, "TYPE": "INFO", "MSG": [
                 f"'{stdfFilename}' is converted into CSV in {totalTime:.4f} seconds."]})
