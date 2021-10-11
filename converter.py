from multiprocessing.queues import Queue
import os
from time import sleep
from typing import Counter
from PyQt5.QtCore import Q_ARG, QMetaObject, pyqtSignal
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import *
from PyQt5 import *
from PyQt5.QtCore import QThread
from gui import *
import sys
import glob
import multiprocessing
import threading
from stdf_csv import process
from datetime import datetime
import ftplib
import zipfile
class ThreadCommunicate(QThread):
    logInfo = pyqtSignal(list)
    logError = pyqtSignal(list)
    setPrgVal = pyqtSignal(int)
    addTab = pyqtSignal(dict)
    appendCSVInfo = pyqtSignal(dict)
    def __init__(self, tasks, msgQueue):
        super().__init__()
        self.__tasks = tasks
        self.__msgQueue:Queue = msgQueue
    def run(self):
        jobDone = 0
        progressGrain = int(100/(len(self.__tasks)*3))
        progress = 0

        while jobDone != len(self.__tasks):
            msg = self.__msgQueue.get()
            if msg["STATUS"] == "FINISH":
                jobDone += 1
                self.logInfo.emit(msg["MSG"])
                self.addTab.emit(msg["DATA"])
                self.appendCSVInfo.emit(msg["DATA"])
            elif msg["STATUS"] == "RUNNING":
                if msg["TYPE"]=="INFO":self.logInfo.emit(msg["MSG"])
            elif msg["STATUS"] == "EXCEPT":
                self.logError.emit(msg["MSG"])
                jobDone += 1
            if msg["PROGRESS"]: progress += 1
            self.setPrgVal.emit(progress*progressGrain)
        self.setPrgVal.emit(100)    
        self.logInfo.emit(["All Conversions Finish."])

class ThreadCompress(QThread):
    logInfo = pyqtSignal(list)
    logError = pyqtSignal(list)
    setPrgVal = pyqtSignal(int)
    def __init__(self, allCSVInfo, outputFolder,):
        super().__init__()
        self.__allCSVFileInfo = allCSVInfo
        self.__outputFolder = outputFolder
    def run(self):
        #Compress Archive
        cwd = os.getcwd()
        os.chdir(self.__outputFolder)
        sub = [e["FOLDER"] for e in self.__allCSVFileInfo]
        zipFN = f"{datetime.now().strftime(r'%d-%m-%Y %H.%M.%S')}_CSV.zip"
        grain = int(100/len(sub))
        self.logInfo.emit([f"Start to compress the files into '{zipFN}'..."])
        try:
            zf = zipfile.ZipFile(os.path.join(self.__outputFolder, zipFN), "w")
            count = 0
            for dir in sub:
                self.logInfo.emit([f"Compressing '{dir}'..."])
                self.setPrgVal.emit(count*grain)
                for filename in os.listdir(dir):
                    f = os.path.join(dir, filename)
                    zf.write(f)
                count+=1
            self.setPrgVal.emit(100)
            self.logInfo.emit(["Compression Done..."])
            zf.close()
            os.chdir(cwd)
            return zipFN
        except Exception as e:
            self.logError.emit([">An exception was caught while compressing...", ">"+str(e), ">Compression failed...", "Upload failed..."])
            

class mainWindow(QtWidgets.QMainWindow):
    addLog = pyqtSignal(str)
    addTab = pyqtSignal(QWidget, str)
    insertText = pyqtSignal(str)
    clearTab = pyqtSignal()
    setPrgVal = pyqtSignal(int)
    def __init__(self) -> None:
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.allSTDFList.setSelectionMode(
            QAbstractItemView.ExtendedSelection)
        self.ui.selSTDFList.setSelectionMode(
            QAbstractItemView.ExtendedSelection)
        # setup link
        #self.ui.testButton.setVisible(False)
        self.ui.browseButton.clicked.connect(self.__browseClick)
        self.ui.selAllButton.clicked.connect(self.__selAllClick)
        self.ui.deSelAllButton.clicked.connect(self.__deSelAllClick)
        self.ui.toRightButton.clicked.connect(self.__toRightClick)
        self.ui.toLeftButton.clicked.connect(self.__toLeftClick)
        self.ui.convertButton.clicked.connect(self.__convertClick)
        self.ui.outputButton.clicked.connect(self.__selectClick)
        self.ui.uploadButton.clicked.connect(self.__uploadClick)
        self.ui.testButton.clicked.connect(self.__testClick)
        #Signal Connections
        self.addLog.connect(self.ui.logEdit.insertHtml)
        self.addTab.connect(self.ui.convertedTab.addTab)
        self.clearTab.connect(self.ui.convertedTab.clear)
        self.setPrgVal.connect(self.ui.progressBar.setValue)
        self.insertText.connect(self.ui.logEdit.append)
        self.__allCSVFileInfo = []

    def __browseClick(self):
        path = str(QFileDialog.getExistingDirectory(self, "Select Directory"))
        if len(path.strip()) == 0:
            return
        self.ui.pathEdit.setText(path)
        fns = glob.glob(path + "/**/*.stdf", recursive=True)
        self.ui.allSTDFList.clear()
        self.ui.selSTDFList.clear()
        self.ui.convertedTab.clear()
        self.__allCSVFileInfo = []
        self.ui.allSTDFList.addItems(fns)
    def __logInfo(self, msg:list):
        if not isinstance(msg,list):msg = [msg]
        for m in msg:
            self.insertText.emit(f'''[{datetime.now().strftime(r'%H:%M:%S %d-%m-%Y')}][Info]:{m}''')
            #self.addLog.emit(f'''[<label style="color:grey;">{datetime.now().strftime(r'%H:%M:%S %d-%m-%Y')}</label>][<label style="color:blue;">Info</label>]:{m}<br>''')
    def __logWarn(self, msg:list):
        if not isinstance(msg,list):msg = [msg]
        for m in msg:
            self.addLog.emit(f'''[<label style="color:grey;">{datetime.now().strftime(r'%H:%M:%S %d-%m-%Y')}</label>][<label style="color:#FFBF00;">Warn</label>]:{m}<br>''')
    def __logErr(self, msg:list):
        if not isinstance(msg,list):msg = [msg]
        for m in msg:
            self.addLog.emit(f'''[<label style="color:grey;">{datetime.now().strftime(r'%H:%M:%S %d-%m-%Y')}</label>][<label style="color:red;">Error</label>]:{m}<br>''')
    
    def __selAllClick(self):
        fns = [self.ui.allSTDFList.item(i).text()
               for i in range(self.ui.allSTDFList.count())]
        self.ui.selSTDFList.addItems(fns)
        self.ui.allSTDFList.clear()
    def __testClick(self):
        pass
        

    def __addCSVTab(self, file):
        newTab = QWidget()
        layout = QVBoxLayout()
        list = QListWidget()
        list.addItems(file["CSV"])
        label = QLabel("Folder: "+file["FOLDER"])
        layout.addWidget(label)
        layout.addWidget(list)
        newTab.setLayout(layout)
        self.addTab.emit(newTab, "CSV"+str(self.ui.convertedTab.count()+1))
        
    def __deSelAllClick(self):
        fns = [self.ui.selSTDFList.item(i).text()
               for i in range(self.ui.selSTDFList.count())]
        self.ui.allSTDFList.addItems(fns)
        self.ui.selSTDFList.clear()

    def __toRightClick(self):
        items = self.ui.allSTDFList.selectedItems()
        for item in items:
            self.ui.allSTDFList.takeItem(self.ui.allSTDFList.row(item))
            self.ui.selSTDFList.addItem(item.text())

    def __toLeftClick(self):
        items = self.ui.selSTDFList.selectedItems()
        for item in items:
            self.ui.selSTDFList.takeItem(self.ui.selSTDFList.row(item))
            self.ui.allSTDFList.addItem(item.text())

    def __convertClick(self):
        if not os.path.isdir(self.ui.outputEdit.text()):
            QMessageBox.critical(
                self, "Error", "Please choose a valid ouput folder.")
            return
        fn = [self.ui.selSTDFList.item(i).text()
              for i in range(self.ui.selSTDFList.count())]
        if len(fn) == 0:
            QMessageBox.critical(self, "Error", "No files are selected.")
            return
        for file in fn:
            if not os.path.isfile(file):
                QMessageBox.critical(self, "Error", f"'{file}' is not a file.")
                return
        self.__tasks = []
        self.__outputFolder = self.ui.outputEdit.text()
        for f in fn:
            self.__tasks.append([f, self.ui.outputEdit.text()])
        threading.Thread(target=self.__threadProcess).start()

    def __selectClick(self):
        self.ui.outputEdit.setText(
            str(QFileDialog.getExistingDirectory(self, "Select Directory")))

    def __uploadClick(self):
        if len(self.__allCSVFileInfo) == 0:
            QMessageBox.critical(self, "Error","No converted CSV files were found.", QMessageBox.Yes)
            return
        threading.Thread(target=self.__theradUpload).start()
    def __ftpUploadProgress(self, bytes):
        grain = int(100000000/(self.__zipFileSize / 8192))
        self.__ftpUploadBlockCount+=1
        self.setPrgVal.emit(self.__ftpUploadBlockCount*grain)
    def __theradUpload(self):
        self.__setUiDisableState()
        sleep(0.1)
        t = ThreadCompress(self.__allCSVFileInfo, self.__outputFolder)
        t.logInfo.connect(self.__logInfo)
        t.logError.connect(self.__logErr)
        t.setPrgVal.connect(self.ui.progressBar.setValue)
        zipFN = t.run()
        self.__logInfo(f"Start to upload '{zipFN}' onto server 'server ip'...")
        self.__ftpUploadBlockCount = 0
        ftp = ftplib.FTP('server ip','name','pass')
        fullZipFN = os.path.join(self.__outputFolder, zipFN)
        self.__zipFileSize = os.path.getsize(fullZipFN)
        QMetaObject.invokeMethod(self.ui.progressBar, "setMaximum", Qt.QueuedConnection, Q_ARG(int, 100000000))
        with open(fullZipFN,"rb") as f:
            ftp.storbinary(f"STOR {zipFN}", f, callback=self.__ftpUploadProgress)
        QMetaObject.invokeMethod(self.ui.progressBar, "setMaximum", Qt.QueuedConnection, Q_ARG(int, 100))    
        self.setPrgVal.emit(100)
        self.__logInfo("Upload finished.")
        self.__setUiEnableState()
        self.clearTab.emit()
        self.__allCSVFileInfo = []
        os.remove(fullZipFN)
    def __appendConvertedCSVInfo(self, info:dict):
        self.__allCSVFileInfo.append(info)
    def __threadProcess(self):
        self.__setUiDisableState()
        sleep(0.1)#Delay for Disabling
        tasks = self.__tasks
        manager = multiprocessing.Manager()
        self.__msgQueue = manager.Queue()
        pool = multiprocessing.Pool()
        for task in tasks:
            task.append(self.__msgQueue)
        
        t = ThreadCommunicate(tasks, self.__msgQueue)
        t.logInfo.connect(self.__logInfo)
        t.logError.connect(self.__logErr)
        t.setPrgVal.connect(self.ui.progressBar.setValue)
        t.addTab.connect(self.__addCSVTab)
        t.appendCSVInfo.connect(self.__appendConvertedCSVInfo)
        t.start()

        pool.map(process, tasks)#Blocking
        self.__setUiEnableState()
        QMetaObject.invokeMethod(self.ui.selSTDFList, "clear", Qt.QueuedConnection, )
    def __setUiDisableState(self):
        self.ui.convertButton.setEnabled(False)
        self.ui.browseButton.setEnabled(False)
        self.ui.outputButton.setEnabled(False)
        self.ui.selAllButton.setEnabled(False)
        self.ui.deSelAllButton.setEnabled(False)
        self.ui.uploadButton.setEnabled(False)
        self.ui.toLeftButton.setEnabled(False)
        self.ui.toRightButton.setEnabled(False)
    def __setUiEnableState(self):
        self.ui.convertButton.setEnabled(True)
        self.ui.browseButton.setEnabled(True)
        self.ui.outputButton.setEnabled(True)
        self.ui.selAllButton.setEnabled(True)
        self.ui.deSelAllButton.setEnabled(True)
        self.ui.uploadButton.setEnabled(True)
        self.ui.toLeftButton.setEnabled(True)
        self.ui.toRightButton.setEnabled(True)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = mainWindow()
    window.show()
    sys.exit(app.exec())
