'''
    Assessment :
Make a Folder named Processing
Write a code that makes a file(txt) every second in the Processing folder
Make another folder in-que
Write a code that picks all files from processing and move all the files to in-que every 5 second
make another folder named Processed
Write a code that picks file from in-que folder and updates a column in MySQL/mongoDB table as 0/1 and move the file to the Processed folder
Also, make sure no files moved from Processing to in-que until in-que folder is empty
This means even if in-que folder have even 1 file no new file to be copied from the Processing folder and the in-que folder to be empty to to move files from the processing folder
'''

from time import sleep
from threading import *
import shutil
import os
import mysql.connector as con

''' I Solved This problem using multi-threading, Because i think this is the best solution for solve this type of problems  '''

'''
Folder names:
    Processing , in-que , Processed
        
'''



'''Processing Class is responsible to create file on every second'''
class Processing(Thread):
    def run(self):
        Processing_counter = 1
        while(True):
            open("Processing\\file_"+str(Processing_counter)+".txt","w")
            Processing_counter += 1
            sleep(1)

'''In-que Class is responsible to move file from Processing to in-que when que is Empty'''
class In_que(Thread):
    def run(self):
        src = 'Processing'
        dest = 'in-que'
        while(True):
            sleep(5)
            if len(os.listdir(dest))==0:
                files = os.listdir(src)
                for file in files:
                    file_name = os.path.join(src, file)
                    shutil.move(file_name, dest)

'''Processed Class is responsible to move file from que to Processed and update the column of mysql '''
class Processed(Thread):
    def run(self):
        mydb = con.connect(host="localhost", user="root", passwd="", database="bilateralassessment")
        mycursor = mydb.cursor()
        dest = 'Processed'
        src = 'in-que'
        while(True):
            if len(os.listdir(src)) != 0:
                files = os.listdir(src)
                for file in files:
                    s = "INSERT INTO `processes` (`ProcessValue`, `Status`) VALUES (%s, %s)"
                    b1 = (file, 1)
                    mycursor.execute(s, b1)
                    mydb.commit()
                    file_name = os.path.join(src, file)
                    shutil.move(file_name, dest)

'''Creating objects of every thread'''
Processing_thread = Processing()
Queue_thread = In_que()
Processed_thread = Processed()
'''Start the Thread'''
Processing_thread.start()
Queue_thread.start()
Processed_thread.start()