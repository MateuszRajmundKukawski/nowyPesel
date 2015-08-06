# -*- coding: utf-8 -*-
from Tkinter import *
import tkMessageBox
from tkFileDialog import askopenfilename
import threading

 

import os


from db_utils.dbtools import DbTools
from db_utils.dbutils import UpdatePesel

class MyApp(Frame):
    def __init__(self, parent):
        Frame.__init__(self, parent)
        self.parent = parent
        self.initUI()
        self.userUI()
        self.pack()

    def initUI(self):
        self.parent.geometry("200x100")
        self.parent.title("Update PESEL")

    def userUI(self):
        self.pad_x = 10
        self.pad_y = 5
        self.buttonWidth = 12
        self.newthread = None
        self.file_name = StringVar()
        self.runApp_text = StringVar()
        self.runApp_text.set('?')
        self.workdir_fullpath = None
        self.path_file = 'db_utils/app.data'

        self.file_fullpath = None
        self.peselfile_fullpath = None

        self.initMenu()
        ########################
        
        self.get_file_Button = Button(self, text="Select base", command=self.get_file, width=self.buttonWidth)
        self.get_file_Button.grid(row=0, column=0, sticky=W, padx=self.pad_x,pady = self.pad_y)

        self.file_name = StringVar()
        self.file_name.set('?')
        self.file_name_Label = Label(self, textvariable=self.file_name)
        self.file_name_Label.grid(row=0, column=1)
        ########################
        
        self.pesel_name_Button = Button(self, text="PeselFile", command=self.get_pesel_file, width=self.buttonWidth)
        self.pesel_name_Button.grid(row=1, column=0, sticky=W, padx=self.pad_x,pady = self.pad_y)
        
        self.pesel_name = StringVar()
        self.pesel_name.set('?')
        self.pesel_name_Label = Label(self, textvariable=self.pesel_name)
        self.pesel_name_Label.grid(row=1, column=1)
        ########################
        self.runApp_Button = Button(self, text="runAPP", command=self.runApp, width=self.buttonWidth)
        self.runApp_Button.grid(row=2, column=0, sticky=W, padx=self.pad_x,pady = self.pad_y)
        
        self.runApp_Label = Label(self, textvariable=self.runApp_text)
        self.runApp_Label.grid(row=2, column=1, sticky=W, padx=self.pad_x,pady = self.pad_y)
        #########################


    def initMenu(self):
        menubar = Menu(self)
        self.parent.config(menu=menubar)

        fileMenu = Menu(menubar)
        fileMenu.add_command(label="Info", command=self.app_info)
        fileMenu.add_command(label="Exit", command=self.onExit)
        menubar.add_cascade(label="File", menu=fileMenu)

    def onExit(self):

        self.quit()

    def get_file(self):

        self.get_lastdir()
        self.file_fullpath = askopenfilename(
            initialdir=self.workdir_fullpath,
            filetypes=[("Fdb files", "*.fdb")], )
        if os.path.isfile(self.file_fullpath):
            self.set_lastdir(os.path.dirname(self.file_fullpath))
            self.file_name.set(os.path.basename(self.file_fullpath))
            self.test_connection()
            
    def get_pesel_file(self):

        self.get_lastdir()
        self.peselfile_fullpath = askopenfilename(
            initialdir=self.workdir_fullpath,
            filetypes=[("Files", "*.*")], )
        self.pesel_name.set(os.path.basename(self.peselfile_fullpath))
        

    def get_lastdir(self):

        if os.path.isfile(self.path_file):
            with open(self.path_file) as f:
                tmp = f.read()
                if os.path.isdir(tmp):
                    self.workdir_fullpath = tmp
                else:
                    self.workdir_fullpath = "c:/"
        else:
                    self.workdir_fullpath = "c:/"

    def set_lastdir(self, dirpath):

        with open(self.path_file, 'w') as f:
            f.write(dirpath)

    def app_info(self):

        msgText = 'Aplikacja zasila baze danych\nnumerami PESEL'
        tkMessageBox.showinfo("Info", msgText)

    def runApp(self):
        if self.file_fullpath  and self.peselfile_fullpath:
        #print self.peselfile_fullpath
            print self.file_fullpath
            if not self.newthread or not self.newthread.is_alive():
                x = tkMessageBox
                self.runApp_text.set('running')
                self.newthread = UpdetePeselThread(self.runApp_text, self.peselfile_fullpath, self.file_fullpath, x)
                self.newthread.start()
        else:
            msgText = 'Error'
            tkMessageBox.showerror("Error", msgText)
            

    def test_connection(self):
        try:
            DbTools(self.file_fullpath)
            msgText = 'Connection successful'
            tkMessageBox.showinfo("Info", msgText)
        except:
            msgText = 'Connection error'
            tkMessageBox.showerror("Error", msgText)
        
        
class UpdetePeselThread(threading.Thread, Tk):
 
    def __init__(self, runApp_text, peselfile_fullpath, db_file_path, mb ):
        threading.Thread.__init__(self)
        self.peselfile_fullpath = peselfile_fullpath
        self.file_fullpath = db_file_path
        self.daemon = True        
        self.textvariable = runApp_text
        self.koniec = False
        self.mb = mb
 
    def run(self):
        
        x = UpdatePesel(pesel_file=self.peselfile_fullpath, db_file_path=self.file_fullpath)
        x.minorityReport()
        tmp = x.updaet_db()
        x.nullPSL()
        x.closeConnection()
        #print 'rtn =', tmp
        #self.mb.showinfo('tt', 'tt')
        #self.textvariable.set('po')
        
                 
        if tmp is False:              
            self.textvariable.set('OK')
             
        else:
            self.textvariable.set('Braki.txt')
        x.generateReport()
        x.closeConnection()
             
      
          

app_window = Tk()
app = MyApp(parent=app_window)
app.mainloop()