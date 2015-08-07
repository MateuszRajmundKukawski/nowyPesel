import re, codecs, os

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import func, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker 
from _sqlite3 import Row


class UpdatePesel(object):
    def __init__(self, pesel_file, db_file_path, password='masterkey', usr='SYSDBA',  ):
        
        
        self.dbfile_path = db_file_path
        self.connection_string = ('firebird+fdb://{user}:{admin_pass}@localhost/{file_path}').format(user=usr,admin_pass=password,file_path=db_file_path)
        self.bd_connect()
        self.fname = pesel_file
        Base = declarative_base()
        Base.metadata.create_all(self.engine)         
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.paternName = self.connection_string.split('/')[-1][:-4]
        self.workdir_path = os.path.dirname(self.connection_string.split('localhost/')[-1])
        self.header_csv = ['id', 'id_2', 'cos', 'cos2', 'plec',
                           'nzw', 'nzw2', 'pim', 'dim', 'imo',
                           'imm', 'cos3', 'kod', 'miej', 'ul',
                           'nr_bd', 'nr_miesz', 'cos4', 'cos5',
                           'cos6', 'cos7', 'cos8', 'psl']

        
        
    def bd_connect(self):

        self.engine = create_engine(self.connection_string, echo=False)
        self.connection = self.engine.connect()


            
    def set_pesel_file(self):

        with codecs.open(self.fname, 'r', 'utf-8') as f:
            self.plik_lista = [ line.rstrip( "\n" ).split('\t') for line in f if re.search(self.paternName, line)]
        psl_list = []
        bad_psl = []
        psl_double = []
        
        for row in self.plik_lista:
            val_psl = row[22].strip()            
            
            if len(val_psl) <> 11 :
            ## zly numer pesel w pliku
                bad_psl.append(val_psl)                
            elif val_psl not in psl_list:
            ## lista bez dubli    
                psl_list.append(val_psl)
            else:
            # duble    
                psl_double.append(val_psl)
                
        self.correct_psl_list = []
        self.bad_psl_list = []
        self.double_psl_list = []
                
        for row in self.plik_lista:
            val_psl = row[22].strip()
            if not val_psl in psl_double and len(val_psl)==11:
                self.correct_psl_list.append(row)
            elif len(val_psl) <> 11:
                self.bad_psl_list.append(row)
            else:                
                self.double_psl_list.append(row)
             

   
        
        self.bad_pesel_file = ('/'.join((self.workdir_path, self.paternName+'_bledy_pesel.csv')))
        self.double_pesel_file = ('/'.join((self.workdir_path, self.paternName+'_duble.csv')))
        self.writeFile(self.bad_pesel_file, self.bad_psl_list)
        self.writeFile(self.double_pesel_file, self.double_psl_list)
        
        
        
    def writeFile(self, filename, filelist):
        with codecs.open(filename, 'w', 'windows-1250') as f:
            f.write(';'.join(self.header_csv)+'\n')
            for row in filelist:
                f.write(';'.join(row)+'\n')

        
         
    def updateRow(self, sampleRow):
        
    
        for i in range(len(sampleRow)):
            #sampleRow[i]=sampleRow[i].lower()
            if sampleRow[i].strip()=='':
                sampleRow[i]=None
        plecval =sampleRow[4]
        try:
            pslval=str(sampleRow[22]).strip()
        except:
            pslval = sampleRow[22]
        
        pimval=sampleRow[7].encode('windows-1250')
        dimval= sampleRow[8]
        nzwval=sampleRow[5].encode('windows-1250')
        if sampleRow[9]:
            oimval=sampleRow[9].encode('windows-1250')
        else:
            oimval=sampleRow[9]
        if sampleRow[10]:
            mimval=sampleRow[10].encode('windows-1250')
        else:
            mimval=sampleRow[10]
        try:
            kodval = sampleRow[12].replace(' ', '')
        except:
            kodval=sampleRow[12]
       
        if sampleRow[13] <> None:
            nazval = sampleRow[13].upper().encode('windows-1250')
            nazval = '%'+nazval+'%'
        else:
            nazval = ''
        
        #nraval = re.sub('"', '', sampleRow[15])
        try:
            nraval = re.sub('"', '', sampleRow[15])
            nraval = str(nraval)
        except:
            nraval=sampleRow[15]
        if  nraval is not  None and re.search('0,', nraval):
            nraval= None

        ############################
        # test czy pesel juz jest w bazie!!!!
        ############################
        if self.session.query(Osoby).filter(Osoby.psl == pslval.strip()).count()>=1:
            return 'jest_w_bazie'
#         if self.session.query(Osoby).filter(Osoby.psl <> 666).count()>=1:
#             return 'jest_w_bazie2'
        
        rawquery = self.session.query(Osoby.uid).join(Adresy).\
            filter(Osoby.plec==plecval,\
                 Osoby.pim==pimval,\
                 Osoby.dim==dimval,\
                 Osoby.nzw==nzwval,\
                 Osoby.oim==oimval,\
                 Osoby.mim ==mimval,\
                func.upper(Adresy.naz).like(nazval),\
                Adresy.kod==kodval,\
                func.trim(Adresy.nra)==nraval,\
                )        
        q=rawquery.subquery()        
        rowcunt = 0
        #print plecval, pimval, dimval, nzwval, oimval, mimval, nazval, kodval, nraval
        rowcunt = rawquery.count()        
        #rowcunt = q.count()
        if rowcunt ==  0:            
            return 'nie_znalazl'
#         elif rowcunt == 1 :
#             tt = self.session.query(Osoby.psl).filter(Osoby.uid.in_(q)).all()
#             if tt[0][0] <> None: print tt[0]
#             #else: print 'jest None'
#             return 'jest_w_bazie2'
    
        elif rowcunt == 1:
            
            self.session.query(Osoby).filter(Osoby.uid.in_(q)).\
                update({Osoby.psl: pslval}, synchronize_session='fetch')
                            
            self.session.commit()
            #print self.session.query(Osoby.psl, Osoby.pim).filter(Osoby.uid.in_(q)).all()
            return rowcunt
        
        elif rowcunt >1:
            return 'dubel'
        
    def updaet_db(self):
        self.set_pesel_file()
        
        self.inBaseFile = ('/'.join((self.workdir_path, self.paternName+'_jest_w_bazie.csv')))
        self.okFile = ('/'.join((self.workdir_path, self.paternName+'_przeszly.csv')))
        #self.badPSL = ('/'.join((self.workdir_path, self.paternName+'_zlyPesel.txt')))
        self.dubleVal = ('/'.join((self.workdir_path, self.paternName+'_blednyWynik_wiekszy_zwrot.csv')))
        self.otherVal = ('/'.join((self.workdir_path, self.paternName+'_inne.csv')))
#         
#         self.inDB = codecs.open(self.inBaseFile, 'w', 'windows-1250')
#         self.okF = codecs.open(self.okFile, 'w', 'windows-1250')
#         #self.badFile = codecs.open(self.badPSL, 'w', 'windows-1250')
#         self.dubleF =  codecs.open(self.dubleVal, 'w', 'windows-1250')
#         self.othVal = codecs.open(self.otherVal, 'w', 'windows-1250')
        
        with  codecs.open(self.inBaseFile, 'w', 'windows-1250') as self.inDB, codecs.open(self.okFile, 'w', 'windows-1250') as self.okF, codecs.open(self.dubleVal, 'w', 'windows-1250') as self.dubleF, codecs.open(self.otherVal, 'w', 'windows-1250') as self.othVal:
            self.inDB.write(';'.join(self.header_csv)+';opis\n')
            self.okF.write(';'.join(self.header_csv)+';opis\n')
            self.dubleF.write(';'.join(self.header_csv)+';opis\n')
            self.othVal.write(';'.join(self.header_csv)+';opis\n')
            
        
        
            for sampleRow in self.correct_psl_list:
                newRow = sampleRow[:]
    #             if len(sampleRow[22].strip()) <> 11 and re.search(self.paternName, sampleRow[0]):
    #                 self.badFile.write('\t'.join(newRow).strip('\r')+'\t'+str(self.updateRow(sampleRow))+'\n')
                
                returnVal = self.updateRow(sampleRow)
                if returnVal == 1:
                    self.okF.write(';'.join(newRow).strip('\r')+';'+str(returnVal)+'\n')
                elif returnVal in ('jest_w_bazie', 'jest_w_bazie2'):
                    self.inDB.write(';'.join(newRow).strip('\r')+';'+returnVal+'\n')
                elif returnVal == 'dubel':
                    self.dubleF.write(';'.join(newRow).strip('\r')+';'+returnVal+'\n')
                else:
                    self.othVal.write(';'.join(newRow).strip('\r')+';'+returnVal+'\n')
#         self.inDB.close()
#         self.okF.close()
#         #self.badFile.close()
#         self.dubleF.close()
#         self.othVal.close()
        
            
         
                
                
        statinfo = os.stat(self.inBaseFile)
        if statinfo.st_size == 0:            
            os.remove(self.inBaseFile)
            return False            
        else:                       
            return True
         
    def minorityReport(self):
       
        self.nonPslFile = self.dbfile_path[:-4]+'_pusteStart.txt'
        #print nonPslFile
        #with open(nonPslFile, 'w') as f:
        nonPslList =  self.session.query(Osoby).filter(Osoby.psl == None).all()
        nonPslList += self.session.query(Osoby).filter(Osoby.psl == '').all()
        
        #print nonPslList
        with open(self.nonPslFile, 'w') as f:
            f.write('OSOBA\n')
            for person in nonPslList:
                f.write(str(person)+'\n')
                
    def nullPSL(self):
        #print 'in nullPSL '
        self.nullPslFile = self.dbfile_path[:-4]+'_pusteKoniec.txt'
        nonPslList = self.session.query(Osoby).filter(Osoby.psl == None).all()
        nonPslList += self.session.query(Osoby).filter(Osoby.psl == '').all()
        #print nonPslList
        with open(self.nullPslFile, 'w') as f:
            f.write('OSOBA\n')
            for person in nonPslList:
                f.write(str(person)+'\n')
        
    def generateReport(self):
        
        repFile = self.dbfile_path[:-4]+'_Raport.csv'
        flist = (self.bad_pesel_file, self.double_pesel_file, self.nonPslFile, self.nullPslFile, self.inBaseFile, self.okFile, self.dubleVal, self.otherVal)
        repDic = {}
        for workFile in flist:
            print workFile
            if os.path.isfile(workFile):
                with open(workFile) as f:
                    tmpList = [row.rstrip() for row in f]
                    repDic[os.path.basename(workFile)] = str(len(tmpList)-1)
            else:
                repDic[os.path.basename(workFile)] = 'Null'
        with open(repFile, 'w') as f:
            
            for _key in sorted(repDic):
                f.write(';'.join([_key, repDic[_key]]))
                f.write('\n')
            #f.write('wiewiorki')
            update_rows = int(repDic[os.path.basename(self.nonPslFile)])-int(repDic[os.path.basename(self.nullPslFile)])
            f.write(';'.join(['Uaktualniono:', str(update_rows)]))
            f.write('\n')
            control_val = int(repDic[os.path.basename(self.okFile)]) - update_rows                 
            f.write(';'.join(['Kontrola:', str(control_val)]
                             ))
    def closeConnection(self):
        
        self.connection.close()
        self.engine = None
                                                        
                                                             
                    
                
        
        
    
    
        

Base = declarative_base()

class Osoby(Base):
    __tablename__ = 'osoby'
    uid = Column(Integer, primary_key=True)
    plec = Column(String)
    psl = Column(String)
    pim = Column(String)
    dim = Column(String)
    nzw = Column(String)
    oim = Column(String)
    mim = Column(String)
    radr = Column(Integer,  ForeignKey('adresy.id'))

    def __init__(self,plec, psl , pim , dim , nzw , oim , mim , radr):
        self.plec = plec
        self.psl  = psl
        self.pim  = pim
        self.dim  = dim
        self.nzw  = nzw
        self.oim  = oim
        self.mim  = mim
        self.radr = radr
    def __repr__(self):
        #return "%s %s: pesel: %s"%(self.pim, self.nzw, self.psl)
        return "%s %s "%(self.pim, self.nzw)
    

class Adresy(Base):
    __tablename__ = 'adresy'
    uid = Column(Integer, primary_key=True)
    kod = Column(String)
    naz = Column(String)
    id = Column(Integer)
    nra = Column(String)

    def _init_(self, kod, naz, id, nra):
        self.kod = kod
        self.naz = naz
        self.id = id
        self.nra = nra
    def __repr__(self):
        return self.naz


class TestPesel(object):
    def __init__(self, pesel_file, db_file_path):
        
        self.fname = pesel_file
        self.dbfile_path = db_file_path
        self.paternName = os.path.basename(self.dbfile_path)[:-4]
        self.workdir_path = os.path.dirname(self.fname)
        
            
    def set_pesel_file(self):

        with codecs.open(self.fname, 'r', 'utf-8') as f:
            self.plik_lista = [ line.rstrip( "\n" ).split('\t') for line in f if re.search(self.paternName, line)]
        psl_list = []
        bad_psl = []
        psl_double = []
        
        for row in self.plik_lista:
            val_psl = row[22].strip()            
            
            if len(val_psl) <> 11 :
            ## zly numer pesel w pliku
                bad_psl.append(val_psl)                
            elif val_psl not in psl_list:
            ## lista bez dubli    
                psl_list.append(val_psl)
            else:
            # duble    
                psl_double.append(val_psl)
                
        self.correct_psl_list = []
        self.bad_psl_list = []
        self.double_psl_list = []
                
        for row in self.plik_lista:
            val_psl = row[22].strip()
            if not val_psl in psl_double and len(val_psl)==11:
                self.correct_psl_list.append(row)
            elif len(val_psl) <> 11:
                self.bad_psl_list.append(row)
            else:
                
                self.double_psl_list.append(row)
             

   
        
        self.bad_pesel_file = ('/'.join((self.workdir_path, self.paternName+'_bledy_pesel.csv')))
        self.double_pesel_file = ('/'.join((self.workdir_path, self.paternName+'_duble.csv')))
        self.writeFile(self.bad_pesel_file, self.bad_psl_list)
        self.writeFile(self.double_pesel_file, self.double_psl_list)
        
        
        
    def writeFile(self, filename, filelist):
        with codecs.open(filename, 'w', 'windows-1250') as f:
            for row in filelist:
                f.write(';'.join(row)+'\n')
                
        
                
                
                
            
             
        
            




if __name__ == '__main__':
    bdpath = 'e:/__BAZY_ZSIN__/PESEL/BAZY_UPDATE/1815012.fdb'
    connection_string = 'firebird+fdb://SYSDBA:masterkey@localhost/%s'%bdpath
    popesel = 'e:/__BAZY_ZSIN__/PESEL/BAZY_UPDATE/po_PESEL_tt'
    psl_file = '%s'%popesel
    
    print os.path.isfile(psl_file)
    x = TestPesel(psl_file, bdpath)
    x.set_pesel_file()
    
    #print os.path.dirname(connection_string.split('localhost/')[-1])
    
#     x = UpdatePesel(db_file_path = connection_string, pesel_file=psl_file)
#     x.minorityReport()
       
    
    #x.ud_db()
    print 'end'
    

