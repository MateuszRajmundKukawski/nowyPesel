import re, codecs, os

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import func, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker 


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
        
        
    def bd_connect(self):

        self.engine = create_engine(self.connection_string, echo=False)
        self.connection = self.engine.connect()
        
    def set_pesel_file(self):

        with codecs.open(self.fname, 'r', 'utf-8') as f:
            self.plik_lista = [ line.rstrip( "\n" ).split('\t') for line in f]
        
        
         
    def updateRow(self, sampleRow):
        
    
        for i in range(len(sampleRow)):
            #sampleRow[i]=sampleRow[i].lower()
            if sampleRow[i]=='':
                sampleRow[i]=None
        plecval =sampleRow[4]
        try:
            pslval=str(sampleRow[22]).strip('\r')
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
        try:
            nraval = str(sampleRow[15])
        except:
            nraval=sampleRow[15]
        if re.search('0,', nraval):
            nraval= None

        ############################
        # test czy pesel juz jest w bazie!!!!
        ############################
        if self.session.query(Osoby).filter(Osoby.psl == pslval.strip()).count()>=1:
            return 'jest_w_bazie'
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
        #rowcunt = 0
        rowcunt = rawquery.count()        
        #rowcunt = q.count()
        if rowcunt ==  0:            
            return 'nie_znalazl'
    
        elif rowcunt == 1:
            
            self.session.query(Osoby).filter(Osoby.uid.in_(q)).\
                update({Osoby.psl: pslval}, synchronize_session='fetch')            
            self.session.commit()
            return rowcunt
        elif rowcunt >1:
            return 'dubel'
        
    def updaet_db(self):
        self.set_pesel_file()
        
        self.inBaseFile = ('/'.join((self.workdir_path, self.paternName+'_jest_w_bazie.txt')))
        self.okFile = ('/'.join((self.workdir_path, self.paternName+'_przeszly.txt')))
        self.badPSL = ('/'.join((self.workdir_path, self.paternName+'_zlyPesel.txt')))
        self.dubleVal = ('/'.join((self.workdir_path, self.paternName+'_blednyWynik.txt')))
        
        with codecs.open(self.inBaseFile, 'w', 'windows-1250') as f:
            with codecs.open(self.okFile, 'w', 'windows-1250') as okf:
            
                for sampleRow in self.plik_lista:                
                    newRow = sampleRow[:]                
                    if len(sampleRow[22].strip()) == 11 and re.search(self.paternName, sampleRow[0]):                    
                        if self.updateRow(sampleRow) <> 1:                        
                            f.write('\t'.join(newRow).strip('\r')+'\t'+str(self.updateRow(sampleRow))+'\n')
                        else:
                            okf.write('\t'.join(newRow).strip('\r')+'\t'+str(self.updateRow(sampleRow))+'\n')
                    elif len(sampleRow[22].strip()) <> 11 and re.search(self.paternName, sampleRow[0]):
                        f.write('\t'.join(newRow).strip('\r')+'\t'+str(self.updateRow(sampleRow))+'\n')
                
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
        nonPslList = self.session.query(Osoby).filter(Osoby.psl == None).all()
        with open(self.nonPslFile, 'w') as f:
            for person in nonPslList:
                f.write(str(person)+'\n')
                
    def nullPSL(self):
         
        self.nullPslFile = self.dbfile_path[:-4]+'_pusteKoniec.txt'
        nonPslList = self.session.query(Osoby).filter(Osoby.psl == None).all()
        with open(self.nullPslFile, 'w') as f:
            for person in nonPslList:
                f.write(str(person)+'\n')
        
    def generateReport(self):
        
        repFile = self.dbfile_path[:-4]+'_Raport.csv'
        flist = (self.nonPslFile, self.faultFile, self.okFile, self.nullPslFile)
        repDic = {}
        for workFile in flist:
            if os.path.isfile(workFile):
                with open(workFile) as f:
                    tmpList = [row.rstrip() for row in f]
                    repDic[os.path.basename(workFile)] = str(len(tmpList))
            else:
                repDic[os.path.basename(workFile)] = 'Null'
        with open(repFile, 'w') as f:
            
            for _key, _val in repDic.items():
                f.write(';'.join([_key, _val]))
                f.write('\n')
    def closeConnection(self):
        
        self.connection.close()
                                                        
                                                             
                    
                
        
        
    
    
        

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







if __name__ == '__main__':
    bdpath = 'e:/__BAZY_ZSIN__/PESEL/BAZY_UPDATE/1815012.fdb'
    connection_string = 'firebird+fdb://SYSDBA:masterkey@localhost/%s'%bdpath
    popesel = 'e:/__BAZY_ZSIN__/PESEL/BAZY_UPDATE/po_PESEL'
    psl_file = '%s'%popesel
    
    
    print os.path.dirname(connection_string.split('localhost/')[-1])
    x = UpdatePesel(db_file_path = connection_string, pesel_file=psl_file)
    x.minorityReport()
       
    
    #x.ud_db()
    print 'end'
    

