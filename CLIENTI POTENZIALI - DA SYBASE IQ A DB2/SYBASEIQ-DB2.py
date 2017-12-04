# import the library
import pyodbc
import jaydebeapi
import jpype
import time
import os 

start_time = time.time()

#CONNESSIONE A SYBASE IQ
cnxn = pyodbc.connect("DSN=melc6h1_dmcomm")
cursiq = cnxn.cursor()
########FINE SYBASE IQ
#CONNESSIONE A DB2
jar = 'db2jcc4.jar' # location of the jdbc driver jar
args='-Djava.class.path=%s' % jar
jvm = jpype.getDefaultJVMPath()
jpype.startJVM(jvm, args)
conn=jaydebeapi.connect('com.ibm.db2.jcc.DB2Driver', 'jdbc:db2://10.1.12.69:50000/s69mk0su',['db2inst1','db2inst1']) #connessione al db2
curs=conn.cursor()
#########FINE DB2

#######BACKUP DATI SU DB2
print ("INIZIO ESPORTAZIONE TABELLA DA DB2 PER BACKUP")
oggi=time.strftime('%d-%m-%Y')#data di oggi

backup="AZMAIL-BACKUP"+oggi+".txt"

if os.path.isfile(backup):
	controllo1 = input("Esiste già un backup con la data di oggi, sovrascrivo? SI-NO ")
	if controllo1=="SI" or controllo1=="si" or controllo1=="Si":
		controllo2 = input("Sei sicuro? SI-NO")
		if controllo2=="SI" or controllo2=="si" or controllo2=="Si":
			print ("PROVO A SOVRASCRIVERE IL FILE...\n")
		else:
			exit()
	else:
		exit()

	
iniz_bck_time=time.time()
curs.execute("SELECT * FROM DIGI.TABUTE_AZMAIL")
rows=curs.fetchall()
file = open(backup,"w") 
for row in rows:
	controllocampo1=0
	for field in row:
		field=str(field)
		field=field.replace("None","")#ELIMINO I NULL ESTRATTI...
		if controllocampo1>0: #IL PRIMO CAMPO NON è PRECEDUTO DA ","
			file.write(',"'+field+'"')
		else:
			file.write('"'+field+'"')
		controllocampo1=controllocampo1+1
	file.write ("\n")
file.close()
########FINE BACKUP
bck_time=time.time()



########PROCEDURA SYBASE -> DB2
print ("INIZIO PROCEDURA IMPORTAZIONE DA SYBASE IQ VERSO DB2")
cursiq.execute("SELECT codice_cliente,e_mail FROM DBA.clienti_email") #SELEZIONO TUTTA LA TABELLA
rows=cursiq.fetchall()
daimportare=len(rows)
countmodificate=0
countnuove=0
for row in rows: #per ogni riga
	codcli=row[0]
	email=row[1]
	curs.execute("SELECT DG_INDIRIZZO_EMAIL FROM DIGI.TABUTE_AZMAIL WHERE MK_CONTROPARTE='"+codcli+"' AND MK_TIPO_CONTROPART='CLIENT' AND MK_UNITA_ORGANIZ='CNTFOR'")
	esisteibm=len(curs.fetchall())#lunghezza array estratto, conto le righe insomma...
	if esisteibm>0: #SE GIA ESISTE AGGIORNO SOLO LA MAIL
		curs.execute("UPDATE DIGI.TABUTE_AZMAIL SET DG_INDIRIZZO_EMAIL='"+email+"',MK_FATT_MAIL='1' WHERE MK_CONTROPARTE='"+codcli+"'")
		curs.execute("commit")
		countmodificate=countmodificate+1
	else:#ALTRIMENTI LO INSERISCO
		curs.execute("INSERT INTO DIGI.TABUTE_AZMAIL (DG_C_SOC,DG_C_DIVS,DG_C_VERS,MK_DIVISIONE,MK_TIPO_CONTROPART,MK_CONTROPARTE,MK_UNITA_ORGANIZ,MK_PROGR_2,DG_INDIRIZZO_EMAIL,MK_FATT_MAIL) VALUES ('0100','00','00','00','CLIENT','"+codcli+"','CNTFOR','1','"+email+"','1')")
		curs.execute("commit")
		countnuove=countnuove+1

daimportare=str(daimportare)
countnuove=str(countnuove)
countmodificate=str(countmodificate)
print ("\n\n\n\n")
print ("RIGHE SU SYBASE IQ: "+daimportare)
print ("RIGHE INSERITE SU DB2: "+countnuove)
print ("RIGHE MODIFICATE SU DB2: "+countmodificate)

end_time=time.time()
print ("\n\n\n\n")
print("--- TEMPO REALE IMPIEGATO PER IL BACKUP: %s secondi ---" % (bck_time - iniz_bck_time))
print("--- TEMPO IMPIEGATO PER L'IMPORTAZIONE DA SYBASE IQ: %s secondi ---" % (end_time - bck_time))
print("--- TEMPO TOTALE DI ESECUZIONE: %s secondi ---" % (end_time - start_time))