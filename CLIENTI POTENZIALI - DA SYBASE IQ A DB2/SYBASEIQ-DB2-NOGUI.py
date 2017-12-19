# import the library
import pyodbc
import jaydebeapi
import jpype
import time
import os 
import decimal

start_time = time.time()



##############################################APERTURA FILE INI IMPOSAZIONI###############################################################
file = open("setting.ini", "r") 
for riga in file:
	if riga.find("|")!=-1: #SOLO LE RIGHE DELLE IMPOSTAZIONI CHE CONTENGONO IL | VENGONO CONSIDERATE, GLI ALTRI SONO COMMENTI
		riga=riga.replace(" ","") #TOGLO GLI SPAZI BIANCHI
		impostazione,valore=riga.split("|"); #PRENDE IMPOSTAZIONE E VALORE IMPOSTAZIONE USANDO COME SEPARATORE I :
		if impostazione=='server':
			server=valore
			server=server.replace("\n","")
		if impostazione=='database':
			dbname=valore
			dbname=dbname.replace("\n","")
		if impostazione=='porta':
			porta=valore
			porta=porta.replace("\n","")
		if impostazione=='utente':
			user=valore
			user=user.replace("\n","")
		if impostazione=='password':
			password=valore
			password=password.replace("\n","")
		
##############################################FINE FILE INI IMPOSAZIONI###################################################################



#CONNESSIONE A SYBASE IQ
cnxn = pyodbc.connect("DSN=melc6h1_dmcomm")
cursiq = cnxn.cursor()
########FINE SYBASE IQ
#CONNESSIONE A DB2
jar = 'db2jcc4.jar' # location of the jdbc driver jar
args='-Djava.class.path=%s' % jar
jvm = jpype.getDefaultJVMPath()
jpype.startJVM(jvm, args)
jdbc_string="jdbc:db2://"+server+":"+porta+"/"+dbname
conn=jaydebeapi.connect("com.ibm.db2.jcc.DB2Driver", jdbc_string,[user,password]) #connessione al db2
curs=conn.cursor()
#########FINE DB2


start_time=time.time() 
cursiq.execute("SELECT id,forma,ragione_sociale,p_iva,cod_fiscale,telefono,fax,email,banca,coordinate_banca,pagamento,fido,divisione,accesso_b2b,email_b2b,note,indirizzo_sede,provincia_sede,comune_sede,cap_sede,codice_paese_sede,indirizzo_spedizione,provincia_spedizione,comune_spedizione,cap_spedizione,codice_paese_spedizione,data_acquisizione,agente,sigla_provincia_sede,sigla_provincia_spedizione, data_registrazione FROM DBA.clienti_potenziali") #SELEZIONO TUTTA LA TABELLA
rows=cursiq.fetchall()
daimportare=len(rows)
countmodificate=0
countnuove=0
for row in rows: #per ogni riga
	#############################################DATI DA SYBASE#
	id=str(row[0])
	#forma=str(row[1])
	ragione_sociale=str(row[2]).strip().replace("'"," ") #TOGLO GLI SPAZI BIANCHI
	p_iva=str(row[3]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	cod_fiscale=str(row[4]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if cod_fiscale.upper()=='NONE':
		cod_fiscale=" "
	telefono=str(row[5]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	fax=str(row[6]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if fax.upper()=='NONE':
		fax=" "
	email=str(row[7]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	banca=str(row[8]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if banca.upper()=='NONE':
		banca=" "
	coordinate_banca=str(row[9]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if coordinate_banca.upper()=='NONE':
		coordinate_banca=" "
	pagamento=str(row[10]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	fido=str(row[11]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if fido=='None':
		fido='0'
	divisione=str(row[12]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	accesso_b2b=str(row[13]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if accesso_b2b=='True':
		accesso_b2b='1'
	else:
		accesso_b2b='0'
	email_b2b=str(row[14]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	note=str(row[15]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	note=note[:254]
	if note.upper()=='NONE':
		note=" "
	indirizzo_sede=str(row[16]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	provincia_sede=str(row[17]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	comune_sede=str(row[18]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	cap_sede=str(row[19]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if cap_sede.upper()=='NONE':
		cap_sede=" "
	codice_paese_sede=str(row[20]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	indirizzo_spedizione=str(row[21]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	provincia_spedizione=str(row[22]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	comune_spedizione=str(row[23]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	cap_spedizione=str(row[24]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if cap_spedizione.upper()=='NONE':
		cap_spedizione=" "
	codice_paese_spedizione=str(row[25]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	agente=str(row[27]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if agente.upper()=='NONE':
		agente=" "
	sigla_provincia_sede=str(row[28]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if sigla_provincia_sede=='None':
		sigla_provincia_sede='ZZ'
	sigla_provincia_spedizione=str(row[29]).strip().replace("'"," ")#TOGLO GLI SPAZI BIANCHI
	if sigla_provincia_spedizione=='None':
		sigla_provincia_spedizione='ZZ'
	data_registrazione=str(row[30])
	data_acquisizione=str(row[26])
	oggi=time.strftime('%Y-%m-%d')
	if data_registrazione=='None':
		data_registrazione=oggi+' 00:00:00'
	if data_acquisizione=='None':
		data_acquisizione=oggi+' 00:00:00'
	#############################################FINE DATI DA SYBASE#
	
	curs.execute("SELECT DG_PARTITAIVA FROM DIGI.TABUTE_CLIB2B WHERE MK_IDENTIFIC_B2B='"+id+"'")
	esisteibm=len(curs.fetchall())#lunghezza array estratto, conto le righe insomma...
	if esisteibm>0: #SE GIA ESISTE AGGIORNO I DATI
		query="UPDATE DIGI.TABUTE_CLIB2B "
		query=query+"SET MK_IDENTIFIC_B2B='"+id+"',MK_RAG_SOC='"+ragione_sociale+"',DG_PARTITAIVA='"+p_iva+"',DG_CF='"+cod_fiscale+"',MK_NUM_TELEF_L16='"+telefono+"',MK_NUM_FAX_6='"+fax+"',DG_E_MAIL='"+email+"',MK_BANCA_L48='"+banca+"',MK_COORDIN_BANCA='"+coordinate_banca+"',DG_CONPAG='"+pagamento+"',MK_IMPORTO_FIDO_1='"+fido+"',DG_DIVISIONE='"+divisione+"',MK_ACCESSO_B2B='"+accesso_b2b+"',DG_E_MAIL1='"+email+"',MK_DESCR_NOTE='"+note+"',"
		query=query+"MK_INDIRIZZO_SEDE='"+indirizzo_sede+"',MK_PROVINCIA_SEDE='"+provincia_sede+"',MK_PROV_SIGLA_SEDE='"+sigla_provincia_sede+"',MK_COMUNE_SEDE='"+comune_sede+"',MK_CAP_SEDE='"+cap_sede+"',MK_COD_PAESE_SEDE='"+codice_paese_sede+"',"
		query=query+"MK_INDIRIZZO_SPED='"+indirizzo_spedizione+"',MK_PROVINCIA_SPED='"+provincia_spedizione+"',MK_PROV_SIGLA_SPED='"+sigla_provincia_spedizione+"',MK_COMUNE_SPED='"+comune_spedizione+"',MK_CAP_SPED='"+cap_spedizione+"',MK_COD_PAESE_SPED='"+codice_paese_spedizione+"',"
		query=query+"MK_AGENTE='"+agente+"',MK_DT_IMMIS_B2B='"+data_acquisizione+"' WHERE MK_IDENTIFIC_B2B='"+id+"'"
		try:
			#curs.execute(query)
			print ("SALTO ESISTENTE")
		except:
			print(query)
		curs.execute("commit")
		countmodificate=countmodificate+1
	else:#ALTRIMENTI LO INSERISCO
		query="INSERT INTO DIGI.TABUTE_CLIB2B (DG_C_TRACC_REC,DG_C_SOC,DG_C_DIVS,DG_C_VERS,"
		query=query+"DG_PARTITAIVA,DG_CF,MK_IDENTIFIC_B2B) "
		query=query+"VALUES ('CLIB2B','0100','00','00','"+p_iva+"','"+cod_fiscale+"','"+id+"')"
		try:##INSERISCO SOLO PIVA E CF POI AGGIORNO
			curs.execute(query)
		except:
			print(query)
		curs.execute("commit")
		query="UPDATE DIGI.TABUTE_CLIB2B "
		query=query+"SET MK_IDENTIFIC_B2B='"+id+"',MK_RAG_SOC='"+ragione_sociale+"',DG_PARTITAIVA='"+p_iva+"',DG_CF='"+cod_fiscale+"',MK_NUM_TELEF_L16='"+telefono+"',MK_NUM_FAX_6='"+fax+"',DG_E_MAIL='"+email+"',MK_BANCA_L48='"+banca+"',MK_COORDIN_BANCA='"+coordinate_banca+"',DG_CONPAG='"+pagamento+"',MK_IMPORTO_FIDO_1='"+fido+"',DG_DIVISIONE='"+divisione+"',MK_ACCESSO_B2B='"+accesso_b2b+"',DG_E_MAIL1='"+email+"',MK_DESCR_NOTE='"+note+"',"
		query=query+"MK_INDIRIZZO_SEDE='"+indirizzo_sede+"',MK_PROVINCIA_SEDE='"+provincia_sede+"',MK_PROV_SIGLA_SEDE='"+sigla_provincia_sede+"',MK_COMUNE_SEDE='"+comune_sede+"',MK_CAP_SEDE='"+cap_sede+"',MK_COD_PAESE_SEDE='"+codice_paese_sede+"',"
		query=query+"MK_INDIRIZZO_SPED='"+indirizzo_spedizione+"',MK_PROVINCIA_SPED='"+provincia_spedizione+"',MK_PROV_SIGLA_SPED='"+sigla_provincia_spedizione+"',MK_COMUNE_SPED='"+comune_spedizione+"',MK_CAP_SPED='"+cap_spedizione+"',MK_COD_PAESE_SPED='"+codice_paese_spedizione+"',"
		query=query+"MK_AGENTE='"+agente+"',MK_DT_IMMIS_B2B='"+data_acquisizione+"' WHERE MK_IDENTIFIC_B2B='"+id+"'"
		try:##INSERISCO SOLO PIVA E CF POI AGGIORNO
			curs.execute(query)
		except:
			print(query)
		curs.execute("commit")
		countnuove=countnuove+1
		
end_time=time.time()	
tempo=str(int(end_time-start_time))	
daimportare=str(daimportare)
countnuove=str(countnuove)
countmodificate=str(countmodificate)
print ("IMPORTAZIONE COMPLETATA", "RIGHE SU SYBASE IQ: "+daimportare+"\nRIGHE INSERITE SU DB2: "+countnuove+"\nRIGHE SALTATE SU DB2: "+countmodificate)
print("TEMPO PER IL PASSAGGIO: "+tempo+" secondi")