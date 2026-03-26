#-*- coding:utf-8 -*-
import os;
import re;
from ftplib import FTP;
import pyodbc;
import datetime

#Variables

#"path" est la variable qui reçoit le chemin absolue du répertoire courant
path = os.getcwd()

#Connexion BD

server = '<DB_SERVER>' #nom 
database = '<DB_NAME>' #nom de la base de donnÃ©e
username = '<DB_USERNAME>' #nom utilisateur BD
psswrd = '<DB_PASSWORD>' #mot de passe BD
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};server='+server+';database='+database+';uid='+username+';pwd='+ psswrd)#connexion Ã  la BD

# Cettte requête permet d'obtenir le nombre de Valeurs qui peuple la table "Valeur", cela permet d'automatiser la production de clef primaire

request = 'SELECT COUNT(*) FROM Valeur';
cursor1 = cnxn.execute(request);
for c in cursor1:
    increV = int(c[0]) #invreV est le nombre de valeur inscrit en base, cela permet d'automatiser la déclaration de la clef primaire
cursor1.close()

#Connexion FTP

host = "<FTP_HOST>"; # adresse du serveur FTP
password = "<FTP_PASSWORD>"; # votre mot de passe
user = "<FTP_USERNAME>"; # votre identifiant
connect = FTP(host, user, password); # on se connecte


##Met le listage du répertoire dans lDossier

lDossier = []
connect.retrlines('LIST ./', lDossier.append) ## Listage du repertoire a telecharge

#Pour chaque nom de dossier dans le dossier courant

for d in lDossier:
        print d.split()[8]
        numEnr = d.split()[8].split('R') #permet d'avoir le numéro de l'enregistreur d'ou provient les données, l'enregistreur 1 devra avoir un nom de répertoire dans le serveur FTP de test1


#Met le listage du répertoire "d.split()[8]" dans l

        l = [];
        connect.retrlines('LIST ./'+d.split()[8], l.append) ## Listage du repertoire a telecharge

    #Pour chaque fichier "i.split()[8]" dans le dossier "d.split()[8]"
    
        for i in l: #pour chaque fichier

#Téléchargement fichier FTP

            pat = re.search('^old(.)*', i.split()[8])
            if pat == None:
                tof = os.path.isfile('./fileYokogawa/'+i.split()[8]) #si le fichier existe, il a était téléchargé
                print tof
                if tof == False: 
                    connect.retrbinary('RETR ./'+d.split()[8]+'/'+i.split()[8], open('./fileYokogawa/'+i.split()[8], 'wb').write)#télécharge le fichier i.split()[8] du dossier d.split()[8] du serveur ftp
                
		

#Extraction des valeurs et mise en BD		
		
        for i in l: #pour chaque fichier
            print i.split()[8]
            pat = re.search('^old(.)*', i.split()[8]) #recherche au debut de la chaine de caractère i.split()[8] la chaine de caractère 'old'
            if pat == None:               #si il trouve la chaine de caratère
                with open("./fileYokogawa/"+i.split()[8], "r") as f :
                        files = (f.read()).split("Sampling Data"); #on sépare le fichier en 2 au niveau de Sampling Data
                        pat3 = re.search('[a-zA-Z]* Data', files[1]) #regarde si il y a d'autre chaine avec 'Data' à l'intérieur
                        if pat3 != None: #si il trouve la chaine ' Data' précéder d'un mot 
                            filesMD = re.split('[a-zA-Z]* Data',files[1]) #on sépare les différentes parties qui nous intéressent
                            files[1] = filesMD[0]
                            fichierSys = open("sys.txt", "a")
                            fichierSys.write(i.split()[8]+' '+filesMD[1]+'\n')#Ecrit dans le fichier sys.txt le nom du fichier en cours de traitement, et ce qui se trouve entre les deux prochaine chaines '[a-zA-Z]* Data'
                            fichierSys.close()
                        filesValeurs = files[1].split("\t"); #filesValeurs contient la premiere date de la premire ligne, en effet chaque valeur est séparer par une tabulation
                        sep = filesValeurs[0].split()#separe la date et l'heure
                        separationDate = sep[0].split("/") #separe la date en jours, mois, année
                        date = datetime.date(int(separationDate[0]), int(separationDate[1]), int(separationDate[2]))#met sous format date la date de la premier ligne du fichier
                        try:
                            request = 'INSERT INTO Fichier VALUES (\''+i.split()[8]+'\',\''+str(date)+'\',\''+numEnr[1]+'\',\''+path+'\\fileYokogawa\\'+i.split()[8]+'\')' #met le fichier en BD
                            cursor2 = cnxn.execute(request);
                            cursor2.close()
                            cnxn.commit()
                        except:
                            #connect.rename(d.split()[8]+'/'+i.split()[8],d.split()[8]+'/old'+i.split()[8]) # renommage de fichier
                            print "Vous essayer de rentrer deux fois le meme fichier"
                        #else:
                        connect.rename(d.split()[8]+'/'+i.split()[8],d.split()[8]+'/old'+i.split()[8]) # renommage de fichier une fois l'enregistrement en BD fait
                        for stringCourant in filesValeurs: #pour chaque date ou donnée 
                                if stringCourant != filesValeurs[0]: #pour toute les valeurs sauf filesValeurs[0]
                                    pat2 = re.search('^[0-1]\n', stringCourant) #je recherche si il y a un numero entre 0 ou 1 et un retour à la ligne
                                    if pat2 != None: #si je trouve, cela veut dire que stringCourant correspond à la date/heure
                                            valeurSplit = stringCourant.split("\n");#je separe stringCourant pour en gardé que la date/heure
                                            if valeurSplit[1]: #si valeurSplit exist 
                                                    sep = valeurSplit[1].split(); #je separe la date et l'heure
                                                    separationDate = sep[0].split("/")#separe la date en jours, mois, année
                                                    date = datetime.date(int(separationDate[0]), int(separationDate[1]), int(separationDate[2])) #met sous format date la date de la ligne pour la mettre en BD
                                       
                                    else: #sinon je mets stringCourant qui est une donnée en BD
                                        try:
                                            request = 'INSERT INTO Valeur VALUES (\'v'+str(increV)+'\','+stringCourant+',\''+i.split()[8]+'\',\''+str(date)+'\',\''+sep[1]+'\')' #mise en BD la donnée traité
                                            cursor3 = cnxn.execute(request);
                                            cnxn.commit();
                                            
                                        except:
                                            cursor3.close()
                                            print 'Cette valeur n\'a pas était enregistré en BD'
                                        else:
                                            cursor3.close()
                                            increV = increV+1# je réactualise la prochaine clef primaire des données
                                            
                                        
connect.quit()
connect.close()
cnxn.close()