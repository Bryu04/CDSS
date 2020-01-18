#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ce module secondaire va définir un nouveau class (ESO Archive) dérivé de
# l'archive basique pour permettre queries sur les archives des Téléscopes de
# l'observatoire du Sud Europeanne. Elle comprend l'observatoires de Paranal,
# celle de la Silla, de l'APEX, de l'ALMA, de l'ELT et plein d'autres.
#
# L'observatoire de Paranal contient le VLT, VISTA, VST, SPECULOOS et NGTS. Celle
# de la Silla 
#
# Ce module va prendre en compte tous les instruments qu'abrite ESO notamment:
# VIRCAM,OmegaCAM,VIMOS,WFI,HAWKI,FORS1,FORS2,SOFI.
#
# Ce module permettra d'obtenir les images astronomiques de tous les instruments
# de ESO seulement à partir du code principal.

################### On va importer les modules standards ########################

import sys
import io
import os
import math
import re
import logging
import requests
import numpy as np
import pandas as pd
from getpass import getpass
from tqdm import tqdm
from astroquery.simbad import Simbad as sd
from requests.auth import HTTPBasicAuth

########################## Paramètres de départ #################################

# Comme son nom indique, on va définir les paramètres de départ, soit le nom de
# l'objet qu'on veux chercher, le rayon de la sphère céleste en degrés, les
# instruments de ESO et les urls (site web) de ESO.

tap_url = "http://archive.eso.org/wdb/wdb/eso/eso_archive_main/query"
request_url = "https://dataportal.eso.org/rh/api/requests/"
request_url2 = "http://dataportal.eso.org/rh/api/requests/"
download_url = "https://dataportal.eso.org/dataPortal/api/requests/"
#download_url = "http://dataportal.eso.org/dataPortal/requests/"
INSTRUMENT = ['VIRCAM','OmegaCAM','VIMOS','FORS2', 'WFI', 'SOFI', 'FORS1'
              'HAWKI']
dt4bias = 15.0  # Délai maximum pour les biais plats et les ombres en jours
                # juliens
dt4flat = 365.0 # Idem pour flats

directory = os.getcwd()

def eso_parameter_in(Object):

    output_directory = directory+'/{}/ESO/'.format(Object)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return output_directory


################################### Bar de status ###############################

# On va créer une fonction qui va afficher ou mettre à jour une barre de
# progression de la console

def update_process(total,progress):
    bar_length, status = 70, ""
    progress = float(progress) / float(total)
    if progress >= 1.:
        progress, status = 1, "\r\n"
    block = int(round(bar_length * progress))
    text = "\r[{}] {:.0f}% {}".format("#" * block
                                      + "_" * (bar_length - block),
                                      round(progress * 100, 0),
                                      status)
    sys.stdout.write(text)
    sys.stdout.flush()

if __name__== "__main__":
    ################################### LOG FILE ################################

    # Dans cette partie, on va configurer un logger qui va nous donner les info
    # au fil du temps.

    # Création de l'objet logger qui va nous servir à écrire dans les logs
    logger = logging.getLogger()

    # On met le niveau du logger à DEBUG, comme ça il écrit tout
    logger.setLevel(logging.DEBUG)


    # On crée un formateur qui va ajouter le temps, le niveau de chaque message
    # quand on écrira un message dans le log

    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
  
    # Création d'un handler qui va écrire les messages du niveau INFO ou supérieur
    # dans le sys.stderr

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    # Enfin on ajoute les handlers au logger principal
    logger.addHandler(console)


############################## Requêtes de l'objet ###############################

# On va introduire les différents caractéristiques de chaque instrument de ESO
# afin de préciser les données reçu lors de la requête d'images.

INSTRUMENTS = ",".join(["'{0}'".format(i) for i in INSTRUMENT])

# ---------------------- Cas spherical search box ----------------------------

def query_object(Object,obj_center,Sphere_Radius,inst):

    if inst == "WFI":
        INST = "WFI"
        width = Sphere_Radius + 0.56 # champ visuel = 34' x 33'

    elif inst == "VIRCAM":
        INST = "VIRCAM"
        width = Sphere_Radius + 1.15 # champ visuel = 1.2° x 1.1°

    elif inst == "OmegaCAM":
        INST = "OMEGACAM"
        width = Sphere_Radius + 1.00 # champ visuel = 1° x 1°

    elif inst == "VIMOS":
        INST = "VIMOS"
        width = Sphere_Radius + 0.5 # champ visuel = 28' x 32'

    elif inst == "FORS1":
        INST = "FORS1"
        width = Sphere_Radius + 0.11 # champ visuel = 6.8' x 6.8'

    elif inst == "FORS2":
        INST = "FORS2"
        width = Sphere_Radius + 0.11 # champ visuel = 6.8' x 6.8'

    elif inst == "SOFI":
        INST = "SOFI"
        width = Sphere_Radius + 0.082 # champ visuel = 4.92' x 4.92'

    elif inst == "HAWKI":
        INST = "HAWKI"
        width = Sphere_Radius + 0.125 # champ visuel = 7.5' x 7.5'

    else:
        sys.exit("Instrument not provided")

    # Voir Programmatic Access dans le site de archive.eso:/
        
    query = {'target': Object, 'ra': str(obj_center[0]),
             'dec': str(obj_center[1]), 'box': str(width),
             'instrument': INST, 'degrees_or_hours': 'degrees',
             'dp_cat': 'SCIENCE','wdbo': 'csv', 'top': str(20000),
             'tab_origfile': 'on', 'resolver': 'simbad','dp_tech': 'IMAGE'}

    return query

# ---------------------- Cas rectangular search box ----------------------------

def query_object_2(Object,obj_center,Ra_box,DEC_box,inst):

    if inst == "WFI":
        INST = "WFI"
        Rwidth = Ra_box + 0.56 # champ visuel = 34' x 33'
        Dwidth = DEC_box + 0.56

    elif inst == "VIRCAM":
        INST = "VIRCAM"
        Rwidth = Ra_box + 1.15 # champ visuel = 1.2° x 1.1°
        Dwidth = DEC_box + 1.15

    elif inst == "OmegaCAM":
        INST = "OMEGACAM"
        Rwidth = Ra_box + 1.00 # champ visuel = 1° x 1°
        Dwidth = DEC_box + 1.00

    elif inst == "VIMOS":
        INST = "VIMOS"
        Rwidth = Ra_box + 0.5 # champ visuel = 28' x 32'
        Dwidth = DEC_box + 0.5

    elif inst == "FORS1":
        INST = "FORS1"
        Rwidth = Ra_box + 0.11 # champ visuel = 6.8' x 6.8'
        Dwidth = DEC_box + 0.11

    elif inst == "FORS2":
        INST = "FORS2"
        Rwidth = Ra_box + 0.11 # champ visuel = 6.8' x 6.8'
        Dwidth = DEC_box + 0.11

    elif inst == "SOFI":
        INST = "SOFI"
        Rwidth = Ra_box + 0.082 # champ visuel = 4.92' x 4.92'
        Dwidth = DEC_box + 0.082

    elif inst == "HAWKI":
        INST = "HAWKI"
        Rwidth = Ra_box + 0.125 # champ visuel = 7.5' x 7.5'
        Dwidth = DEC_box + 0.125

    else:
        sys.exit("Instrument not provided")

    # Voir Programmatic Access dans le site de archive.eso:/

    query = {'target': Object, 'ra': (str(obj_center[0])-str(Rwidth))+'..'+
             (str(obj_center[0])+str(Rwidth)),
             'dec': (str(obj_center[1])-str(Dwidth))+'..'+
             (str(obj_center[0])-str(Rwidth)),'instrument': INST,
             'degrees_or_hours': 'degrees','dp_cat': 'SCIENCE','wdbo': 'csv',
             'top': str(20000),'tab_origfile': 'on','resolver': 'simbad',
             'dp_tech': 'IMAGE'}

    return query

###################### Searching Scientific Images ##############################

# ------------------- Cas spherical search box -----------------------------

def eso_search_images(Object,obj_center,Sphere_Radius,tap_url):

    ############################## Instrument SOFI ############################

    Orig_Id ={}
    logging.info("Searching in SOFI")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.
        

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"SOFI"))

    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=3, escapechar='>', quotechar='"',
                                comment='#', index_col=False)


        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
    
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i

        # ------------ Recherche des images ayant pas de noms défini -----------

        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms

        logging.info("Searching for unnamed objects present in SOFI")

        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte


        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1

        #print counter
        #print Table_obs
        #sys.exit("test 2.2")

        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
                    # La dernière ligne permet de changer les valeurs d'un tableau
                    # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format
        # pour les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")

        if len(Table_obs) != 0:
    
            planeURI_sofi = Table_obs['Dataset ID']
            dates_sofi = Table_obs['MJD-OBS']
            filters_sofi = Table_obs['Filter']
            orig_id_sofi = Table_obs['Orig Name']

            N_sofi = len(planeURI_sofi)

            #print Orig_Id

        else:
            N_sofi = 0
            planeURI_sofi = []
            dates_sofi = []
            filters_sofi = []
            orig_id_sofi = []

    except Exception as e:
        N_sofi = 0
        planeURI_sofi = []
        dates_sofi = []
        filters_sofi = []
        orig_id_sofi = []

    logging.info("SOFI images: "+str(N_sofi))
    Table_obs = None
    logging.info("------------------------------------------")
    #
    #sys.exit("test 3")

    ################################ Instrument WFI ############################

    logging.info("Searching in WFI")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"WFI"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ------------ Recherche des images ayant pas de noms défini ----------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in WFI")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                                   
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                                                        
        if len(Table_obs) != 0:
                                
            planeURI_wfi = Table_obs['Dataset ID']
            dates_wfi = Table_obs['MJD-OBS']
            filters_wfi = Table_obs['Filter']
            orig_id_wfi = Table_obs['Orig Name']
                                
            N_wfi = len(planeURI_wfi)
                            
            #print Orig_Id
                            
        else:
            N_wfi = 0
            planeURI_wfi = []
            dates_wfi = []
            filters_wfi = []
            orig_id_wfi = []

    except Exception as e:
        N_wfi = 0
        planeURI_wfi = []
        dates_wfi = []
        filters_wfi = []
        orig_id_wfi = []

    logging.info("WFI images: "+str(N_wfi))
    Table_obs = None
    logging.info("------------------------------------------")

    #sys.exit("test 4")

    ########################### Instrument VIRCAM ##############################

    logging.info("Searching in VIRCAM")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"VIRCAM"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # -------------- Recherche des images ayant pas de noms défini ---------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la recherche,
        # il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in VIRCAM")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_vircam = Table_obs['Dataset ID']
            dates_vircam = Table_obs['MJD-OBS']
            filters_vircam = Table_obs['Filter']
            orig_id_vircam = Table_obs['Orig Name']
                                
            N_vircam = len(planeURI_vircam)
                            
            #print Orig_Id
                            
        else:
            N_vircam = 0
            planeURI_vircam = []
            dates_vircam = []
            filters_vircam = []
            orig_id_vircam = []

    except Exception as e:
        N_vircam = 0
        planeURI_vircam = []
        dates_vircam = []
        filters_vircam = []
        orig_id_vircam = []

    logging.info("VIRCAM images: "+str(N_vircam))
    Table_obs = None
    logging.info("------------------------------------------")

    ########################### Instrument OmegaCAM ###########################

    logging.info("Searching in OmegaCAM")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"OmegaCAM"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ------------- Recherche des images ayant pas de noms défini -----------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in OmegaCAM")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_omegacam = Table_obs['Dataset ID']
            dates_omegacam = Table_obs['MJD-OBS']
            filters_omegacam = Table_obs['Filter']
            orig_id_omegacam = Table_obs['Orig Name']
                                
            N_omegacam = len(planeURI_omegacam)
                            
            #print Orig_Id
                            
        else:
            N_omegacam = 0
            planeURI_omegacam = []
            dates_omegacam = []
            filters_omegacam = []
            orig_id_omegacam = []

    except Exception as e:
        N_omegacam = 0
        planeURI_omegacam =[]
        dates_omegacam = []
        filters_omegacam = []
        orig_id_omegacam = []

    logging.info("OmegaCAM images: "+str(N_omegacam))
    Table_obs = None
    logging.info("------------------------------------------")

    #sys.exit("test 5")

    ########################### Instrument VIMOS ###############################

    logging.info("Searching in VIMOS")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"VIMOS"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini ------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in VIMOS")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                            
                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_vimos = Table_obs['Dataset ID']
            dates_vimos = Table_obs['MJD-OBS']
            filters_vimos = Table_obs['Filter']
            orig_id_vimos = Table_obs['Orig Name']
                                
            N_vimos = len(planeURI_vimos)
                            
            #print Orig_Id
                            
        else:
            N_vimos = 0
            planeURI_vimos = []
            dates_vimos = []
            filters_vimos = []
            orig_id_vimos = []

    except Exception as e:
        N_vimos = 0
        planeURI_vimos = []
        dates_vimos = []
        filters_vimos = []
        orig_id_vimos = []

    logging.info("VIMOS images: "+str(N_vimos))
    Table_obs = None
    logging.info("------------------------------------------")

    ############################ Instrument FORS1 #############################

    logging.info("Searching in FORS1")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"FORS1"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['OBJECT']
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini -------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in FORS1")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                            
                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                          
        if len(Table_obs) != 0:
                                
            planeURI_fors1 = Table_obs['Dataset ID']
            dates_fors1 = Table_obs['MJD-OBS']
            filters_fors1 = Table_obs['Filter']
            orig_id_fors1 = Table_obs['Orig Name']
                                
            N_fors1 = len(planeURI_fors1)
                            
            #print Orig_Id
                            
        else:
            N_fors1 = 0
            planeURI_fors1 = []
            dates_fors1 = []
            filters_fors1 = []
            orig_id_fors1 = []

    except Exception as e:
        N_fors1 = 0
        planeURI_fors1 = []
        dates_fors1 = []
        filters_fors1 = []
        orig_id_fors1 = []

    logging.info("FORS1 images: "+str(N_fors1))
    Table_obs = None
    logging.info("------------------------------------------")

    ######################### Instrument FORS2 ###############################

    logging.info("Searching in FORS2")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"FORS2"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini -------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in FORS2")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                                   
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_fors2 = Table_obs['Dataset ID']
            dates_fors2 = Table_obs['MJD-OBS']
            filters_fors2 = Table_obs['Filter']
            orig_id_fors2 = Table_obs['Orig Name']
                                
            N_fors2 = len(planeURI_fors2)
                            
            #print Orig_Id
                            
        else:
            N_fors2 = 0
            planeURI_fors2 = []
            dates_fors2 = []
            filters_fors2 = []
            orig_id_fors2 = []

    except Exception as e:
        N_fors2 = 0
        planeURI_fors2 = []
        dates_fors2 = []
        filters_fors2 = []
        orig_id_fors2 = []

    logging.info("FORS2 images: "+str(N_fors2))
    Table_obs = None
    logging.info("------------------------------------------")

    ######################### Instrument HAWKI ###############################

    logging.info("Searching in HAWKI")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object(Object,obj_center,
                                                  Sphere_Radius,"HAWKI"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini ------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in HAWKI")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_hawki = Table_obs['Dataset ID']
            dates_hawki = Table_obs['MJD-OBS']
            filters_hawki = Table_obs['Filter']
            orig_id_hawki = Table_obs['Orig Name']
                                
            N_hawki = len(planeURI_hawki)
                            
            #print Orig_Id
                            
        else:
            N_hawki = 0
            planeURI_hawki = []
            dates_hawki = []
            filters_hawki = []
            orig_id_hawki = []

    except Exception as e:
        N_hawki = 0
        planeURI_hawki = []
        dates_hawki = []
        filters_hawki = []
        orig_id_hawki = []

    logging.info("HAWKI images: "+str(N_hawki))
    Table_obs = None
    logging.info("------------------------------------------")

    return (N_sofi,planeURI_sofi,dates_sofi,filters_sofi,orig_id_sofi,N_wfi,
            planeURI_wfi,dates_wfi,filters_wfi,orig_id_wfi,N_vircam,
            planeURI_vircam,dates_vircam,filters_vircam,orig_id_vircam,
            N_omegacam,planeURI_omegacam,dates_omegacam,filters_omegacam,
            orig_id_omegacam,N_vimos,planeURI_vimos,dates_vimos,filters_vimos,
            orig_id_vimos,N_fors1,planeURI_fors1,dates_fors1,filters_fors1,
            orig_id_fors1,N_fors2,planeURI_fors2,dates_fors2,filters_fors2,
            orig_id_fors2,N_hawki,planeURI_hawki,dates_hawki,filters_hawki,
            orig_id_hawki,Orig_Id)

# ------------------- Cas rectangular search box -----------------------------

def eso_search_images_2(Object,obj_center,Ra_box,DEC_box,tap_url):

    ############################## Instrument SOFI ############################

    Orig_Id ={}
    logging.info("Searching in SOFI")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.
        

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"SOFI"))

    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=3, escapechar='>', quotechar='"',
                                comment='#', index_col=False)


        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
    
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i

        # ------------ Recherche des images ayant pas de noms défini -----------

        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms

        logging.info("Searching for unnamed objects present in SOFI")

        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte


        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1

        #print counter
        #print Table_obs
        #sys.exit("test 2.2")

        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
                    # La dernière ligne permet de changer les valeurs d'un tableau
                    # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format
        # pour les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")

        if len(Table_obs) != 0:
    
            planeURI_sofi = Table_obs['Dataset ID']
            dates_sofi = Table_obs['MJD-OBS']
            filters_sofi = Table_obs['Filter']
            orig_id_sofi = Table_obs['Orig Name']

            N_sofi = len(planeURI_sofi)

            #print Orig_Id

        else:
            N_sofi = 0
            planeURI_sofi = []
            dates_sofi = []
            filters_sofi = []
            orig_id_sofi = []

    except Exception as e:
        N_sofi = 0
        planeURI_sofi = []
        dates_sofi = []
        filters_sofi = []
        orig_id_sofi = []

    logging.info("SOFI images: "+str(N_sofi))
    Table_obs = None
    logging.info("------------------------------------------")
    #
    #sys.exit("test 3")

    ################################ Instrument WFI ############################

    logging.info("Searching in WFI")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"WFI"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ------------ Recherche des images ayant pas de noms défini ----------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in WFI")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                                   
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                                                        
        if len(Table_obs) != 0:
                                
            planeURI_wfi = Table_obs['Dataset ID']
            dates_wfi = Table_obs['MJD-OBS']
            filters_wfi = Table_obs['Filter']
            orig_id_wfi = Table_obs['Orig Name']
                                
            N_wfi = len(planeURI_wfi)
                            
            #print Orig_Id
                            
        else:
            N_wfi = 0
            planeURI_wfi = []
            dates_wfi = []
            filters_wfi = []
            orig_id_wfi = []

    except Exception as e:
        N_wfi = 0
        planeURI_wfi = []
        dates_wfi = []
        filters_wfi = []
        orig_id_wfi = []

    logging.info("WFI images: "+str(N_wfi))
    Table_obs = None
    logging.info("------------------------------------------")

    #sys.exit("test 4")

    ########################### Instrument VIRCAM ##############################

    logging.info("Searching in VIRCAM")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"VIRCAM"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # -------------- Recherche des images ayant pas de noms défini ---------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la recherche,
        # il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in VIRCAM")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_vircam = Table_obs['Dataset ID']
            dates_vircam = Table_obs['MJD-OBS']
            filters_vircam = Table_obs['Filter']
            orig_id_vircam = Table_obs['Orig Name']
                                
            N_vircam = len(planeURI_vircam)
                            
            #print Orig_Id
                            
        else:
            N_vircam = 0
            planeURI_vircam = []
            dates_vircam = []
            filters_vircam = []
            orig_id_vircam = []

    except Exception as e:
        N_vircam = 0
        planeURI_vircam = []
        dates_vircam = []
        filters_vircam = []
        orig_id_vircam = []

    logging.info("VIRCAM images: "+str(N_vircam))
    Table_obs = None
    logging.info("------------------------------------------")

    ########################### Instrument OmegaCAM ###########################

    logging.info("Searching in OmegaCAM")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"OmegaCAM"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ------------- Recherche des images ayant pas de noms défini -----------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in OmegaCAM")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_omegacam = Table_obs['Dataset ID']
            dates_omegacam = Table_obs['MJD-OBS']
            filters_omegacam = Table_obs['Filter']
            orig_id_omegacam = Table_obs['Orig Name']
                                
            N_omegacam = len(planeURI_omegacam)
                            
            #print Orig_Id
                            
        else:
            N_omegacam = 0
            planeURI_omegacam = []
            dates_omegacam = []
            filters_omegacam = []
            orig_id_omegacam = []

    except Exception as e:
        N_omegacam = 0
        planeURI_omegacam =[]
        dates_omegacam = []
        filters_omegacam = []
        orig_id_omegacam = []

    logging.info("OmegaCAM images: "+str(N_omegacam))
    Table_obs = None
    logging.info("------------------------------------------")

    #sys.exit("test 5")

    ########################### Instrument VIMOS ###############################

    logging.info("Searching in VIMOS")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"VIMOS"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini ------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in VIMOS")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                            
                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_vimos = Table_obs['Dataset ID']
            dates_vimos = Table_obs['MJD-OBS']
            filters_vimos = Table_obs['Filter']
            orig_id_vimos = Table_obs['Orig Name']
                                
            N_vimos = len(planeURI_vimos)
                            
            #print Orig_Id
                            
        else:
            N_vimos = 0
            planeURI_vimos = []
            dates_vimos = []
            filters_vimos = []
            orig_id_vimos = []

    except Exception as e:
        N_vimos = 0
        planeURI_vimos = []
        dates_vimos = []
        filters_vimos = []
        orig_id_vimos = []

    logging.info("VIMOS images: "+str(N_vimos))
    Table_obs = None
    logging.info("------------------------------------------")

    ############################ Instrument FORS1 #############################

    logging.info("Searching in FORS1")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"FORS1"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['OBJECT']
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini -------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in FORS1")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                            
                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                          
        if len(Table_obs) != 0:
                                
            planeURI_fors1 = Table_obs['Dataset ID']
            dates_fors1 = Table_obs['MJD-OBS']
            filters_fors1 = Table_obs['Filter']
            orig_id_fors1 = Table_obs['Orig Name']
                                
            N_fors1 = len(planeURI_fors1)
                            
            #print Orig_Id
                            
        else:
            N_fors1 = 0
            planeURI_fors1 = []
            dates_fors1 = []
            filters_fors1 = []
            orig_id_fors1 = []

    except Exception as e:
        N_fors1 = 0
        planeURI_fors1 = []
        dates_fors1 = []
        filters_fors1 = []
        orig_id_fors1 = []

    logging.info("FORS1 images: "+str(N_fors1))
    Table_obs = None
    logging.info("------------------------------------------")

    ######################### Instrument FORS2 ###############################

    logging.info("Searching in FORS2")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"FORS2"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini -------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in FORS2")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                                   
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_fors2 = Table_obs['Dataset ID']
            dates_fors2 = Table_obs['MJD-OBS']
            filters_fors2 = Table_obs['Filter']
            orig_id_fors2 = Table_obs['Orig Name']
                                
            N_fors2 = len(planeURI_fors2)
                            
            #print Orig_Id
                            
        else:
            N_fors2 = 0
            planeURI_fors2 = []
            dates_fors2 = []
            filters_fors2 = []
            orig_id_fors2 = []

    except Exception as e:
        N_fors2 = 0
        planeURI_fors2 = []
        dates_fors2 = []
        filters_fors2 = []
        orig_id_fors2 = []

    logging.info("FORS2 images: "+str(N_fors2))
    Table_obs = None
    logging.info("------------------------------------------")

    ######################### Instrument HAWKI ###############################

    logging.info("Searching in HAWKI")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    r = requests.get(tap_url, params=query_object_2(Object,obj_center,
                                                    Ra_box,DEC_box,"HAWKI"))
    #print r.url
    rawTable = r.content
    #with open('query2.csv', 'wb') as f:
    #    f.write(rawTable)


    # On récupère les données sous forme de tableau en utilisant le module pandas

    try:
        Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0,
                                quoting=1, quotechar='"',
                                comment='#', index_col=False)
                            
                            
        #print Table_obs.keys()
        #print Table_obs['Release_Date'][1]
        #print Table_obs['Filter'][1]
        #print Table_obs['MJD-OBS'][1]
        #print Table_obs
                            
        for ij , j in enumerate(Table_obs['Dataset ID']):
            for it, i in enumerate(Table_obs['Orig Name']):
                if it == ij:
                    Orig_Id[j] = i
                            
        # ----------- Recherche des images ayant pas de noms défini ------------
                            
        # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
        # peuvent être important à les étudier. Pour les intégrer dans la
        # recherche, il faut leur changer de noms
                            
        logging.info("Searching for unnamed objects present in HAWKI")
                            
        null_columns = Table_obs.columns[Table_obs.isnull().any()]
        # On recherche dans le tableau, les colonnes où il y a des éléments vides
        # et on fait le compte
                                            
        counter = 0
        for f in Table_obs['OBJECT'].isnull():
            if f ==True:
                counter +=1
                            
        #print counter
        #print Table_obs
        #sys.exit("test 2.2")
                            
        if counter != 0:
            logging.info("There are "+str(counter)+" unnamed images found")
            c = 0
            # On fait le changement
            N = len(Table_obs['OBJECT'])
            for it, i in enumerate(Table_obs['OBJECT']):
                update_process(N,it)
                for ft, f in enumerate(Table_obs['OBJECT'].isnull()):
                    if f ==True:
                        if it == ft:
                            c +=1
                            Table_obs.loc[it, 'OBJECT'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
            logging.info(str(c)+" unnamed images have been named")
        else:
            logging.info("No unnamed image has been found")

        # Maintenant on regarde s'il existe des erreurs concernant le format pour
        # les filtres utilises.

        gi = 0
        for it, i in enumerate(Table_obs['Filter']):
	    if 'filter_name' in i:
		# print i
		(w1,w2,w3,w4) = i.split('=')
		(y1,y2,y3) = w4.split('"')
		(x1,x2) = y2.split('<')
		u = '{},{}'.format(x1,w4[-5:-1])
		# print u
		Table_obs.loc[it, 'Filter'] = u
		gi +=1
        logging.info(str(gi)+" Errors in filter name has been found and corrected")
                            
        if len(Table_obs) != 0:
                                
            planeURI_hawki = Table_obs['Dataset ID']
            dates_hawki = Table_obs['MJD-OBS']
            filters_hawki = Table_obs['Filter']
            orig_id_hawki = Table_obs['Orig Name']
                                
            N_hawki = len(planeURI_hawki)
                            
            #print Orig_Id
                            
        else:
            N_hawki = 0
            planeURI_hawki = []
            dates_hawki = []
            filters_hawki = []
            orig_id_hawki = []

    except Exception as e:
        N_hawki = 0
        planeURI_hawki = []
        dates_hawki = []
        filters_hawki = []
        orig_id_hawki = []

    logging.info("HAWKI images: "+str(N_hawki))
    Table_obs = None
    logging.info("------------------------------------------")

    return (N_sofi,planeURI_sofi,dates_sofi,filters_sofi,orig_id_sofi,N_wfi,
            planeURI_wfi,dates_wfi,filters_wfi,orig_id_wfi,N_vircam,
            planeURI_vircam,dates_vircam,filters_vircam,orig_id_vircam,
            N_omegacam,planeURI_omegacam,dates_omegacam,filters_omegacam,
            orig_id_omegacam,N_vimos,planeURI_vimos,dates_vimos,filters_vimos,
            orig_id_vimos,N_fors1,planeURI_fors1,dates_fors1,filters_fors1,
            orig_id_fors1,N_fors2,planeURI_fors2,dates_fors2,filters_fors2,
            orig_id_fors2,N_hawki,planeURI_hawki,dates_hawki,filters_hawki,
            orig_id_hawki,Orig_Id)


################ Fonction pour la recherche de FLAT, BIAS et DARK ################

# La plupart des images astronomiques ne sont pas encore calibrées. Pour cela, il
# faudra les calibrer manuellement grâce à les fichers de type BIAS, DARK et FLAT.

# On définit une fonction query pour les objets BIAS et DARK pour une date donnée.

def query_bias_dark(date,interval,inst):
    
    if inst == "SOFI":
        INST = "SOFI"

    elif inst == "WFI":
        INST = "WFI"
              
    elif inst == "VIRCAM":
        INST = "VIRCAM"

    elif inst == "OmegaCAM":
        INST = "OMEGACAM"

    elif inst == "VIMOS":
        INST = "VIMOS"

    elif inst == "FORS1":
        INST = "FORS1"

    elif inst == "FORS2":
        INST = "FORS2"

    elif inst == "HAWKI":
        INST = "HAWKI"

    else:
        sys.exit("Not a valid instrument. Modify query_bias_dark")


    # Voir Programmatic Access dans le site de archive.eso:/

    query = {'instrument': INST, 'degrees_or_hours': 'degrees',
             'dp_cat': 'CALIB','wdbo': 'csv', 'top': str(20000),
             'tab_origfile': 'on', 'resolver': 'simbad',
             'mjd_obs':str(date-0.5*interval)+'..'+str(date+0.5*interval),
             'dp_tech': 'IMAGE','dp_type': ['DARK','BIAS']}

    return query

# On fait de même pour les objets FLAT et on ajoute un filtre.

def query_flat(date,interval,Filter,inst):

    if inst == "SOFI":
        INST = "SOFI"

    elif inst == "WFI":
        INST = "WFI"
              
    elif inst == "VIRCAM":
        INST = "VIRCAM"

    elif inst == "OmegaCAM":
        INST = "OMEGACAM"

    elif inst == "VIMOS":
        INST = "VIMOS"

    elif inst == "FORS1":
        INST = "FORS1"

    elif inst == "FORS2":
        INST = "FORS2"

    elif inst == "HAWKI":
        INST = "HAWKI"

    else:
        sys.exit("Not a valid instrument. Modify query_flat")


    # Voir Programmatic Access dans le site de archive.eso:/

    query = {'instrument': INST, 'degrees_or_hours': 'degrees',
             'dp_cat': 'CALIB','wdbo': 'csv', 'top': str(20000),
             'tab_origfile': 'on', 'resolver': 'simbad',
             'mjd_obs':str(date-0.5*interval)+'..'+str(date+0.5*interval),
             'dp_tech': 'IMAGE','dp_type': ['FLAT'],'filter_path':Filter}


    return query

# Maintenant on va chercher ces dossiers pour les instruments CFHT

def Search_FBD(dates,filters,orig_id,inst,Orig_Id,tap_url):

    if inst not in ["SOFI","WFI","VIRCAM","OmegaCAM",
                    "VIMOS","FORS1","FORS2","HAWKI"]:
        sys.exit("Instrument not defined")

    N = len(dates)
    logging.info("Searching BIAS, DARK and FLAT for "+inst+" observations")

    # La fonction np.empty renvoie un nouveau tableau de forme et de type donné,
    # avec des valeurs aléatoires. Dans ce cas N listes de 10 valeurs.

    URIs_bias = np.empty((N,10), dtype=object)
    URIs_dark = np.empty((N,10), dtype=object)
    URIs_flat = np.empty((N,10), dtype=object)

    for i,(date,filt,orID) in enumerate(zip(dates,filters,orig_id)):
        update_process(N,i)

        #print date
        #print filt
        try:
            # Idem que payload voir étapes précédents

            r = requests.get(tap_url, params=query_bias_dark(date,dt4bias,inst))
            rawTable = r.content

            # On récupère les données sous forme de tableau en utilisant le
            # module pandas
            
            temp_bds = pd.read_csv(io.StringIO(rawTable.decode('utf-8')),
                                   header=0,quoting=1, quotechar='"',
                                   comment='#', index_col=False)
            # print temp_bds.keys()

            # De plus on identifie les différent type d'objet; Bias ou Dark
            temp_bias = temp_bds.loc[temp_bds['Type'] ==
                                     'BIAS'].reset_index(drop=True)

            temp_dark = temp_bds.loc[temp_bds['Type'] ==
                                    'DARK'].reset_index(drop=True)

        except Exception as e:
            temp_bias = []
            temp_dark = []

        #------------------------------- BIAS ----------------------------------
        URIs_bias[i,:] = None
        if len(temp_bias) != 0:
            # On ne garde que des recents objets
            dt_bias = np.abs(temp_bias['MJD-OBS'] - date)
            try:
                bias_id = np.argsort(dt_bias)[:10]
            except Exception as e:
                logging.warning(str("Object ",orID," with less than 10 BIAS."))
                bias_id = np.argsort(dt_bias)
                pass

            # En cas de durée très longue
            if any(dt_bias[bias_id] > dt4bias):
                logging.warning("Bias time span for object "+str(orID)+" :"
                                +str(np.max(dt_bias[bias_id]))+" MJDs.")

            # On ajoute ces fichiers dans leur tableau correspondant.
            URIs_bias[i,:len(bias_id)] = temp_bias['Dataset ID'].iloc[bias_id]
            
            # On garde aussi leur noms origines
            for ij , j in enumerate(temp_bias['Dataset ID'].iloc[bias_id]):
                for lt, l in enumerate(temp_bias['Orig Name'].iloc[bias_id]):
                    if lt == ij:
                        Orig_Id[j] = l
                            

        else:
            logging.warning("No BIAS file for object: "+str(orID))

        #------------------------------- DARK ----------------------------------
        URIs_dark[i,:] = None
        if len(temp_dark) != 0:
            # On ne garde que des recents objets
            dt_dark = np.abs(temp_dark['MJD-OBS'] - date)
            try:
                dark_id = np.argsort(dt_dark)[:10]
            except Exception as e:
                logging.warning(str("Object ",orID," with less than 10 DARKS."))
                dark_id = np.argsort(dt_dark)
                pass
    
            # En cas de durée très longue
            if any(dt_dark[dark_id] > dt4bias):
                logging.warning("Dark time span for object "+str(orID)+" :"
                                +str(np.max(dt_dark[dark_id]))+" MJDs.")
    
            # On ajoute ces fichiers dans leur tableau correspondant.
            URIs_dark[i,:len(dark_id)] = temp_dark['Dataset ID'].iloc[dark_id]

            # On garde aussi leur noms origines
            for ij , j in enumerate(temp_dark['Dataset ID'].iloc[dark_id]):
                for lt, l in enumerate(temp_dark['Orig Name'].iloc[dark_id]):
                    if lt == ij:
                        Orig_Id[j] = l
                            

        else:
            logging.warning("No DARK file for object: "+str(orID))

        temp_bds = None
        temp_bias = []
        temp_dark = []


        #------------------------------ FLAT ------------------------------------

        try:
            r = requests.get(tap_url, params=query_flat(date,dt4flat,filt,inst))
            rawTable = r.content
    
            # On récupère les données sous forme de tableau en utilisant le
            # module pandas

            # Conditionnement sur sofi
            if inst == "SOFI":
                temp_flat = pd.read_csv(io.StringIO(rawTable.decode('utf-8')),
                                        header=0,quoting=3, escapechar='>',
                                        quotechar='"',comment='#',index_col=False)
            else:
                temp_flat = pd.read_csv(io.StringIO(rawTable.decode('utf-8')),
                                        header=0,quoting=1, quotechar='"',
                                        comment='#', index_col=False)

            # print temp_flat

        except Exception as e:
            temp_flat = []

        URIs_flat[i,:] = None
        if len(temp_flat) != 0:
            # On ne garde que des recents objets
            dt_flat = np.abs(temp_flat['MJD-OBS'] - date)
            try:
                flat_id = np.argsort(dt_flat)[:10]
            except Exception as e:
                logging.warning(str("Object ",orID," with less than 10 FLATS."))
                flat_id = np.argsort(dt_flat)
                pass
    
            # En cas de durée très longue
            if any(dt_flat[flat_id] > dt4flat):
                logging.warning("Flat time span for object "+str(orID)+" :"
                                +str(np.max(dt_dark[dark_id]))+" MJDs.")
    
            # On ajoute ces fichiers dans leur tableau correspondant.
            URIs_flat[i,:len(flat_id)] = temp_flat['Dataset ID'].iloc[flat_id]

            # On garde aussi leur noms origines
            for ij , j in enumerate(temp_flat['Dataset ID'].iloc[flat_id]):
                for lt, l in enumerate(temp_flat['Orig Name'].iloc[flat_id]):
                    if lt == ij:
                        Orig_Id[j] = l
                            

        else:
            logging.warning("No FLAT file for object: "+str(orID))

        temp_flat = []

    # Création des listes planeURI des objets de type FBD
    planeURI_fbd = np.concatenate((URIs_bias.flatten(), URIs_dark.flatten(),
                                   URIs_flat.flatten()), axis=0)

    planeURI_fbd = np.delete(planeURI_fbd, np.where(planeURI_fbd == None)[0])

    logging.info("There are "+str(len(planeURI_fbd))+" calibration files for "
                 +inst+" instrument")

    # Il faut retirer les fichiers dupliquées
    planeURI_fbd = list(set(planeURI_fbd))
    logging.info("There are "+str(len(planeURI_fbd))
                 +" unique calibration files for "+inst+" instrument")
    logging.info("-----------------------------------------------")

    return planeURI_fbd


###############################################################################

#################### Soummit la requete des images astronomiques ##############

# Avant de telecharger, il faut envoyer la requete des images sur le site de
# ESO. Pour cela il faut une autorisation, soit un identifiant et un mot de
# passe. On défini donc:

def summitting_eso_images(usr,pss,request_url,request_url2,planeURI_sofi,
                          planeURI_sofi_fbd,planeURI_wfi,planeURI_wfi_fbd,
                          planeURI_vircam,planeURI_vircam_fbd,planeURI_omegacam,
                          planeURI_omegacam_fbd,planeURI_vimos,
                          planeURI_vimos_fbd,planeURI_fors1,planeURI_fors1_fbd,
                          planeURI_fors2,planeURI_fors2_fbd,planeURI_hawki,
                          planeURI_hawki_fbd):

    # On doit additionner les 2 listes planeURI: images + calibrations.

    planeURI_sofi = np.concatenate((planeURI_sofi,planeURI_sofi_fbd),axis=0)
    planeURI_wfi = np.concatenate((planeURI_wfi,planeURI_wfi_fbd),axis=0)
    planeURI_vircam = np.concatenate((planeURI_vircam,planeURI_vircam_fbd),axis=0)
    planeURI_omegacam = np.concatenate((planeURI_omegacam,planeURI_omegacam_fbd),
                                       axis=0)
    planeURI_vimos = np.concatenate((planeURI_vimos,planeURI_vimos_fbd),axis=0)
    planeURI_fors1 = np.concatenate((planeURI_fors1,planeURI_fors1_fbd),axis=0)
    planeURI_fors2 = np.concatenate((planeURI_fors2,planeURI_fors2_fbd),axis=0)
    planeURI_hawki = np.concatenate((planeURI_hawki,planeURI_hawki_fbd),axis=0)

    # On retire les doublures.
    planeURI_sofi = set(planeURI_sofi)
    planeURI_wfi = set(planeURI_wfi)
    planeURI_vircam = set(planeURI_vircam)
    planeURI_omegacam = set(planeURI_omegacam)
    planeURI_vimos = set(planeURI_vimos)
    planeURI_fors1 = set(planeURI_fors1)
    planeURI_fors2 = set(planeURI_fors2)
    planeURI_hawki = set(planeURI_hawki)

    reqlim = 10000 # seulement 10000 images par requete
    header = {'Accept': 'text/plain'}

    listrequest = (list(planeURI_sofi)+list(planeURI_wfi)+list(planeURI_vircam)+
                   list(planeURI_omegacam)+list(planeURI_vimos)+
                   list(planeURI_fors1)+list(planeURI_fors2)+list(planeURI_hawki))

    #print type(listrequest)
    #print listrequest

    dic = {}
    l = len(listrequest)
    a = 0

    while l > reqlim:
        a += 1
        l -= reqlim

    for i in range(a+1):
        logging.info('submitting request {}'.format(i))
        payload = {'dataset':','.join(['SAF+{}'.format(j)for j in
                                       listrequest[i*reqlim:reqlim*(i+1)]])}
        d = requests.post(request_url+usr+'/submission', headers=header,
                          auth=HTTPBasicAuth(usr, pss), data=payload)
        q = requests.get(request_url2+usr+'/recentRequests', headers=header,
                         auth=HTTPBasicAuth(usr, pss))
        with open('requestRecent.txt', 'wb') as f:
            for line in q.content:
                if line == ',':
                    line = '\n'
                    f.write(line)
                else:
                    f.write(line)
                
        f = open('requestRecent.txt', 'r')
        for line in f:
            mots = line.split()
            dic[mots[0]] = listrequest[i*reqlim:reqlim*(i+1)]

        return (planeURI_sofi,planeURI_wfi,planeURI_vircam,planeURI_omegacam,
                planeURI_vimos,planeURI_fors1,planeURI_fors2,planeURI_hawki,dic)

#print dic
#sys.exit('Test 7')

#################### Telechargement des images astronomiques ####################

# Lors de la telechargement sur le site de ESO, il faut une autorisation, soit un
# identifiant et un mot de passe. Les images telechargEes ici sont sous forme
# compressEes (A inclure dans le format du fichier).

def download_eso_images(usr,pss,download_url,dic,Orig_Id,N_sofi,N_wfi,N_vircam,
                        N_omegacam,N_vimos,N_fors1,N_fors2,N_hawki,planeURI_sofi,
                        planeURI_wfi,planeURI_vircam,planeURI_omegacam,
                        planeURI_vimos,planeURI_fors1,planeURI_fors2,
                        planeURI_hawki,output_directory):

    # ------------------- Téléchargement des images SOFI ------------------------
    if N_sofi != 0:
        logging.info("Downloading images from SOFI instrument")
        out_dir = output_directory+"/SOFI/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_sofi):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")

    #sys.exit("test 8")

    # ------------------- Téléchargement des images WFI -------------------------
    if N_wfi != 0:
        logging.info("Downloading images from WFI instrument")
        out_dir = output_directory+"/WFI/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_wfi):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")
    # ------------------- Téléchargement des images VIRCAM ----------------------
    if N_vircam != 0:
        logging.info("Downloading images from VIRCAM instrument")
        out_dir = output_directory+"/VIRCAM/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_vircam):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")
    # ------------------- Téléchargement des images OmegaCAM -------------------
    if N_omegacam != 0:
        logging.info("Downloading images from OmegaCAM instrument")
        out_dir = output_directory+"/OmegaCAM/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_omegacam):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")
    # ------------------- Téléchargement des images VIMOS -----------------------
    if N_vimos != 0:
        logging.info("Downloading images from VIMOS instrument")
        out_dir = output_directory+"/VIMOS/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_vimos):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")
    # ------------------- Téléchargement des images FORS1 -----------------------
    if N_fors1 != 0:
        logging.info("Downloading images from FORS1 instrument")
        out_dir = output_directory+"/FORS1/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_fors1):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")
    # ------------------- Téléchargement des images FORS2 -----------------------
    if N_fors2 != 0:
        logging.info("Downloading images from FORS2 instrument")
        out_dir = output_directory+"/FORS2/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_fors2):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")
    # ------------------- Téléchargement des images HAWKI ----------------------
    if N_hawki != 0:
        logging.info("Downloading images from HAWKI instrument")
        out_dir = output_directory+"/HAWKI/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_hawki):
            for k in dic.keys():
                if plist in dic.get(k):
                    link = (download_url+usr+'/'+k+'/SAF/{}/{}.fits.Z'
                            .format(plist,plist))
                    img = Orig_Id[plist]
                    r = requests.get(link, auth=HTTPBasicAuth(usr, pss),
                                     stream=True)
                    if r.status_code == 200:
                        total_length = int(r.headers.get('content-length', 0))
                        block_length = 1024
                        wrote = 0

                        with open(out_dir+"/{}.Z".format(img), 'wb') as f:
                            for data in tqdm(r.iter_content(block_length),
                                             total=math.ceil
                                             (total_length//block_length),
                                             unit='KB',unit_scale=True):
                                wrote += len(data)
                                f.write(data)

                        if total_length != 0 and wrote != total_length:
                            logging.error("Something went wrong")

                        logging.info("The image "+img+" have been downloaded.")

                    else:
                        logging.info("The image "+img+" cannot be downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")

    logging.info("All ESO images downloaded")

    return

################################################################################

if __name__ == "__main__":

    # ---------------------- Paramètres d'entrées -----------------------

    Object = raw_input("Select an object : ")# Nom de l'objet sans (" ")
    nObj = len(Object)
    if nObj < 3:
        sys.exit("You must provide the target name")
    # On définit une limite minimale de caractères ou une erreur si absence de
    # caractères.

    # On propose le choix de la recherche (sphère celeste ou blog rectangulaire)

    Box_search = raw_input('''Choose r for circular area or b for rectangular
                              area : ''')

    if Box_search == "r":
        Sphere_Radius = float(input("Select the sphere's radius in deg : "))
    elif Box_search == "b":
        Ra_box = float(input("Select the length of the search box in deg : "))
        DEC_box = float(input("Select the width of the search box in deg : "))
    else:
        sys.exit("You should choose between r and b; circle or rectangle")

    # ----------- Coordonnées de l'objet en degrés ------------------
    sd.reset_votable_fields()
    sd.remove_votable_fields('coordinates')
    sd.add_votable_fields('ra(d;A;ICRS)', 'dec(d;D;ICRS)')

    table = sd.query_object(Object, wildcard=False)

    RA_center_deg = table[0][1]
    DEC_center_deg = table[0][2]

    CENTRE = np.array([RA_center_deg, DEC_center_deg])

    output_directory = eso_parameter_in(Object)

    # ----------------- Identifiant et mot de passe ------------------------------

    usr = raw_input("Enter ESO username : ")
    pss = getpass("Enter ESO password : ")


    logging.info("Searching Scientific images")

    if Box_search == "r":
        (N_sofi,planeURI_sofi,dates_sofi,
         filters_sofi,orig_id_sofi,N_wfi,
         planeURI_wfi,dates_wfi,filters_wfi,
         orig_id_wfi,N_vircam,planeURI_vircam,
         dates_vircam,filters_vircam,orig_id_vircam,
         N_omegacam,planeURI_omegacam,dates_omegacam,
         filters_omegacam,orig_id_omegacam,N_vimos,
         planeURI_vimos,dates_vimos,filters_vimos,
         orig_id_vimos,N_fors1,planeURI_fors1,
         dates_fors1,filters_fors1,orig_id_fors1,
         N_fors2,planeURI_fors2,dates_fors2,
         filters_fors2,orig_id_fors2,N_hawki,
         planeURI_hawki,dates_hawki,filters_hawki,
         orig_id_hawki,Orig_Id) = eso_search_images(Object,CENTRE,
                                                    Sphere_Radius,tap_url)

    elif Box_search == "b":
        (N_sofi,planeURI_sofi,dates_sofi,
         filters_sofi,orig_id_sofi,N_wfi,
         planeURI_wfi,dates_wfi,filters_wfi,
         orig_id_wfi,N_vircam,planeURI_vircam,
         dates_vircam,filters_vircam,orig_id_vircam,
         N_omegacam,planeURI_omegacam,dates_omegacam,
         filters_omegacam,orig_id_omegacam,N_vimos,
         planeURI_vimos,dates_vimos,filters_vimos,
         orig_id_vimos,N_fors1,planeURI_fors1,
         dates_fors1,filters_fors1,orig_id_fors1,
         N_fors2,planeURI_fors2,dates_fors2,
         filters_fors2,orig_id_fors2,N_hawki,
         planeURI_hawki,dates_hawki,filters_hawki,
         orig_id_hawki,Orig_Id) = eso_search_images_2(Object,CENTRE,
                                                      Ra_box,DEC_box,tap_url)



    logging.info("Searching images calibrations")

    
    # --- Recherche des BIAS, DARKS et FLATS dans les instruments  ------

    # Maintenant on cherche les calibrations des images pour les instruments
 

    if N_sofi != 0:
        planeURI_sofi_fbd = Search_FBD(dates_sofi,filters_sofi,
                                       orig_id_sofi,"SOFI",Orig_Id,tap_url)
    else:
        planeURI_sofi_fbd = []

    if N_wfi != 0:
        planeURI_wfi_fbd = Search_FBD(dates_wfi,filters_wfi,
                                      orig_id_wfi,"WFI",Orig_Id,tap_url)
    else:
        planeURI_wfi_fbd = []

    if N_vircam != 0:
        planeURI_vircam_fbd = Search_FBD(dates_vircam,filters_vircam,
                                         orig_id_vircam,"VIRCAM",Orig_Id,
                                         tap_url)
    else:
        planeURI_vircam_fbd = []

    if N_omegacam != 0:
        planeURI_omegacam_fbd = Search_FBD(dates_omegacam,filters_omegacam,
                                           orig_id_omegacam,"OmegaCAM",
                                           Orig_Id,tap_url)
    else:
        planeURI_omegacam_fbd = []

    if N_vimos != 0:
        planeURI_vimos_fbd = Search_FBD(dates_vimos,filters_vimos,
                                        orig_id_vimos,"VIMOS",Orig_Id,tap_url)
    else:
        planeURI_vimos_fbd = []

    if N_fors1 != 0:
        planeURI_fors1_fbd = Search_FBD(dates_fors1,filters_fors1,
                                        orig_id_fors1,"FORS1",Orig_Id,tap_url)
    else:
        planeURI_fors1_fbd = []

    if N_fors2 != 0:
        planeURI_fors2_fbd = Search_FBD(dates_fors2,filters_fors2,
                                        orig_id_fors2,"FORS2",Orig_Id,tap_url)
    else:
        planeURI_fors2_fbd = []

    if N_hawki != 0:
        planeURI_hawki_fbd = Search_FBD(dates_hawki,filters_hawki,
                                        orig_id_hawki,"HAWKI",Orig_Id,tap_url)
    else:
        planeURI_hawki_fbd = []


    logging.info("Submitting Request")

    (planeURI_sofi,planeURI_wfi,
     planeURI_vircam,planeURI_omegacam,
     planeURI_vimos,planeURI_fors1,
     planeURI_fors2,planeURI_hawki,
     dic) = summitting_eso_images(usr,pss,request_url,request_url2,planeURI_sofi,
                                  planeURI_sofi_fbd,planeURI_wfi,planeURI_wfi_fbd,
                                  planeURI_vircam,planeURI_vircam_fbd,
                                  planeURI_omegacam,planeURI_omegacam_fbd,
                                  planeURI_vimos,planeURI_vimos_fbd,
                                  planeURI_fors1,planeURI_fors1_fbd,
                                  planeURI_fors2,planeURI_fors2_fbd,
                                  planeURI_hawki,planeURI_hawki_fbd)

    

    logging.info("Downloading images")

    download_eso_images(usr,pss,download_url,dic,Orig_Id,N_sofi,N_wfi,N_vircam,
                        N_omegacam,N_vimos,N_fors1,N_fors2,N_hawki,planeURI_sofi,
                        planeURI_wfi,planeURI_vircam,planeURI_omegacam,
                        planeURI_vimos,planeURI_fors1,planeURI_fors2,
                        planeURI_hawki,output_directory)


    logging.info("Done")




