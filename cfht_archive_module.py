#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ce module secondaire va définir un nouveau class (CFHT Archive) dérivé de
# l'archive basique pour permettre queries sur les archives du Téléscope Canada-
# France-Hawaii.
#
# Ce module va prendre en compte tous les instruments qu'abrite CFHT notamment:
# MegaPrime, WIRCam, UH8K, CFH12K, et plein d'autres.
#
# Ce module permettra d'obtenir les images astronomiques de tous les instruments
# de CFHT seulement à partir du code principal.

################### On va importer les modules standards ########################

import sys
import io
import os
import math
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
# instruments de CFHT et les urls (site web) de CFHT


#if nObj >= 4:
    #directory = str(sys.argv[3])
#else:
    #directory = os.getcwd() # retourne le répertoire de travail actuel
                            # d'un processus

# Les 2 dernières lignes sert à créer un dossier contenant les images astronique
# de l'objet en question.

tap_url = "http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap/sync?"
#download_url ="http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/caom2obs/auth-pkg?"
download_url= "http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/"
INSTRUMENT = ['MegaPrime','WIRCam','UH8K','UH8K MOSAIC CAMERA', 'CFH12K MOSAIC']
dt4bias = 15.0  # Délai maximum pour les biais plats et les ombres en jours
                # juliens
dt4flat = 365.0 # Idem pour flats


directory = os.getcwd()

def cfht_parameter_in(Object):
    
    output_directory = directory+'/{}/CFHT/'.format(Object)
    
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

if __name__ == "__main__":
    ############################### LOG FILE #####################################
    
    # Dans cette partie, on va configurer un logger qui va nous donner les info au
    # fil du temps.
    
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

# On va introduire les différents caractéristiques de chaque instrument de CFHT
# afin de préciser les données reçu lors de la requête d'images.

INSTRUMENTS = ",".join(["'{0}'".format(i) for i in INSTRUMENT])

# ------------------------ Cas spherical search box ---------------------

def query_object(obj_center,Sphere_Radius,inst):

    if inst == "MegaPrime":
        INST = "'MegaPrime'"
        width = Sphere_Radius + 0.95 # champ visuel = 0.96 deg x
    
    elif inst == "WIRCam":
        INST = "'WIRCam'"
        width = Sphere_Radius + 0.36 # champ visuel = 21.5'
    
    elif inst == "UH8K":
        INST = "'UH8K','UH8K MOSAIC CAMERA'"
        width = Sphere_Radius + 0.48 # champ visuel = 29'

    elif inst == "CFH12K":
        INST = "'CFH12K MOSAIC'"
        width = Sphere_Radius + 0.7 # champ visuel = 42' x 28'

    else:
        sys.exit("Instrument not provided")

    # Voir ADQL dans le site de cadc:/
    string_0 = '''SELECT TOP 10000 Plane.planeURI AS "Plane URI",\
                  Plane.productID AS "Product ID", \
                  Observation.instrument_name AS "Instrument",\
                  Plane.time_bounds_lower AS "Start Date",\
                  Plane.time_exposure AS "Int. Time",\
                  Plane.energy_bandpassName AS "Filter",\
                  Observation.target_name AS "Target Name",\
                  Plane.dataProductType AS "Data Type" \
                  FROM caom2.Plane AS Plane \
                  JOIN caom2.Observation AS Observation ON Plane.obsID =
                  Observation.obsID \
                  WHERE  ( INTERSECTS( RANGE_S2D('''
    
    string_1 = '''),\
                  Plane.position_bounds ) = 1 AND Observation.instrument_name 
                  IN ( '''

    string_2 = ''') \
                  AND Observation.type = 'OBJECT' \
                  AND  ( Plane.quality_flag IS NULL OR Plane.quality_flag !=
                  'junk' ) )'''

    query = (string_0+str(obj_center[0]-0.5*width)+','
             +str(obj_center[0]+0.5*width)+','
             +str(str(obj_center[1]-0.5*width))+','
             +str(str(obj_center[1]+0.5*width))
             +string_1+INST+string_2)

    #print type(string_0)
    #print type(string_1)
    #print type(string_2)
    #print type(INST)
        
    
    return query

# ------------------- Cas rectangular search box ---------------------

def query_object_2(obj_center,Ra_box,DEC_box,inst):
    
    if inst == "MegaPrime":
        INST = "'MegaPrime'"
        Rwidth = Ra_box + 0.95 # champ visuel = 0.96 deg x 0.94 deg
        Dwidth = DEC_box + 0.95
    
    elif inst == "WIRCam":
        INST = "'WIRCam'"
        Rwidth = Ra_box + 0.36 # champ visuel = 21.5'
        Dwidth = DEC_box + 0.36
    
    elif inst == "UH8K":
        INST = "'UH8K','UH8K MOSAIC CAMERA'"
        Rwidth = Ra_box + 0.48 # champ visuel = 29'
        Dwidth = DEC_box + 0.48
    
    elif inst == "CFH12K":
        INST = "'CFH12K MOSAIC'"
        Rwidth = Ra_box + 0.7 # champ visuel = 42' x 28'
        Dwidth = DEC_box + 0.7
    
    else:
        sys.exit("Instrument not provided")
    
    # Voir ADQL dans le site de cadc:/
    string_0 = '''SELECT TOP 10000 Plane.planeURI AS "Plane URI",\
        Plane.productID AS "Product ID", \
        Observation.instrument_name AS "Instrument",\
        Plane.time_bounds_lower AS "Start Date",\
        Plane.time_exposure AS "Int. Time",\
        Plane.energy_bandpassName AS "Filter",\
        Observation.target_name AS "Target Name",\
        Plane.dataProductType AS "Data Type" \
        FROM caom2.Plane AS Plane \
        JOIN caom2.Observation AS Observation ON Plane.obsID =
        Observation.obsID \
        WHERE  ( INTERSECTS( RANGE_S2D('''
    
    string_1 = '''),\
        Plane.position_bounds ) = 1 AND Observation.instrument_name
        IN ( '''
    
    string_2 = ''') \
        AND Observation.type = 'OBJECT' \
        AND  ( Plane.quality_flag IS NULL OR Plane.quality_flag !=
        'junk' ) )'''
    
    query = (string_0+str(obj_center[0]-0.5*Rwidth)+','
             +str(obj_center[0]+0.5*Rwidth)+','
             +str(str(obj_center[1]-0.5*Dwidth))+','
             +str(str(obj_center[1]+0.5*Dwidth))
             +string_1+INST+string_2)
    
    #print type(string_0)
    #print type(string_1)
    #print type(string_2)
    #print type(INST)
    
    
    return query

##################### Searching Scientific Images #################################

# -------------------- Cas sphérical search box ---------------------

def cfht_search_images(obj_center,Sphere_Radius,tap_url):
    
    ############################## Instrument MEGAPRIME #####################
    
    logging.info("Searching in MegaPrime")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.
        
    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
               'QUERY':query_object(obj_center,Sphere_Radius,"MegaPrime")}
                                                                     
    rawTable = requests.get(tap_url, params=payload).content

    # On récupère les données sous forme de tableau en utilisant le module pandas

    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)

    # ---------------- Recherche des images ayant pas de noms défini ------------

    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms

    logging.info("Searching for unnamed objects present in MegaPrime")

    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte


    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1

    #print counter
    #print Table_obs
    #sys.exit("test 2.2")

    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
            # La dernière ligne permet de changer les valeurs d'un tableau
            # en évitant l'erreur SettingWithCopyWarning.
        logging.info(st(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")

    # ------ Effacer les objets mal identifiés et callibration des images -------

    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.

    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                     lambda x: "BIAS" in x or "FLAT" in x or
                                     "DARK" in x, Table_obs['"Target Name"']))[0]

        # On veut avoir que des images callibrées (finit par *******p)
        cond_megaprime = np.where(np.array(
                                    [s[-1:] for s in Table_obs['"Product ID"']]
                                       ) == 'p')[0]
        if len(wrong_objects) != 0:
            cond_megaprime = np.setdiff1d(cond_megaprime, wrong_objects)

        planeURI_megaprime = Table_obs['"Plane URI"'][cond_megaprime]

        N_megaprime = len(planeURI_megaprime)

    else:
        N_megaprime = 0
        planeURI_megaprime = []

    logging.info("MegaPrime images: "+str(N_megaprime))
    Table_obs = None
    logging.info("------------------------------------------")

    #sys.exit("test 3")

    ############################ Instrument WIRCAM ###############################

    logging.info("Searching in WIRCam")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
               'QUERY':query_object(obj_center,Sphere_Radius,"WIRCam")}

    rawTable = requests.get(tap_url, params=payload).content

    # On récupère les données sous forme de tableau en utilisant le module pandas

    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)

    # ---------------- Recherche des images ayant pas de noms défini -------------

    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms

    logging.info("Searching for unnamed objects present in WIRCam")

    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte

    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1

    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
        # La dernière ligne permet de changer les valeurs d'un tableau
        # en évitant l'erreur SettingWithCopyWarning.
        logging.info(str(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")

    # ------ Effacer les objets mal identifiés et callibration des images --------

    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.

    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                 lambda x: "BIAS" in x or "FLAT" in x or "DARK" in
                                 x, Table_obs['"Target Name"']))[0]
        # On veut avoir que des images callibrées (finit par *******p)
        cond_wircam = np.where(np.array(
                                    [s[-1:] for s in Table_obs['"Product ID"']]
                                    ) == 'p')[0]
        if len(wrong_objects) != 0:
            cond_wircam = np.setdiff1d(cond_wircam, wrong_objects)
                                                                    
        planeURI_wircam = Table_obs['"Plane URI"'][cond_wircam]
                                                                    
        N_wircam = len(planeURI_wircam)

    else:
        N_wircam = 0
        planeURI_wircam = []

    logging.info("WIRCam images: "+str(N_wircam))
    Table_obs = None
    logging.info("------------------------------------------")

    #sys.exit("test 4")

    ########################## Instrument CFH12K #################################

    logging.info("Searching in CFH12K")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
               'QUERY':query_object(obj_center,Sphere_Radius,"CFH12K")}

    rawTable = requests.get(tap_url, params=payload).content

    # On récupère les données sous forme de tableau en utilisant le module pandas

    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)

    # ---------------- Recherche des images ayant pas de noms défini -------------

    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms

    logging.info("Searching for unnamed objects present in CFH12K")

    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte

    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1

    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
        # La dernière ligne permet de changer les valeurs d'un tableau
        # en évitant l'erreur SettingWithCopyWarning.
        logging.info(st(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")

    # ------ Effacer les objets mal identifiés et callibration des images --------

    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.

    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                 lambda x: "BIAS" in x or "FLAT" in x or "DARK" in
                                 x, Table_obs['"Target Name"']))[0]
        # On veut avoir que des images callibrées (finit par *******p)
        cond_cfh12 = np.where(np.array(
                                   [s[-1:] for s in Table_obs['"Product ID"']])
                                    == 'p')[0]
        if len(wrong_objects) != 0:
            cond_cfh12 = np.setdiff1d(cond_cfh12, wrong_objects)
                                                                    
        planeURI_cfh12 = Table_obs['"Plane URI"'][cond_cfh12]
        dates_cfh12    = Table_obs['"Start Date"'][cond_cfh12]
        filters_cfh12  = Table_obs['"Filter"'][cond_cfh12]
        pIDs_cfh12     = Table_obs['"Product ID"'][cond_cfh12]
        # Les 3 dernières lignes seront utiles pour la calibration des images.

        N_cfh12 = len(planeURI_cfh12)

    else:
        N_cfh12 = 0
        planeURI_cfh12 = []
        dates_cfh12 = []
        filters_cfh12 = []
        pIDs_cfh12 = []

    logging.info("CFH12K images: "+str(N_cfh12))
    Table_obs = None
    logging.info("------------------------------------------")

    ############################ Instrument UH8K ###############################

    logging.info("Searching in UH8K")

    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.

    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
               'QUERY':query_object(obj_center,Sphere_Radius,"UH8K")}

    rawTable = requests.get(tap_url, params=payload).content

    # On récupère les données sous forme de tableau en utilisant le module pandas

    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)

    # ---------------- Recherche des images ayant pas de noms défini -------------

    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms

    logging.info("Searching for unnamed objects present in UH8K")

    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte

    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1

    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
        # La dernière ligne permet de changer les valeurs d'un tableau
        # en évitant l'erreur SettingWithCopyWarning.
        logging.info(st(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")

    # ------ Effacer les objets mal identifiés et callibration des images --------

    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.

    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                 lambda x: "BIAS" in x or "FLAT" in x or "DARK" in
                                 x, Table_obs['"Target Name"']))[0]
        # On veut avoir que des images callibrées (finit par *******p)
        cond_uh8k = np.where(np.array(
                                  [s[-1:] for s in Table_obs['"Product ID"']]
                                   ) == 'p')[0]
        if len(wrong_objects) != 0:
            cond_uh8k = np.setdiff1d(cond_uh8k, wrong_objects)
                                                                    
        planeURI_uh8k = Table_obs['"Plane URI"'][cond_uh8k]
        dates_uh8k    = Table_obs['"Start Date"'][cond_uh8k]
        filters_uh8k  = Table_obs['"Filter"'][cond_uh8k]
        pIDs_uh8k     = Table_obs['"Product ID"'][cond_uh8k]
        # Les 3 dernières lignes seront utiles pour la calibration des images.

        N_uh8k = len(planeURI_uh8k)

    else:
        N_uh8k = 0
        planeURI_uh8k = []
        dates_uh8k = []
        filters_uh8k = []
        pIDs_uh8k = []

    logging.info("UH8K images: "+str(N_uh8k))
    Table_obs = None
    logging.info("------------------------------------------")

    #sys.exit("test 5")

    return (N_megaprime,planeURI_megaprime,N_wircam,planeURI_wircam,N_cfh12,
            planeURI_cfh12,dates_cfh12,filters_cfh12,pIDs_cfh12,N_uh8k,
            planeURI_uh8k,dates_uh8k,filters_uh8k,pIDs_uh8k)



# ----------------- Cas rectangular search box --------------------------------

def cfht_search_images_2(obj_center,Ra_box,DEC_box,tap_url):
    
    ############################## Instrument MEGAPRIME ##########################
    
    logging.info("Searching in MegaPrime")
    
    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.
    
    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
               'QUERY':query_object_2(obj_center,Ra_box,DEC_box,"MegaPrime")}
    
    rawTable = requests.get(tap_url, params=payload).content
    
    # On récupère les données sous forme de tableau en utilisant le module pandas
    
    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)
    
    # ---------------- Recherche des images ayant pas de noms défini ------------
    
    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms
    
    logging.info("Searching for unnamed objects present in MegaPrime")
    
    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte
    
    
    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1
    
    #print counter
    #print Table_obs
    #sys.exit("test 2.2")
    
    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
        # La dernière ligne permet de changer les valeurs d'un tableau
        # en évitant l'erreur SettingWithCopyWarning.
        logging.info(st(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")
    
    # ------ Effacer les objets mal identifiés et callibration des images -------
    
    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.
    
    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                     lambda x: "BIAS" in x or "FLAT" in x or
                                     "DARK" in x, Table_obs['"Target Name"']))[0]
                                     
        # On veut avoir que des images callibrées (finit par *******p)
        cond_megaprime = np.where(np.array(
                                     [s[-1:] for s in Table_obs['"Product ID"']]
                                        ) == 'p')[0]
        if len(wrong_objects) != 0:
            cond_megaprime = np.setdiff1d(cond_megaprime, wrong_objects)
            
        planeURI_megaprime = Table_obs['"Plane URI"'][cond_megaprime]
                                                                        
        N_megaprime = len(planeURI_megaprime)
    
    else:
        N_megaprime = 0
        planeURI_megaprime = []
    
    logging.info("MegaPrime images: "+str(N_megaprime))
    Table_obs = None
    logging.info("------------------------------------------")
    
    #sys.exit("test 3")
    
    ############################ Instrument WIRCAM ###############################
    
    logging.info("Searching in WIRCam")
    
    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.
    
    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
               'QUERY':query_object_2(obj_center,Ra_box,DEC_box,"WIRCam")}
    
    rawTable = requests.get(tap_url, params=payload).content
    
    # On récupère les données sous forme de tableau en utilisant le module pandas
    
    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)
    
    # ---------------- Recherche des images ayant pas de noms défini -------------
    
    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms
    
    logging.info("Searching for unnamed objects present in WIRCam")
    
    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte
    
    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1
    
    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
        # La dernière ligne permet de changer les valeurs d'un tableau
        # en évitant l'erreur SettingWithCopyWarning.
        logging.info(str(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")
    
    # ------ Effacer les objets mal identifiés et callibration des images --------
    
    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.
    
    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                lambda x: "BIAS" in x or "FLAT" in x or "DARK" in
                                x, Table_obs['"Target Name"']))[0]
        # On veut avoir que des images callibrées (finit par *******p)
        cond_wircam = np.where(np.array(
                                    [s[-1:] for s in Table_obs['"Product ID"']]
                                    ) == 'p')[0]
        if len(wrong_objects) != 0:
            cond_wircam = np.setdiff1d(cond_wircam, wrong_objects)
                                                                     
        planeURI_wircam = Table_obs['"Plane URI"'][cond_wircam]
                                                                     
        N_wircam = len(planeURI_wircam)
    
    else:
        N_wircam = 0
        planeURI_wircam = []
    
    logging.info("WIRCam images: "+str(N_wircam))
    Table_obs = None
    logging.info("------------------------------------------")
    
    #sys.exit("test 4")
    
    ########################## Instrument CFH12K #################################
    
    logging.info("Searching in CFH12K")
    
    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.
    
    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
               'QUERY':query_object_2(obj_center,Ra_box,DEC_box,"CFH12K")}
    
    rawTable = requests.get(tap_url, params=payload).content
    
    # On récupère les données sous forme de tableau en utilisant le module pandas
    
    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)
    
    # ---------------- Recherche des images ayant pas de noms défini -------------
    
    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms
    
    logging.info("Searching for unnamed objects present in CFH12K")
    
    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte
    
    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1
    
    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
        # La dernière ligne permet de changer les valeurs d'un tableau
        # en évitant l'erreur SettingWithCopyWarning.
        logging.info(st(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")
    
    # ------ Effacer les objets mal identifiés et callibration des images --------
    
    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.
    
    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                lambda x: "BIAS" in x or "FLAT" in x or "DARK" in
                                x, Table_obs['"Target Name"']))[0]
        # On veut avoir que des images callibrées (finit par *******p)
        cond_cfh12 = np.where(np.array(
                                    [s[-1:] for s in Table_obs['"Product ID"']])
                                    == 'p')[0]
        if len(wrong_objects) != 0:
            cond_cfh12 = np.setdiff1d(cond_cfh12, wrong_objects)
                                                           
        planeURI_cfh12 = Table_obs['"Plane URI"'][cond_cfh12]
        dates_cfh12    = Table_obs['"Start Date"'][cond_cfh12]
        filters_cfh12  = Table_obs['"Filter"'][cond_cfh12]
        pIDs_cfh12     = Table_obs['"Product ID"'][cond_cfh12]
        # Les 3 dernières lignes seront utiles pour la calibration des images.
                                                           
        N_cfh12 = len(planeURI_cfh12)
    
    else:
        N_cfh12 = 0
        planeURI_cfh12 = []
        dates_cfh12 = []
        filters_cfh12 = []
        pIDs_cfh12 = []
    
    logging.info("CFH12K images: "+str(N_cfh12))
    Table_obs = None
    logging.info("------------------------------------------")
    
    ############################ Instrument UH8K ###############################
    
    logging.info("Searching in UH8K")
    
    # On veut passer des paramètres dans les URLs. Le module Requests permet de
    # fournir ces arguments sous forme de dictionnaire, en utilisant l’argument
    # params.
    
    payload = {'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'CSV',
        'QUERY':query_object_2(obj_center,Ra_box,DEC_box,"UH8K")}
    
    rawTable = requests.get(tap_url, params=payload).content
    
    # On récupère les données sous forme de tableau en utilisant le module pandas
    
    Table_obs = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)
    
    # ---------------- Recherche des images ayant pas de noms défini -------------
    
    # Certaines images, récent ou pas, n'ont pas de noms propres (NaN). Ils
    # peuvent être important à les étudier. Pour les intégrer dans la recherche,
    # il faut leur changer de noms
    
    logging.info("Searching for unnamed objects present in UH8K")
    
    null_columns = Table_obs.columns[Table_obs.isnull().any()]
    # On recherche dans le tableau, les colonnes où il y a des éléments vides et
    # on fait le compte
    
    counter = 0
    for f in Table_obs['"Target Name"'].isnull():
        if f ==True:
            counter +=1
    
    if counter != 0:
        logging.info("There are "+str(counter)+" unnamed images found")
        c = 0
        # On fait le changement
        N = len(Table_obs['"Target Name"'])
        for it, i in enumerate(Table_obs['"Target Name"']):
            update_process(N,it)
            for ft, f in enumerate(Table_obs['"Target Name"'].isnull()):
                if f ==True:
                    if it == ft:
                        c +=1
                        Table_obs.loc[it, '"Target Name"'] = "unnamed_obj_"+str(c)
        # La dernière ligne permet de changer les valeurs d'un tableau
        # en évitant l'erreur SettingWithCopyWarning.
        logging.info(st(c)+" unnamed images have been named")
    else:
        logging.info("No unnamed image has been found")
    
    # ------ Effacer les objets mal identifiés et callibration des images --------
    
    # Certains observations de type BIAS, DARK ou FLAT sont marqués comme OBJECT.
    # Ces objects doivent être retirés.
    
    if len(Table_obs) != 0:
        wrong_objects = np.where(map(
                                lambda x: "BIAS" in x or "FLAT" in x or "DARK" in
                                x, Table_obs['"Target Name"']))[0]
        # On veut avoir que des images callibrées (finit par *******p)
        cond_uh8k = np.where(np.array(
                                    [s[-1:] for s in Table_obs['"Product ID"']]
                                     ) == 'p')[0]
        if len(wrong_objects) != 0:
            cond_uh8k = np.setdiff1d(cond_uh8k, wrong_objects)
                                                                   
        planeURI_uh8k = Table_obs['"Plane URI"'][cond_uh8k]
        dates_uh8k    = Table_obs['"Start Date"'][cond_uh8k]
        filters_uh8k  = Table_obs['"Filter"'][cond_uh8k]
        pIDs_uh8k     = Table_obs['"Product ID"'][cond_uh8k]
        # Les 3 dernières lignes seront utiles pour la calibration des images.
                                                                   
        N_uh8k = len(planeURI_uh8k)
    
    else:
        N_uh8k = 0
        planeURI_uh8k = []
        dates_uh8k = []
        filters_uh8k = []
        pIDs_uh8k = []

    logging.info("UH8K images: "+str(N_uh8k))
    Table_obs = None
    logging.info("------------------------------------------")
    
    #sys.exit("test 5")
    
    return (N_megaprime,planeURI_megaprime,N_wircam,planeURI_wircam,N_cfh12,
            planeURI_cfh12,dates_cfh12,filters_cfh12,pIDs_cfh12,N_uh8k,
            planeURI_uh8k,dates_uh8k,filters_uh8k,pIDs_uh8k)


################ Fonction pour la recherche de FLAT, BIAS et DARK ################

# La plupart des images astronomiques ne sont pas encore calibrées. Pour cela, il
# faudra les calibrer manuellement grâce à les fichers de type BIAS, DARK et FLAT.

# On définit une fonction query pour les objets BIAS et DARK pour une date donnée.

def query_bias_dark(date,interval,inst):
    
     if inst == "CFH12K":
          INST = "('CFH12K MOSAIC')"

     elif inst == "UH8K":
          INST = "('UH8K','UH8K MOSAIC CAMERA')"

     else:
          sys.exit("Not a valid instrument. Modify query_bias_dark")

     # Voir ADQL sur le site cadc:/
     string_0 = '''SELECT Plane.planeURI AS "Plane URI",
                   Plane.productID AS "Product ID",
                   Observation.instrument_name AS "Instrument",
                   Plane.time_bounds_lower AS "Start Date",
                   Plane.time_exposure AS "Int. Time",
                   Plane.energy_bandpassName AS "Filter",
                   Observation.type AS "Obs. Type"
                   FROM caom2.Plane AS Plane
                   JOIN caom2.Observation AS Observation ON Plane.obsID
                   = Observation.obsID
                   WHERE ( INTERSECTS( INTERVAL('''

     string_1 = '''), Plane.time_bounds_samples ) = 1
                   AND Observation.instrument_name IN '''

     string_2 = '''AND Observation.type IN ( 'BIAS','DARK' )
                   AND ( Plane.quality_flag IS NULL OR Plane.quality_flag !=
                   'junk' ) )'''

     query = (string_0+str(date-0.5*interval)+','+str(date+0.5*interval)
              +string_1+INST+string_2)

     return query

# On fait de même pour les objets FLAT et on ajoute un filtre.

def query_flat(date,interval,Filter,inst):

      if inst == "CFH12K":
         INST = "('CF12K MOSAIC')"
         OTYP = "('FLAT')"

      elif inst == "UH8K":
         INST = "('UH8K','UH8K MOSAIC CAMERA')"
         OTYP = "('FLAT','FLATFIELD')"

      else:
         sys.exit("Not a valid instrument. Modify query_flat")

      # Voir ADQL
      string_0 = '''SELECT Plane.planeURI AS "Plane URI",
                    Plane.productID AS "Product ID",
                    Observation.instrument_name AS "Instrument",
                    Plane.time_bounds_lower AS "Start Date",
                    Plane.time_exposure AS "Int. Time",
                    Plane.energy_bandpassName AS "Filter",
                    Observation.type AS "Obs. Type"
                    FROM caom2.Plane AS Plane
                    JOIN caom2.Observation AS Observation ON Plane.obsID =
                    Observation.obsID
                    WHERE ( INTERSECTS( INTERVAL('''

      string_1 = '''), Plane.time_bounds_samples ) = 1
                    AND Observation.instrument_name IN '''

      string_2 = "AND Plane.energy_bandpassName = "

      string_3 = "AND Observation.type IN "

      string_4 = '''AND ( Plane.quality_flag IS NULL OR Plane.quality_flag !=
                    'junk' ) )'''

      query = (string_0+str(date-0.5*interval)+','+str(date+0.5*interval)
               +string_1+INST+string_2+"'{0}'".format(Filter)
               +string_3+OTYP+string_4)

      return query

# Maintenant on va chercher ces dossiers pour les instruments CFHT

def Search_FBD(dates,filters,pIDs,inst,tap_url):

    if inst not in ["CFH12K","UH8K"]:
        sys.exit("Instrument not defined")
    
    
    N = len(dates)
    logging.info("Searching BIAS, DARK and FLAT for "+inst+" observations")

    # La fonction np.empty renvoie un nouveau tableau de forme et de type donné,
    # avec des valeurs aléatoires. Dans ce cas N listes de 10 valeurs.

    URIs_bias = np.empty((N,10), dtype=object)
    URIs_dark = np.empty((N,10), dtype=object)
    URIs_flat = np.empty((N,10), dtype=object)

    for i,(date,filt,pID) in enumerate(zip(dates,filters,pIDs)):
        update_process(N,i)

        # Idem que payload voir étapes précédents
        temp_load = {'REQUEST':'doQuery', 'LANG':'ADQL', 'FORMAT':'CSV',
                     'QUERY':query_bias_dark(date,dt4bias,inst)}

        rawTable = requests.get(tap_url, params=temp_load).content

        # On récupère les données sous forme de tableau en utilisant le module
        # pandas
        temp_bds = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)

        # De plus on identifie les différent type d'objet; Bias ou Dark
        temp_bias = temp_bds.loc[temp_bds['"Obs. Type"'] ==
                                 'BIAS'].reset_index(drop=True)

        temp_dark = temp_bds.loc[temp_bds['"Obs. Type"'] ==
                                 'DARK'].reset_index(drop=True)

        #------------------------------- BIAS ----------------------------------
        URIs_bias[i,:] = None
        if len(temp_bias) != 0:
            # On ne garde que des recents objets
            dt_bias = np.abs(temp_bias['"Start Date"'] - date)
            try:
                bias_id = np.argsort(dt_bias)[:10]
            except Exception as e:
                logging.warning(str("Object ",pID," with less than 10 BIAS."))
                bias_id = np.argsort(dt_bias)
                pass

            # En cas de durée très longue
            if any(dt_bias[bias_id] > dt4bias):
                logging.warning("Bias time span for object "+str(pID)+" :"
                                +str(np.max(dt_bias[bias_id]))+" MJDs.")

            # On ajoute ces fichiers dans leur tableau correspondant.
            URIs_bias[i,:len(bias_id)] = temp_bias['"Plane URI"'].iloc[bias_id]

        else:
            logging.warning("No BIAS file for object: "+str(pID))

        #------------------------------- DARK ----------------------------------
        URIs_dark[i,:] = None
        if len(temp_dark) != 0:
            # On ne garde que des recents objets
            dt_dark = np.abs(temp_dark['"Start Date"'] - date)
            try:
                dark_id = np.argsort(dt_dark)[:10]
            except Exception as e:
                logging.warning(str("Object ",pID," with less than 10 DARKS."))
                dark_id = np.argsort(dt_dark)
                pass
    
            # En cas de durée très longue
            if any(dt_dark[dark_id] > dt4bias):
                logging.warning("Dark time span for object "+str(pID)+" :"
                                +str(np.max(dt_dark[dark_id]))+" MJDs.")
    
            # On ajoute ces fichiers dans leur tableau correspondant.
            URIs_dark[i,:len(dark_id)] = temp_dark['"Plane URI"'].iloc[dark_id]

        else:
            logging.warning("No DARK file for object: "+str(pID))


        #------------------------------ FLAT ------------------------------------
        temp_load = {'REQUEST': 'doQuery', 'LANG': 'ADQL','FORMAT':'CSV',
                     'QUERY':query_flat(date,dt4flat,filt,"CFH12K")}

        rawTable = requests.get(tap_url, params=temp_load).content
    
        # On récupère les données sous forme de tableau en utilisant le module
        # pandas
        temp_flat = pd.read_csv(io.StringIO(rawTable.decode('utf-8')), header=0)

        URIs_flat[i,:] = None
        if len(temp_flat) != 0:
            # On ne garde que des recents objets
            dt_flat = np.abs(temp_flat['"Start Date"'] - date)
            try:
                flat_id = np.argsort(dt_flat)[:10]
            except Exception as e:
                logging.warning(str("Object ",pID," with less than 10 FLATS."))
                flat_id = np.argsort(dt_flat)
                pass
    
            # En cas de durée très longue
            if any(dt_flat[flat_id] > dt4flat):
                logging.warning("Flat time span for object "+str(pID)+" :"
                                +str(np.max(dt_dark[dark_id]))+" MJDs.")
    
            # On ajoute ces fichiers dans leur tableau correspondant.
            URIs_flat[i,:len(flat_id)] = temp_flat['"Plane URI"'].iloc[flat_id]

        else:
            logging.warning("No FLAT file for object: "+str(pID))

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


#sys.exit("test 6")

##################################################################################

##################### Telechargement des images astronomiques ####################

# Lors de la telechargement sur le site de CFHT, il faut une autorisation, soit un
# identifiant et un mot de passe. On défini donc:

def download_cfht_images(usr,pss,download_url,N_megaprime,N_wircam,N_cfh12,N_uh8k,
                         planeURI_megaprime,planeURI_wircam,planeURI_cfh12,
                         planeURI_cfh12_fbd,planeURI_uh8k,planeURI_uh8k_fbd,
                         output_directory):
    
    # ------------------- Téléchargement des images MegaPrime ------------------
    if N_megaprime != 0:
        logging.info("Downloading images from MegaPrime instrument")
        out_dir = output_directory+"/MegaPrime/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")

        else:
            logging.info("The directory "+str(out_dir)+" is already created.")


        for plist in tqdm(planeURI_megaprime):
            
            (aa,bb) = plist.split(':')
            (kk,cc,ff) = bb.split('/')
        
            link = download_url+'{}/{}.fits.fz?'.format(kk,ff)
        
            r = requests.get(link, auth=HTTPBasicAuth(usr, pss), stream=True)
            total_length = int(r.headers.get('content-length', 0))
            block_length = 1024
            wrote = 0
            
            if not os.path.exists(out_dir+"/{}.fits.fz".format(ff)):
                with open(out_dir+"/{}.fits.fz".format(ff), 'wb') as f:
                    for data in tqdm(r.iter_content(block_length), total=math.ceil
                                     (total_length//block_length), unit='KB',
                                      unit_scale=True):
                        wrote += len(data)
                        f.write(data)

                if total_length != 0 and wrote != total_length:
                    logging.error("Something went wrong")

                logging.info("The image "+ff+" has been downloaded.")
                    
            else:
                logging.info("The image "+ff+" has already been downloaded.")

        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")

    #sys.exit("test 7")

    # -------------------- Téléchargement des images WIRCam ----------------------
    if N_wircam != 0:
        logging.info("Downloading images from WIRCam instrument")
        out_dir = output_directory+"/WIRCam/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")
    
        else:
            logging.info("The directory "+str(out_dir)+" is already created.")
    
    
    
        for plist in tqdm(planeURI_wircam):
            
            (aa,bb) = plist.split(':')
            (kk,cc,ff) = bb.split('/')

            link = download_url+'{}/{}.fits.fz?'.format(kk,ff)
    
            r = requests.get(link, auth=HTTPBasicAuth(usr, pss), stream=True)
            total_length = int(r.headers.get('content-length', 0))
            block_length = 1024
            wrote = 0
        
            if not os.path.exists(out_dir+"/{}.fits.fz".format(ff)):
                with open(out_dir+"/{}.fits.fz".format(ff), 'wb') as f:
                    for data in tqdm(r.iter_content(block_length), total=math.ceil
                                     (total_length//block_length), unit='KB',
                                     unit_scale=True):
                        wrote += len(data)
                        f.write(data)
        
                if total_length != 0 and wrote != total_length:
                    logging.error("Something went wrong")

                logging.info("The image "+ff+" has been downloaded.")
                    
            else:
                logging.info("The image "+ff+" has already been downloaded.")
    
        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")

    # ------------------- Téléchargement des images CFH12K -----------------------
    if N_cfh12 != 0:
        logging.info("Downloading images from CFH12K instrument")
        out_dir = output_directory+"/CFH12K/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")
    
        else:
            logging.info("The directory "+str(out_dir)+" is already created.")

        # On doit additionner les 2 listes planeURI: images + calibrations.
        planeURI_cfh12 = np.concatenate((planeURI_cfh12,planeURI_cfh12_fbd),
                                        axis=0)

        # On retire les doublures.
        planeURI_cfh12 = set(planeURI_cfh12)

    
        for plist in tqdm(planeURI_cfh12):
        
            (aa,bb) = plist.split(':')
            (kk,cc,ff) = bb.split('/')
        
            link = download_url+'{}/{}.fits.fz?'.format(kk,ff)
            r = requests.get(link, auth=HTTPBasicAuth(usr, pss), stream=True)
            total_length = int(r.headers.get('content-length', 0))
            block_length = 1024
            wrote = 0
        
            if not os.path.exists(out_dir+"/{}.fits.fz".format(ff)):
                with open(out_dir+"/{}.fits.fz".format(ff), 'wb') as f:
                    for data in tqdm(r.iter_content(block_length), total=math.ceil
                                     (total_length//block_length), unit='KB',
                                     unit_scale=True):
                        wrote += len(data)
                        f.write(data)
        
                if total_length != 0 and wrote != total_length:
                    logging.error("Something went wrong")

                logging.info("The image "+ff+" have been downloaded.")
                    
            else:
                logging.info("The image "+ff+" has already been downloaded.")
    
        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")

    # ---------------------- Téléchargement des images UH8K ----------------------
    if N_uh8k != 0:
        logging.info("Downloading images from UH8K instrument")
        out_dir = output_directory+"/UH8K/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info("The directory "+str(out_dir)+" have been created.")
    
        else:
            logging.info("The directory "+str(out_dir)+" is already created.")
    
        # On doit additionner les 2 listes planeURI: images + calibrations.
        planeURI_uh8k = np.concatenate((planeURI_uh8k,planeURI_uh8k_fbd),axis=0)
    
        # On retire les doublures.
        planeURI_uh8k = set(planeURI_uh8k)

    
        for plist in tqdm(planeURI_uh8k):
        
            (aa,bb) = plist.split(':')
            (kk,cc,ff) = bb.split('/')

            link = download_url+'{}/{}.fits.fz?'.format(kk,ff)
            r = requests.get(link, auth=HTTPBasicAuth(usr, pss), stream=True)
            total_length = int(r.headers.get('content-length', 0))
            block_length = 1024
            wrote = 0
        
            if not os.path.exists(out_dir+"/{}.fits.fz".format(ff)):
                with open(out_dir+"/{}.fits.fz".format(ff), 'wb') as f:
                    for data in tqdm(r.iter_content(block_length), total=math.ceil
                                     (total_length//block_length), unit='KB',
                                     unit_scale=True):
                        wrote += len(data)
                        f.write(data)
        
                if total_length != 0 and wrote != total_length:
                    logging.error("Something went wrong")

                logging.info("The image "+ff+" have been downloaded.")
                    
            else:
                logging.info("The image "+ff+" has already been downloaded.")
    
        logging.info("Download completed ")
        logging.info("--------------------------------------------------------")


    logging.info("All CFHT images downloaded")
    return

##################################################################################

if __name__ == "__main__":
    
    # -------------------------- Paramètres d'entrées --------------------
    
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
        Sphere_Radius = float(input('''Select the sphere's radius in deg : '''))
    elif Box_search == "b":
        Ra_box = float(input("Select the length of the search box in deg : "))
        DEC_box = float(input("Select the width of the search box in deg : "))
    else:
        sys.exit('''You should choose between r and b; circle or rectangle''')

    # --------- Coordonnées de l'objet en degrés ------------------
    sd.reset_votable_fields()
    sd.remove_votable_fields('coordinates')
    sd.add_votable_fields('ra(d;A;ICRS)', 'dec(d;D;ICRS)')

    table = sd.query_object(Object, wildcard=False)

    RA_center_deg = table[0][1]
    DEC_center_deg = table[0][2]

    CENTRE = np.array([RA_center_deg, DEC_center_deg])

    output_directory = cfht_parameter_in(Object)

    # ----------------- Identifiant et mot de passe ------------------------------

    usr = raw_input("Enter CFHT username : ")
    pss = getpass("Enter CFHT password : ")

    logging.info("Searching Scientific images")

    if Box_search == "r":
        (N_megaprime,planeURI_megaprime,
         N_wircam,planeURI_wircam,N_cfh12,
         planeURI_cfh12,dates_cfh12,
         filters_cfh12,pIDs_cfh12,N_uh8k,
         planeURI_uh8k,dates_uh8k,
         filters_uh8k,pIDs_uh8k) = cfht_search_images(CENTRE,Sphere_Radius,
                                                      tap_url)

    elif Box_search == "b":
        (N_megaprime,planeURI_megaprime,
         N_wircam,planeURI_wircam,N_cfh12,
         planeURI_cfh12,dates_cfh12,
         filters_cfh12,pIDs_cfh12,N_uh8k,
         planeURI_uh8k,dates_uh8k,
         filters_uh8k,pIDs_uh8k) = cfht_search_images_2(CENTRE,Ra_box,DEC_box,
                                                        tap_url)

    logging.info("Searching images calibrations")

    # -- Recherche des BIAS, DARKS et FLATS dans les instruments CFH12K et UH8K --

    # Maintenant on cherche les calibrations des images pour les instruments
    # CFH12K et UH8K

    if N_cfh12 != 0:
        planeURI_cfh12_fbd = Search_FBD(dates_cfh12,filters_cfh12,
                                        pIDs_cfh12,"CFH12K",tap_url)

    else:
        logging.info("No images calibrations was found in CFH12K")
        planeURI_cfh12_fbd = []

    if N_uh8k != 0:
        planeURI_uh8k_fbd = Search_FBD(dates_uh8k,filters_uh8k,
                                       pIDs_uh8k,"UH8K",tap_url)

    else:
        logging.info("No images calibrations was found in UH8K")
        planeURI_uh8k_fbd = []


    logging.info("Downloading images")

    
    download_cfht_images(usr,pss,download_url,N_megaprime,N_wircam,N_cfh12,N_uh8k,
                         planeURI_megaprime,planeURI_wircam,planeURI_cfh12,
                         planeURI_cfh12_fbd,planeURI_uh8k,planeURI_uh8k_fbd,
                         output_directory)

    # Si les images se téléchargent très vite et sont vide, alors verifie la lien
    # tap_url et download_url sur le site du archive cfht
    # lien ici (http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/fr/doc/tap/)
    # Si ça ne marche toujours pas alors la meilleur solution reste à selectioner
    # quelques images sur le site du cfht et de télécharger la liste des urls.

    logging.info("Done")

