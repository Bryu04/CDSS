#! /usr/bin/python
# -*- coding: utf-8 -*-
#
# Ce code principale va permettre de rechercher des images astronomiques venant de
# plusieurs d'archivage des téléscopes en un seul requête.
#

################### On va importer les modules standards ########################

import sys
import io
import os
import logging
import requests
import numpy as np
import pandas as pd
from getpass import getpass
from datetime import datetime
from tqdm import tqdm # bar de status
from astropy.coordinates import SkyCoord as sk
from astropy import units as u
from astropy.time import Time
from astropy.io import fits
from astropy.io import ascii
from astropy.table import Table
from astroquery.simbad import Simbad as sd
from requests.auth import HTTPBasicAuth

###################### Importation des modules personnels ####################

import cfht_archive_module as cfht
import smoka_archive_module as smoka
import eso_archive_module as eso

##################################################################################

# Comme cette requête va télécharger des images de hautes qualités, cela risque de
# prendre beaucoup de temps, voire plusieurs semaines. On peut améliorer les
# recherches en utilisant plusieurs processeurs.

########################## Création des processeurs multiples ####################

def start_pool(project_list):
    if __name__ == '__main__':
        p = Pool(processes=1) # On se limite à 10 processeurs max
        p.map(common_function,project_list)
        p.close()
        p.join()
        
        print 'workers=',p.processes

# Création de la tache commune.

def common_function(j):
    f = open(j, 'r')
    for ligne in f:
        obj = wget.download(ligne)
        print obj

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

########################## Paramètres de départ #################################

# Comme son nom indique, on va définir les paramètres de départ, soit le nom de
# l'objet qu'on veux chercher, le rayon de la sphère céleste en degrés, les
# instruments de SMOKA et les urls (site web) de SMOKA

if __name__ == "__main__":

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
        sys.exit("You should choose between r and b; circle or rectangle box")


directory = os.getcwd() # retourne le répertoire de travail actuel
                        # d'un processus

output_directory = directory+'/{}'.format(Object)
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Les 2 dernières lignes sert à créer un dossier contenant les images
# astronique de l'objet en question.


################################### LOG FILE #####################################

# Dans cette partie, on va configurer un logger qui va nous donner les info au fil
# du temps.

# Création de l'objet logger qui va nous servir à écrire dans les logs
logger = logging.getLogger()

# On met le niveau du logger à DEBUG, comme ça il écrit tout
logger.setLevel(logging.DEBUG)


# On crée un formateur qui va ajouter le temps, le niveau de chaque message quand
# on écrira un message dans le log

formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
fmt = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        '%m-%d %H:%M')
# Création d'un handler qui va écrire les messages du niveau INFO ou supérieur
# dans le sys.stderr

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

# On va crée un second handler qui va sauvegarder tous les messages dans un
# ficher

fh = logging.FileHandler(output_directory+"/Search.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)

# Enfin on ajoute les handlers au logger principal
logger.addHandler(console)
logger.addHandler(fh)

############################ Coordonnées de l'objet #############################

# On va utiliser le module Simbad pour récuperer les coordonées équatoriales de
# l'objet qu'on veut trouver dans la sphère céleste.

sd.reset_votable_fields()
sd.remove_votable_fields('coordinates')
sd.add_votable_fields('ra(:;A;ICRS;J2000)', 'dec(:;D;ICRS;J2000)')

table = sd.query_object(Object, wildcard=False)

# print table.keys()
a = table[0][1]
b = table[0][2]
# print a
# print b

r1 = a.split(':')
d1 = b.split(':')
# print len(d1)

# sys.exit("test 1")

if len(d1) < 3:
    t = []
    for i in d1:
        t.append(i.split('.'))
    s = t[0]
    o = t[1]
    d1 = s + o
    d1[2] = '00'

# print t
# print len(t)
# print d1[0]

r = '{} {} {}'.format(r1[0],r1[1],r1[2])
d = '{} {} {}'.format(d1[0],d1[1],d1[2])

# print r
# print d

logging.info("Object: "+str(Object))
logging.info("Coordinates[ICRS]: RA, DEC = {}  {}".format(r,d))

# ----------------- Cordonnées de l'objet en degrés ------------------

sd.reset_votable_fields()
sd.remove_votable_fields('coordinates')
sd.add_votable_fields('ra(d;A;ICRS)', 'dec(d;D;ICRS)')

table = sd.query_object(Object, wildcard=False)

RA_center_deg = table[0][1]
DEC_center_deg = table[0][2]

# print a

# sys.exit("test 2")

CENTRE = np.array([RA_center_deg, DEC_center_deg])

if __name__ == "__main__":
  
    # ----------------- Identifiant et mot de passe ------------------------------
    
    cfht_usr = raw_input("Enter CFHT username : ")
    cfht_pss = getpass("Enter CFHT password : ")
    eso_usr = raw_input("Enter ESO username : ")
    eso_pss = getpass("Enter ESO password : ")


    # ------------------------------ SMOKA ---------------------------
    logging.info("Running SMOKA archive")
    output_directory = smoka.smoka_parameter_in(Object,Box_search,Sphere_Radius)
    
    logging.info("Searching images in Suprime-Cam")
    if Box_search == "r":
        smoka.SuprimeCam_Search(Object,CENTRE,Sphere_Radius,smoka.FOV_SUP,
                                smoka.dt_bias,smoka.dt_flat,smoka.min_calib,
                                output_directory)
    elif Box_search == "b":
        smoka.SuprimeCam_Search_2(Object,CENTRE,Ra_box,DEC_box,smoka.FOV_SUP,
                                  smoka.dt_bias,smoka.dt_flat,smoka.min_calib,
                                  output_directory)
    logging.info("End SUP")
    logging.info("---------------------------------------")
    
    logging.info("Searching images in HyperSuprime-Cam")
    if Box_search == "r":
        smoka.HyperSuprimeCam_Search(Object,CENTRE,Sphere_Radius,smoka.FOV_HSC,
                                     smoka.dt_bias,smoka.dt_flat,smoka.dt_dark,
                                     smoka.min_calib,output_directory)
    elif Box_search == "b":
        smoka.HyperSuprimeCam_Search_2(Object,CENTRE,Ra_box,DEC_box,smoka.FOV_HSC,
                                       smoka.dt_bias,smoka.dt_flat,smoka.dt_dark,
                                       smoka.min_calib,output_directory)
    logging.info("End HSC")
    logging.info("---------------------------------------")

    logging.info("End SMOKA archive")

    # -------------------------------- CFHT ------------------------------
    logging.info("Running CFHT archive")
    output_directory = cfht.cfht_parameter_in(Object)

    logging.info("Searching Scientific images")

    if Box_search == "r":
        (N_megaprime,planeURI_megaprime,
         N_wircam,planeURI_wircam,N_cfh12,
         planeURI_cfh12,dates_cfh12,
         filters_cfh12,pIDs_cfh12,N_uh8k,
         planeURI_uh8k,dates_uh8k,
         filters_uh8k,pIDs_uh8k) = cfht.cfht_search_images(CENTRE,Sphere_Radius,
                                                           cfht.tap_url)

    elif Box_search == "b":
        (N_megaprime,planeURI_megaprime,
         N_wircam,planeURI_wircam,N_cfh12,
         planeURI_cfh12,dates_cfh12,
         filters_cfh12,pIDs_cfh12,N_uh8k,
         planeURI_uh8k,dates_uh8k,
         filters_uh8k,pIDs_uh8k) = cfht.cfht_search_images_2(CENTRE,Ra_box,
                                                             DEC_box,
                                                             cfht.tap_url)

    logging.info("Searching images calibrations")

    # -- Recherche des BIAS, DARKS et FLATS dans les instruments CFH12K et UH8K --

    # Maintenant on cherche les calibrations des images pour les instruments
    # CFH12K et UH8K

    if N_cfh12 != 0:
        planeURI_cfh12_fbd = cfht.Search_FBD(dates_cfh12,filters_cfh12,
                                             pIDs_cfh12,"CFH12K",cfht.tap_url)

    else:
        logging.info("No images calibrations was found in CFH12K")
        planeURI_cfh12_fbd = []

    if N_uh8k != 0:
        planeURI_uh8k_fbd = cfht.Search_FBD(dates_uh8k,filters_uh8k
                                            ,pIDs_uh8k,"UH8K",cfht.tap_url)

    else:
        logging.info("No images calibrations was found in UH8K")
        planeURI_uh8k_fbd = []


    logging.info("Downloading images")

    cfht.download_cfht_images(cfht_usr,cfht_pss,cfht.download_url,N_megaprime,
                              N_wircam,N_cfh12,N_uh8k,planeURI_megaprime,
                              planeURI_wircam,planeURI_cfh12,planeURI_cfh12_fbd,
                              planeURI_uh8k,planeURI_uh8k_fbd,output_directory)

    logging.info("End CFHT archive")

    # ----------------------------- ESO ------------------------------------
    logging.info("Running ESO archive")
    output_directory = eso.eso_parameter_in(Object)

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
         orig_id_hawki,Orig_Id) = eso.eso_search_images(Object,CENTRE,
                                                        Sphere_Radius,
                                                        eso.tap_url)

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
         orig_id_hawki,Orig_Id) = eso.eso_search_images_2(Object,CENTRE,Ra_box,
                                                          DEC_box,eso.tap_url)



    logging.info("Searching images calibrations")

    # --- Recherche des BIAS, DARKS et FLATS dans les instruments  ------

    # Maintenant on cherche les calibrations des images pour les instruments
 

    if N_sofi != 0:
        planeURI_sofi_fbd = eso.Search_FBD(dates_sofi,filters_sofi,
                                           orig_id_sofi,"SOFI",Orig_Id,
                                           eso.tap_url)
    else:
        planeURI_sofi_fbd = []

    if N_wfi != 0:
        planeURI_wfi_fbd = eso.Search_FBD(dates_wfi,filters_wfi,
                                          orig_id_wfi,"WFI",Orig_Id,eso.tap_url)
    else:
        planeURI_wfi_fbd = []

    if N_vircam != 0:
        planeURI_vircam_fbd = eso.Search_FBD(dates_vircam,filters_vircam,
                                             orig_id_vircam,"VIRCAM",Orig_Id,
                                             eso.tap_url)
    else:
        planeURI_vircam_fbd = []

    if N_omegacam != 0:
        planeURI_omegacam_fbd = eso.Search_FBD(dates_omegacam,filters_omegacam,
                                               orig_id_omegacam,"OmegaCAM",
                                               Orig_Id,eso.tap_url)
    else:
        planeURI_omegacam_fbd = []

    if N_vimos != 0:
        planeURI_vimos_fbd = eso.Search_FBD(dates_vimos,filters_vimos,
                                            orig_id_vimos,"VIMOS",Orig_Id,
                                            eso.tap_url)
    else:
        planeURI_vimos_fbd = []

    if N_fors1 != 0:
        planeURI_fors1_fbd = eso.Search_FBD(dates_fors1,filters_fors1,
                                            orig_id_fors1,"FORS1",Orig_Id,
                                            eso.tap_url)
    else:
        planeURI_fors1_fbd = []

    if N_fors2 != 0:
        planeURI_fors2_fbd = eso.Search_FBD(dates_fors2,filters_fors2,
                                            orig_id_fors2,"FORS2",Orig_Id,
                                            eso.tap_url)
    else:
        planeURI_fors2_fbd = []

    if N_hawki != 0:
        planeURI_hawki_fbd = eso.Search_FBD(dates_hawki,filters_hawki,
                                            orig_id_hawki,"HAWKI",Orig_Id,
                                            eso.tap_url)
    else:
        planeURI_hawki_fbd = []


    logging.info("Submitting Request")

    (planeURI_sofi,planeURI_wfi,
     planeURI_vircam,planeURI_omegacam,
     planeURI_vimos,planeURI_fors1,
     planeURI_fors2,planeURI_hawki,
     dic) = eso.summitting_eso_images(eso_usr,eso_pss,eso.request_url,
                                      eso.request_url2,planeURI_sofi,
                                      planeURI_sofi_fbd,planeURI_wfi,
                                      planeURI_wfi_fbd,planeURI_vircam,
                                      planeURI_vircam_fbd,planeURI_omegacam,
                                      planeURI_omegacam_fbd,planeURI_vimos,
                                      planeURI_vimos_fbd,planeURI_fors1,
                                      planeURI_fors1_fbd,planeURI_fors2,
                                      planeURI_fors2_fbd,planeURI_hawki,
                                      planeURI_hawki_fbd)

    

    logging.info("Downloading images")

    eso.download_eso_images(eso_usr,eso_pss,eso.download_url,dic,Orig_Id,
                            N_sofi,N_wfi,N_vircam,N_omegacam,N_vimos,
                            N_fors1,N_fors2,N_hawki,planeURI_sofi,
                            planeURI_wfi,planeURI_vircam,planeURI_omegacam,
                            planeURI_vimos,planeURI_fors1,planeURI_fors2,
                            planeURI_hawki,output_directory)

    logging.info("End ESO archive")

    logging.info("Done")


