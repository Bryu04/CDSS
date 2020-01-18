#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ce module va définir un nouveau class (SMOKA Archive, acronyme du système
# d'archivage de Subaru Mitaka Okayama Kiso) dérivé de l'archive basique pour
# permettre queries sur les archives du Téléscope Subaru, le télescope de 188 cm à
# l'Observatoire astrophysique d'Okayama, et le télescope Schumidt de 105 cm à
# l'Observatoire de Kiso de l'Université de Tokyo.
#
# Ce module va prendre en compte tous les instruments qu'abrite SMOKA notamment:
# Suprime-Cam, Hyper Suprime-Cam et plein d'autres. Il va seulement les chercher
# et il va créer un mail d'envoi pour une requête d'images. On se concentre
# seulement sur ces 2 instruments notamment que les données de Suprime-Cam sont
# fixes car l'instrument n'est plus active, par contre celles de HSC peuvent être
# mises à jour.

################### On va importer les modules standards ########################

import sys
import io
import os
import logging
import requests
import numpy as np
import pandas as pd
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

########################## Paramètres de départ #################################

# Comme son nom indique, on va définir les paramètres de départ, soit le nom de
# l'objet qu'on veux chercher, le rayon de la sphère céleste en degrés, les
# instruments de SMOKA et les urls (site web) de SMOKA


# End class

#if __name__ == "__main__": # Pour éviter qu'il se lance lors de l'importation

    #if nObj >= 4:
        #directory = str(sys.argv[3])
    #else:
    #directory = os.getcwd() # retourne le répertoire de travail actuel
                                # d'un processus

    # Les 2 dernières lignes sert à créer un dossier contenant les images astronique de l'objet en question.

#else:
    #main_code.smoka.smokaArchive(Object,Box_search,directory)

tap_url = "http://smoka.nao.ac.jp/search"
INSTRUMENT = ['SuprimeCam','HyperSuprimeCam']
dt_bias = [5,15]  # Délai maximum pour les Biais Flats et Darks en jours
                  # juliens
dt_flat = [30,365] # Idem pour Flats

dt_dark = [5,15]

min_calib = 5 # nombre min des images calibrées

# Field of view (http://smoka.nao.ac.jp/help/help.jsp)
FOV_SUP_arcsec = 2000/2 * u.arcsec
FOV_HSC_arcsec = 5400/2 * u.arcsec
FOV_SUP = FOV_SUP_arcsec.to(u.deg).value
FOV_HSC = FOV_HSC_arcsec.to(u.deg).value

#try:
#    (Object,Box_search,Sphere_Radius) = main_code.parameter_in()
#
#except Exception as e:
#    (Object,Box_search,Ra_box,DEC_box) = main_code.parameter_in()
#

directory = os.getcwd()

def smoka_parameter_in(Object,Box_search,Sphere_Radius):
    
    output_directory = directory+'/{}/SMOKA/'.format(Object)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return output_directory


#def smoka_parameter_in_2(Object,Box_search,Ra_box,DEC_box):
#    
#    output_directory = directory+'/{}/SMOKA/'.format(Object)
#    if not os.path.exists(output_directory):
#        os.makedirs(output_directory)

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

# On va introduire les différents caractéristiques de chaque instrument de SMOKA
# afin de préciser les données reçu lors de la requête d'images. On définit
# d'abord 2 fonctions specifique pour chaque instrument.

####################### MISE A JOUR LES PARAMETRES D'ENTREE ######################

# Le site de SMOKA nous donne acces au listes d'observations qui ont été faites
# par leurs instruments de mesures. Seul l'instrument HyperSuprimeCam est
# opérationnel.

sm_dir = directory+'/SMOKA/'
if not os.path.exists(sm_dir):
    os.makedirs(sm_dir)

def update_hsc_data(a):
    Data = {}
    logging.info("Searching for update for HSC data")
    a = a.split('/')
    d = a[-1].split('.')
    e = d[0].split('_')
    f = int(e[1])
    
    date = datetime.now()
    year = date.year
    s = 'https://smoka.nao.ac.jp/status/obslog/HSC_{}.txt'.format(year)
    r = requests.head(s)
    
    while r.status_code != 200:
        year = year - 1
        s = 'https://smoka.nao.ac.jp/status/obslog/HSC_{}.txt'.format(year)
        r = requests.head(s)
    
    if year != f:
        logging.info("A new version of HSC data is available")
        logging.info("New version for year "+str(year))
        counter = 0

        # On doit d'abord suprimer l'ancien version
        if os.path.exists(sm_dir+'/HSC.txt'):
            os.remove(sm_dir+'/HSC.txt')
        if os.path.exists(sm_dir+'/HSC.fits'):
            os.remove(sm_dir+'/HSC.fits')
        
        while r.status_code == 200:
            obj = requests.get(s)
            if counter == 0:
                with open (sm_dir+'/HSC.txt', 'a') as f:
                    f.write(obj.content)
            else:
                with open (sm_dir+'/HSC.txt', 'a') as f:
                    with open(sm_dir+'/hsc_dat.txt', 'w') as g:
                        g.write(obj.content)
                    
                    g = open(sm_dir+'/hsc_dat.txt', 'r')
                    for line in g:
                        if not '#' in line:
                            f.write(line)
                    g.close()
                    os.remove(sm_dir+'/hsc_dat.txt')
            year = year - 1
            s = 'https://smoka.nao.ac.jp/status/obslog/HSC_{}.txt'.format(year)
            r = requests.head(s)
            counter +=1
        
        g = open(sm_dir+'/HSC.txt', 'r')
        for lines in g:
            if '#' in lines:
                header = lines.split()
        for i in header:
            Data[i] = []
        
        y = open(sm_dir+'/HSC.txt', 'r')
        y.readline()
        for line in y:
            #print line
            data = line.split()
            for ij, j in enumerate(data):
                for it, i in enumerate(header):
                    if it == ij:
                        Data[i].append(j)

        g.close()
        y.close()
        
        for i in Data.keys():
            Data[i] = np.array(Data[i])
        
        #print Data.keys()
        
        ascii.write([Data[i] for i in header], sm_dir+'/HSC.ascii',
                    names=header, overwrite=True)
        table = Table.read(sm_dir+'/HSC.ascii', format='ascii')
        table = np.array(table)
        fits.writeto(sm_dir+'/HSC.fits',table)
    
    else:
        logging.info("The data is up to date")

# On fait de même pour SuprimeCam même s'il est inopérationnel

def update_sup_data(a):
    Data = {}
    logging.info("Searching for update for SUP data")
    a = a.split('/')
    d = a[-1].split('.')
    e = d[0].split('_')
    f = int(e[1])
    
    date = datetime.now()
    year = date.year
    s = 'https://smoka.nao.ac.jp/status/obslog/SUP_{}.txt'.format(year)
    r = requests.head(s)
    
    while r.status_code != 200:
        year = year - 1
        s = 'https://smoka.nao.ac.jp/status/obslog/SUP_{}.txt'.format(year)
        r = requests.head(s)
    
    if year != f:
        logging.info("A new version of SUP data is available")
        logging.info("New version for year "+str(year))
        counter = 0

        # On doit d'abord suprimer l'ancien version
        if os.path.exists(sm_dir+'/SuprimeCam.txt'):
            os.remove(sm_dir+'/SuprimeCam.txt')
        if os.path.exists(sm_dir+'/SuprimeCam.fits'):
            os.remove(sm_dir+'/SuprimeCam.fits')
        
        while r.status_code == 200:
            obj = requests.get(s)
            if counter == 0:
                with open (sm_dir+'/SuprimeCam.txt', 'a') as f:
                    f.write(obj.content)
            else:
                with open (sm_dir+'/SuprimeCam.txt', 'a') as f:
                    with open(sm_dir+'/sup_dat.txt', 'w') as g:
                        g.write(obj.content)
                    
                    g = open(sm_dir+'/sup_dat.txt', 'r')
                    for line in g:
                        if not '#' in line:
                            f.write(line)
                    g.close()
                    os.remove(sm_dir+'/sup_dat.txt')
            year = year - 1
            s = 'https://smoka.nao.ac.jp/status/obslog/SUP_{}.txt'.format(year)
            r = requests.head(s)
            counter +=1
        
        g = open(sm_dir+'/SuprimeCam.txt', 'r')
        for lines in g:
            if '#' in lines:
                header = lines.split()
        for i in header:
            Data[i] = []
        
        y = open(sm_dir+'/SuprimeCam.txt', 'r')
        y.readline()
        for line in y:
            #print line
            data = line.split()
            for ij, j in enumerate(data):
                for it, i in enumerate(header):
                    if it == ij:
                        Data[i].append(j)

        g.close()
        y.close()
        
        for i in Data.keys():
            Data[i] = np.array(Data[i])
        
        #print Data.keys()
        
        ascii.write([Data[i] for i in header], sm_dir+'/SuprimeCam.ascii',
                    names=header,overwrite=True)
        table = Table.read(sm_dir+'/SuprimeCam.ascii', format='ascii')
        table = np.array(table)
        fits.writeto(sm_dir+'/SuprimeCam.fits',table)
    
    else:
        logging.info("The data is up to date")



############################# SUPRIME-CAM INSTRUMENT #############################

# -------------------- Cas boîte de recherche sphérique --------------------

def SuprimeCam_Search(Object,obj_center,Sphere_Radius,FOV,dt_bias,dt_flat,
                      min_calib,output_directory):
    
    # ---------------------- Limites de la boite Recherche ------------
    RA_min = obj_center[0] - Sphere_Radius
    RA_max = obj_center[0] + Sphere_Radius
    DEC_min = obj_center[1] - Sphere_Radius
    DEC_max = obj_center[1] + Sphere_Radius

    # ----------- Limites de la boite + FOV
    RA_min = RA_min - FOV
    RA_max = RA_max + FOV
    DEC_min = DEC_min - FOV
    DEC_max = DEC_max + FOV
    
    # Input file

    try:
        f = open(sm_dir+'/SuprimeCam.txt')
        f.readline()
        f.readline()
        first_line = f.readline()
        data = first_line.split()
        y = data[1].split('-')
        year = int(y[0])
        f.close()
        b = 'https://smoka.nao.ac.jp/status/obslog/SUP_{}.txt'.format(year)
        update_sup_data(b)
        in_file = fits.open(sm_dir+'/SuprimeCam.fits')

    except Exception as e:
        logging.info("No SUP entry data")
        logging.info("Creating or Updating new SUP data entries")
        b = 'https://smoka.nao.ac.jp/status/obslog/SUP_2014.txt' # On met à une
                                                               # date antérieur
        update_sup_data(b)
        in_file = fits.open(sm_dir+'/SuprimeCam.fits')

    tbdata = in_file[1].data
    in_file.close()
    
    # Scientific images inside the search area
    (bias_frames,bias_dates,bias_filters,
     bias_ra,bias_dec,bias_ut,bias_t_expo,
     bias_data_type,bias_jd) = ([] for i in range(9))
     
    (flats_frames,flats_dates,flats_filters,
      flats_ra,flats_dec,flats_ut,flats_t_expo,
      flats_data_type,flats_jd) = ([] for i in range(9))
      
    (frames,dates,filters,ra,dec,ut,exptime,typ,jd) = ([] for i in range(9))
    t_expo = 0
      
      
    # sys.exit("test 0")
      
    for observation in tqdm(tbdata): #[11510:11550]
        #print observation[7]
          
        # Classify bias
        if (observation[17] == 'BIAS' or observation[17] == 'ZERO'):
            bias_frames.append(observation[0])
            bias_dates.append(observation[1])
            bias_filters.append(observation[4])
            bias_ra.append(observation[5])
            bias_dec.append(observation[6])
            bias_ut.append(observation[14])
            bias_t_expo.append(observation[15])
            bias_data_type.append(observation[17])
            bias_jd.append(Time(observation[1]+'T'+observation[14],
                                format='isot',scale='utc').jd)
            # sys.exit("test 1.1")
        # sys.exit("test 1.2")
        # print observation[7]
          
        # Classify flats
        if (observation[17] == 'FLAT' or observation[17] == 'DOMEFLAT'
            or observation[17] == 'SKYFLAT'):
            flats_frames.append(observation[0])
            flats_dates.append(observation[1])
            flats_filters.append(observation[4])
            flats_ra.append(observation[5])
            flats_dec.append(observation[6])
            flats_ut.append(observation[14])
            flats_t_expo.append(observation[15])
            flats_data_type.append(observation[17])
            flats_jd.append(Time(observation[1]+'T'+observation[14],
                                 format='isot',scale='utc').jd)
            # sys.exit("test 1.3")
        # sys.exit("test 1.4")
          
        # Search scientific images
        if (observation[17] != 'FLAT' and observation[17] != 'DOMEFLAT' and
            observation[17] != 'SKYFLAT' and observation[17] != 'BIAS' and
            observation[17] != 'ZERO' and observation[17] != 'DARK'):
            try:
                source_coord = sk(observation[5],observation[6],unit=
                                  (u.hourangle,u.deg))
            except:
                #print('Error: no RA or DEC')
                continue
            RA2000 = source_coord.ra.deg
            DEC2000 = source_coord.dec.deg
            #print RA2000
            #print RA_min
            #print RA_max
            #sys.exit("test 1.5")
              
            if (RA2000 >= RA_min and RA2000 <= RA_max and
                DEC2000 >= DEC_min and DEC2000 <= DEC_max):
                frames.append(observation[0])
                # sys.exit("test 1.6")
                dates.append(observation[1])
                filters.append(observation[4])
                ra.append(observation[5])
                dec.append(observation[6])
                ut.append(observation[14])
                exptime.append(observation[15])
                typ.append(observation[17])
                jd.append(Time(observation[1]+'T'+observation[14],
                               format='isot',scale='utc').jd)
                print (observation[0],observation[1],observation[4],
                       observation[15],observation[17])
                t_expo += float(observation[15])
                # sys.exit("test 1.7")
        # sys.exit("test 1.8")
      
    # sys.exit("test 2")
    logging.info('%i scientific images with a total exp time of %.1f\n'
                   %(len(frames),t_expo))
                           
    # Save output search of scientific images
    out_frames = frames[:]
    out_dates = dates[:]
    out_filters = filters[:]
    out_ra = ra[:]
    out_dec = dec[:]
    out_ut = ut[:]
    out_exptime = exptime[:]
    out_typ = typ[:]
                           
    # Search calibrations for each date
    old_date = 'first_date'
    old_filter = 'first_filter'
    logging.info("Searching BIAS, FLATS in SUP")
    for date in tqdm(jd):
        # Search BIAS
        if (date != old_date):
            for it in range(2):
                n_bias = 0
                for index, value in (enumerate(bias_jd)):
                    if (abs(value - date) < dt_bias[it]):
                        out_frames.append(bias_frames[index])
                        out_dates.append(bias_dates[index])
                        out_filters.append(bias_filters[index])
                        out_ra.append(bias_ra[index])
                        out_dec.append(bias_dec[index])
                        out_ut.append(bias_ut[index])
                        out_exptime.append(bias_t_expo[index])
                        out_typ.append(bias_data_type[index])
                        n_bias += 1
                    if (n_bias > min_calib):
                        break
                if (n_bias < min_calib):
                    logging.warning('''Only %i bias around the night of the %s.'''
                                    %(n_bias,dates[jd.index(date)]))
                               
        # Search FLATS
        if (date != old_date or filters[dates.index(date)] != old_filter):
            for it in range(2):
                n_flats = 0
                for index, value in enumerate(flats_jd):
                    if (flats_filters[index] == filters[jd.index(date)] and
                        abs(value - date) < dt_flat[0]):
                        out_frames.append(flats_frames[index])
                        out_dates.append(flats_dates[index])
                        out_filters.append(flats_filters[index])
                        out_ra.append(flats_ra[index])
                        out_dec.append(flats_dec[index])
                        out_ut.append(flats_ut[index])
                        out_exptime.append(flats_t_expo[index])
                        out_typ.append(flats_data_type[index])
                        n_flats += 1
                    if (n_flats > min_calib):
                        break
                if (n_bias < min_calib):
                    logging.warning('Only %i flats around the night of the %s.' %
                                    (n_flats,dates[jd.index(date)]))
                               
        old_date = date
                           
    # Remove duplicated files
    out_frames_unique, indices = np.unique(out_frames, return_index=True)
    out_frames = [out_frames[i] for i in indices]
    out_dates = [out_dates[i] for i in indices]
    out_filters = [out_filters[i] for i in indices]
    out_ra = [out_ra[i] for i in indices]
    out_dec = [out_dec[i] for i in indices]
    out_ut = [out_ut[i] for i in indices]
    out_exptime = [out_exptime[i] for i in indices]
    out_typ = [out_typ[i] for i in indices]
                           
    n_bias_tot = out_typ.count('BIAS') + out_typ.count('ZERO')
    n_flats_tot = (out_typ.count('FLAT') + out_typ.count('DOMEFLAT')
                   + out_typ.count('SKYFLAT'))
                           
  
    logging.info('Total number of images BIAS = %i \n' %(n_bias_tot))
    logging.info('Total number of images FLATS = %i \n' %(n_flats_tot))
    logging.info('Total number of images (scientific + BIAS + FLATS) = %i \n' %
                   (len(out_frames)))
                                          
    # Output file
    logging.info("Writing mail for SUP instrument")
    N_max = 1000 #Maximum number of images per mail
    a = 0
    b = N_max
    k = 1
    bool = True
    while bool == True:
        if not os.path.exists(output_directory+'/SuprimeCam/'):
            os.makedirs(output_directory+'/SuprimeCam/')

        out_file = open(output_directory+'/SuprimeCam/'+Object+'_SUP_mail_'
                        +str(k)+'.txt', 'w')
        out_file.write('SMOKAID \t \n \n')
        out_file.write('PURPOSE \t Research(star formation)\n \n')
        ascii.write([out_frames[a:b],out_dates[a:b]], out_file,
                    names=['#FRAME_ID', 'DATE_OBS'])
        out_file.close()
        if len(out_frames) > b:
            a += N_max
            b += N_max
            k += 1
        else:
            bool = False
                                          
    # Output file
    list_file = open(output_directory+'/SuprimeCam/'+Object+
                     '_SUP_list.txt', 'w')
    ascii.write([out_frames,out_dates,out_filters,out_ra,out_dec,out_ut,
                 out_exptime,out_typ],list_file,names=
                ['#FRAME_ID', 'DATE_OBS', 'FILTER', 'RA2000', 'DEC2000',
                 'UT_STR', 'EXPTIME', 'DATA_TYP'])
    list_file.close()

    return

# --------------------- Cas boîte de recherche rectangulaire ----------------

def SuprimeCam_Search_2(Object,obj_center,Ra_box,DEC_box,FOV,dt_bias,dt_flat,
                        min_calib,output_directory):
    
    # ---------------------- Limites de la boite Recherche ------------
    RA_min = obj_center[0] - Ra_box
    RA_max = obj_center[0] + Ra_box
    DEC_min = obj_center[1] - DEC_box
    DEC_max = obj_center[1] + DEC_box
    
    # ----------- Limites de la boite + FOV
    RA_min = RA_min - FOV
    RA_max = RA_max + FOV
    DEC_min = DEC_min - FOV
    DEC_max = DEC_max + FOV
    
    # Input file
    
    try:
        f = open(sm_dir+'/SuprimeCam.txt')
        f.readline()
        f.readline()
        first_line = f.readline()
        data = first_line.split()
        y = data[1].split('-')
        year = int(y[0])
        f.close()
        b = 'https://smoka.nao.ac.jp/status/obslog/SUP_{}.txt'.format(year)
        update_sup_data(b)
        in_file = fits.open(sm_dir+'/SuprimeCam.fits')
    
    except Exception as e:
        logging.info("No SUP entry data")
        logging.info("Creating or Updating new SUP data entries")
        b = 'https://smoka.nao.ac.jp/status/obslog/SUP_2014.txt' # On met à une
                                                               # date antérieur
        update_sup_data(b)
        in_file = fits.open(sm_dir+'/SuprimeCam.fits')
    
    tbdata = in_file[1].data
    in_file.close()
    
    # Scientific images inside the search area
    (bias_frames,bias_dates,bias_filters,
     bias_ra,bias_dec,bias_ut,bias_t_expo,
     bias_data_type,bias_jd) = ([] for i in range(9))
     
    (flats_frames,flats_dates,flats_filters,
     flats_ra,flats_dec,flats_ut,flats_t_expo,
     flats_data_type,flats_jd) = ([] for i in range(9))
      
    (frames,dates,filters,ra,dec,ut,exptime,typ,jd) = ([] for i in range(9))
    t_expo = 0
      
      
    # sys.exit("test 0")
      
    for observation in tqdm(tbdata): #[11510:11550]
        #print observation[7]
          
        # Classify bias
        if (observation[17] == 'BIAS' or observation[17] == 'ZERO'):
            bias_frames.append(observation[0])
            bias_dates.append(observation[1])
            bias_filters.append(observation[4])
            bias_ra.append(observation[5])
            bias_dec.append(observation[6])
            bias_ut.append(observation[14])
            bias_t_expo.append(observation[15])
            bias_data_type.append(observation[17])
            bias_jd.append(Time(observation[1]+'T'+observation[14],
                                format='isot', scale='utc').jd)
            # sys.exit("test 1.1")
        # sys.exit("test 1.2")
        # print observation[7]
          
        # Classify flats
        if (observation[17] == 'FLAT' or observation[17] == 'DOMEFLAT'
            or observation[17] == 'SKYFLAT'):
            flats_frames.append(observation[0])
            flats_dates.append(observation[1])
            flats_filters.append(observation[4])
            flats_ra.append(observation[5])
            flats_dec.append(observation[6])
            flats_ut.append(observation[14])
            flats_t_expo.append(observation[15])
            flats_data_type.append(observation[17])
            flats_jd.append(Time(observation[1]+'T'+observation[14],
                                   format='isot',scale='utc').jd)
            # sys.exit("test 1.3")
        # sys.exit("test 1.4")
          
        # Search scientific images
        if (observation[17] != 'FLAT' and observation[17] != 'DOMEFLAT' and
            observation[17] != 'SKYFLAT' and observation[17] != 'BIAS' and
            observation[17] != 'ZERO' and observation[17] != 'DARK'):
            try:
                source_coord = sk(observation[5],observation[6],
                                  unit=(u.hourangle,u.deg))
            except:
                #print('Error: no RA or DEC')
                continue
            RA2000 = source_coord.ra.deg
            DEC2000 = source_coord.dec.deg
            #print RA2000
            #print RA_min
            #print RA_max
            #sys.exit("test 1.5")
              
            if (RA2000 >= RA_min and RA2000 <= RA_max and
                DEC2000 >= DEC_min and DEC2000 <= DEC_max):
                frames.append(observation[0])
                # sys.exit("test 1.6")
                dates.append(observation[1])
                filters.append(observation[4])
                ra.append(observation[5])
                dec.append(observation[6])
                ut.append(observation[14])
                exptime.append(observation[15])
                typ.append(observation[17])
                jd.append(Time(observation[1]+'T'+observation[14],
                               format='isot',scale='utc').jd)
                print (observation[0],observation[1],observation[4],
                       observation[15],observation[17])
                t_expo += float(observation[15])
                # sys.exit("test 1.7")
            # sys.exit("test 1.8")

    # sys.exit("test 2")
    logging.info('%i scientific images with a total exp time of %.1f\n'
                   %(len(frames),t_expo))
                   
    # Save output search of scientific images
    out_frames = frames[:]
    out_dates = dates[:]
    out_filters = filters[:]
    out_ra = ra[:]
    out_dec = dec[:]
    out_ut = ut[:]
    out_exptime = exptime[:]
    out_typ = typ[:]
                   
    # Search calibrations for each date
    old_date = 'first_date'
    old_filter = 'first_filter'
    logging.info("Searching BIAS, FLATS in SUP")
    for date in tqdm(jd):
        # Search BIAS
        if (date != old_date):
            for it in range(2):
                n_bias = 0
                for index, value in (enumerate(bias_jd)):
                    if (abs(value - date) < dt_bias[it]):
                        out_frames.append(bias_frames[index])
                        out_dates.append(bias_dates[index])
                        out_filters.append(bias_filters[index])
                        out_ra.append(bias_ra[index])
                        out_dec.append(bias_dec[index])
                        out_ut.append(bias_ut[index])
                        out_exptime.append(bias_t_expo[index])
                        out_typ.append(bias_data_type[index])
                        n_bias += 1
                    if (n_bias > min_calib):
                        break
                if (n_bias < min_calib):
                    logging.warning('''Only %i bias around the night of the %s.'''
                                    %(n_bias,dates[jd.index(date)]))
                       
        # Search FLATS
        if (date != old_date or filters[dates.index(date)] != old_filter):
            for it in range(2):
                n_flats = 0
                for index, value in enumerate(flats_jd):
                    if (flats_filters[index] == filters[jd.index(date)] and
                        abs(value - date) < dt_flat[0]):
                        out_frames.append(flats_frames[index])
                        out_dates.append(flats_dates[index])
                        out_filters.append(flats_filters[index])
                        out_ra.append(flats_ra[index])
                        out_dec.append(flats_dec[index])
                        out_ut.append(flats_ut[index])
                        out_exptime.append(flats_t_expo[index])
                        out_typ.append(flats_data_type[index])
                        n_flats += 1
                    if (n_flats > min_calib):
                        break
                if (n_bias < min_calib):
                    logging.warning('Only %i flats around the night of the %s.' %
                                    (n_flats,dates[jd.index(date)]))
                       
        old_date = date
                   
    # Remove duplicated files
    out_frames_unique, indices = np.unique(out_frames, return_index=True)
    out_frames = [out_frames[i] for i in indices]
    out_dates = [out_dates[i] for i in indices]
    out_filters = [out_filters[i] for i in indices]
    out_ra = [out_ra[i] for i in indices]
    out_dec = [out_dec[i] for i in indices]
    out_ut = [out_ut[i] for i in indices]
    out_exptime = [out_exptime[i] for i in indices]
    out_typ = [out_typ[i] for i in indices]
                   
    n_bias_tot = out_typ.count('BIAS') + out_typ.count('ZERO')
    n_flats_tot = (out_typ.count('FLAT') + out_typ.count('DOMEFLAT')
                   + out_typ.count('SKYFLAT'))
                                  
                                  
    logging.info('Total number of images BIAS = %i \n' %(n_bias_tot))
    logging.info('Total number of images FLATS = %i \n' %(n_flats_tot))
    logging.info('Total number of images (scientific + BIAS + FLATS) = %i \n' %
                 (len(out_frames)))
                                               
    # Output file
    logging.info("Writing mail for SUP instrument")
    N_max = 1000 #Maximum number of images per mail
    a = 0
    b = N_max
    k = 1
    bool = True
    while bool == True:
        if not os.path.exists(output_directory+'/SuprimeCam/'):
            os.makedirs(output_directory+'/SuprimeCam/')
                                                   
        out_file = open(output_directory+'/SuprimeCam/'+Object+'_SUP_mail_'
                        +str(k)+'.txt', 'w')
        out_file.write('SMOKAID \t \n \n')
        out_file.write('PURPOSE \t Research(star formation)\n \n')
        ascii.write([out_frames[a:b],out_dates[a:b]], out_file,
                    names=['#FRAME_ID', 'DATE_OBS'])
        out_file.close()
        if len(out_frames) > b:
            a += N_max
            b += N_max
            k += 1
        else:
            bool = False
                                               
    # Output file
    list_file = open(output_directory+'/SuprimeCam/'+Object+'_SUP_list.txt',
                     'w')
    ascii.write([out_frames,out_dates,out_filters,out_ra,out_dec,
                 out_ut,out_exptime,out_typ],list_file,
                names=['#FRAME_ID', 'DATE_OBS', 'FILTER', 'RA2000', 'DEC2000',
                       'UT_STR', 'EXPTIME', 'DATA_TYP'])
    list_file.close()
                                                                             
    return


########################## HYPER SUPRIME-CAM INSTRUMENT ##########################

# --------------------- Cas spherical search box ------------------------

def HyperSuprimeCam_Search(Object,obj_center,Sphere_Radius,FOV,dt_bias,dt_flat,
                           dt_dark,min_calib,output_directory):
    
    # ---------------------- Limites de la boite Recherche ------------
    RA_min = obj_center[0] - Sphere_Radius
    RA_max = obj_center[0] + Sphere_Radius
    DEC_min = obj_center[1] - Sphere_Radius
    DEC_max = obj_center[1] + Sphere_Radius
   
    # ----------- Limites de la boite + FOV
    RA_min = RA_min - FOV
    RA_max = RA_max + FOV
    DEC_min = DEC_min - FOV
    DEC_max = DEC_max + FOV
    
    # Input file
    try:
        f = open(sm_dir+'/HSC.txt')
        f.readline()
        f.readline()
        first_line = f.readline()
        data = first_line.split()
        y = data[1].split('-')
        year = int(y[0])
        f.close()
        b = 'https://smoka.nao.ac.jp/status/obslog/HSC_{}.txt'.format(year)
        update_hsc_data(b)
        in_file = fits.open(sm_dir+'/HSC.fits')

    except Exception as e:
        logging.info("No HSC entry data")
        logging.info("Creating or Updating new HSC data entries")
        b = 'https://smoka.nao.ac.jp/status/obslog/HSC_2014.txt' # On met à une
                                                             # date antérieur
        update_hsc_data(b)
        in_file = fits.open(sm_dir+'/HSC.fits')


    tbdata = in_file[1].data
    in_file.close()
    
    # Scientific images inside the search area
    (bias_frames,bias_dates,bias_filters,
     bias_ra,bias_dec,bias_ut,bias_t_expo,
     bias_data_type,bias_jd) = ([] for i in range(9))
     
    (flats_frames,flats_dates,flats_filters,
     flats_ra,flats_dec,flats_ut,flats_t_expo,
     flats_data_type,flats_jd) = ([] for i in range(9))
      
    (dark_frames,dark_dates,dark_filters,
     dark_ra,dark_dec,dark_ut,dark_t_expo,
     dark_data_type,dark_jd) = ([] for i in range(9))
       
    (frames,dates,filters,ra,dec,ut,exptime,typ,jd) = ([] for i in range(9))
    t_expo = 0
       
    for observation in tqdm(tbdata): #
           
        # Classify bias
        if (observation[16] == 'BIAS' or observation[16] == 'ZERO'):
            bias_frames.append(observation[0])
            bias_dates.append(observation[1])
            bias_filters.append(observation[3])
            bias_ra.append(observation[4])
            bias_dec.append(observation[5])
            bias_ut.append(observation[13])
            bias_t_expo.append(observation[14])
            bias_data_type.append(observation[16])
            bias_jd.append(Time(observation[1]+'T'+observation[13],
                                format='isot',scale='utc').jd)
           
        # Classify flats
        if (observation[16] == 'FLAT' or observation[16] == 'DOMEFLAT'
            or observation[16] == 'SKYFLAT'):
            flats_frames.append(observation[0])
            flats_dates.append(observation[1])
            flats_filters.append(observation[3])
            flats_ra.append(observation[4])
            flats_dec.append(observation[5])
            flats_ut.append(observation[13])
            flats_t_expo.append(observation[14])
            flats_data_type.append(observation[16])
            flats_jd.append(Time(observation[1]+'T'+observation[13],
                                    format='isot', scale='utc').jd)
           
        # Classify darks
        if (observation[16] == 'DARK'):
            dark_frames.append(observation[0])
            dark_dates.append(observation[1])
            dark_filters.append(observation[3])
            dark_ra.append(observation[4])
            dark_dec.append(observation[5])
            dark_ut.append(observation[13])
            dark_t_expo.append(observation[14])
            dark_data_type.append(observation[16])
            dark_jd.append(Time(observation[1]+'T'+observation[13],
                                   format='isot', scale='utc').jd)
           
        # Search scientific images
        if (observation[16] != 'FLAT' and observation[16] != 'DOMEFLAT' and
            observation[16] != 'SKYFLAT' and observation[16] != 'BIAS' and
            observation[16] != 'ZERO' and observation[16] != 'DARK'):
            try:
                source_coord = sk(observation[4],observation[5],
                                  unit=(u.hourangle,u.deg))
            except:
                #print('Error: no RA or DEC')
                continue
            RA2000 = source_coord.ra.deg
            DEC2000 = source_coord.dec.deg
               
            if (RA2000 >= RA_min and RA2000 <= RA_max and
                DEC2000 >= DEC_min and DEC2000 <= DEC_max):
                frames.append(observation[0])
                dates.append(observation[1])
                filters.append(observation[3])
                ra.append(observation[4])
                dec.append(observation[5])
                ut.append(observation[13])
                exptime.append(observation[14])
                typ.append(observation[16])
                jd.append(Time(observation[1]+'T'+observation[13],
                               format='isot',scale='utc').jd)
                print (observation[0],observation[1],observation[3],
                       observation[14],observation[16])
                t_expo += observation[14]
       
   
    logging.info('%i scientific images with a total exp time of %.1f\n' %
                 (len(frames),t_expo))
                            
    # Save output search of scientific images
    out_frames = frames[:]
    out_dates = dates[:]
    out_filters = filters[:]
    out_ra = ra[:]
    out_dec = dec[:]
    out_ut = ut[:]
    out_exptime = exptime[:]
    out_typ = typ[:]
                            
    # Search calibrations for each date
    old_date = 'first_date'
    old_filter = 'first_filter'
    logging.info("Searching BIAS, FLATS & DARKS in HSC")
    for date in tqdm(jd):
        # Search BIAS
        if (date != old_date):
            for it in range(2):
                n_bias = 0
                for index, value in enumerate(bias_jd):
                    if (abs(value - date) < dt_bias[it]):
                        out_frames.append(bias_frames[index])
                        out_dates.append(bias_dates[index])
                        out_filters.append(bias_filters[index])
                        out_ra.append(bias_ra[index])
                        out_dec.append(bias_dec[index])
                        out_ut.append(bias_ut[index])
                        out_exptime.append(bias_t_expo[index])
                        out_typ.append(bias_data_type[index])
                        n_bias += 1
                    if (n_bias > min_calib):
                        break
                if(n_bias < min_calib):
                    logging.warning('Only %i bias around the night of the %s.'%
                                    (n_bias,dates[jd.index(date)]))
                                
        # Search FLATS
        if (date != old_date or filters[dates.index(date)] != old_filter):
            for it in range(2):
                n_flats = 0
                for index, value in enumerate(flats_jd):
                    if (flats_filters[index] == filters[jd.index(date)]
                        and abs(value - date) < dt_flat[0]):
                        out_frames.append(flats_frames[index])
                        out_dates.append(flats_dates[index])
                        out_filters.append(flats_filters[index])
                        out_ra.append(flats_ra[index])
                        out_dec.append(flats_dec[index])
                        out_ut.append(flats_ut[index])
                        out_exptime.append(flats_t_expo[index])
                        out_typ.append(flats_data_type[index])
                        n_flats += 1
                    if (n_flats > min_calib):
                        break
                if (n_bias < min_calib):
                    logging.warning('Only %i flats around the night of the %s.'%
                                    (n_flats,dates[jd.index(date)]))
                                
        # Search DARKS
        if (date != old_date):
            for it in range(2):
                n_dark = 0
                for index, value in enumerate(dark_jd):
                    if (abs(value - date) < dt_dark[it]):
                        out_frames.append(dark_frames[index])
                        out_dates.append(dark_dates[index])
                        out_filters.append(dark_filters[index])
                        out_ra.append(dark_ra[index])
                        out_dec.append(dark_dec[index])
                        out_ut.append(dark_ut[index])
                        out_exptime.append(dark_t_expo[index])
                        out_typ.append(dark_data_type[index])
                        n_dark += 1
                    if (n_dark > min_calib):
                        break
                if (n_dark < min_calib):
                    logging.warning('Only %i darks around the night of the %s.' %
                                    (n_dark,dates[jd.index(date)]))
                                
        old_date = date
                            
    # Remove duplicated files
    out_frames_unique, indices = np.unique(out_frames, return_index=True)
    out_frames = [out_frames[i] for i in indices]
    out_dates = [out_dates[i] for i in indices]
    out_filters = [out_filters[i] for i in indices]
    out_ra = [out_ra[i] for i in indices]
    out_dec = [out_dec[i] for i in indices]
    out_ut = [out_ut[i] for i in indices]
    out_exptime = [out_exptime[i] for i in indices]
    out_typ = [out_typ[i] for i in indices]
                            
    n_bias_tot = out_typ.count('BIAS') + out_typ.count('ZERO')
    n_flats_tot = (out_typ.count('FLAT') + out_typ.count('DOMEFLAT')
                   + out_typ.count('SKYFLAT'))
    n_darks_tot = out_typ.count('DARK')
                            
    logging.info('Total number of images BIAS = %i \n' %(n_bias_tot))
    logging.info('Total number of images FLATS = %i \n' %(n_flats_tot))
    logging.info('Total number of images DARKS = %i \n' %(n_darks_tot))
    logging.info('''Total number of images (scientific + BIAS + FLATS + DARKS)
                 = %i \n''' %(len(out_frames)))
                                  
    # Output file
    logging.info("Writing mail for HSC instrument")
    N_max = 1000 #Maximum number of images per mail
    a = 0
    b = N_max
    k = 1
    bool = True
    while bool == True:
        if not os.path.exists(output_directory+'/HyperSuprimeCam/'):
            os.makedirs(output_directory+'/HyperSuprimeCam/')

        out_file = open(output_directory+'/HyperSuprimeCam/'+Object+
                        '_HSC_mail_'+str(k)+'.txt', 'w')
        out_file.write('SMOKAID \t \n \n')
        out_file.write('PURPOSE \t Research(star formation)\n \n')
        ascii.write([out_frames[a:b],out_dates[a:b]], out_file,
                    names=['#FRAME_ID', 'DATE_OBS'])
        out_file.close()
        if len(out_frames) > b:
            a += N_max
            b += N_max
            k += 1
        else:
            bool = False
                                  
    # Output file
    list_file = open(output_directory+'/HyperSuprimeCam/'+Object+
                     '_HSC_list.txt', 'w')
    ascii.write([out_frames,out_dates,out_filters,out_ra,out_dec,out_ut,
                out_exptime,out_typ],list_file, names=
                ['#FRAME_ID', 'DATE_OBS', 'FILTER', 'RA2000', 'DEC2000',
                 'UT_STR', 'EXPTIME', 'DATA_TYP'])
    list_file.close()

    return

# ---------------------- Cas Rectangular search box ---------------------

def HyperSuprimeCam_Search_2(Object,obj_center,Ra_box,DEC_box,FOV,dt_bias,
                             dt_flat,dt_dark,min_calib,output_directory):
    
    # ---------------------- Limites de la boite Recherche ------------
    RA_min = obj_center[0] - Ra_box
    RA_max = obj_center[0] + Ra_box
    DEC_min = obj_center[1] - DEC_box
    DEC_max = obj_center[1] + DEC_box
    
    # ----------- Limites de la boite + FOV
    RA_min = RA_min - FOV
    RA_max = RA_max + FOV
    DEC_min = DEC_min - FOV
    DEC_max = DEC_max + FOV
    
    # Input file
    try:
        f = open(sm_dir+'/HSC.txt')
        f.readline()
        f.readline()
        first_line = f.readline()
        data = first_line.split()
        y = data[1].split('-')
        year = int(y[0])
        f.close()
        b = 'https://smoka.nao.ac.jp/status/obslog/HSC_{}.txt'.format(year)
        update_hsc_data(b)
        in_file = fits.open(sm_dir+'/HSC.fits')
    
    except Exception as e:
        logging.info("No HSC entry data")
        logging.info("Creating or Updating new HSC data entries")
        b = 'https://smoka.nao.ac.jp/status/obslog/HSC_2014.txt' # On met à une
                                                               # date antérieur
        update_hsc_data(b)
        in_file = fits.open(sm_dir+'/HSC.fits')
    
    
    tbdata = in_file[1].data
    in_file.close()
    
    # Scientific images inside the search area
    (bias_frames,bias_dates,bias_filters,
     bias_ra,bias_dec,bias_ut,bias_t_expo,
     bias_data_type,bias_jd) = ([] for i in range(9))
     
    (flats_frames,flats_dates,flats_filters,
     flats_ra,flats_dec,flats_ut,flats_t_expo,
     flats_data_type,flats_jd) = ([] for i in range(9))
      
    (dark_frames,dark_dates,dark_filters,
     dark_ra,dark_dec,dark_ut,dark_t_expo,
     dark_data_type,dark_jd) = ([] for i in range(9))
       
    (frames,dates,filters,ra,dec,ut,exptime,typ,jd) = ([] for i in range(9))
    t_expo = 0
       
    for observation in tqdm(tbdata): #
           
        # Classify bias
        if (observation[16] == 'BIAS' or observation[16] == 'ZERO'):
            bias_frames.append(observation[0])
            bias_dates.append(observation[1])
            bias_filters.append(observation[3])
            bias_ra.append(observation[4])
            bias_dec.append(observation[5])
            bias_ut.append(observation[13])
            bias_t_expo.append(observation[14])
            bias_data_type.append(observation[16])
            bias_jd.append(Time(observation[1]+'T'+observation[13],
                                format='isot',scale='utc').jd)
           
        # Classify flats
        if (observation[16] == 'FLAT' or observation[16] == 'DOMEFLAT'
            or observation[16] == 'SKYFLAT'):
            flats_frames.append(observation[0])
            flats_dates.append(observation[1])
            flats_filters.append(observation[3])
            flats_ra.append(observation[4])
            flats_dec.append(observation[5])
            flats_ut.append(observation[13])
            flats_t_expo.append(observation[14])
            flats_data_type.append(observation[16])
            flats_jd.append(Time(observation[1]+'T'+observation[13],
                                 format='isot',scale='utc').jd)
           
        # Classify darks
        if (observation[16] == 'DARK'):
            dark_frames.append(observation[0])
            dark_dates.append(observation[1])
            dark_filters.append(observation[3])
            dark_ra.append(observation[4])
            dark_dec.append(observation[5])
            dark_ut.append(observation[13])
            dark_t_expo.append(observation[14])
            dark_data_type.append(observation[16])
            dark_jd.append(Time(observation[1]+'T'+observation[13],
                                format='isot',scale='utc').jd)
           
        # Search scientific images
        if (observation[16] != 'FLAT' and observation[16] != 'DOMEFLAT' and
            observation[16] != 'SKYFLAT' and observation[16] != 'BIAS' and
            observation[16] != 'ZERO' and observation[16] != 'DARK'):
            try:
                source_coord = sk(observation[4],observation[5],
                                  unit=(u.hourangle,u.deg))
            except:
                #print('Error: no RA or DEC')
                continue
            RA2000 = source_coord.ra.deg
            DEC2000 = source_coord.dec.deg
               
            if (RA2000 >= RA_min and RA2000 <= RA_max and
                DEC2000 >= DEC_min and DEC2000 <= DEC_max):
                frames.append(observation[0])
                dates.append(observation[1])
                filters.append(observation[3])
                ra.append(observation[4])
                dec.append(observation[5])
                ut.append(observation[13])
                exptime.append(observation[14])
                typ.append(observation[16])
                jd.append(Time(observation[1]+'T'+observation[13],
                               format='isot',scale='utc').jd)
                print (observation[0],observation[1],observation[3],
                       observation[14],observation[16])
                t_expo += observation[14]
       
       
    logging.info('%i scientific images with a total exp time of %.1f\n' %
                 (len(frames),t_expo))
                    
    # Save output search of scientific images
    out_frames = frames[:]
    out_dates = dates[:]
    out_filters = filters[:]
    out_ra = ra[:]
    out_dec = dec[:]
    out_ut = ut[:]
    out_exptime = exptime[:]
    out_typ = typ[:]
                    
    # Search calibrations for each date
    old_date = 'first_date'
    old_filter = 'first_filter'
    logging.info("Searching BIAS, FLATS & DARKS in HSC")
    for date in tqdm(jd):
        # Search BIAS
        if (date != old_date):
            for it in range(2):
                n_bias = 0
                for index, value in enumerate(bias_jd):
                    if (abs(value - date) < dt_bias[it]):
                        out_frames.append(bias_frames[index])
                        out_dates.append(bias_dates[index])
                        out_filters.append(bias_filters[index])
                        out_ra.append(bias_ra[index])
                        out_dec.append(bias_dec[index])
                        out_ut.append(bias_ut[index])
                        out_exptime.append(bias_t_expo[index])
                        out_typ.append(bias_data_type[index])
                        n_bias += 1
                    if (n_bias > min_calib):
                        break
                if(n_bias < min_calib):
                    logging.warning('Only %i bias around the night of the %s.'%
                                    (n_bias,dates[jd.index(date)]))
                        
        # Search FLATS
        if (date != old_date or filters[dates.index(date)] != old_filter):
            for it in range(2):
                n_flats = 0
                for index, value in enumerate(flats_jd):
                    if (flats_filters[index] == filters[jd.index(date)] and
                        abs(value - date) < dt_flat[0]):
                        out_frames.append(flats_frames[index])
                        out_dates.append(flats_dates[index])
                        out_filters.append(flats_filters[index])
                        out_ra.append(flats_ra[index])
                        out_dec.append(flats_dec[index])
                        out_ut.append(flats_ut[index])
                        out_exptime.append(flats_t_expo[index])
                        out_typ.append(flats_data_type[index])
                        n_flats += 1
                    if (n_flats > min_calib):
                        break
                if (n_bias < min_calib):
                    logging.warning('Only %i flats around the night of the %s.'%
                                    (n_flats,dates[jd.index(date)]))
                        
        # Search DARKS
        if (date != old_date):
            for it in range(2):
                n_dark = 0
                for index, value in enumerate(dark_jd):
                    if (abs(value - date) < dt_dark[it]):
                        out_frames.append(dark_frames[index])
                        out_dates.append(dark_dates[index])
                        out_filters.append(dark_filters[index])
                        out_ra.append(dark_ra[index])
                        out_dec.append(dark_dec[index])
                        out_ut.append(dark_ut[index])
                        out_exptime.append(dark_t_expo[index])
                        out_typ.append(dark_data_type[index])
                        n_dark += 1
                    if (n_dark > min_calib):
                        break
                if (n_dark < min_calib):
                    logging.warning('Only %i darks around the night of the %s.' %
                                    (n_dark,dates[jd.index(date)]))
                        
        old_date = date
                    
    # Remove duplicated files
    out_frames_unique, indices = np.unique(out_frames, return_index=True)
    out_frames = [out_frames[i] for i in indices]
    out_dates = [out_dates[i] for i in indices]
    out_filters = [out_filters[i] for i in indices]
    out_ra = [out_ra[i] for i in indices]
    out_dec = [out_dec[i] for i in indices]
    out_ut = [out_ut[i] for i in indices]
    out_exptime = [out_exptime[i] for i in indices]
    out_typ = [out_typ[i] for i in indices]
                    
    n_bias_tot = out_typ.count('BIAS') + out_typ.count('ZERO')
    n_flats_tot = (out_typ.count('FLAT') + out_typ.count('DOMEFLAT')
                   + out_typ.count('SKYFLAT'))
    n_darks_tot = out_typ.count('DARK')
                                   
    logging.info('Total number of images BIAS = %i \n' %(n_bias_tot))
    logging.info('Total number of images FLATS = %i \n' %(n_flats_tot))
    logging.info('Total number of images DARKS = %i \n' %(n_darks_tot))
    logging.info('''Total number of images (scientific + BIAS + FLATS + DARKS)
                 = %i \n''' %(len(out_frames)))
                                   
    # Output file
    logging.info("Writing mail for HSC instrument")
    N_max = 1000 #Maximum number of images per mail
    a = 0
    b = N_max
    k = 1
    bool = True
    while bool == True:
        if not os.path.exists(output_directory+'/HyperSuprimeCam/'):
            os.makedirs(output_directory+'/HyperSuprimeCam/')
                                       
        out_file = open(output_directory+'/HyperSuprimeCam/'+Object+
                        '_HSC_mail_'+str(k)+'.txt', 'w')
        out_file.write('SMOKAID \t \n \n')
        out_file.write('PURPOSE \t Research(star formation)\n \n')
        ascii.write([out_frames[a:b],out_dates[a:b]], out_file,
                    names=['#FRAME_ID', 'DATE_OBS'])
        out_file.close()
        if len(out_frames) > b:
            a += N_max
            b += N_max
            k += 1
        else:
            bool = False
                                   
    # Output file
    list_file = open(output_directory+'/HyperSuprimeCam/'+Object+
                     '_HSC_list.txt', 'w')
    ascii.write([out_frames,out_dates,out_filters,out_ra,out_dec,
                 out_ut,out_exptime,out_typ],list_file,
                names=['#FRAME_ID', 'DATE_OBS', 'FILTER', 'RA2000',
                       'DEC2000','UT_STR', 'EXPTIME', 'DATA_TYP'])
    list_file.close()
                                                                 
    return

###############################################################################

if __name__ == "__main__":
    
    # ----------------------- Paramètres d'entrées --------------
    
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
        sys.exit("You should choose between r and b; sphere or rectangle box")


    # --------- Coordonnées de l'objet en degrés ------------------
    sd.reset_votable_fields()
    sd.remove_votable_fields('coordinates')
    sd.add_votable_fields('ra(d;A;ICRS)', 'dec(d;D;ICRS)')

    table = sd.query_object(Object, wildcard=False)

    RA_center_deg = table[0][1]
    DEC_center_deg = table[0][2]

    CENTRE = np.array([RA_center_deg, DEC_center_deg])

    output_directory = smoka_parameter_in(Object,Box_search,Sphere_Radius)
    
    logging.info("Searching images in Suprime-Cam")
    if Box_search == "r":
        SuprimeCam_Search(Object,CENTRE,Sphere_Radius,FOV_SUP,dt_bias,dt_flat,
                          min_calib,output_directory)
    elif Box_search == "b":
        SuprimeCam_Search_2(Object,CENTRE,Ra_box,DEC_box,FOV_SUP,dt_bias,
                            dt_flat,min_calib,output_directory)
    logging.info("End SUP")
    logging.info("---------------------------------------")

    logging.info("Searching images in HyperSuprime-Cam")
    if Box_search == "r":
        HyperSuprimeCam_Search(Object,CENTRE,Sphere_Radius,FOV_HSC,dt_bias,
                               dt_flat,dt_dark,min_calib,output_directory)
    elif Box_search == "b":
        HyperSuprimeCam_Search_2(Object,CENTRE,Ra_box,DEC_box,FOV_HSC,dt_bias,
                                 dt_flat,dt_dark,min_calib,output_directory)
    logging.info("End HSC")
    logging.info("---------------------------------------")


    logging.info("Done")


