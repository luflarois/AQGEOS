#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Modulos para checar e buscar dados GEOS


 __author__ = "Luiz Flavio Rodrigues"
 __copyright__ = "Copyright 2020 By Luiz Flavio"
 __credits__ = ["SciOrbis Team"]
 __license__ = "GPL"
 __version__ = "1.0.1"
 __maintainer__ = "Luiz Flavio Rodrigues"
 __email__ = "luiz.rodrigues@sciorbis.kinghost.net"
 __status__ = "Em producao"

"""
import os
import time
from datetime import date,timedelta
from datetime import datetime
import sys
from urllib.request import urlopen
import requests
import subprocess
from multiprocessing.pool import ThreadPool
import numpy as np
from pathlib import Path
  
def main(argv):

	ini = datetime.now()
	dt_string = ini.strftime("%d/%m/%Y %H:%M:%S")
	print("Inicio: ", dt_string)	
	
	for dia in argv:
		print("Buscando dados para o dia: ",dia)

	folder="/share/bramsrd/dist/BRAMS/data/GEOS/"+dia
	print("Criando o diretorio "+folder)
	if not os.path.exists(folder): 
		try:
			os.system("mkdir "+folder)
		except:
			print("Erro na criacao de "+folder+", verifique!")
			sys.exit() 

	os.chdir(folder)

	print(buscaExistencia(dia,"00"))
	
	commands,filesIn=criaCommands(dia,"00")
	
	#print(commands)
	#print(filesIn)
	withErrors=[]

	for b in range(0,len(commands),8):
		for n in range(b,b+8):
			if n>0:
				break
			arqLog="out_SM{0:03d}".format(n)+'.log'
			log=(open(arqLog,"w"))
			print("{0:02d}".format(n))
			print(commands[n])
			proc=subprocess.Popen([commands[n], "", ""],stdout=log, stderr=log, shell=True)
			#print("Baixando "+filesIn[n]+", proc #"+str(proc.pid))

		proc.wait()
	
		for n in range(b,b+8):
			if n>0:
				break
			arqLogR="out_SM{0:03d}".format(n)+'.log'
			print("Aguardando terminar "+filesIn[n])
			isOk=checkDownloadOk(arqLogR,filesIn[n])
			if not isOk:
				print("************************************************************")
				print("Erro ao baixar "+filesIn[n]+" - Veja log: "+arqLogR)
				print("************************************************************")
				withErrors.append(n)
				
		end = datetime.now()
		dt_string = end.strftime("%d/%m/%Y %H:%M:%S")
		print("Fim:  ", dt_string," - ",end-ini)
	
	if len(withErrors)==0:
		sys.exit()
	
	print("\n\n\nRefazendo download para arquivos com erro")
	while len(withErrors)!=0:
		withErrors=baixa(commands,filesIn,withErrors,ini)
		
	
def baixa(commands,filesIn,withErrors,ini):
	newErrors=[]
	for n in withErrors:
		print("n={0:02d}".format(n))
		print(commands[n])
		print(filesIn[n])
		arqLog="out_SM{0:03d}".format(n)+'.log'
		log=(open(arqLog,"w"))
		proc=subprocess.Popen([commands[n], "", ""],stdout=log, stderr=log, shell=True)
		print("Baixando "+filesIn[n]+", proc #"+str(proc.pid))
	
	proc.wait()
	
	for n in withErrors:
		arqLogR="out_SM{0:03d}".format(n)+'.log'
		print("Aguardando terminar "+filesIn[n])
		isOk=checkDownloadOk(arqLogR,filesIn[n])
		if not isOk:
			print("************************************************************")
			print("Erro ao baixar "+filesIn[n]+" - Veja log: "+arqLogR)
			print("************************************************************")
			newErrors.append(n)
#			sys.exit()
				
	end = datetime.now()
	dt_string = end.strftime("%d/%m/%Y %H:%M:%S")
	print("Fim:  ", dt_string," - ",end-ini)	
	return newErrors

def buscaExistencia(dia,rodada):
	"""
	Busca e espera que o ultimo horario esteja presente para iniciar a fazer download dos dados.
	O ultimo horario eh 10 dias a partir da data fornecida.

	"""
	#https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg/fcast/tavg1_2d_lnd_Nx/tavg1_2d_lnd_Nx.20200429_00.info
	fPrefix="https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg/fcast/tavg1_2d_lnd_Nx/tavg1_2d_lnd_Nx."
	fSufix=dia[0:4]+dia[4:6]+dia[6:8]+"_"+rodada+".info"

	file=fPrefix+fSufix
	print("Verificando se arquivo esta presente: "+file+", aguarde...")

	fileNotThere=True
	while fileNotThere: 
		fileNotThere=False
		try:
			url=urlopen(file)
		except:
			fileNotThere=True	

	return "OK - Is there!"

def checkDownloadOk(logFile,fileName):
	#lats4d: created netcdf4 file GEOS.20200427_00+20200507_00.nc4
	while True:
		f = open(logFile, 'r')
		linhas=f.readlines()
		for i in range(len(linhas)):
			if "lats4d: created netcdf4 file "+fileName in linhas[i]:
				return True
			if "lats4d: exiting from GrADS..." in linhas[i]:
				return False
		f.close()

def criaCommands(dia,rodada):
	"""
	Preenche commands com o comando para pegar os arquivos do GEOS
	"""
#https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg/fcast/tavg1_2d_lnd_Nx/tavg1_2d_lnd_Nx.20200429_00
	cmds=[]
	filesIn=[]
	fPrefix="lats4d.sh -i https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg/fcast/tavg1_2d_lnd_Nx"
	fPrefix=fPrefix+"/tavg1_2d_lnd_Nx."
	fPrefix=fPrefix+dia[0:4]+dia[4:6]+dia[6:8]+"_"+rodada+" -time "
	fMiddle=" -ftype sdf -o GEOS.SM."
	fsufix =" -format netcdf4 -gzip 2 -vars gwetroot gwetprof prectot -lon -85 -30 -lat -60 20 -v "
	
	data_e_hora = datetime.strptime(dia+' 00:00', '%Y%m%d %H:%M')
	for hour in range(0,240,3):
		dia_now=data_e_hora+timedelta(hours = hour)
		mes=dia_now.strftime("%b")
		mes=mes.lower()
		outName=dia[0:4]+dia[4:6]+dia[6:8]+"_"+rodada+"+{0:04d}{1:02d}{2:02d}_{3:02d}30" \
		         .format(dia_now.year,dia_now.month,dia_now.day,dia_now.hour)
		cmds.append(fPrefix+"{0:02d}:30".format(dia_now.hour)+"z"+"{0:02d}".format(dia_now.day)+ \
		         mes+"{0:4d}".format(dia_now.year)+" {0:02d}:30".format(dia_now.hour)+"z"+ \
		         "{0:02d}".format(dia_now.day)+mes+"{0:4d}".format(dia_now.year)+fMiddle+ \
		         outName+fsufix)
		filesIn.append("GEOS_SM."+outName+".nc4")
	return cmds,filesIn
		
if __name__ == "__main__":
   main(sys.argv[1:])

