PROG =	makeIC

SRCS =	dump.f90 geraCI.f90 ModDateUtils.f90 modMemory.f90 \
        modUtils.f90 modAnalysis.f90

OBJS =	dump.o geraCI.o ModDateUtils.o modMemory.o \
        modUtils.o modAnalysis.o

##############################################################
#As variaveis abaixos devem ser alteradas segundo 
#sua area
##############################################################
#Compilador C paralelo que voce usa:
CCOMPILER = /home/luiz.flavio/apps/bin/mpicc
#Compilador Fortran paralelo que voce usa:
FCOMPILER = /home/luiz.flavio/apps/bin/mpif90
#Diretorio com a biblioteca/includes NetCDF Fortran que voce usa:
NETCDFDIR = /home/luiz.flavio/apps/
#Diretorio com a biblioteca/includes grib2 Fortran que voce usa:
WGRIB2DIR = /home/luiz.flavio/apps/
#Diretorio com a biblioteca/includes HDF5 Fortran que voce usa:
HDF5DIR = /home/luiz.flavio/apps/
#Diretorio onde a saida sera colocada no "make install"
BINDIR = /home/luiz.flavio/apps/
#Diretorio onde a estao os fontes
MAINDIR =/home/luiz.flavio/src
#############################################################
#############################################################

LIBS =	-L$(WGRIB2DIR)/lib -lwgrib2 -L$(NETCDFDIR)/lib -lnetcdf \
        -lnetcdff -L$(HDF5DIR)/lib -lhdf5 -lhdf5_hl 


INC = -I$(WGRIB2DIR)/lib -I$(NETCDFDIR)/include -U$(HDF5DIR)/include

CC = $(CCOMPILER)
CFLAGS = -O
FC = $(FCOMPILER)
FFLAGS = -O3 -I../include/
F90 = $(FCOMPILER)
F90FLAGS = -O3 $(INC) -fbacktrace -Warray-bounds
LDFLAGS =  

all: $(PROG)

$(PROG): $(OBJS)
	$(F90) $(LDFLAGS) -o $@ $(OBJS) $(LIBS) $(INC)

clean:
	rm -f $(PROG) $(OBJS) *.mod

.SUFFIXES: $(SUFFIXES) .f90

.f90.o:
	$(F90) $(F90FLAGS) -c $<

install:
	install -D $(PROG) $(BINDIR)/$(notdir $(PROG))
	\cp -f $(MAINDIR)/namelist $(BINDIR)/namelist

dump.o: 
geraCI.o: dump.o modAnalysis.o modMemory.o modUtils.o
ModDateUtils.o: dump.o
modAnalysis.o: ModDateUtils.o modMemory.o modUtils.o





