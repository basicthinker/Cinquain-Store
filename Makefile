# This is for Cinquain-Data FS.
CFLAGS := -std=c++0x

all : cinqfs
	g++ -g `pkg-config fuse --libs` -o cinqfs cinq_data_fs.o

cinqfs : cinq_data_fs.cpp
	g++ -g -Wall `pkg-config fuse --cflags` ${CFLAGS} -o cinq_data_fs.o -c cinq_data_fs.cpp

clean :
	rm *.o cinqfs
