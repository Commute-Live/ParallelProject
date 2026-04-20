TARGET = transit-analysis

SRC_DIR = src
INC_DIR = include

MPI_SRC = $(SRC_DIR)/transit_analysis_mpi.c
CUDA_SRC = $(SRC_DIR)/transit_analysis_cuda.cu
MPI_OBJ = transit_analysis_mpi.o
CUDA_OBJ = transit_analysis_cuda.o

CFLAGS ?= -O3 -std=c11 -Wall -Wextra -pedantic -I$(INC_DIR)
NVCCFLAGS ?= -O3 -std=c++14 -I$(INC_DIR) -gencode arch=compute_70,code=sm_70
LDFLAGS ?=
LDLIBS ?= -L$(CUDA_HOME)/lib64 -lcudart -lm

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(MPI_OBJ) $(CUDA_OBJ)
	mpicc $(CFLAGS) $(LDFLAGS) $(MPI_OBJ) $(CUDA_OBJ) $(LDLIBS) -o $@

$(MPI_OBJ): $(MPI_SRC) $(INC_DIR)/transit_analysis.h $(INC_DIR)/clockcycle.h
	mpicc $(CFLAGS) -c $(MPI_SRC) -o $@

$(CUDA_OBJ): $(CUDA_SRC) $(INC_DIR)/transit_analysis.h
	nvcc $(NVCCFLAGS) -c $(CUDA_SRC) -o $@

clean:
	rm -f $(TARGET) $(MPI_OBJ) $(CUDA_OBJ)
