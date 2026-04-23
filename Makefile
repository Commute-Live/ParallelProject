TARGET = transit-analysis

SRC_DIR = src
INC_DIR = include
SCRIPT_DIR = scripts

MPI_SRC = $(SRC_DIR)/transit_analysis_mpi.c
CUDA_SRC = $(SRC_DIR)/transit_analysis_cuda.cu
MPI_OBJ = transit_analysis_mpi.o
CUDA_OBJ = transit_analysis_cuda.o

CFLAGS ?= -O3 -std=c11 -Wall -Wextra -pedantic -I$(INC_DIR)
NVCCFLAGS ?= -O3 -std=c++14 -I$(INC_DIR) -gencode arch=compute_70,code=sm_70
LDFLAGS ?=
LDLIBS ?= -L$(CUDA_HOME)/lib64 -lcudart -lm

.PHONY: all clean summary strong weak submit

all: $(TARGET)

$(TARGET): $(MPI_OBJ) $(CUDA_OBJ)
	mpicc $(CFLAGS) $(LDFLAGS) $(MPI_OBJ) $(CUDA_OBJ) $(LDLIBS) -o $@

$(MPI_OBJ): $(MPI_SRC) $(INC_DIR)/transit_analysis.h $(INC_DIR)/clockcycle.h
	mpicc $(CFLAGS) -c $(MPI_SRC) -o $@

$(CUDA_OBJ): $(CUDA_SRC) $(INC_DIR)/transit_analysis.h
	nvcc $(NVCCFLAGS) -c $(CUDA_SRC) -o $@

clean:
	rm -f $(TARGET) $(MPI_OBJ) $(CUDA_OBJ)

summary:
	cd $(SCRIPT_DIR) && sbatch sbatch_summary.sh

strong:
	cd $(SCRIPT_DIR) && sbatch sbatch_strong.sh

weak:
	cd $(SCRIPT_DIR) && sbatch sbatch_weak.sh

submit:
	cd $(SCRIPT_DIR) && sbatch sbatch_summary.sh
	cd $(SCRIPT_DIR) && sbatch sbatch_strong.sh
	cd $(SCRIPT_DIR) && sbatch sbatch_weak.sh
