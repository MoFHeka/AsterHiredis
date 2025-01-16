# File paths
SRC_DIR := .
BUILD_DIR := ./build_main_cluster
OBJ_DIR := $(BUILD_DIR)/obj

# Compilation flags
CC := g++
LD := g++
INCLUDE_PATH := .
CFLAGS := -gdwarf-2 -O0 #-Wl,--unresolved-symbols=ignore-in-object-files
CFLAGS += $(foreach dir, $(INCLUDE_PATH), -I$(dir))
LDFLAGS = $(CFLAGS)

# Files to be compiled
TARGET := main_cluster
MAIN := main_cluster.cc
# SRCS := $(wildcard $(SRC_DIR)/*.cc)
SRCS := $(MAIN) \
				cluster.cc \
				shards.cc \
				connection.cc \
				reader.cc \
				writer.cc \
				reply.cc \
				alloc.cc \
				sds.cc
OBJS := $(SRCS:%.cc=$(OBJ_DIR)/%.o)

# Don't remove *.o files automatically
.SECONDARY: $(OBJS)
	
build: $(TARGET)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cc 
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) `pkg-config --libs --cflags --static seastar` -c $< -o $@

$(TARGET):$(OBJS)
	@mkdir -p $(BUILD_DIR)
	$(LD) $(LDFLAGS) $(OBJS) `pkg-config --libs --cflags --static seastar` -o $(TARGET)

.PHONY: build clean

clean:
	rm -rf $(BUILD_DIR)
	rm -f $(TARGET)

