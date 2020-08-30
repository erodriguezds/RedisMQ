#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=./
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=rmutil
endif

ifndef SRC_DIR
	SRC_DIR=src
endif

ifndef OUT_DIR
	OUT_DIR=bin
endif


all: module.so

module.so:
	$(MAKE) -C ./$(SRC_DIR)
	cp ./$(SRC_DIR)/module.so ./$(OUT_DIR)

clean: FORCE
	rm -rf *.xo *.so *.o
	rm -rf ./$(SRC_DIR)/*.xo ./$(SRC_DIR)/*.so ./$(SRC_DIR)/*.o
	rm -rf ./$(OUT_DIR)/*.xo ./$(OUT_DIR)/*.so ./$(OUT_DIR)/*.o
	rm -rf ./$(RMUTIL_LIBDIR)/*.so ./$(RMUTIL_LIBDIR)/*.o ./$(RMUTIL_LIBDIR)/*.a

FORCE:
