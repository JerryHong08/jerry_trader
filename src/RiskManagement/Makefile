# Makefile for the RiskManagement C++ shared library
#
# Usage:
#   make          — build librisk_engine.so
#   make clean    — remove build artifacts
#   make install  — copy .so next to this Makefile (default target also does this)
#
# The compiled library is loaded at runtime by risk_engine_bridge.py via ctypes.

CXX      ?= g++
CXXFLAGS := -std=c++17 -O2 -fPIC -Wall -Wextra
SRCS     := risk_engine.cpp risk_engine_c_api.cpp
OBJS     := $(SRCS:.cpp=.o)
TARGET   := librisk_engine.so

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -shared -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

risk_engine.o:       risk_engine.cpp risk_engine.h
risk_engine_c_api.o: risk_engine_c_api.cpp risk_engine_c_api.h risk_engine.h

clean:
	rm -f $(OBJS) $(TARGET)
