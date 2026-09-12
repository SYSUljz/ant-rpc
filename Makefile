# ==============================================================================
# Makefile for ant_rpc (C++20 io_uring Web Server)
# ==============================================================================

CXX ?= g++
CXXFLAGS ?= -std=c++20 -Wall -Wextra -O2 -Iinclude
LDFLAGS ?= -luring

BUILD_DIR ?= build
BIN_TARGET ?= $(BUILD_DIR)/ant_rpc_test

# Find all C++ source and header files for formatting
SRC_FILES := $(shell find include test -type f \( -name "*.hpp" -o -name "*.cpp" -o -name "*.h" -o -name "*.c" \))

.PHONY: all build run format format-check tidy tidy-fix clean help

all: build

build:
	@mkdir -p $(BUILD_DIR) && cd $(BUILD_DIR) && cmake .. && $(MAKE) -j$(nproc)
	@echo "Build successful: $(BIN_TARGET)"

run: build
	./$(BIN_TARGET)

format:
	@echo "Formatting C++ files with clang-format..."
	@clang-format -i --style=file $(SRC_FILES)
	@echo "Formatting complete!"

format-check:
	@echo "Checking C++ code format with clang-format..."
	@clang-format --dry-run --Werror --style=file $(SRC_FILES)
	@echo "All files conform to formatting standards."

tidy:
	@if [ ! -f $(BUILD_DIR)/compile_commands.json ]; then \
		echo "compile_commands.json not found. Configuring build directory..."; \
		mkdir -p $(BUILD_DIR) && cd $(BUILD_DIR) && cmake ..; \
	fi
	@echo "Running clang-tidy checks on project source files..."
	@if command -v run-clang-tidy > /dev/null 2>&1; then \
		run-clang-tidy -p $(BUILD_DIR) -header-filter='.*/include/ant_rpc/.*' 'test/.*\.cpp$$'; \
	else \
		clang-tidy -p $(BUILD_DIR) -header-filter='.*/include/ant_rpc/.*' $(shell find test -type f -name "*.cpp"); \
	fi
	@echo "clang-tidy check completed."

tidy-fix:
	@if [ ! -f $(BUILD_DIR)/compile_commands.json ]; then \
		mkdir -p $(BUILD_DIR) && cd $(BUILD_DIR) && cmake ..; \
	fi
	@echo "Running clang-tidy with automatic fixes..."
	@if command -v run-clang-tidy > /dev/null 2>&1; then \
		run-clang-tidy -p $(BUILD_DIR) -fix -header-filter='.*/include/ant_rpc/.*' 'test/.*\.cpp$$'; \
	else \
		clang-tidy -p $(BUILD_DIR) -fix -header-filter='.*/include/ant_rpc/.*' $(shell find test -type f -name "*.cpp"); \
	fi
	@echo "clang-tidy auto-fix completed."

clean:
	@rm -rf $(BUILD_DIR) /tmp/ant_rpc_test
	@echo "Cleaned build artifacts."

help:
	@echo "Available targets:"
	@echo "  make format       - Format all .hpp and .cpp files using clang-format"
	@echo "  make format-check - Check formatting compliance without modifying files"
	@echo "  make tidy         - Run clang-tidy static analysis on project code"
	@echo "  make tidy-fix     - Run clang-tidy and apply automatic fixes"
	@echo "  make build        - Compile the ant_rpc test binary"
	@echo "  make run          - Compile and run the ant_rpc test binary"
	@echo "  make clean        - Remove build directory"
