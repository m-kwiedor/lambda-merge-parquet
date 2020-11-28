current_dir = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

pkg_dir := "python"

build-FastParquetLayer:
		docker run --rm -v $(current_dir):/foo -w /foo lambci/lambda:build-python3.7 bash -c \
	    "pip install -r requirements.txt -t $(pkg_dir) ; rm -rf $(pkg_dir)/fastparquet/test/ ; rm -rf $(pkg_dir)/pandas/tests/ ; find \"$(pkg_dir)\" -name \"*.so\" | xargs strip"
		mv -f $(current_dir)$(pkg_dir) $(ARTIFACTS_DIR)/python

# 	@echo "Creating the Dependency Folder"
# 	mkdir -p "$(ARTIFACTS_DIR)/python"

# 	@echo "Installing the Python Dependencies"
# 	python -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/python"

# 	@echo "Removing fastparquet tests, pandas and numpy"
# 	rm -rf "$(ARTIFACTS_DIR)/python/fastparquet/test/"
# 	rm -rf "$(ARTIFACTS_DIR)/python/pandas/"
# 	rm -rf "$(ARTIFACTS_DIR)/python/numpy/"
# 	rm -rf "$(ARTIFACTS_DIR)/python/numba/"	
# 	rm -rf "$(ARTIFACTS_DIR)/python/llvmlite/"
	
# 	@echo "Download pandas"
# 	wget https://files.pythonhosted.org/packages/bf/4c/cb7da76f3a5e077e545f9cf8575b8f488a4e8ad60490838f89c5cdd5bb57/pandas-1.1.4-cp37-cp37m-manylinux1_x86_64.whl -P $(ARTIFACTS_DIR)

# 	@echo "Decompressing pandas"
# 	unzip -o $(ARTIFACTS_DIR)/pandas-1.1.4-cp37-cp37m-manylinux1_x86_64.whl -d $(ARTIFACTS_DIR)/python
# 	rm -f $(ARTIFACTS_DIR)/pandas-1.1.4-cp37-cp37m-manylinux1_x86_64.whl
# 	@echo "Removing pandas tests"
# 	rm -rf "$(ARTIFACTS_DIR)/python/pandas/tests/"
	
# 	@echo "Download numpy"
# 	wget https://files.pythonhosted.org/packages/5e/f2/9e562074f835b9b1227ca156f787be4554ae6bbe293c064337c4153cc4c8/numpy-1.19.4-cp37-cp37m-manylinux1_x86_64.whl -P $(ARTIFACTS_DIR)

# 	@echo "Decompressing numpy"
# 	unzip -o $(ARTIFACTS_DIR)/numpy-1.19.4-cp37-cp37m-manylinux1_x86_64.whl -d $(ARTIFACTS_DIR)/python
# 	rm -f $(ARTIFACTS_DIR)/numpy-1.19.4-cp37-cp37m-manylinux1_x86_64.whl

# 	@echo "Download llvmlite"
# 	wget https://files.pythonhosted.org/packages/4b/df/88c6d62e39947c5b1f2b1eb2239d1aa3013d6a61a75730f8b8997b6e66c6/llvmlite-0.34.0-cp37-cp37m-manylinux2010_x86_64.whl -P $(ARTIFACTS_DIR)

# 	@echo "Decompressing llvmlite"
# 	unzip -o $(ARTIFACTS_DIR)/llvmlite-0.34.0-cp37-cp37m-manylinux2010_x86_64.whl -d $(ARTIFACTS_DIR)/python
# 	rm -f $(ARTIFACTS_DIR)/llvmlite-0.34.0-cp37-cp37m-manylinux2010_x86_64.whl

# 	@echo "Download numba"
# 	wget https://files.pythonhosted.org/packages/c4/57/53b6f67bcd16ef0455db2ecaaadfa2768130edce4db32538bc5f1cde4918/numba-0.51.2-cp37-cp37m-manylinux2014_x86_64.whl -P $(ARTIFACTS_DIR)

# 	@echo "Decompressing llvmlite"
# 	unzip -o $(ARTIFACTS_DIR)/numba-0.51.2-cp37-cp37m-manylinux2014_x86_64.whl -d $(ARTIFACTS_DIR)/python
# 	rm -f $(ARTIFACTS_DIR)/numba-0.51.2-cp37-cp37m-manylinux2014_x86_64.whl


# 	#@echo "Strip .so files"
# 	#find "$(ARTIFACTS_DIR)/python/" -name "*.so" | xargs strip -x
