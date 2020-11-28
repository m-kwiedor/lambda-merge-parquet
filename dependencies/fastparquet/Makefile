current_dir = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

pkg_dir := "python"

build-FastParquetLayer:
		docker run --rm -v $(current_dir):/foo -w /foo lambci/lambda:build-python3.7 bash -c \
	    "pip install -r requirements.txt -t $(pkg_dir) ; rm -rf $(pkg_dir)/fastparquet/test/ ; rm -rf $(pkg_dir)/pandas/tests/ ; find \"$(pkg_dir)\" -name \"*.so\" | xargs strip"
		mv -f $(current_dir)$(pkg_dir) $(ARTIFACTS_DIR)/python
