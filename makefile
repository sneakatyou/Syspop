override CONDA=$(CONDA_BASE)/bin/conda
override MAMBA=$(CONDA_BASE)/bin/mamba
override PKG=syspop

clear_env:
	rm -rf $(CONDA_BASE)/envs/$(PKG)
	$(MAMBA) index $(CONDA_BASE)/conda-bld

clear_all:
	rm -rf $(CONDA_BASE)/envs/$(PKG)
	rm -rf $(CONDA_BASE)/pkgs/$(PKG)*
	rm -rf $(CONDA_BASE)/conda-bld/linux-64/$(PKG)*
	rm -rf $(CONDA_BASE)/conda-bld/$(PKG)*
	rm -rf $(CONDA_BASE)/conda-bld/linux-64/.cache/paths/$(PKG)*
	rm -rf $(CONDA_BASE)/conda-bld/linux-64/.cache/recipe/$(PKG)*
	$(MAMBA) index $(CONDA_BASE)/conda-bld


env: clear_all
	$(MAMBA) env create -f env.yml

upload_data:
	git lfs track "etc/data/raw_nz/**"
	git add "etc/data/raw_nz"
	git commit -m "add large data"
	git push