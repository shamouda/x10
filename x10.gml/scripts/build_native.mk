###################################################
#This make file is used for building executable 
#running on C++ backend sockets/MPI/PAMI transport.
###################################################

##---------------
#Input settings
##---------------
#$(gml_path)    ## gml installation path
#$(gml_inc)     ## gml include path
#$(gml_lib)     ## gml library path
#$(build_path)  ## application target build path
#$(target)      ## application target name
#$(target_list) ## list of targets
#$(X10_FLAG)    ## X10 compiling option flags
#$(X10CXX)      ## X10 compiler
#$(POST_PATH)   ## Post compiling include path
#$(POST_LIBS)   ## Post compiling include libs.
#$(GML_ELEM_TYPE) ## float or double

###################################################
x10src		= $(target).x10

###################################################
# Source files and paths
###################################################


##----------------------------------
## This directory is required for building native backend
GML_NATIVE_JAR  = $(base_dir_elem)/lib/native_gml.jar
GML_NAT_OPT	= -classpath $(GML_NATIVE_JAR) -x10lib $(base_dir_elem)/native_gml.properties

###################################################
# X10 file built rules
################################################### 

# enable CPU profiling with google-perftools
PROFILE ?=
ifdef PROFILE
  X10_FLAG += -gpt
endif

X10RTIMPL ?= sockets

#vj: used to depend on gml_inc
$(target)_native_$(GML_ELEM_TYPE)	: $(x10src) $(depend_src) 
	        @echo "X10_HOME is |$(X10_HOME)|"
		$(X10CXX) -g -x10rt ${X10RTIMPL} $(GML_NAT_OPT) $(X10_FLAG) $< -o $@ \
		-post ' \# $(POST_PATH) \# $(POST_LIBS)'

###short-keys
native		: $(target)_native_$(GML_ELEM_TYPE)

###
all_native	:
			$(foreach src, $(target_list), $(MAKE) target=$(src) native; )

##--------
## clean
clean	::
		rm -f $(target)_native*

clean_all ::
		$(foreach f, $(target_list), rm -rf $(f)_native* $(f)_pami*; )

###----------
help	::
	@echo "------------------- build for native sockets, MPI or PAMI transport ------------";
	@echo " make native       : build default target $(target) for native backend";
	@echo " make all_native   : build all targets [ $(target_list) ] for native backend";
	@echo " make clean      : remove default built binary $(target)_native";
	@echo " make clean_all  : remove all builds for the list of targets";
	@echo "";
