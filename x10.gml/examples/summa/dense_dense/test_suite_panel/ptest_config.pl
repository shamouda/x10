#!/usr/bin/perl

#
#  This file is part of the X10 project (http://x10-lang.org).
#
#  This file is licensed to You under the Eclipse Public License (EPL);
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.opensource.org/licenses/eclipse-1.0.php
#
#  (C) Copyright IBM Corporation 2006-2014.
#

#-----------------------------------------------------------------
# This script contains testing configuration for  benchmark test
# All definations overwrite the default value in ptest script.
#-----------------------------------------------------------------

printf "\#Setting SUMMA dense matrix multiplication scalability test on panel size\n";

#----------------------
# Matrix configurations
#----------------------

# Matrix column/row size list for the benchmark test suite
@PanelList =  (16, 32, 64, 128, 160, 192, 224, 256 );
printf "\#Set panel size list                 : ( @PanelList )\n";

#---------------------------
# Execution topology setting
#---------------------------
# List of number of node used in tests
@NodeList      = (8);		
printf "\#Set list of nodes to test on        : ( @NodeList )\n";

$ProcPerNode   = 5;               # Process/core/place running on each node
printf "\#Set number of cores per node        : $ProcPerNode\n";

#--------------------------
# Test execution setting
#--------------------------

# Batch job submition options. The following is an example used for Triloka cluster.
$BatchModeOpt = "--mem=15000 -p Batch2h";
printf "\#Set batch job submission options    : $BatchModeOpt\n";

$gml_path="../../../..";             # x10.gml root path
printf "\#Set x10.gml root path               : $gml_path\n";
$jopts   ="-classpath ../build:$gml_path/lib/managed_gml.jar -libpath ../build:$gml_path/lib";
# Execution parameters for managed backend

$itnum   = 10;                    # Testing iteration parameter
printf "\#Set number of iterations for test   : $itnum\n";

$LogFile = "./TestOut_panel.log";  # Output log file name
printf "\#Set output log file                 : $LogFile\n";

#-------------------------

#
# List of tests, including executable name, resource allocation, and execution launch wraper.
#
# Syntax of batch job launch command:
#   [alloc] -N[num of nodes] -n[num of places] [batch opts] [wrapper] [name] [test execution parameters]
#
# [name]    : test executable file name
# [alloc]   : resource allocation command. Such as salloc, psub, or lsub
# [wrapper] : x10 native and managed backend launch wrapper
#
# This format is used to submit jobs to slurm system.
# You may change it to meet your own batch system, such as OpenPBS, LSF or Loadleveler
# Or use the dry run output to creat your batch system's job submission script.
#
@TestExecList = (
	{name => "../DistDenseBench_mpi",  alloc => "salloc", 	wrapper => "$gml_path/scripts/srun.mvapich"},
	{name => "../DistDenseBench_sock", alloc => "salloc", 	wrapper => "$gml_path/scripts/srun.x10sock"},
	{name => "DistDenseBench",         alloc => "salloc",  wrapper => "$gml_path/scripts/srun.x10java x10 -mx 2048M $jopts"}
);


$RunOpts = "4000 2000 2000 $itnum";
printf "\#Set execution parameter             : $RunOpts\n";

