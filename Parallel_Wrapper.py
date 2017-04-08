#
# Updated: 6 April 2017
# Author:  Joshua S Brown
#
# Purpose:
#          This script is designed to run an executable in parallel in the
#          context of a linux environment, by making use of the subprocess
#          and multiprocessing commands in python 2.7
#
# Dependencies:
#          This script requires that the commonly used linux executable 'top' is
#          available on the local enviornment.
#          It also makes use of 'grep' and 'awk'
#
import os,             \
       re,             \
	   multiprocessing,\
	   time,           \
	   subprocess,     \
	   math,           \
	   array,          \
	   sys,            \
	   socket,         \
	   argparse

from multiprocessing import Pool, Value, Lock

###############################################################################
# Functions
###############################################################################

# This function works by examing the output from top. Top is a standard linux
# shell command often used to monitor memroy and tasks being performed by the
# system. CheckMemory will look to see what other instances of the executable
# are being run in parallel (ExeVariable). It will then monitor the memory to
# ensure that the memory of the combined runs are not exceeding the constraints
def CheckMemory(AvgMeanMemoryUsed,
                NumPar           ,
                MemPerProc       ,
                high             ,
                count            ,
                ExeVariable      ,
                OrigDirs         ):

	f=open(OrigDirs+"/"+'Python.log','a')
	f.write("In CheckMemory\n")
	f.close()
	#This is for the Memory that is allocated not necessarily the memory used
	#It is an upper threshold
	bashCommand="top -n 1 -b | grep "+str(ExeVariable[0])+\
	            " | grep $(whoami) | awk '{print $5\"\t\"$6}'"

	MemoryFind = subprocess.check_output(bashCommand, shell=True)
	MemoryFind = re.split(';|, |\*|\n|\t',MemoryFind)
	MemoryFind = filter(None, MemoryFind)
	#num is equivalent to the number of processes running
	loop = int(len(MemoryFind))
	MemoryTot=0
	Memory     = []
	MemoryUsed = []

	for t in range(0,(loop)/2-1): #-1 here is taking account array indexing
		Memory.append(MemoryFind[t*2])
		MemoryUsed.append(MemoryFind[t*2+1])

	num=int(len(Memory))-1

	MemoryTotUsed=0

	#This is for the that is currently used
	bashCommand = "top -n 1 -b | grep "+str(ExeVariable[0])+" | grep $(whoami) | awk '{print $6}'"
	MemoryUsed  = subprocess.check_output(bashCommand, shell=True)
	MemoryUsed  = re.split(';|, |\*|\n',MemoryUsed)
	MemoryUsed  = filter(None, MemoryUsed)
	CheckVar    = int(len(MemoryUsed))

	fi=open(OrigDirs+"/"+'Memory.log','a')
	if float(num)!=-1:
		fi.write(str(num))
		fi.write('\n')
		fi.write(str(Memory[num]))
		fi.write('\n')
		fi.write(str(CheckVar))
		fi.write('\n')
		fi.write(str(MemoryUsed[num]))
		fi.write('\n')
	else:
		fi.write('Currently no running processors\n')
 	fi.close()

	if float(num)==-1:
		fi=open(OrigDirs+"/"+'Memory.log','a')
		fi.write("There are no running instances of "+ExeVariable[0]+"\n")
		fi.write("Either the simulations have finished or   \n")
		fi.write("they have crashed\n")
		fi.close()
		MemoryLarge=0
	else:
		#This for loop is for the memory that was allocated
		t=0
		while t <=num:
        		Memory2 = str(Memory[t])
        		MemTy   = Memory2[-1:]

			if MemTy == "m":
               			Memory2   = (float(Memory2[:-1]))*1000
               			MemoryTot = MemoryTot+Memory2
       			elif MemTy == "g":
				Memory2=(float(Memory2[:-1]))*1000000
               			MemoryTot = MemoryTot+Memory2
       			else:
               			Memory2   = (float(Memory2))
               			MemoryTot = MemoryTot+Memory2


			if t==0:
               			MemorySmall = Memory2
       			elif Memory2<MemorySmall:
               			MemorySmall = Memory2

       			if t==0:
               			MemoryLarge = Memory2
       			elif Memory2>MemoryLarge:
               			MemoryLarge = Memory2
			t=t+1


		#This for loop for t in range(0,num):p is for the memory that was actually used
		t=0
		while t <=num:

        		MemoryUsed2 = str(MemoryUsed[t])
        		MemTyUsed   = MemoryUsed2[-1:]

			if MemTyUsed == "m":
               			MemoryUsed2   = (float(MemoryUsed2[:-1]))*1000
               			MemoryTotUsed = MemoryTotUsed+MemoryUsed2
       			elif MemTyUsed == "g":
				MemoryUsed2=(float(MemoryUsed2[:-1]))*1000000
               			MemoryTotUsed = MemoryTotUsed+MemoryUsed2
       			else:
               			MemoryUsed2   = (float(MemoryUsed2))
               			MemoryTotUsed = MemoryTotUsed+MemoryUsed2

			if float(MemoryUsed2)>float(high):
				high = float(MemoryUsed2)

       			t=t+1
	if float(num)!=-1:
		MeanMemory = float(MemoryTotUsed)/(1000*float(num+1))
		LargeDiff  = float(MemoryLarge-MemorySmall)/1000;

		#in units of B
		AvgMeanMemoryUsed = (MeanMemory+float(AvgMeanMemoryUsed))/2

		#Comparison is in mB
		if AvgMeanMemoryUsed>MemoryLarge/1000:
			Check = AvgMeanMemoryUsed
		else:
			Check = MemoryLarge/1000

		#print MemoryTotUsed/1000+Check

		#Comparison is in mB
		fi=open(OrigDirs+"/"+'Memory.log','a')
		if (MemoryTotUsed/1000+Check)<(float(NumPar)*MemPerProc*1000):
			############################################################################
			#Writing to Memory log file
			fi.write("Total Available Memory allowed to use in gb "+str(float(NumPar)*MemPerProc)+"\n")
			fi.write("Total Memory used in gb "+str(float(MemoryTotUsed)/1000000)+"\n")
			fi.write("Total Memory used per process in m " +str(MeanMemory)+"\n")
			fi.write("Average Mean Memory used over time m " + str(AvgMeanMemoryUsed)+"\n")
			fi.write("Largest amount of Memory allocated in m " + str(float(MemoryLarge)/1000)+"\n")
			fi.write("Smallest amount of Memory allocated in m " + str(float(MemorySmall)/1000)+"\n")
			fi.write("Largest difference in Memory allocated m " +str(LargeDiff)+"\n")
			fi.write("Total processors currently running: "+str(num+1)+"\n")
        		fi.write("It's a go\n")
			fi.write("Launching processor: "+str(num+2)+"\n\n")
		else:
			if count<5:
				fi.write("Total Available Memory allowed to use in gb "+str(float(NumPar)*MemPerProc)+"\n")
				fi.write("Total Memory used in gb "+str(float(MemoryTotUsed)/1000000)+"\n")
				fi.write("Total Memory used per process in m " +str(MeanMemory)+"\n")
				fi.write("Average Mean Memory used over time m " + str(AvgMeanMemoryUsed)+"\n")
				fi.write("Largest amount of Memory allocated in m " + str(float(MemoryLarge)/1000)+"\n")
				fi.write("Smallest amount of Memory allocated in m " + str(float(MemorySmall)/1000)+"\n")
				fi.write("Largest difference in Memory allocated m " +str(LargeDiff)+"\n")
				fi.write("Total processors currently running: "+str(num+1)+"\n")
				fi.write("Memory usage high sleeping for 1 minute\n\n")
				count=count+1
			#sleep by default for 10 minutes
			time.sleep(60)

		fi.close()
	else:
		MeanMemory=0
		fi=open(OrigDirs+"/"+'Memory.log','a')
		if float(high)/1000>float(NumPar)*MemPerProc*1000:
			fi=open('Memory.log','a')
			fi.write("\nThe job probably crashed: used resources\n")
			fi.write("are above threshold\n")
			fi.write("Value in m: " +str(high/1000000)+"\n")
			fi.write("Threshold in m: "+str(float(NumPar)*MemPerProc)+"\n\n")
		else:
			fi.write("\nThe job probably finished early used resources\n")
			fi.write("are below threshold\n")
			fi.write("Value in gb: " +str(high/1000000)+"\n")
			fi.write("Threshold in gb: "+str(float(NumPar)*MemPerProc)+"\n\n")
		fi.close()


	return AvgMeanMemoryUsed, MemoryTotUsed, MemoryLarge, num, high, count

# This function continually checks the memory if it discovers that the memory
# limits have not been exceeded and that there is room to submit another job
# it exits the loop
def CheckMemoryLoop(AvgMeanMemoryUsed,NumPar,MemPerProc, high,ExeVariable,OrigDirs):
	f=open(OrigDirs+"/"+'Python.log','a')
	f.write("In CheckMemoryLoop\n")
	f.close()
	count=0
	output=CheckMemory(AvgMeanMemoryUsed,
                       NumPar           ,
                       MemPerProc       ,
                       high             ,
                       count            ,
                       ExeVariable      ,
                       OrigDirs         )

	f=open(OrigDirs+"/"+'Python.log','a')
	f.write("Returned from CheckMemory\n")
	f.close()
	AvgMeanMemoryUsed = float(output[0])
	MemoryTotUsed     = float(output[1])
	MemoryLarge       = float(output[2])
	num               = float(output[3])
	high              = float(output[4])
	count             = int(output[5])

	#Comparison is in mB
	if AvgMeanMemoryUsed>MemoryLarge/1000:
		Check=AvgMeanMemoryUsed
	else:
		Check=MemoryLarge/1000

	while ((MemoryTotUsed/1000+Check)>(float(NumPar)*MemPerProc*1000) or num==-1) and MemoryLarge!=0:
		output=CheckMemory(AvgMeanMemoryUsed, NumPar, MemPerProc, high, count)
		AvgMeanMemoryUsed=float(output[0])
		MemoryTotUsed=float(output[1])
		MemoryLarge=float(output[2])
		num=float(output[3])
		high=float(output[4])
		count=int(output[5])

		fi=open(OrigDirs+"/"+'Memory.log','a')
		fi.write("\nIn while loop count "+str(count)+"\n")
		fi.close()

		if AvgMeanMemoryUsed>MemoryLarge/1000:
			Check=AvgMeanMemoryUsed
		else:
			Check=MemoryLarge/1000

	return AvgMeanMemoryUsed, num, high ,MemoryTotUsed, Check

# Simulate will call the executable to be run in parallel after checking to see
# if there is adequate room in the memory to call another instance of the
# executable.
def Simulate( ArgVariable,ExeVariable,DirVariable, k,OrigDirs):

    try:
        if  DirVariable[k]:
            if not os.path.exists(DirVariable[k]):
                os.makedirs(DirVariable[k])
                os.chdir(DirVariable[k])

        l.acquire()
        time.sleep(10)
        f=open(OrigDirs+"/"+'Python.log','a')
        f.write("OrigDirs "+OrigDirs+"\n")
        f.write("k value "+str(k)+"\n")
        f.write("ExeVariable "+str(ExeVariable[0])+"\n")
        f.write("ArgVariable "+str(ArgVariable[k])+"\n")
        f.write("DirVariable "+str(DirVariable[k])+"\n")
        f.close()
        add = CheckMemoryLoop(AvgMeanMemoryUsed.value,
                              TotalParallelProc.value,
                              MemPerProc.value       ,
                              high.value             ,
                              ExeVariable            ,
                              OrigDirs               )
        f=open(OrigDirs+"/"+'Python.log','a')
        f.write("Returned from CheckMemoryLoop\n")
        f.close()
        AvgMeanMemoryUsed.value=(AvgMeanMemoryUsed.value+float(add[0]))/2
        num=float(add[1])
        high.value=float(add[2])
        MemoryTotUsed=float(add[3])
        Check=float(add[4])
        l.release()
                #num=0 means the process crashed or finished really quickly
        if (MemoryTotUsed/1000+Check)<float(TotalParallelProc.value)*float(MemPerProc.value)*1000:
            f=open(OrigDirs+"/"+'Python.log','a')
            f.write("k value Actually launched "+str(k)+"\n")
            f.write("ArgVariable "+str(ArgVariable[k])+"\n")
            f.write("ExeVariable "+str(ExeVariable[0])+"\n")
            if ArgVariable[k]:
                a, b = ArgVariable[k].split(" ")
            else:
                a = ""
                b = ""

            f.write("ArgVariable "+a+" "+b+"\n")
            f.close()
            if OrigDirs:
                out=subprocess.Popen([OrigDirs+"/"+ExeVariable[0],str(a),str(b)],stdout=subprocess.PIPE).communicate()[0]
            else:
                out=subprocess.Popen(["./"+ExeVariable[0],str(a),str(b)],stdout=subprocess.PIPE).communicate()[0]

    except (KeyboardInterrupt, SystemExit):
        print "Exiting..."

    if  DirVariable[k]:
        os.chdir(OrigDirs)

	return out

###############################################################################
# Implementation
###############################################################################

###############################################################################
# Step 1 Parse Arguments
# Determine how many processors are available, and how much memory is
# available for each processor
parser = argparse.ArgumentParser(description=
'%(prog)s will run an executable in parallel. The amount of memory allocated '+\
'for each processor can be specefied as well')
parser.add_argument('--num_proc',nargs=1,type=int,default=1,help='The number '+\
' of processors that will be run in parallel')
parser.add_argument('--memory',nargs=1,type=int,default=2000,help='The amount'+\
' of memory that will be allocated for each processor in units of MB')
parser.add_argument('--num_runs',nargs=1,type=int,default=1,help='The (amount'+\
' of work)/(number of runs) that needs to be done. If num_runs is 4 and '     +\
'num_proc is 4 then all runs will occur at the same time. However, if '       +\
'num_runs is 10 and num_procs is still 4 then at most 4 runs will occur at a '+\
'time until all the runs are finished')
parser.add_argument('--run_file',nargs=1,required=True,help='The name of the '+\
'executable that is to be run in parallel.')
parser.add_argument('--run_args',nargs=1,default="",help='The arguments that '+\
'will be passed to the executable. Note that if your file outputs to a file ' +\
'they will all write to the same files. To identify each run to a sperate '   +\
'file the PAR keyword can be used anywhere in the command line options. Where'+\
' it is used it will be replaced with a number unique to the run id.')
parser.add_argument('--run_dir',nargs=1,default="",help='The name of the '    +\
'folder where the executable will be run. Similar to the executable arguments'+\
' if you want to specify a unique directory for each run you can use the PAR' +\
' keywork which will be replaced with a number unique to the run')
args = parser.parse_args()

# Check that executable exists
print args.run_file[0]
if os.path.isfile(args.run_file[0]) is False:
    print "ERROR file "+args.run_file[0]+" does not exist"
    sys.exit()

###############################################################################
# Step 2 Declare variables
# The Value declaration is used to ensure the variables can be seen by all
# processors
# Used to manage the memory consumption
AvgMeanMemoryUsed = Value('d',0.0)
TotalParallelProc = Value('d',0.0)
TotalMemory       = Value('d',0.0)
#Value used to see if the memory is at the threshold
high   = Value('d',0.0)
# Number of available processors
if isinstance( args.num_proc, int):
    NumPar = Value('i',args.num_proc)
    NP = args.num_proc
else:
    NumPar = Value('i',int(args.num_proc[0]))
    NP = args.num_proc[0]
# Memory per processor this is based on the system architecture of the HPC given
# in giga bytes
if isinstance( args.memory, int):
    MemPerProc = Value('d',float(args.memory)/1000.00)
else:
    MemPerProc = Value('d',float(args.memory[0])/1000.00)

# The program to be run
ProgramRun = args.run_file[0]
# Number of runs
if isinstance( args.num_runs, int):
    NumRun = args.num_runs
else:
    NumRun = int(args.num_runs[0])


# Update number of runs so it is at least equal to the number of processors
print NumRun
print NP
if NumRun<NP:
    NumRun = NP

manager     = multiprocessing.Manager()
ArgVariable = manager.list()
ExeVariable = manager.list()
DirVariable = manager.list()

# Arguments that are passed to ProgramRun
if isinstance( args.run_args, str):
    Args = args.run_args
else:
    Args = args.run_args[0]

# Check to see if a run directory has been specified
if isinstance( args.run_dir, str):
    Dirs = args.run_dir
else:
    Dirs = args.run_dir[0]

#ArgVariable.append(str(args.run_args))
ExeVariable.append(str(ProgramRun))
###############################################################################
# Step 3 Writing to Python log file
f = open('Python.log','w')
f.write("Program "+str(ProgramRun)[1:-1]+"\n")
f.write("Num Pararallel Runs "+str(NumPar)+"\n")
f.write("Run Number "+str(NumRun)+"\n")
f.write(str(ProgramRun)+" arguments "+str(Args)+"\n")
f.write("Executable directory "+str(Dirs)+"\n")

#Total Memory currently being used by ProgramRun
MemoryTot = 0

# Remove Memory.log file if it exists in current directory
try:
	os.remove('Memory.log')
except OSError:
	pass

l = Lock()

###############################################################################
# Step 4 Beginning Setup
# Check to see if there are already jobs with the same executable running on the
# current host
HostName = socket.gethostname()
HomeDir  = os.getenv("HOME")

# Checking to see if there is already a job running on the node by calling the
# top executable which should be available on most linux systems
bashCommand="top -n 1 -b | grep "+str(ExeVariable[0])+\
            " | grep $(whoami) | awk '{print $5}'"
print bashCommand
Memory=subprocess.check_output(bashCommand, shell=True)
Memory=re.split(';|, |\*|\n',Memory)
Memory=filter(None, Memory)
#num is equivalent to the number of processes running on the node
num=int(len(Memory))-1

# Creating invisible directory in home directory. This folder is used to store
# information related to what host are being used as well as the amount of
# memory and processors being currently used
if not os.path.isdir(HomeDir+"/."+str(ProgramRun)):
	os.mkdir(HomeDir+"/."+str(ProgramRun))

# Total allowed memory for the currently submitted job
TotalMemory = MemPerProc.value*NumPar.value
MaxMemory   = MemPerProc.value*NumPar.value

# If a file exists within the home directory with the name of the host and there
# are currently processors running with the same name of the current executable
# then open the file and determine how much combined memory is available
if os.path.exists(HomeDir+"/."+str(ProgramRun)+"/"+HostName) and  num!=-1:
	# Read the number of processors that are being used
	with open(HomeDir+"/."+str(ProgramRun)+"/"+HostName,'r') as f2:
		while True:
			# Number of processors running
			processors = f2.readline()
			if not processors: break
			# Amount of memory available to currently running processors
			memory = f2.readline()
			if not memory: break
			# Total Memory available to combined system
			TotalMemory.value = TotalMemory.value +\
			float(processors)*float(memory)

	f2.close()
	if firstline == "" or num==-1:
		firstline=0

	# Add the currently reserved memory from this job an the number of
	# processors that have been reserved for this job
	f2 = open(HomeDir+"/."+str(ProgramRun)+"/"+HostName,'a')
	f2.write(str(NumPar.value)+'\n')
	f2.write(str(MemPerProc.value)+'\n')
	f2.close()
else:
# If there are no processors running on the node currently or the hostfile does
# not exist create a new one, using the hostname as the file name
	f2=open(HomeDir+"/."+str(ProgramRun)+"/"+HostName,'w')
	f2.write(str(NumPar.value)+'\n')
	f2.write(str(MemPerProc.value)+'\n')
	TotalParallelProc.value=float(NumPar.value)
	f2.close()

###############################################################################
# Step 5 Calling Main

print "Before Main"
print NumPar.value
print NumRun
f.write("Beginning Parallel Loop\n")
f.close()
# The main function executes and runs in parallel by cycling through the
# available processors. It will continue to submit jobs until all of the
# processors are busy. It will then wait to submit the next job in the pool.
if __name__ == '__main__':
	pool=Pool(processes=NumPar.value)
	for k in range(0 ,NumRun):
            f = open('Python.log','a')
            OrigDirs = str(os.getcwd())
            if Args:
                ArgVariable.append(Args.replace("PAR",str(k)))
            else:
                ArgVariable.append(Args)

            if Dirs:
                DirVariable.append(Dirs.replace("PAR",str(k)))
            else:
                DirVariable.append(Dirs)

            f.write("ArgVariable "+str(ArgVariable[k])+"\n")
            f.write("Program in k range loop "+str(k)+"\n")
            f.write("ExeVariable "+str(ExeVariable[0])+"\n")
            f.write("DirVariable "+str(DirVariable[0])+"\n")
            f.write("OrigDirs "+str(OrigDirs)+"\n")
            f.close()
            pool.apply_async(Simulate,args=(ArgVariable,ExeVariable,DirVariable,k,OrigDirs))


pool.close()
pool.join()
sys.exit()
