#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#include <Windows.h>
#else
#include <unistd.h>
#endif

int main(int argc, char * argv[]){

    if (argc==3) {
      mkdir(argv[2], 0777);
    }
    int size = 500000000;

    int * ar = (int *) malloc(size*sizeof(int));

    int pollingDelay = 20000000;

    int i = 0;
    for(;i<size;i++){
        ar[i] = i;
    }
    //sleep:
    #ifdef _WIN32
    sleep(pollingDelay);
    #else
    usleep(pollingDelay);  /* sleep for 100 milliSeconds */
    #endif

    free(ar);
    return 0;
}
