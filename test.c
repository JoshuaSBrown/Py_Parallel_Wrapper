#include <stdlib.h>
#include <stdio.h>

#ifdef _WIN32
#include <Windows.h>
#else
#include <unistd.h>
#endif

int main(void){

    int size = 500000000;

    int * ar = (int *) malloc(size*sizeof(int));

    int pollingDelay = 20000;

    int i = 0;
    for(;i<size;i++){
        ar[i] = i;
    }
    //sleep:
    #ifdef _WIN32
    sleep(pollingDelay);
    #else
    usleep(pollingDelay*1000);  /* sleep for 100 milliSeconds */
    #endif

    free(ar);
    return 0;
}
