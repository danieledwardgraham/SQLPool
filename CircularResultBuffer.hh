/* Class: CircularResultBuffer
 Implements efficient and safe result buffer.
 v. 1.0, 9/21/2009, D. Graham
*/

#include <iostream>
#include <list>
#include <pthread.h>

using namespace std;

class msg_t;

#ifndef SHM_LOG_T
#define SHM_LOG_T
class shm_log_t {
   public:
      shm_log_t(size_t);
      int write_message(string, msg_t *);
      ~shm_log_t();
   private:
      char *shm_ptr;
      void *attach_ptr;
      int intid;
      size_t logsize;
      pthread_mutex_t mut;
}; 
#endif

class CircularResultBuffer {
    public:
        CircularResultBuffer(char *, shm_log_t *);
        int reset();
        int startResult(int);
        int addResultData(list<char>);
        list<char> readResult(int);
        int getNextDataId();
        int setLog(shm_log_t *);
    private:
        int findDataId(int);
        int getData(char *,size_t);
        int putData(char *,size_t, int);
        int travData(size_t);
        char buf[100];
        char buf2[100];
        char * startAddress;
        char * endAddress;
        char * firstResOffset;
        char * nextDataId;
        size_t currLength;
        char * currLengthAddr;
        char * currStartOfRes;
        char * currResPtr;
        char * startOfData;
        size_t segSize;
        int dataId;
        shm_log_t *shm_log;
};

/* Implementation */

CircularResultBuffer::CircularResultBuffer(char * ptr, shm_log_t *log) {
    this->startAddress = ptr;
    this->segSize = *((size_t *) this->startAddress);
    this->endAddress = this->startAddress + this->segSize;
    this->nextDataId = this->startAddress + sizeof(size_t);
    this->firstResOffset = this->nextDataId + sizeof(int);
    this->startOfData = this->firstResOffset + sizeof(size_t);
    this->currStartOfRes = NULL;
    this->currLengthAddr = NULL;
    this->currResPtr = NULL;
    this->currLength = 0;
    this->shm_log = log;
}

int CircularResultBuffer::reset() {
    if (Debug) {
        shm_log->write_message("CircularResultBuffer::reset called\n", NULL);
    }
    this->segSize = *((size_t *) this->startAddress);
    this->endAddress = this->startAddress + this->segSize;
    this->nextDataId = this->startAddress + sizeof(size_t);
    this->firstResOffset = this->nextDataId + sizeof(int);
    this->startOfData = this->firstResOffset + sizeof(size_t);
    this->currStartOfRes = NULL;
    this->currLengthAddr = NULL;
    this->currResPtr = NULL;
    this->currLength = 0;
    *((size_t *) this->firstResOffset) = 0;
    *((int *) this->startOfData) = -1;
    return 0;
}

int CircularResultBuffer::getData(char * b, size_t sz) {
    size_t i;
    if (sz >= 100) {
        return(-1);
    }
    for (i = 0; i < sz; i++) {
        b[i] = *(this->currResPtr);
        this->currResPtr = this->currResPtr + 1;
        if (this->currResPtr == this->endAddress) {
            if (Debug) {
                shm_log->write_message("CircularResultBuffer: wrapping around end of seg on getData\n", NULL);
            }
            this->currResPtr = this->startOfData;
        }
    }
    return(0);
}

int CircularResultBuffer::travData(size_t sz) {
    size_t i;
    char logmsg[200];
    if (Debug) {
        sprintf(logmsg, "CircularResultBuffer: travData called with size %u\n", sz);
        shm_log->write_message(logmsg, NULL);
    }
    if (sz > this->segSize) {
        shm_log->write_message("Error. travData sz parameter too big.\n", NULL);
        return(-1);
    }
    for (i = 0; i < sz; i++) {
        this->currResPtr = this->currResPtr + 1;
        if (this->currResPtr == this->endAddress) {
            if (Debug) {
                sprintf(logmsg, "CircularResultBuffer: wrapping around end of seg on travData. (sz = %u)\n", sz);
                shm_log->write_message(logmsg, NULL);
            }
            this->currResPtr = this->startOfData;
        }
    }
    return(0);
}

int CircularResultBuffer::putData(char * b, size_t sz, int ignore) {
    size_t i;
    char * saveResPtr;
    size_t k;
    if (sz >= 100) {
        return(-1);
    }
    for (i = 0; i < sz; i++) {
        if ((this->currResPtr == this->currStartOfRes) && !ignore) {
            return(-2); /* result larger than data segment */
        }
        if ((this->currResPtr == (this->startOfData + *((size_t *) this->firstResOffset))) && !ignore) {
            /* Overwrite old data */
            if (Debug) {
                shm_log->write_message("CircularResultBuffer : Overwriting old data\n", NULL);
            }
            saveResPtr = this->currResPtr;
            this->travData(sizeof(int));
            this->getData(this->buf2, sizeof(size_t));
            k = *((size_t *) this->buf2);
            this->travData(k);
            *((size_t *) this->firstResOffset) = this->currResPtr - this->startOfData;
            this->currResPtr = saveResPtr;
        }
        *(this->currResPtr) = b[i];
        this->currResPtr = this->currResPtr + 1;
        if (this->currResPtr == this->endAddress) {
            this->currResPtr = this->startOfData;
            if (Debug) {
                shm_log->write_message("CircularResultBuffer: wrapping around end of seg on putData\n", NULL);
            }
        }
        ignore = 0;
    }
    return(0);
}

int CircularResultBuffer::findDataId(int data_id) {
    size_t infiniteCnt = 0;
    size_t k;
    if (Debug) { 
        shm_log->write_message("CircularResultBuffer: Start of findDataId\n", NULL);
    } 
    this->currStartOfRes = startOfData + *((size_t *) this->firstResOffset);
    if (this->currStartOfRes == this->endAddress) {
        if (Debug) {
            shm_log->write_message("CircularResultBuffer: wrapping around end of seg on findDataId\n", NULL);
        }
        this->currStartOfRes = this->startOfData;
    }
    this->currResPtr = this->currStartOfRes;
    this->getData(this->buf, sizeof(int));
    this->currLengthAddr = this->currResPtr;
    while (*((int *) this->buf) != data_id) {
        infiniteCnt++;
        if (infiniteCnt >= this->segSize) {
            shm_log->write_message("CircularResultBuffer: Error - infinite count reached in findDataId\n", NULL);
            return(-1);
        }
        if (*((int *) this->buf) == -1) {
            shm_log->write_message("CircularResultBuffer: Error - end of result sequence reached in findDataId\n", NULL);
            return(-2);
        }
        this->getData(this->buf, sizeof(size_t));
        k = *((size_t *) this->buf);
        if (this->travData(k) < 0) {
            return(-3);
        }
        this->currStartOfRes = this->currResPtr;
        this->getData(this->buf, sizeof(int));
        this->currLengthAddr = this->currResPtr;
    }
    if (Debug) {
        shm_log->write_message("CircularResultBuffer: End of findDataId\n", NULL);
    }
    return(0);
}
        
int CircularResultBuffer::startResult(int data_id) {
    int ret;
    char * saveResPtr;
    if (Debug) {
        shm_log->write_message("CircularResultBuffer: begin of startResult\n", NULL);
    }
    if ((ret = this->findDataId(-1)) < 0) {
        this->reset();
        if ((this->findDataId(-1)) < 0) {
            cerr << "Panic: Error Corrupt data segment\n";
            exit(-1);
        }
        return(ret);
    }
    this->dataId = data_id;
/*    *((int *) this->buf) = data_id;
    this->currResPtr = this->currStartOfRes;
    if ((ret = this->putData(this->buf, sizeof(int), 1)) < 0) {
        this->reset();
        return(ret);
    } */
    this->currResPtr = this->currStartOfRes;
    this->travData(sizeof(int));
    *((size_t *) this->buf) = 0;
    if ((ret = this->putData(this->buf, sizeof(size_t), 0)) < 0) {
        this->reset();
        return(ret);
    }
    saveResPtr = this->currResPtr;
    *((int *) this->buf) = -1;
    if ((ret = this->putData(this->buf, sizeof(int), 0)) < 0) {
        this->reset();
        return(ret);
    }
    this->currResPtr = saveResPtr;
    this->currLength = 0;
    return(0);
}

int CircularResultBuffer::addResultData(list<char> res) {
    char * saveResPtr;
    int ret;
    if (Debug) {
        shm_log->write_message("CircularResultBuffer: start of addResultData\n", NULL);
    }
    if (this->currResPtr == NULL) {
        shm_log->write_message("CircularResultBuffer: Error - did not call startResult\n", NULL);
        return(-3); /* Did not call startResult */
    }
    while (!res.empty()) {
        this->buf[0] = res.front();
        if ((ret = this->putData(this->buf, 1, 0)) < 0) {
            this->reset();
            return(ret);
        }
        this->currLength++; 
        res.pop_front();
    }
    *((size_t *) this->buf) = this->currLength;
    saveResPtr = this->currResPtr;
    this->currResPtr = this->currLengthAddr;
    if ((ret = this->putData(this->buf, sizeof(size_t), 0)) < 0) {
        this->reset();
        return(ret);
    }
    this->currResPtr = saveResPtr;
    saveResPtr = this->currResPtr;
    *((int *) this->buf) = -1;
    if ((ret = this->putData(this->buf, sizeof(int), 0)) < 0) {
        this->reset();
        return(ret);
    }
    *((int *) this->buf) = this->dataId;
    this->currResPtr = this->currStartOfRes;
    if ((ret = this->putData(this->buf, sizeof(int), 1)) < 0) {
        this->reset();
        return(ret);
    } 
    this->currResPtr = saveResPtr;
    return(0);
}

list<char> CircularResultBuffer::readResult(int data_id) {
    list<char> res;
    size_t sz, i;
    if (Debug) {
        shm_log->write_message("CircularResultBuffer: start of readResult\n", NULL);
    }
    if (this->findDataId(data_id) < 0) {
        return(res);
    }
    this->getData(this->buf, sizeof(size_t));
    sz = *((size_t *) this->buf);
    for (i = 0; i < sz; i++) {
        this->getData(this->buf, 1);
        res.push_back(this->buf[0]);
    }
    return(res);
}

int CircularResultBuffer::getNextDataId() {
    int ret;
    ret = *((int *) this->nextDataId);
    *((int *) this->nextDataId) = (ret + 1) % 1000000;
    return(ret);
}

int CircularResultBuffer::setLog(shm_log_t *log) {
    this->shm_log = log;
    return(0);
}
 
