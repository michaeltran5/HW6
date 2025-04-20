#include "catalog.h"
#include "query.h"
#include <string.h>

const Status QU_Select(const string & result, 
                       const int projCnt, 
                       const attrInfo projNames[],
                       const attrInfo *attr, 
                       const Operator op, 
                       const char *attrValue)
{
    cout << "Doing QU_Select" << endl;

    Status status;
    AttrDesc* attrs = new AttrDesc[projCnt];
    int reclen = 0;

    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrs[i]);
        if (status != OK) {
            delete[] attrs;
            return status;
        }
        reclen += attrs[i].attrLen;
    }

    if (attr != NULL) {
        AttrDesc attrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
        if (status != OK) {
            delete[] attrs;
            return status;
        }

        char* filter = nullptr;

        // Convert attrValue to appropriate binary format
        if (attrDesc.attrType == INTEGER) {
            filter = new char[sizeof(int)];
            int val = atoi(attrValue);
            memcpy(filter, &val, sizeof(int));
        } else if (attrDesc.attrType == FLOAT) {
            filter = new char[sizeof(float)];
            float val = atof(attrValue);
            memcpy(filter, &val, sizeof(float));
        } else if (attrDesc.attrType == STRING) {
            filter = new char[attrDesc.attrLen];
            memset(filter, 0, attrDesc.attrLen);
            int copyLen = strlen(attrValue);
            if (copyLen > attrDesc.attrLen) copyLen = attrDesc.attrLen;
            memcpy(filter, attrValue, copyLen);
        }

        // Now do the scan + select
        cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

        HeapFileScan* scan = new HeapFileScan(projNames[0].relName, status);
        if (status != OK) {
            delete[] attrs;
            delete[] filter;
            return status;
        }

        InsertFileScan resultFile(result, status);
        if (status != OK) {
            delete scan;
            delete[] attrs;
            delete[] filter;
            return status;
        }

        status = scan->startScan(attrDesc.attrOffset,
                                 attrDesc.attrLen,
                                 (Datatype)attrDesc.attrType,
                                 filter, op);
        delete[] filter;

        if (status != OK) {
            delete scan;
            delete[] attrs;
            return status;
        }

        RID rid;
        while ((status = scan->scanNext(rid)) == OK) {
            Record rec;
            status = scan->getRecord(rec);
            if (status != OK) break;

            char* outputData = new char[reclen];
            memset(outputData, 0, reclen);

            int outOffset = 0;
            for (int i = 0; i < projCnt; i++) {
                memcpy(outputData + outOffset,
                       (char*)rec.data + attrs[i].attrOffset,
                       attrs[i].attrLen);
                outOffset += attrs[i].attrLen;
            }

            Record outRec;
            outRec.data = outputData;
            outRec.length = reclen;

            RID outRID;
            status = resultFile.insertRecord(outRec, outRID);

            delete[] outputData;
            if (status != OK) break;
        }

        if (status == FILEEOF) status = OK;

        scan->endScan();
        delete scan;
    } 
    else {
        cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

        HeapFileScan* scan = new HeapFileScan(projNames[0].relName, status);
        if (status != OK) {
            delete[] attrs;
            return status;
        }

        InsertFileScan resultFile(result, status);
        if (status != OK) {
            delete scan;
            delete[] attrs;
            return status;
        }

        status = scan->startScan(0, 0, STRING, NULL, EQ);
        if (status != OK) {
            delete scan;
            delete[] attrs;
            return status;
        }

        RID rid;
        while ((status = scan->scanNext(rid)) == OK) {
            Record rec;
            status = scan->getRecord(rec);
            if (status != OK) break;

            char* outputData = new char[reclen];
            memset(outputData, 0, reclen);

            int outOffset = 0;
            for (int i = 0; i < projCnt; i++) {
                memcpy(outputData + outOffset,
                       (char*)rec.data + attrs[i].attrOffset,
                       attrs[i].attrLen);
                outOffset += attrs[i].attrLen;
            }

            Record outRec;
            outRec.data = outputData;
            outRec.length = reclen;

            RID outRID;
            status = resultFile.insertRecord(outRec, outRID);

            delete[] outputData;
            if (status != OK) break;
        }

        if (status == FILEEOF) status = OK;

        scan->endScan();
        delete scan;
    }

    delete[] attrs;
    return status;
}
