/**
 * Michael Tran - 9083087123
 * 
 */

#include "catalog.h"
#include "query.h"
#include <iostream>
#include <cstdlib> 

using namespace std;

const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation. Result of the selection is stored in the result relation called result.
 * This is a helper method that organizes the data and then calls ScanSelect
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
    cout << "Doing QU_Select" << endl;
    Status status;

    AttrDesc* projNamesArr = new AttrDesc[projCnt];
    AttrDesc* projNamesPtr = nullptr;

    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projNamesArr[i]);
        if (status != OK) {
            delete[] projNamesArr;
            return status;
        }
    }

    if (attr != nullptr) {
        projNamesPtr = new AttrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, *projNamesPtr);
        if (status != OK) {
            delete[] projNamesArr;
            delete projNamesPtr;
            return status;
        }
    }

    int reclength = 0;
    for (int i = 0; i < projCnt; i++) {
        reclength += projNamesArr[i].attrLen;
    }

    status = ScanSelect(result, projCnt, projNamesArr, projNamesPtr, op, attrValue, reclength);

    delete[] projNamesArr;
    if (projNamesPtr) delete projNamesPtr;

    return status;
}

/*
 * This method performs selection using a HeapFileScan.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    Status status;

    Record outputTable;
    RID tempRID;
    Record tempRec;

    InsertFileScan resultRel(result, status);
    if (status != OK) {
        return status;
    }

    outputTable.length = reclen;
    outputTable.data = new char[reclen];

    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK) {
        delete[] (char*)outputTable.data;
        return status;
    }

    if (attrDesc == nullptr) {
        status = scan.startScan(0, 0, STRING, nullptr, EQ);
    } else {
        switch (attrDesc->attrType) {
            case STRING:
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
                break;
            case FLOAT: {
                float fval = atof(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, (char*)&fval, op);
                break;
            }
            case INTEGER: {
                int ival = atoi(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, (char*)&ival, op);
                break;
            }
        }
    }

    if (status != OK) {
        delete[] (char*)outputTable.data;
        return status;
    }

    while (scan.scanNext(tempRID) == OK) {
        if ((status = scan.getRecord(tempRec)) != OK) {
            delete[] (char*)outputTable.data;
            return status;
        }

        int offset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy((char*)outputTable.data + offset,
                   (char*)tempRec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        RID outRID;
        status = resultRel.insertRecord(outputTable, outRID);
        if (status != OK) {
            delete[] (char*)outputTable.data;
            return status;
        }
    }

    if ((status = scan.endScan()) != OK && status != FILEEOF) {
        delete[] (char*)outputTable.data;
        return status;
    }

    delete[] (char*)outputTable.data;
    return OK;
}
