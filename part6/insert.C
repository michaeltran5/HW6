/**
 * @file insert.C
 * Author: Shrey Katyal
 * ID: 9086052256
*/ 
#include "catalog.h"
#include "query.h"
#include <cstring>
#include <iostream>
#include <cstdlib>

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
    Status status;
    int relAttrCnt;
    AttrDesc *attrs = nullptr;
    
    cout << "Inserting into relation " << relation << endl;
    
    if (relation.empty()) return BADCATPARM;
    
    status = attrCat->getRelInfo(relation, relAttrCnt, attrs);
    if (status != OK) return status;
    
    if (attrCnt != relAttrCnt) {
        free(attrs);
        return ATTRNOTFOUND;
    }
    
    int recordSize = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        recordSize += attrs[i].attrLen;
    }
    
    char *recData = new char[recordSize];
    if (!recData) {
        free(attrs);
        return INSUFMEM;
    }
    memset(recData, 0, recordSize);
    
    for (int i = 0; i < attrCnt; i++) {
        bool attrFound = false;
        
        for (int j = 0; j < relAttrCnt; j++) {
            if (strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {
                attrFound = true;
                
                char *destAddr = recData + attrs[j].attrOffset;
                char *stringValue = (char*)attrList[i].attrValue;
                
                if (attrList[i].attrType != attrs[j].attrType) {
                    delete[] recData;
                    free(attrs);
                    return ATTRTYPEMISMATCH;
                }
                
                switch (attrs[j].attrType) {
                    case INTEGER: {
                        int intVal = atoi(stringValue);
                        memcpy(destAddr, &intVal, sizeof(int));
                        break;
                    }
                    case FLOAT: {
                        float floatVal = atof(stringValue);
                        memcpy(destAddr, &floatVal, sizeof(float));
                        break;
                    }
                    case STRING: {
                        strncpy(destAddr, stringValue, attrs[j].attrLen);
                        destAddr[attrs[j].attrLen - 1] = '\0';
                        break;
                    }
                    default:
                        delete[] recData;
                        free(attrs);
                        return BADCATPARM;
                }
                
                break;
            }
        }
        
        if (!attrFound) {
            delete[] recData;
            free(attrs);
            return ATTRNOTFOUND;
        }
    }
    
    InsertFileScan *ifs = new InsertFileScan(relation, status);
    if (status != OK) {
        delete[] recData;
        free(attrs);
        return status;
    }
    
    Record rec;
    rec.data = recData;
    rec.length = recordSize;
    
    RID rid;
    status = ifs->insertRecord(rec, rid);
    
    delete[] recData;
    delete ifs;
    free(attrs);
    
    return status;
}

