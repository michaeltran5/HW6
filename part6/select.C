#include "catalog.h"
#include "query.h"
#include <string.h>


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
    
    Status status;
    AttrDesc *attrs;
    int reclen = 0;
    AttrDesc attrDesc;
    char *filter = NULL;
    
    // Get projection attribute information
    int nonConstProjCnt = projCnt;
    if ((status = attrCat->getRelInfo(projNames[0].relName, nonConstProjCnt, attrs)) != OK) {
        return status;
    }
    
    // Calculate result record length
    for (int i = 0; i < projCnt; i++) {
        reclen += attrs[i].attrLen;
    }
    
    // If we have a selection condition
    if (attr != NULL) {
        // Get the attribute descriptor for the selection attribute
        if ((status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc)) != OK) {
            delete [] attrs;
            return status;
        }
        
        // Convert the filter value to the appropriate type
        switch (attrDesc.attrType) {
            case INTEGER: {
                int intVal = atoi(attrValue);
                filter = new char[sizeof(int)];
                memcpy(filter, &intVal, sizeof(int));
                break;
            }
            case FLOAT: {
                float floatVal = atof(attrValue);
                filter = new char[sizeof(float)];
                memcpy(filter, &floatVal, sizeof(float));
                break;
            }
            case STRING: {
                filter = new char[attrDesc.attrLen];
                memset(filter, 0, attrDesc.attrLen);
                int len = strlen(attrValue);
                if (len > attrDesc.attrLen)
                    len = attrDesc.attrLen;
                memcpy(filter, attrValue, len);
                break;
            }
        }
        
        // Call ScanSelect with the filter
        status = ScanSelect(result, projCnt, attrs, &attrDesc, op, filter, reclen);
        
        // Clean up
        delete [] filter;
    } else {
        // No selection condition, do a full table scan
        status = ScanSelect(result, projCnt, attrs, NULL, op, NULL, reclen);
    }
    
    delete [] attrs;
    return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    
    Status status;
    HeapFileScan *scan = new HeapFileScan(projNames[0].relName, status);
    if (status != OK) {
        return status;
    }
    
    RID rid;
    Record rec;
    Record outRec;
    
    // Allocate memory for the output record
    char *outputData = new char[reclen];
    
    // Open the result heap file
    InsertFileScan resultFile(result, status);
    if (status != OK) {
        delete [] outputData;
        delete scan;
        return status;
    }
    
    // Start the scan with or without a filter
    if (attrDesc != NULL) {
        status = scan->startScan(attrDesc->attrOffset, 
                              attrDesc->attrLen, 
                              (Datatype)attrDesc->attrType, 
                              filter, 
                              op);
    } else {
        // No filter, scan all records
        status = scan->startScan(0, 0, STRING, NULL, EQ);
    }
    
    if (status != OK) {
        delete [] outputData;
        delete scan;
        return status;
    }
    
    // Scan through all matching records
    while ((status = scan->scanNext(rid)) == OK) {
        // Get the current record
        status = scan->getRecord(rec);
        if (status != OK) {
            break;
        }
        
        // Project the attributes into the output record
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputData + outputOffset, 
                  (char *)rec.data + projNames[i].attrOffset, 
                  projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }
        
        // Create the output record
        outRec.data = outputData;
        outRec.length = reclen;
        
        // Insert into result relation
        RID outRID;
        status = resultFile.insertRecord(outRec, outRID);
        if (status != OK) {
            break;
        }
    }
    
    // Check if we ended because of an error or end of file
    if (status != FILEEOF) {
        delete [] outputData;
        delete scan;
        return status;
    }
    
    // Clean up
    scan->endScan();
    delete scan;
    delete [] outputData;
    
    return OK;
}