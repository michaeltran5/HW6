/**
 * @file delete.C
 * Author: Hassan Murayr
 * ID: 9084721316
 * Description: Implementation of delete.C
 */
#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *      OK on success
 *      an error code otherwise
 */
const Status QU_Delete(const string & relation,
                       const string & attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)
{
    Status status;
        RID rid;
    AttrDesc attrDesc;
    HeapFileScan *hfs;

        if(relation.empty()){
                return BADCATPARM;
        }

    if(attrName.empty()) {
        // delete all records > creating scanner object
        hfs = new HeapFileScan(relation, status);
        if (status != OK)
        {
            return status;
        }
        // start the scan
        hfs->startScan(0, 0, STRING, NULL, op); //EQ?

        while ((status = hfs->scanNext(rid)) != FILEEOF)
        {
            status = hfs->deleteRecord();
            if (status != OK)
            {
                return status;
            }
        }
    }
    else // if not empty
    {
        hfs = new HeapFileScan(relation, status);
        if (status != OK)
        {
            return status;
        }

        // get attribute metadata
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK)
        {
            delete hfs;
            return status;
        }

        // switch for data type
        int intVal;
        float floatVal;
        switch (type)
        {
        case INTEGER:
            intVal = atoi(attrValue);
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&intVal, op);
            break;
        case FLOAT:
            floatVal = atof(attrValue);
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&floatVal, op);
            break;
        // string
        default:
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, attrValue, op);
            break;
        }

        if (status != OK)
        {
            delete hfs;
            return status;
        }

        while ((status = hfs->scanNext(rid)) == OK)
        {
            status = hfs->deleteRecord();
            if (status != OK)
            {
                delete hfs;
                return status;
            }
        }
    }
    // not end of file yet
    if (status != FILEEOF)
    {
        delete hfs;
        return status;
    }

    // end scan cuz end of file
    hfs->endScan();
    delete hfs;

    return OK;
}
