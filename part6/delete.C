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
    HeapFileScan* hfs = nullptr;
    RID rid;

    if (relation.empty()) return BADCATPARM;

    hfs = new HeapFileScan(relation, status);
    if (status != OK) {
        delete hfs;
        return status;
    }

    if (attrName.empty()) {
        // Delete all records
        status = hfs->startScan(0, 0, INTEGER, nullptr, op);
    } else {
        // Get attribute metadata
        AttrDesc attrDesc;
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK) {
            delete hfs;
            return status;
        }

        // Validate type consistency
        if (static_cast<Datatype>(attrDesc.attrType) != type) {
            delete hfs;
            return BADCATPARM;
        }

        // Convert string filter value to binary
        void* filterValue = malloc(attrDesc.attrLen);
        if (!filterValue) {
            delete hfs;
            return INSUFMEM;
        }

        switch(type) {
            case INTEGER:
                *((int*)filterValue) = atoi(attrValue);
                break;
            case FLOAT:
                *((float*)filterValue) = atof(attrValue);
                break;
            case STRING:
                strncpy((char*)filterValue, attrValue, attrDesc.attrLen);
                break;
            default:
                free(filterValue);
                delete hfs;
                return BADCATPARM;
        }

        // Start scan with properly converted value
        status = hfs->startScan(attrDesc.attrOffset,attrDesc.attrLen,type,(char*)filterValue,op);
        free(filterValue);
    }

    if (status != OK) {
        delete hfs;
        return status;
    }

    // Delete matching records
    while ((status = hfs->scanNext(rid)) == OK) {
        status = hfs->deleteRecord();
        if (status != OK) break;
    }

        // just to see if we hit eof
        if(status == FILEEOF){
                status = OK;
        }
        // end scan
        Status endStatus = hfs->endScan();
        delete hfs;

        if(status == OK){
                status = endStatus;
        }
        return status;
}
