/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <exceptions/page_not_pinned_exception.h>
#include <exceptions/file_exists_exception.h>
#include <exceptions/hash_not_found_exception.h>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------


    BTreeIndex::BTreeIndex(const std::string & relationName,
                           std::string & outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType)
    {
        this->bufMgr = bufMgrIn;
        this->attrByteOffset = attrByteOffset;
        this->scanExecuting = false;
        this->leafOccupancy = 0;
        this->nodeOccupancy = 0;
        this->attributeType = attrType;

        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        outIndexName = idxStr.str();

        try {
            // create a index file
            this->file = new BlobFile(outIndexName, true); // assign a page file in the mem
            Page *pg; // alloc a page, casting to pg it is a temp file*

            // store the header information
            bufMgr->allocPage(file, this->headerPageNum, pg); // alloc a header in the buff, headerpagenum will be set to 1 in this method
            IndexMetaInfo *metaInfo = (IndexMetaInfo*) pg;
            metaInfo->attrByteOffset = attrByteOffset; // set the IndexMetaInfo
            strcpy(metaInfo->relationName, relationName.c_str());

            // alloc a root page, store the root info
            bufMgr->allocPage(file, this->rootPageNum, pg);
            metaInfo->rootPageNo = rootPageNum;

            // find the header page by hPN in the buffer and unpin it
            try {
                this->bufMgr->unPinPage(file,headerPageNum, true);
            } catch (PageNotPinnedException exception){
            }

            // create a new nonleaf node, well it is root
            NonLeafNodeInt *rootLeaf = (NonLeafNodeInt*) pg;
            memset(rootLeaf, 0, sizeof(NonLeafNodeInt));
            rootLeaf->level = 1;

            /**
             * first node???
             */
            Page *newpg;
            PageId newID;
            bufMgr->allocPage(file,newID,newpg);
            LeafNodeInt *firstLeafNode = (LeafNodeInt*) newpg;
            memset(firstLeafNode, 0, sizeof(LeafNodeInt));
            firstLeafNode->rightSibPageNo = 0; // maybe useless todo: delete it?
            bufMgr->unPinPage(file, newID, true);
            rootLeaf->pageNoArray[0] = newID;


            try {
                this->bufMgr->unPinPage(file, rootPageNum, true);
            } catch (PageNotPinnedException exception){
            }

            FileScan fileScan(relationName, bufMgr);
            RecordId rid;

            // may throw end of file exception
            while(1)
            {
                fileScan.scanNext(rid);
                std::string record = fileScan.getRecord();
                insertEntry(record.c_str()+attrByteOffset,rid);
            }
        } catch(FileExistsException e){ // if the file already exist, then we just set the certain field of the metainfo
            this->file = new BlobFile(outIndexName, false); // just extract the page to the buff
            Page *pg;
            bufMgr->readPage(file, headerPageNum, pg); // header is always the first, PageNo TODO: does not change?

            IndexMetaInfo* testing = (IndexMetaInfo*) pg;
            if(testing->attrByteOffset != attrByteOffset || strcmp(testing->relationName, relationName.c_str()) != 0){
                try {
                    bufMgr->unPinPage(file, headerPageNum, false);
                } catch (PageNotPinnedException exception){
                }
                throw BadIndexInfoException(""); // todo: do we need that???
            }

            this->rootPageNum = ((IndexMetaInfo*) pg)->rootPageNo;

            try {
                bufMgr->unPinPage(file, 1, false);
            } catch (PageNotPinnedException e){
            }

        } catch (EndOfFileException e){
            // save Btee index file to disk
            bufMgr->flushFile(file);
        }

    }


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex()
    {
        scanExecuting = false;
        try {
            bufMgr->unPinPage(file, currentPageNum, false);
        } catch (PageNotPinnedException exception){ // TODO: pinned exception and bad buffer???
        } catch (HashNotFoundException exception1) { // in the uPP->Lookup
        }
        bufMgr->flushFile(file);
        file->~File();
    }

    BTreeIndex::NewNode BTreeIndex::leafSpliter(int position, LeafNodeInt* fullLeaf, RIDKeyPair<int> ridKeyPair){
        PageId newpgid;
        Page* newpg;
        bufMgr->allocPage(file,newpgid, newpg);
        LeafNodeInt *newLeaf = (LeafNodeInt*) newpg;
        memset(newLeaf, 0, sizeof(LeafNodeInt));
        if ((INTARRAYLEAFSIZE-1)/2 < position){ // put right pos > (INTARRAYLEAFSIZE-1)/2
            for (int j = (INTARRAYLEAFSIZE+1)/2; j < position; ++j) {
                newLeaf->keyArray[j-(INTARRAYLEAFSIZE+1)/2] = fullLeaf->keyArray[j];
                fullLeaf->keyArray[j] = 0;
                newLeaf->ridArray[j-(INTARRAYLEAFSIZE+1)/2] = fullLeaf->ridArray[j];
                fullLeaf->ridArray[j].page_number = 0; // todo: set slot to zero???
            }
            newLeaf->keyArray[position-(INTARRAYLEAFSIZE+1)/2] = ridKeyPair.key;
            newLeaf->ridArray[position-(INTARRAYLEAFSIZE+1)/2] = ridKeyPair.rid;
            for (int i = position+1; i < INTARRAYLEAFSIZE+1; ++i) {
                newLeaf->keyArray[i - (INTARRAYLEAFSIZE-1)/2] = fullLeaf->keyArray[i-1];
                fullLeaf->keyArray[i-1] = 0;
                newLeaf->ridArray[i - (INTARRAYLEAFSIZE-1)/2] = fullLeaf->ridArray[i-1];
                fullLeaf->ridArray[i-1].page_number = 0;
            }
        } else{
            for (int j = (INTARRAYLEAFSIZE-1)/2; j < INTARRAYLEAFSIZE; ++j) {
                newLeaf->keyArray[j-(INTARRAYLEAFSIZE-1)/2] = fullLeaf->keyArray[j];
                fullLeaf->keyArray[j] = 0;
                newLeaf->ridArray[j-(INTARRAYLEAFSIZE-1)/2] = fullLeaf->ridArray[j];
                fullLeaf->ridArray[j].page_number = 0; // todo: set slot to zero???
            }

            for (int i = (INTARRAYLEAFSIZE-1)/2; i > position; --i) {
                fullLeaf->keyArray[i] = fullLeaf->keyArray[i-1];
                fullLeaf->ridArray[i] = fullLeaf->ridArray[i-1];
            }
            fullLeaf->keyArray[position] = ridKeyPair.key;
            fullLeaf->ridArray[position] = ridKeyPair.rid;
        }

        newLeaf->rightSibPageNo = fullLeaf->rightSibPageNo;
        fullLeaf->rightSibPageNo = newpgid;
        bufMgr->unPinPage(file, newpgid, true);

        NewNode temp = {newLeaf->keyArray[0], newpgid}; //std::cout<<"line184 sp: "<< temp.key<<" "<<temp.pgId<<"\n";
        return temp;

//        //tmp array
//        int keyarr[INTARRAYLEAFSIZE + 1];
//        RecordId ridarr[INTARRAYLEAFSIZE + 1];
//
//        //copy all new records to tmp
//        for(int i=0; i < position; i++) {
//            ridarr[i] = fullLeaf->ridArray[i];
//            keyarr[i] = fullLeaf->keyArray[i];
//        }
//        ridarr[position] = ridKeyPair.rid;
//        keyarr[position] = ridKeyPair.key;
//        for(int i = INTARRAYLEAFSIZE; i > position; i--){
//            keyarr[i] = fullLeaf->keyArray[i-1];
//            ridarr[i] = fullLeaf->ridArray[i-1];
//        }
//        memset(fullLeaf, 0 , sizeof(NonLeafNodeInt));
//
//        //copy back
//        for(int i=0; i< (INTARRAYLEAFSIZE + 1) / 2; i++ ){
//            fullLeaf->keyArray[i] = keyarr[i];
//            fullLeaf->ridArray[i] = ridarr[i];
//        }
//        for(int i= (INTARRAYLEAFSIZE + 1) / 2; i < INTARRAYLEAFSIZE + 1; i++){
//            newLeaf->keyArray[i - (INTARRAYLEAFSIZE + 1) / 2] = keyarr[i];
//            newLeaf->ridArray[i - (INTARRAYLEAFSIZE + 1) / 2] = ridarr[i];
//        }
//
//        //link leaf node
//        newLeaf->rightSibPageNo = fullLeaf->rightSibPageNo;
//        fullLeaf->rightSibPageNo = newpgid;
//
//        //push up
//        NewNode temp = {newLeaf->keyArray[0], newpgid};
//
//        //unpin
//        bufMgr->unPinPage(file, newpgid, true);
//        return temp;
    }

    BTreeIndex::NewNode BTreeIndex::nonLeafSpliter(int position, NonLeafNodeInt* fullLeaf, NewNode &newnode){
        PageId newpgid;
        Page* newpg;
        bufMgr->allocPage(file, newpgid, newpg);
        NonLeafNodeInt* newNonLeaf = (NonLeafNodeInt*) newpg;
        memset(newNonLeaf, 0, sizeof(NonLeafNodeInt));


        int keyarr[INTARRAYNONLEAFSIZE + 1] = {0};
        int pagearr[INTARRAYNONLEAFSIZE + 2] = {0};

        for (int i = 0; i < position; ++i) {
            keyarr[i] = fullLeaf->keyArray[i];
            pagearr[i] = fullLeaf->pageNoArray[i];
        }
        pagearr[position] = fullLeaf->pageNoArray[position];

        keyarr[position] = newnode.key; //std::cout<<"line204 sp: "<< newnode.key<<" "<<newnode.pgId<<"\n";
        pagearr[position + 1] = newnode.pgId;
        for (int j = position + 1; j < INTARRAYNONLEAFSIZE + 1; ++j) {
            keyarr[j] = fullLeaf->keyArray[j-1];
            pagearr[j + 1] = fullLeaf->pageNoArray[j];
        }
        int level = fullLeaf->level;
        memset(fullLeaf, 0, sizeof(NonLeafNodeInt));
        fullLeaf->level = level;

        newNonLeaf->level = level;
        for (int i = 0; i < (INTARRAYNONLEAFSIZE+1)/2; ++i) {
            fullLeaf->keyArray[i] = keyarr[i];
            fullLeaf->pageNoArray[i] = pagearr[i];
        }
        //std::cout<<keyarr[(INTARRAYNONLEAFSIZE+1)/2]<<"\n";
        fullLeaf->pageNoArray[(INTARRAYNONLEAFSIZE+1)/2] = pagearr[(INTARRAYNONLEAFSIZE+1)/2];
        for (int j = (INTARRAYNONLEAFSIZE+1)/2 + 1; j < INTARRAYNONLEAFSIZE+1; ++j) {
            newNonLeaf->keyArray[j - (INTARRAYNONLEAFSIZE+1)/2 -1] = keyarr[j];
            newNonLeaf->pageNoArray[j - (INTARRAYNONLEAFSIZE+1)/2 -1] = pagearr[j];
        }
        newNonLeaf->pageNoArray[INTARRAYNONLEAFSIZE - (INTARRAYNONLEAFSIZE+1)/2] = pagearr[INTARRAYNONLEAFSIZE + 1];

        NewNode temp = {keyarr[(INTARRAYNONLEAFSIZE+1)/2], newpgid};  std::cout<<"line226 nsp: "<< temp.key<<" "<<temp.pgId<<"\n";
        bufMgr->unPinPage(file, newpgid, true);

        return temp;

    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
        RIDKeyPair<int> ridP;
        ridP.set(rid, *((int *) key));

        /**
         * - a new page id num that split the root
         * - a new value that the new root has
         * (- create a new page, set the left page (0) to be the original, right (1) to be the new page, set the rootpage to be the new page)
         *
         */

        NewNode newroot;
        newroot = insertHelper(rootPageNum, ridP, 0);
        if (newroot.pgId != 0){
            std::cout<<newroot.key<<" " << newroot.pgId<<" 290\n";
            PageId newrootId; // new root page
            Page *newrootPage;
            bufMgr->allocPage(file, newrootId, newrootPage);

            NonLeafNodeInt *newrootNode = (NonLeafNodeInt*) newrootPage;
            memset(newrootNode, 0, sizeof(NonLeafNodeInt));
            newrootNode->keyArray[0] = newroot.key;
            newrootNode->pageNoArray[0] = rootPageNum;
            newrootNode->pageNoArray[1] = newroot.pgId;
            newrootNode->level = 0;
            rootPageNum = newrootId;
            std::cout<<rootPageNum<<" becomes root\n";
            bufMgr->unPinPage(file, newrootId, true);
        }
    }

// note we do not know the level of the page only by reading it!!!
    BTreeIndex::NewNode BTreeIndex::insertHelper(PageId pageId, RIDKeyPair<int> ridKeyPair, int level){

        if (level == 1){
            Page *pg;
            bufMgr->readPage(file, pageId, pg);
            LeafNodeInt *leafnode = (LeafNodeInt*) pg;

            int position = 0;

            while (position< INTARRAYLEAFSIZE && leafnode->ridArray[position].page_number != 0 && leafnode->keyArray[position]<ridKeyPair.key)
                ++position;
            int lastnode = position;
            while (lastnode < INTARRAYLEAFSIZE && leafnode->ridArray[lastnode].page_number != 0)
                lastnode++;

            NewNode returned = {0,0};
            if (lastnode >= INTARRAYLEAFSIZE){ // full
                returned = leafSpliter(position, leafnode, ridKeyPair);
                std::cout<<"line327: "<<returned.key << " "<< returned.pgId<<" "<<pageId<<"\n";
            }else{
                if (leafnode->ridArray[position].page_number != 0) {
                    for (int j = lastnode; j > position; --j) {
                        leafnode->ridArray[j] = leafnode->ridArray[j - 1];
                        leafnode->keyArray[j] = leafnode->keyArray[j - 1];
                    }
                }
                // just empty
                leafnode->ridArray[position] = ridKeyPair.rid;
                leafnode->keyArray[position] = ridKeyPair.key;
            }
            leafOccupancy++;
            bufMgr->unPinPage(file, pageId, true);
            return returned;
        }
        else {
            Page *pg;
            bufMgr->readPage(file, pageId, pg);
            NonLeafNodeInt *nonLeafnode = (NonLeafNodeInt*) pg;

            int position = 0;

            while (position < INTARRAYNONLEAFSIZE && nonLeafnode->pageNoArray[position + 1] != 0 && nonLeafnode->keyArray[position]<=ridKeyPair.key)
                ++position;
//            /**
//             * first node???
//             */
//            if (nonLeafnode->pageNoArray[position] == 0){ // an empty index file. i.e. the root level is 0 but it is the "leaf status"
//                Page *newpg;
//                PageId newID;
//                bufMgr->allocPage(file,newID,newpg);
//
//                LeafNodeInt *firstLeafNode = (LeafNodeInt*) newpg;
//                memset(firstLeafNode, 0, sizeof(LeafNodeInt));
//                firstLeafNode->keyArray[0] = ridKeyPair.key;
//                firstLeafNode->ridArray[0] = ridKeyPair.rid;
//
//                nonLeafnode->pageNoArray[0] = newID;
//                firstLeafNode->rightSibPageNo = 0; // maybe useless todo: delete it?
//
//                bufMgr->unPinPage(file, newID, true);
//                bufMgr->unPinPage(file, pageId, true); // first visit the root, so it is true. !!! debug for long time...
//                NewNode temp = {0,0};
//                return temp;
//            }

            /**
             * recursive calls
             */
            bufMgr->unPinPage(file, pageId, false);
            NewNode newnode = insertHelper(nonLeafnode->pageNoArray[position] , ridKeyPair, nonLeafnode->level); // if the level is one, the lower level is leaf node
            // since the splited non-leaf is newly created, thus we can define the level of the new ones by the condition

            /**
             * handle the new node returned
             */
            NewNode returned = {0,0}; // NOTE: the returned page is not the one that is pushed
            if (newnode.pgId != 0){ // a new page returned
                bufMgr->readPage(file,pageId,pg);
                nonLeafnode = (NonLeafNodeInt*) pg;

                int lastnode = position;
                std::cout<<position<<"line357\n";
                while (lastnode < INTARRAYNONLEAFSIZE && nonLeafnode->pageNoArray[lastnode+1]!= 0)
                    lastnode++;
                //std::cout<<lastnode<<" seg test 368\n";
                if (lastnode < INTARRAYNONLEAFSIZE){
                    for (int i = lastnode; i > position ; --i) {
                        nonLeafnode->keyArray[i] = nonLeafnode->keyArray[i-1];
                        nonLeafnode->pageNoArray[i + 1] = nonLeafnode->pageNoArray[i];
                    }
                    //std::cout<<"seg test 374\n";
                    nonLeafnode->keyArray[position] = newnode.key; std::cout<<newnode.key<<"line400\n";
                    nonLeafnode->pageNoArray[position + 1] = newnode.pgId;
                } else{
                    // do something split the nonleafnode
                    //std::cout<<position<<" line369\n";
                    returned = nonLeafSpliter(position, nonLeafnode, newnode);
                }

                bufMgr->unPinPage(file,pageId, true);
                nodeOccupancy++;
            }
            return returned;
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

    const void BTreeIndex::startScan(const void* lowValParm,
                                     const Operator lowOpParm,
                                     const void* highValParm,
                                     const Operator highOpParm)
    {
//    if (scanExecuting == true){ //todo: return or throwexception
//        return ;
//    }
        scanExecuting = true;

        if ((lowOpParm !=GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)){
            throw BadOpcodesException();
        }

        this->lowOp = lowOpParm;
        this->highOp = highOpParm;

        this->lowValInt = *((int*) lowValParm);
        this->highValInt = *((int*) highValParm);

        if (lowValInt > highValInt){
            throw BadScanrangeException();
        }

        // start scan
        currentPageNum = rootPageNum;
        bufMgr->readPage(file, currentPageNum, currentPageData);

        NonLeafNodeInt* findLeaf;
        int level;

        do{
            findLeaf = (NonLeafNodeInt*) currentPageData;
            level = findLeaf->level;
            std::cout<<"Line 446: "<< rootPageNum<<" "<<findLeaf->pageNoArray[0] << "\n";
            int i = 0;
            while (i < INTARRAYNONLEAFSIZE && findLeaf->pageNoArray[i+1] != 0 && lowValInt >= findLeaf->keyArray[i])
                i++;
            std::cout<<"Line 446: "<< i<<" "<<findLeaf->pageNoArray[i] << "\n";
            PageId nextLevel = findLeaf->pageNoArray[i];
            bufMgr->unPinPage(file, currentPageNum, false);
            bufMgr->readPage(file, nextLevel, currentPageData);
            currentPageNum = nextLevel;
        } while (level != 1);
        std::cout<<"finish???\n";
        this->nextEntry = 0;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId& outRid)
    {
        if (scanExecuting == false){
            throw ScanNotInitializedException();
        }

        LeafNodeInt *leafScan;

        while(1){
            leafScan = (LeafNodeInt*) currentPageData;
            if(nextEntry >= INTARRAYLEAFSIZE || leafScan->ridArray[nextEntry].page_number == 0){
                PageId nextLeaf = leafScan->rightSibPageNo;
                std::cout<<"Line 491 "<< nextLeaf<<"\n";
                bufMgr->unPinPage(file,currentPageNum, false);
                if (nextLeaf == 0){ // done
                    throw IndexScanCompletedException();
                }
                currentPageNum = nextLeaf;
                bufMgr->readPage(file, currentPageNum, currentPageData);
                nextEntry = 0;
                continue;
            }

            // not in range
            if ((lowOp == GT && leafScan->keyArray[nextEntry] <= lowValInt) ||
                (lowOp == GTE && leafScan->keyArray[nextEntry] < lowValInt)){
                nextEntry ++;
                continue;
            }

            // done
            if ((highOp == LT && leafScan->keyArray[nextEntry] >= highValInt) ||
                (highOp == LTE && leafScan->keyArray[nextEntry] > highValInt))
                throw IndexScanCompletedException();

            outRid = leafScan->ridArray[nextEntry];
            nextEntry++;
            return;
        }

    }

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan()
    {
        if (scanExecuting == false)
            throw ScanNotInitializedException();
        scanExecuting = false;

        try {
            bufMgr->unPinPage(file, currentPageNum, false);
        } catch (PageNotPinnedException exception){
        } catch (HashNotFoundException exception1){
        }
    }

}



