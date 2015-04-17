#include "SortedDBFile.h"
#include "DBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

//TODO - update as per project requirement

SortedDBFile::SortedDBFile () {
  GenericDBFile::file_type = sorted;
  pipe_in = NULL;
  pipe_out = NULL;
  big_q = NULL;

  is_query_so_created = false;
  is_query_so_exist = false;
  is_bs_done = false;
  is_query_so_rec_exist = false;
}

SortedDBFile::~SortedDBFile () {
    delete pipe_in;
    delete pipe_out;
    delete big_q;
}

int SortedDBFile::Open (char *f_path)
{
  int f_type;
  string meta_file_name;
  meta_file_name.append(f_path);
  //meta_file_name.append(META_FILE_PREFIX);
  meta_file_name.append(".meta");
  ifstream meta_file; 
  meta_file.open(meta_file_name.c_str());
  if (!meta_file) {
    cerr << "Not able to open meta file"<<endl;
    return 0;
  }
  meta_file >> f_type;
  meta_file >> runlen;
  meta_file >> sortorder;
  meta_file.close();

  filepath.append(f_path);
  db_file.Open(1, f_path);
  writingMode = false;
  MoveFirst();

  // read in the second few bits, which is the number of records
  lseek (db_file.GetMyFilDes(), sizeof (off_t), SEEK_SET);
  read (db_file.GetMyFilDes(), &num_rec, sizeof (off_t));

  return 1;
}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup){
  //create a meta file and write file type in it
  string meta_file_name;
  meta_file_name.append(f_path);
  //meta_file_name.append(META_FILE_PREFIX);
  meta_file_name.append(".meta");
  ofstream meta_file; 
  meta_file.open(meta_file_name.c_str());
  if (!meta_file) {
    cerr << "Not able to create meta file"<<endl;
    return 0;
  }
  meta_file << f_type << endl;
  SortInfo sortinfo = *((SortInfo *) startup);
  runlen = sortinfo.runLength;
  meta_file << runlen << endl;
  sortorder = *((OrderMaker *) sortinfo.myOrder);
  meta_file << sortorder; 
  meta_file.close();

  cout<<"Run length: "<<runlen<<endl;
  cout<<"Sort Order: "<<endl;
  sortorder.Print();

  filepath.append(f_path);
  db_file.Open(0, f_path);
  writingMode = true;

  cout<<"Created sorted db file at "<<f_path<<endl;
  return 1;
}

//TODO: use File::AppendPage in this function after below code works fine
void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
  writingMode = true;
  
  if (NULL == big_q) {
      pipe_in = new Pipe(pipe_buffer_size);
      pipe_out = new Pipe(pipe_buffer_size);
      big_q = new BigQ(*pipe_in, *pipe_out, sortorder, runlen);
  }

  //open file from loadpath
  FILE *input_file = fopen(loadpath, "r");
  if (!input_file){
    //#ifdef verbose
    cerr << "Not able to open input file " << loadpath << endl;
    //#endif
    exit(1);
  }

  Record temp;
  while (temp.SuckNextRecord (&f_schema, input_file)){ 
      pipe_in->Insert(&temp);
  }

  fclose(input_file);
}


void SortedDBFile::MoveFirst () {
  if(writingMode){
    MergeDifferential();
  }

  writingMode = false;
  curr_page_index = 0;

  if(0 != db_file.GetLength()){
    db_file.GetPage(&curr_page, curr_page_index);
  }else{
    curr_page.EmptyItOut();
  }
  
  curr_page_index++;
}

void SortedDBFile::Add (Record &rec) {
  cout<<"Add record in sorted dbfile"<<endl;
  if (writingMode) {
    //cout<<"in if"<<endl;
      pipe_in->Insert(&rec);
  }else { //reading mode
    //cout<<"in else"<<endl; 
      writingMode = true;
      if (NULL == big_q) {
        cout<<"creating pipes and bigq"<<endl;
        pipe_in = new Pipe(pipe_buffer_size);
        pipe_out = new Pipe(pipe_buffer_size);
        big_q = new BigQ(*pipe_in, *pipe_out, sortorder, runlen);
      }
      pipe_in->Insert(&rec);
    }
    cout<<"record added in sorted dbfile<<endl";
}

int SortedDBFile::GetNext (Record &fetchme) {
  if(writingMode){
    MergeDifferential();
    MoveFirst();
  }

  writingMode = false;

  if (curr_page.NumRecords() == 0){
    //if reached at the end of current page then fetch next page if current page is not last page
    if (curr_page_index < db_file.GetLength()-1){
      db_file.GetPage(&curr_page, curr_page_index);
      curr_page_index++;
    }
    else{
      return 0;
    }
  }
  
  return curr_page.GetFirst(&fetchme);
}

//TODO: implement binary search using query
int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  //OrderMaker query(&sortorder, &cnf);

  ComparisonEngine comp;
  while (GetNext(fetchme)){
    if (comp.Compare(&fetchme, &literal, &cnf)){
      return 1;
    }
  }

  return 0;
}

/*//TODO: not working as expected
int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  //create query ordermaker if not already createds
  if(!is_query_so_created){
    OrderMaker qom(&sortorder, &cnf);
    query = qom;
    is_query_so_created = true;
    //if number of attribures in query ordermaker is 0 then no sort order matching with cnf exist
    if(query.GetNumAtts() != 0){
      is_query_so_exist = true;
      cout<<"is_query_so_exist: "<<is_query_so_exist<<endl;
    } 
  }

  ComparisonEngine comp;
  if(!is_query_so_exist){
    // if there is no query sort order then no binary search required, just scan the records in file
    while (GetNext(fetchme)){
      if (comp.Compare(&fetchme, &literal, &cnf)){
          return 1;
      }
    }
  }else{
    int is_rec_exit = 0;
    if(!is_bs_done){ //if binary search is not done, then do it
      cout<<"starting binary search"<<endl;
      is_query_so_rec_exist = BinarySearch(fetchme, literal, query, curr_page_index-1, db_file.GetLength()-2);
      is_bs_done = true;
    }else{ //else fetch the next record from the file
      is_rec_exit = GetNext(fetchme);
    }

    cout<<"is_query_so_rec_exist: "<<is_query_so_rec_exist<<" "<<"is_rec_exit: "<<is_rec_exit<<endl;
    //if no record found in binary search or there is no record left to read from file then return 0
    if(!is_query_so_rec_exist || !is_rec_exit){
      return 0;
    }else{
      do{
        if (comp.Compare(&fetchme, &literal, &cnf)){
          return 1;
        }
      }while (GetNext(fetchme));
    }
  }

  return 0;
}*/

int SortedDBFile::BinarySearch(Record &fetchme, Record &literal, OrderMaker &query, off_t startInd, off_t endInd){
  ComparisonEngine comp;
  Page temp_page;
  cout<<"startInd: "<<startInd<<" endInd: "<<endInd<<endl;

  off_t mid = 0;
  int comp_rc = 0;
  while(startInd <= endInd){
    mid  = (startInd + endInd)/2;
    cout<<"mid: "<<mid<<endl;

    if(mid == curr_page_index && curr_page.NumRecords() != 0){
      temp_page = curr_page;
    }else{
      cout<<"loading page "<<mid<<endl;
      db_file.GetPage(&temp_page, mid);
    }

    if(temp_page.GetFirst(&fetchme)){
      comp_rc = comp.Compare(&fetchme, &literal, &query);
      cout<<"comp_rc: "<<comp_rc<<endl;

      if(comp_rc < 0){
        startInd = mid + 1;
      }else if (comp_rc > 0){
        endInd = mid - 1;
      }else{
        if(--mid > 0){
          //if matching record found then backtrace file to find first record matching query sort order
          pair<off_t, int> res = BacktraceFile(fetchme, literal, query, mid, temp_page);
          curr_page_index = res.first;
          curr_page_index++;
          if(res.second){
            db_file.GetPage(&curr_page, res.first);
          }else{
            curr_page = temp_page;
          } 
        }
        return 1;
      }
    }
  }

  if(comp_rc < 0){
    cout<<"loading page "<<mid<<endl;
    db_file.GetPage(&temp_page, mid);
  }else if (comp_rc > 0 && mid > 0){
    mid = mid - 1;
    cout<<"loading page "<<mid<<endl;
    db_file.GetPage(&temp_page, mid);
  }
  
  while(temp_page.GetFirst(&fetchme)){
    int rc = comp.Compare(&fetchme, &literal, &query); 
    cout<<"comparing records: "<<rc<<endl;
    if(!rc){
      curr_page = temp_page;
      curr_page_index = mid;
      curr_page_index++;
      return 1;
    }
  }

  return 0;
}

pair<off_t, int> SortedDBFile::BacktraceFile(Record &fetchme, Record &literal, OrderMaker &query, off_t pageno, Page temp_page){
  //Page temp_page;
  Record temp_rec;
  ComparisonEngine comp;
  int is_full = 1;

  while(pageno > 0){
    db_file.GetPage(&temp_page, pageno);
    if(temp_page.GetFirst(&temp_rec)){
      if(!comp.Compare(&temp_rec, &literal, &query)){ //first record of page match query sort order
        fetchme = temp_rec;
        pageno--;
      }else{
        while(temp_page.GetFirst(&temp_rec)){
          if(!comp.Compare(&temp_rec, &literal, &query)){
           fetchme = temp_rec;
           is_full = 0;
           break;
          }
        }
        pageno++;
      }
    }
  }

  return make_pair(pageno, is_full) ;
} 

int SortedDBFile::Close () {
  //if there is anything that is not written to file yet, write it
  MergeDifferential();
  cout<<"merge differential complete"<<endl;
  //write number of records after number of pages
  lseek (db_file.GetMyFilDes(), sizeof (off_t), SEEK_SET);
  write (db_file.GetMyFilDes(), &num_rec, sizeof (off_t));
  cout<<"num_rec written to file"<<endl;
  
  //close the file
  int file_len = db_file.Close();
  cout<<"file closed"<<endl;
  return 1;
}

//merge the records in big_q to current records in sorted file
void SortedDBFile::MergeDifferential(void) {
  writingMode = false;
  
  if(NULL != big_q){
    //add records from the file to in_pipe
    cout<<"Number of pages in sorted file before merge: "<<db_file.GetLength()<<endl;
    cout<<"Number of records in sorted file before merge: "<<num_rec<<endl;
    Record temp;
    if(db_file.GetLength() != 0){
      MoveFirst();
      while(GetNext(temp)){
        pipe_in->Insert(&temp);
      }
    }
    //close in_pipe
    pipe_in->ShutDown();

    //close file and recreate empty file
    cout<<"Closing sorted db file"<<endl;
    int file_len = db_file.Close();
    cout<<"Number of pages in sorted file before merge: "<<file_len<<endl;
    // reopen file in create mode, starting from 0.
    cout << "Re-opening file in create mode: " << filepath << endl;
    db_file.Open(0, const_cast<char *>(filepath.c_str()));
    curr_page_index = 0;
    num_rec = 0;
    cout<<"Current page index in new sorted file before merge: "<<curr_page_index<<endl;
    cout<<"Number of pages in new sorted file before merge: "<<db_file.GetLength()<<endl;

    //write all records from out pipe to file
    curr_page.EmptyItOut();
    while(pipe_out->Remove(&temp)){
      if(!curr_page.Append(&temp)){
        db_file.AddPage(&curr_page, curr_page_index++);
        curr_page.EmptyItOut();

        //write current record to page
        curr_page.Append(&temp);
      }
      num_rec++;
    }
    //write last page to file on disk
    db_file.AddPage(&curr_page, curr_page_index);
    curr_page.EmptyItOut();
    cout<<"Current page index in new sorted file after merge: "<<curr_page_index<<endl;
    cout<<"Number of pages in new sorted file after merge: "<<db_file.GetLength()<<endl;
    cout<<"Number of records in new sorted file after merge: "<<num_rec<<endl;

    delete pipe_in;
    delete pipe_out;
    delete big_q;
    cout<<"deleted pointers"<<endl;

    pipe_in = NULL;
    pipe_out = NULL;
    big_q = NULL;
    cout<<"pointers set to null"<<endl;
  }

  //move to first record in sorted file
  //MoveFirst();
}