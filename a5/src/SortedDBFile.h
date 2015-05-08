#ifndef SORTED_DBFILE_H
#define SORTED_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include <string>

class SortedDBFile : public GenericDBFile { 
private:
  OrderMaker sortorder, query;
  int runlen;
  string filepath;
  bool is_query_so_created, is_query_so_exist, is_bs_done, is_query_so_rec_exist;

  static const int pipe_buffer_size = 100;
  Pipe *pipe_in;
  Pipe *pipe_out;
  BigQ *big_q;

public:
  SortedDBFile ();
  ~SortedDBFile(); 

  int Create (char *fpath, fType file_type, void *startup);
  int Open (char *fpath);
  void Load (Schema &myschema, char *loadpath);
  int Close ();

  void MoveFirst ();
  void Add (Record &addme);
  int GetNext (Record &fetchme);
  int GetNext (Record &fetchme, CNF &cnf, Record &literal);
  void MergeDifferential(void);
  int BinarySearch(Record &fetchme, Record &literal, OrderMaker &query, off_t startInd, off_t endInd);
  pair<off_t, int> BacktraceFile(Record &fetchme, Record &literal, OrderMaker &query, off_t pageno, Page temp_page);
};

#endif