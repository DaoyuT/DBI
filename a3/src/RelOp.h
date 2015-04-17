#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <pthread.h>

class RelationalOp {
	public:
  	// blocks the caller until the particular relational operator 
  	// has run to completion
  	virtual void WaitUntilDone ();

  	// tell us how much internal memory the operation can use
  	virtual void Use_n_Pages (int n) = 0;

	protected:
  	pthread_t thread;
  	static int create_joinable_thread(pthread_t *thread, void *(*start_routine) (void *), void *arg);
};

class SelectFile : public RelationalOp { 

	private:
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	int numOfPages;

	public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~SelectFile();
	static void * HelperRun(void *context);
	void StartRun();
};

class SelectPipe : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe;
	CNF *selOp;
	Record *literal;
	int numOfPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~SelectPipe();
	static void * HelperRun(void *context);
	void StartRun();
};

class Project : public RelationalOp { 
	private:
	Pipe *inPipe, *outPipe;
	int *keepMe;
	int numAttsInput, numAttsOutput;
	int numOfPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~Project();
	static void * HelperRun(void *context);
	void StartRun();
};

class Join : public RelationalOp { 
	private:
	Pipe *inPipeL, *inPipeR, *outPipe;
	CNF *selOp;
	Record *literal;
	int numOfPages;

	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~Join();
	static void * HelperRun(void *context);
	void StartRun();
	void SortMergeJoin(OrderMaker &sortOrderL, OrderMaker sortOrderR);
	void BlockNestedJoin();
};

class DuplicateRemoval : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe;
	Schema *mySchema;
	int numOfPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~DuplicateRemoval();
	static void * HelperRun(void *context);
	void StartRun();
};

class Sum : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe;
	Function *computeMe;
	int numOfPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~Sum();
	static void * HelperRun(void *context);
	void StartRun();
};

class GroupBy : public RelationalOp {
	private:
	Pipe *inPipe, *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;
	int numOfPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~GroupBy();
	static void * HelperRun(void *context);
	void StartRun();
	void ApplyFunction(Record &rec, int &intResult, double &doubleResult, Type &resType);
	void PackResultInRecord(Type resType, int &intResult, double &doubleResult, Record &groupRec, Record &resultRec);
};

class WriteOut : public RelationalOp {
	private:
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;
	int numOfPages;
	
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

	~WriteOut();
	static void * HelperRun(void *context);
	void StartRun();
};

#endif