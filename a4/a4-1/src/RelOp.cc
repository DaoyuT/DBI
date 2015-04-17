#include "RelOp.h"
#include "BigQ.h"

#include <vector>
#include <sstream>
#include <string>

void RelationalOp::WaitUntilDone() {
  	pthread_join (thread, NULL);
}

int RelationalOp::create_joinable_thread(pthread_t *thread,
                                         void *(*start_routine) (void *), void *arg) {
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  int rc = pthread_create(thread, &attr, start_routine, arg);
  //int rc = pthread_create(thread, NULL, start_routine, arg);
  pthread_attr_destroy(&attr);
  return rc;
}

SelectFile::~SelectFile(){
	//cout<<"In SelectFile::~SelectFile"<<endl;

	this->inFile = NULL;
	this->outPipe = NULL;
	this->selOp = NULL;
	this->literal = NULL;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	//cout<<"In SelectFile::Run"<<endl;

	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, SelectFile::HelperRun, (void *) this);
}

void * SelectFile::HelperRun(void *context) {
	//cout<<"In SelectFile::HelperRun"<<endl;
	((SelectFile *) context)->StartRun();
}

void SelectFile::StartRun(){
	//cout<<"In SelectFile::StartRun"<<endl;

	Record tempRec;

	while(inFile->GetNext(tempRec, *selOp, *literal)){
		outPipe->Insert(&tempRec);
	}

	outPipe->ShutDown();
}

void SelectFile::WaitUntilDone (){
	//cout<<"In SelectFile::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}

void SelectFile::Use_n_Pages (int n) {
	//cout<<"In SelectFile::Use_n_Pages"<<endl;
	this->numOfPages = n;
}


SelectPipe::~SelectPipe(){
	//cout<<"In SelectPipe::~SelectPipe"<<endl;

	this->inPipe = NULL;
	this->outPipe = NULL;
	this->selOp = NULL;
	this->literal = NULL;
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
	//cout<<"In SelectPipe::Run"<<endl;

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, SelectPipe::HelperRun, (void *) this);
}

void * SelectPipe::HelperRun(void *context){
	//cout<<"In SelectPipe::HelperRun"<<endl;
	((SelectPipe *) context)->StartRun();
}

//TODO: not tested
void SelectPipe::StartRun(){
	//cout<<"In SelectPipe::StartRun"<<endl;

	Record tempRec;
	ComparisonEngine comp;

	while(inPipe->Remove(&tempRec)){
		if(comp.Compare(&tempRec, literal, selOp)){
			outPipe->Insert(&tempRec);
		}
	}

	outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone (){
	//cout<<"In SelectPipe::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}
	
void SelectPipe::Use_n_Pages (int n){
	//cout<<"In SelectPipe::Use_n_Pages"<<endl;
	this->numOfPages = n;
}


Project::~Project(){
	//cout<<"In Project::~Project"<<endl;

	this->inPipe = NULL;
	this->outPipe = NULL;
	this->keepMe = NULL;
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
	//cout<<"In Project::Run"<<endl;

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, Project::HelperRun, (void *) this);
}

void * Project::HelperRun(void *context){
	//cout<<"In Project::HelperRun"<<endl;
	((Project *) context)->StartRun();
}

void Project::StartRun(){
	//cout<<"In Project::StartRun"<<endl;

	Record tempRec;
	ComparisonEngine comp;

	while(inPipe->Remove(&tempRec)){
		tempRec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&tempRec);
	}

	outPipe->ShutDown();	
}

void Project::WaitUntilDone (){
	//cout<<"In Project::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}
	
void Project::Use_n_Pages (int n){
	//cout<<"In Project::Use_n_Pages"<<endl;
	this->numOfPages = n;
}


Join::~Join(){
	//cout<<"In Join::~Join"<<endl;
	this->inPipeL = NULL;
	this->inPipeR = NULL;
	this->outPipe = NULL;
	this->selOp = NULL;
	this->literal = NULL;
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	//cout<<"In Join::Run"<<endl;

	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, Join::HelperRun, (void *) this);
}

void * Join::HelperRun(void *context){
	//cout<<"In Join::HelperRun"<<endl;
	((Join *) context)->StartRun();
}

//TODO: try to modify block nested join to make it more efficient
void Join::StartRun(){
	//cout<<"In Join::StartRun"<<endl;

	OrderMaker sortOrderL, sortOrderR;
	int isSortOrderExist = selOp->GetSortOrders(sortOrderL, sortOrderR);

	if(isSortOrderExist){
		//do a sort-merge join
		SortMergeJoin(sortOrderL, sortOrderR);
	}else{
		//nested loop to do a cross product of both input pipes
		BlockNestedJoin();
	}
	
	outPipe->ShutDown();
}

void Join::SortMergeJoin(OrderMaker &sortOrderL, OrderMaker sortOrderR){
	//cout<<"In Join::SortMergeJoin"<<endl;

	Record tempRecL, tempRecR, tempMergeRec;
	ComparisonEngine comp; 

	//cout<<"Join::numOfPages: "<<numOfPages<<endl;

	Pipe outPipeL(numOfPages);
	Pipe outPipeR(numOfPages);
	BigQ bigQL(*inPipeL, outPipeL, sortOrderL, numOfPages);
	BigQ bigQR(*inPipeR, outPipeR, sortOrderR, numOfPages);

	int isEmptyOutPipeL = 0, isEmptyOutPipeR = 0;

	if(!outPipeL.Remove(&tempRecL)){
		isEmptyOutPipeL = 1;
	}
	if(!outPipeR.Remove(&tempRecR)){
		isEmptyOutPipeR = 1;
	}

	const int numAttsL = tempRecL.GetNumAtts();
	const int numAttsR = tempRecR.GetNumAtts();
	const int totalAtts = numAttsL + numAttsR;
	int attsToKeep[totalAtts];

	int count=0;
	for(int i=0; i<numAttsL; i++){
			attsToKeep[count++] = i; 
	}
	for(int i=0; i<numAttsR; i++){
		attsToKeep[count++] = i; 
	}

	vector<Record> bufferL;
	vector<Record> bufferR;
	bufferL.reserve(1000);
	bufferR.reserve(1000);

	//delete this - start
	/*	Attribute IA = {"int", Int};
		Attribute SA = {"string", String};
		Attribute DA = {"double", Double};

		int pAtts = 9;
		int psAtts = 5;
		int liAtts = 16;
		int oAtts = 9;
		int sAtts = 7;
		int cAtts = 8;
		int nAtts = 4;
		int rAtts = 3;

		int outAtts = sAtts + psAtts;
		Attribute ps_supplycost = {"ps_supplycost", Double};
		Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
		Schema join_sch ("join_sch", outAtts, joinatt);*/
	//delete this - end

	while(!isEmptyOutPipeL && !isEmptyOutPipeR){
		//cout<<"comparing records from left and right pipe"<<endl;
		if(0 > comp.Compare(&tempRecL, &sortOrderL, &tempRecR, &sortOrderR)){
			//cout<<"recL < recR"<<endl;
			//move to next record in left pipe
			if(!outPipeL.Remove(&tempRecL)){
				isEmptyOutPipeL = 1;
			}
		}else if(0 < comp.Compare(&tempRecL, &sortOrderL, &tempRecR, &sortOrderR)){
			//cout<<"recL > recR"<<endl;
			//move to next record in right pipe
			if(!outPipeR.Remove(&tempRecR)){
				isEmptyOutPipeR = 1;
			}
		}else{ //records match as per sort order
			//cout<<"recL = recR"<<endl;
			bufferL.push_back(tempRecL);
			bufferR.push_back(tempRecR);

			//fetch all records from left pipe which match current sort order
			Record recL;
			recL.Copy(&tempRecL);
			if(outPipeL.Remove(&tempRecL)){
				//cout<<"fetching matching records from left pipe"<<endl;
				while(!comp.Compare(&tempRecL, &recL, &sortOrderL)){
					//cout<<"adding record to left buffer"<<endl;
					bufferL.push_back(tempRecL);
					if(!outPipeL.Remove(&tempRecL)){
						isEmptyOutPipeL = 1;
						break;
					}
				}
			}else{
				isEmptyOutPipeL = 1;
			}

			//fetch all records from right pipe which match current sort order
			Record recR;
			recR.Copy(&tempRecR);
			if(outPipeR.Remove(&tempRecR)){
				//cout<<"fetching matching records from right pipe"<<endl;
				while(!comp.Compare(&tempRecR, &recR, &sortOrderR)){
					//cout<<"adding record to right buffer"<<endl;
					bufferR.push_back(tempRecR);
					if(!outPipeR.Remove(&tempRecR)){
						isEmptyOutPipeR = 1;
						break;
					}
				}
			}else{
				isEmptyOutPipeR = 1;
			}

			int sizeL = bufferL.size();
			int sizeR = bufferR.size();
			//cout<<"leftBufferSize: "<<sizeL<<" and rightBufferSize: "<<sizeR<<endl;

			//cout<<"joining records from left and right buffers"<<endl;
			//join left and right buffer records which satisfy given cnf
			for(int i=0; i<sizeL; i++){
				for(int j=0; j<sizeR; j++) {
					if(comp.Compare(&bufferL[i], &bufferR[j], literal, selOp)){
						//cout<<"merging left and right record"<<endl;
						tempMergeRec.MergeRecords(&bufferL[i], &bufferR[j], numAttsL, numAttsR, attsToKeep, totalAtts, numAttsL);
						//tempMergeRec.Print(&join_sch);
						outPipe->Insert(&tempMergeRec);
					}
				}
			}

			bufferL.clear();
			bufferR.clear();
		}
	}
}

//TODO: 1) not tested, 2) do it efficiently by loading blocks of data instead of all data
void Join::BlockNestedJoin(){
	//cout<<"In Join::BlockNestedJoin"<<endl;

	Record tempRecL, tempRecR, tempMergeRec;
	ComparisonEngine comp; 

	if(!inPipeL->Remove(&tempRecL)){
		return;
	}
	if(!inPipeR->Remove(&tempRecR)){
		return;
	}

	const int numAttsL = tempRecL.GetNumAtts();
	const int numAttsR = tempRecR.GetNumAtts();
	const int totalAtts = numAttsL + numAttsR;
	int attsToKeep[totalAtts];

	int count=0;
	for(int i=0; i<numAttsL; i++){
			attsToKeep[count++] = i; 
	}
	for(int i=0; i<numAttsR; i++){
		attsToKeep[count++] = i; 
	}

	vector<Record> bufferL;
	do{
		bufferL.push_back(tempRecL);
	}while(inPipeL->Remove(&tempRecL));

	int sizeL = bufferL.size();

	do{
		for(int i=0; i<sizeL; i++){
			if(comp.Compare(&bufferL[i], &tempRecR, literal, selOp)){
				tempMergeRec.MergeRecords(&bufferL[i], &tempRecR, numAttsL, numAttsR, attsToKeep, totalAtts, numAttsL);
				outPipe->Insert(&tempMergeRec);
			}
		}
	}while(inPipeR->Remove(&tempRecR));

	bufferL.clear();
}

void Join::WaitUntilDone (){
	//cout<<"In Join::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}
	
void Join::Use_n_Pages (int n){
	//cout<<"In Join::Use_n_Pages"<<endl;
	this->numOfPages = n;
}


DuplicateRemoval::~DuplicateRemoval(){
	//cout<<"In DuplicateRemoval::~DuplicateRemoval"<<endl;

	this->inPipe = NULL;
	this->outPipe = NULL;
	this->mySchema = NULL;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	//cout<<"In DuplicateRemoval::Run"<<endl;

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, DuplicateRemoval::HelperRun, (void *) this);
}

void * DuplicateRemoval::HelperRun(void *context){
	//cout<<"In DuplicateRemoval::HelperRun"<<endl;
	((DuplicateRemoval *) context)->StartRun();
}

void DuplicateRemoval::StartRun(){
	//cout<<"In DuplicateRemoval::StartRun"<<endl;

	OrderMaker sortOrder(mySchema);
	Pipe tempOutPipe(numOfPages);
	BigQ bigQ(*inPipe, tempOutPipe, sortOrder, numOfPages);

	ComparisonEngine comp;
	Record prevRec, currRec;
	if(tempOutPipe.Remove(&currRec)){
		prevRec.Copy(&currRec);
		outPipe->Insert(&currRec);

		while(tempOutPipe.Remove(&currRec)){
			if(comp.Compare(&prevRec, &currRec, &sortOrder)){
				prevRec.Copy(&currRec);
				outPipe->Insert(&currRec);
			}
		}	
	}

	outPipe->ShutDown();	
}

void DuplicateRemoval::WaitUntilDone (){
	//cout<<"In DuplicateRemoval::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}
	
void DuplicateRemoval::Use_n_Pages (int n){
	//cout<<"In DuplicateRemoval::Use_n_Pages"<<endl;
	this->numOfPages = n;
}


Sum::~Sum(){
	//cout<<"In Sum::~Sum"<<endl;

	this->inPipe = NULL;
	this->outPipe = NULL;
	this->computeMe = NULL;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	//cout<<"In Sum::Run"<<endl;

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, Sum::HelperRun, (void *) this);
}

void * Sum::HelperRun(void *context){
	//cout<<"In Sum::HelperRun"<<endl;
	((Sum *) context)->StartRun();
}

void Sum::StartRun(){
	//cout<<"In Sum::StartRun"<<endl;

	Record tempRec;
	int tempIntResult, intResult = 0;
	double tempDoubleResult, doubleResult = 0.0;
	Type resType;

	while(inPipe->Remove(&tempRec)){
		
		resType = computeMe->Apply(tempRec, tempIntResult, tempDoubleResult);

		if(Int == resType){
			intResult += tempIntResult;
		}else{
			doubleResult += tempDoubleResult;
		}
	}

	//pack result in a record
    stringstream rec;
    Attribute attr;
    attr.name = "SUM";
    if (Int == resType){
        attr.myType = Int;
        rec << intResult;
    }else{
        attr.myType = Double;
       	rec << doubleResult;
    }
    rec << "|";
    
    Schema tempSchema("temp_schema", 1, &attr);
    tempRec.ComposeRecord(&tempSchema, rec.str().c_str());
    outPipe->Insert(&tempRec);

	outPipe->ShutDown();	
}

void Sum::WaitUntilDone (){
	//cout<<"In Sum::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}
	
void Sum::Use_n_Pages (int n){
	//cout<<"In Sum::Use_n_Pages"<<endl;
	this->numOfPages = n;
}


GroupBy::~GroupBy(){
	//cout<<"In GroupBy::~GroupBy"<<endl;

	this->inPipe = NULL;
	this->outPipe = NULL;
	this->groupAtts = NULL;
	this->computeMe = NULL;
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	//cout<<"In GroupBy::Run"<<endl;

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, GroupBy::HelperRun, (void *) this);
}

void * GroupBy::HelperRun(void *context){
	//cout<<"In GroupBy::HelperRun"<<endl;
	((GroupBy *) context)->StartRun();
}

//TODO: outPipe getting full after inserting just 2 records if pipe.bufferSize is 1, why?
void GroupBy::StartRun(){
	//cout<<"In GroupBy::StartRun"<<endl;

	//sort records from input pipe
	Pipe tempOutPipe(numOfPages);
	BigQ bigQ(*inPipe, tempOutPipe, *groupAtts, numOfPages);
	//BigQ bigQ(*inPipe, *outPipe, *groupAtts, numOfPages);

	ComparisonEngine comp;
	Record tempRec1, tempRec2, resRec;

	if(tempOutPipe.Remove(&tempRec1)){
		//cout<<"GroupBy::Processing record"<<endl;

		int intResult = 0;
		double doubleResult = 0.0;
		Type resType;
		ApplyFunction(tempRec1, intResult, doubleResult, resType);
		//cout<<"intResult: "<<intResult<<" doubleResult: "<<doubleResult<<endl;

		while(tempOutPipe.Remove(&tempRec2)){
			//if(tempRec1.IsEqual(&tempRec2)){
			//cout<<"GroupBy::Processing record"<<endl;
			if(!comp.Compare(&tempRec1, &tempRec2, groupAtts)){
				ApplyFunction(tempRec2, intResult, doubleResult, resType);
				//cout<<"intResult: "<<intResult<<" doubleResult: "<<doubleResult<<endl;
			}else{
				PackResultInRecord(resType, intResult, doubleResult, tempRec1, resRec);
				//resRec.Print(&sum_sch);
				outPipe->Insert(&resRec); //TODO-pipe getting full after inserting just 2 records, check why
				//cout<<"inserted record in outPipe"<<endl;
				//break;

				intResult = 0;
				doubleResult = 0.0;
				ApplyFunction(tempRec2, intResult, doubleResult, resType);
				//cout<<"intResult: "<<intResult<<" doubleResult: "<<doubleResult<<endl;

				tempRec1.Consume(&tempRec2);
			}
		}

		PackResultInRecord(resType, intResult, doubleResult, tempRec1, resRec);
		//resRec.Print(&sum_sch);
		outPipe->Insert(&resRec);

	}

	//cout<<"GroupBy::Shutting down outPipe"<<endl;
	outPipe->ShutDown();	
}

void GroupBy::ApplyFunction(Record &rec, int &intResult, double &doubleResult, Type &resType){
	//cout<<"In GroupBy::ApplyFunction"<<endl;

	int tempIntResult;
	double tempDoubleResult;

	resType = computeMe->Apply(rec, tempIntResult, tempDoubleResult);
	if(Int == resType){
		intResult += tempIntResult;
	}else{
		doubleResult += tempDoubleResult;
	}
}

void GroupBy::PackResultInRecord(Type resType, int &intResult, double &doubleResult, Record &groupRec, Record &resultRec){
	//cout<<"In GroupBy::PackResultInRecord"<<endl;

	//create single attribute record with SUM as attribute
	stringstream recSS;
    Attribute attr;
    attr.name = "SUM";
    if (Int == resType){
        attr.myType = Int;
        recSS << intResult;
    }else{
        attr.myType = Double;
       	recSS << doubleResult;
    }
    recSS << "|";
    
    Record sumRec;
    Schema tempSchema("temp_schema", 1, &attr);
    sumRec.ComposeRecord(&tempSchema, recSS.str().c_str());
    //sumRec.Print(&tempSchema);

    const int numAtts = groupAtts->GetNumAtts()+1; //+1 for the new SUM attribute
	int attsToKeep[numAtts];
	//cout<<"PackResultInRecord::numAtts: "<<numAtts<<endl;

	attsToKeep[0] = 0;
	for(int i=1; i<numAtts; i++){
		attsToKeep[i] = (groupAtts->GetWhichAtts())[i-1]; 
		//cout<<"PackResultInRecord::which attr to keep: "<<(groupAtts->GetWhichAtts())[i-1]<<" "<<endl;
	}

	/*Attribute DA = {"SUM", Double};
	Attribute s_nationkey = {"nationkey", Int};
	Attribute sumAtt[] = {DA, s_nationkey};
	Schema sum_sch ("sum_sch", 2, sumAtt);*/

	//merge sum record and attributes from the current group by record into result record
	resultRec.MergeRecords(&sumRec, &groupRec, 1, groupRec.GetNumAtts(), attsToKeep, numAtts, 1);
	//resultRec.Print(&sum_sch);
}

void GroupBy::WaitUntilDone (){
	//cout<<"In GroupBy::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}
	
void GroupBy::Use_n_Pages (int n){
	//cout<<"In GroupBy::Use_n_Pages"<<endl;
	this->numOfPages = n;
}


WriteOut::~WriteOut(){
	//cout<<"In WriteOut::~WriteOut"<<endl;

	this->inPipe = NULL;
	this->outFile = NULL;
	this->mySchema = NULL;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	//cout<<"In WriteOut::Run"<<endl;

	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	this->numOfPages = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, WriteOut::HelperRun, (void *) this);
}

void * WriteOut::HelperRun(void *context){
	//cout<<"In WriteOut::HelperRun"<<endl;
	((WriteOut *) context)->StartRun();
}

void WriteOut::StartRun(){
	//cout<<"In WriteOut::StartRun"<<endl;

	Record tempRec;
	ostringstream os;

	while(inPipe->Remove(&tempRec)){
      //tempRec.Print(mySchema);
      tempRec.Print(mySchema, os);
	}

	fputs(os.str().c_str(), outFile);
}

void WriteOut::WaitUntilDone (){
	//cout<<"In WriteOut::WaitUntilDone"<<endl;
	RelationalOp::WaitUntilDone();
}

void WriteOut::Use_n_Pages (int n) {
	//cout<<"In WriteOut::Use_n_Pages"<<endl;
	this->numOfPages = n;
}