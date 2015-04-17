#ifndef TEST1_H
#define TEST1_H
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include "File.h"
#include "DBFile.h"
#include "test.h"
#include "gtest/gtest.h"

using namespace std;

// make sure that the file path/dir information below is correct
char *dbfile_dir = "../dbfile/"; // dir where binary heap files should be stored
char *tpch_dir ="../data/"; // dir where dbgen tpch files (extension *.tbl) can be found
char *catalog_path = "catalog"; // full path of the catalog file
vector<string> rel_names({"nation","supplier","partsupp","part","customer","orders","region","lineitem"});
char *rel_name = "nation";
relation *rel = new relation (rel_name, new Schema (catalog_path, rel_name), dbfile_dir);
char tbl_path[100]; // construct path of the tpch flat text file
DBFile dbfile;

class DbiEnvironment : public testing::Environment
{
private:
	char *rel_name;
public:
	DbiEnvironment(char *r){
		rel_name = r;
	}
    virtual void SetUp()
    {
    	rel = new relation (rel_name, new Schema (catalog_path, rel_name), dbfile_dir);
		// construct path of the tpch flat text file
		sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
        cout << "DbiEnvironment SetUP" << endl;
    }
    virtual void TearDown()
    {
        
		delete rel;
        cout << "DbiEnvironment TearDown" << endl;
    }
};

TEST(Create, CreateDbFile){
	EXPECT_EQ(1, dbfile.Create(rel->path(), heap, NULL));
	EXPECT_EQ(1, dbfile.Close());
}

TEST(Open, OpenDbFile){
	EXPECT_EQ(1, dbfile.Create(rel->path(), heap, NULL));
	EXPECT_EQ(1, dbfile.Close());

	EXPECT_EQ(1, dbfile.Open(rel->path()));
	EXPECT_EQ(1, dbfile.Close());
}

TEST(Close, CloseDbFile){
	EXPECT_EQ(1, dbfile.Create(rel->path(), heap, NULL));
	EXPECT_EQ(1, dbfile.Close());
}

int CountRec(Schema &f_schema, char *loadpath){
	int recCount = 0;
	//Open the file
	FILE *input_file = fopen(loadpath, "r");
	if (!input_file){
		//#ifdef verbose
		cerr << "Not able to open input file " << loadpath << endl;
		//#endif
		exit(1);
	}
	//Count the num of recs in tbl file (input file)
	Record temp;
	while (temp.SuckNextRecord(&f_schema, input_file)){
		recCount++;
	}
	fclose(input_file);
	return recCount;
}

//Create dbfile and Load it from tbl file, then check if numbers of records of them are the same
TEST(Load, CountRecords) {
	//Create Load and Close
	dbfile.Create (rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.Close ();

	// num of recs in tbl == num of recs in dbfile
	EXPECT_EQ( CountRec(*(rel->schema ()) ,tbl_path)  , dbfile.GetRecNum());
}

//Positive Test Case
TEST(MoveFirst, ComareFirstRecords){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();
	Record first_dbfile;
	bool rec_in_dbfile = false;
	if(dbfile.GetNext(first_dbfile)) rec_in_dbfile = true;
	dbfile.Close();

	FILE *input_file = fopen(tbl_path, "r");
	Record first_tblfile;
	bool rec_in_tblfile = false;
	if(first_tblfile.SuckNextRecord(rel->schema(), input_file)) rec_in_tblfile = true;
	fclose(input_file);

	//first rec in dbfile == first rec in tbl file
	if(rec_in_dbfile && rec_in_tblfile)
		EXPECT_TRUE(first_dbfile.IsEqual(&first_tblfile));
	//first rec == null and first rec = null
	else if(!rec_in_dbfile && !rec_in_tblfile)
		EXPECT_TRUE(1);
	//first rec == null or first rec = null
	else
		EXPECT_TRUE(0);
}

//Negative Test Case
TEST(MoveFirst, CompareFirstAndSecondRecord){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();
	Record rec_dbfile;
	bool rec_in_dbfile = false;
	dbfile.GetNext(rec_dbfile);
	if(dbfile.GetNext(rec_dbfile)) rec_in_dbfile = true;
	dbfile.Close();

	FILE *input_file = fopen(tbl_path, "r");
	Record first_tblfile;
	bool rec_in_tblfile = false;
	if(first_tblfile.SuckNextRecord(rel->schema(), input_file)) rec_in_tblfile = true;
	fclose(input_file);

	//first rec != second rec
	if(rec_in_dbfile && rec_in_tblfile)
		EXPECT_FALSE(rec_dbfile.IsEqual(&first_tblfile));
}

//Check if the first record in dbfile and tbl file match
TEST(Load, CompareFirstRecords){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();
	//dbtemp = first rec in dbfile
	Record dbtemp;
	bool rec_in_dbfile = false;
	if(dbfile.GetNext(dbtemp)) rec_in_dbfile = true;
	dbfile.Close();

	//intemp = first rec in tbl file
	Record intemp;
	FILE *input_file = fopen(tbl_path, "r");
	bool rec_in_tblfile = false;
	if(intemp.SuckNextRecord(rel->schema (), input_file)) rec_in_tblfile = true;
	fclose(input_file);

	//print these two
	//cout << "First record in dbfile and tblfile:" << endl;
	//dbtemp.Print(rel->schema ());
	//intemp.Print(rel->schema ());

	//first rec in dbfile == first rec in tbl file
	if(rec_in_dbfile && rec_in_tblfile)
		EXPECT_TRUE(dbtemp.IsEqual(&intemp));
	//first rec == null and first rec = null
	else if(!rec_in_dbfile && !rec_in_tblfile)
		EXPECT_TRUE(1);
	//first rec == null or first rec = null
	else
		EXPECT_TRUE(0);
}

//Check if the last record in dbfile and tbl file match
TEST(Load, CompareLastRecords){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();	
	
	//dbtemp = lase rec in dbfile
	Record dbtemp;
	bool rec_in_dbfile = false;
	while(dbfile.GetNext(dbtemp)) rec_in_dbfile = true;
	dbfile.Close();

	Record intemp;
	FILE *input_file = fopen(tbl_path, "r");
	//intemplast = last rec in tbl file
	Record intemplast;
	bool rec_in_tblfile = false;
	while(intemp.SuckNextRecord(rel->schema (), input_file)){
		intemplast.Consume(&intemp);
		rec_in_tblfile = true;
	}
	fclose(input_file);

	//print these two	
	//cout << "Last record in dbfile and tblfile:" << endl;
	//dbtemp.Print(rel->schema ());
	//intemplast.Print(rel->schema ());	

	//first rec == last rec
	if(rec_in_dbfile && rec_in_tblfile)
		EXPECT_TRUE(dbtemp.IsEqual(&intemplast));
	//first rec == null and last rec = null
	else if(!rec_in_dbfile && !rec_in_tblfile)
		EXPECT_TRUE(1);
	//first rec == null or last rec = null
	else
		EXPECT_TRUE(0);
}

//Negative Test Case
//Check if the second record in dbfile and first record tbl file match
TEST(Load, CompareFirstAndSecondRecord){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();
	//dbtemp = second rec in dbfile
	Record dbtemp;
	bool rec_in_dbfile = false;
	dbfile.GetNext(dbtemp);
	if(dbfile.GetNext(dbtemp)) rec_in_dbfile = true;
	dbfile.Close();

	//intemp = first rec in tbl file
	Record intemp;
	bool rec_in_tblfile = false;
	FILE *input_file = fopen(tbl_path, "r");
	if(intemp.SuckNextRecord(rel->schema (), input_file)) rec_in_tblfile = true;
	fclose(input_file);

	//first rec != second rec
	if(rec_in_dbfile && rec_in_tblfile)
		EXPECT_FALSE(dbtemp.IsEqual(&intemp));
}

TEST(Add, AddRecord){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();

	Record tempfirst;
	bool rec_in_dbfile = false;
	if(dbfile.GetNext(tempfirst)) rec_in_dbfile = true;

	Record templast;
	if(rec_in_dbfile){
		//add this first rec 
		dbfile.Add(tempfirst);
		//write page into file
		dbfile.Close();

		dbfile.Open(rel->path ());	
		//tempfirst = first rec
		dbfile.MoveFirst();

		//templast = last rec	
		while(dbfile.GetNext(templast));

		dbfile.MoveFirst();
		dbfile.GetNext(tempfirst);
	}	
	dbfile.Close();

	//print these two	
	//cout << "First and last record in dbfile after adding:" << endl;	
	//tempfirst.Print(rel->schema ());
	//templast.Print(rel->schema ());

	//first rec == last rec
	if(rec_in_dbfile)
		EXPECT_TRUE(tempfirst.IsEqual(&templast));
}

TEST(GetNext, CountRecords){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();

	Record temp;
	int count_dbfile = 0;
	while(dbfile.GetNext(temp)){
		count_dbfile++;
	}
	dbfile.Close();

	FILE *input_file = fopen(tbl_path, "r");
	int count_tblfile = 0;
	while(temp.SuckNextRecord(rel->schema(), input_file)){
		count_tblfile++;
	}
	fclose(input_file);

	EXPECT_EQ(count_dbfile, count_tblfile);
}

TEST(GetNext, CompareFirstRecords){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();

	Record rec_dbfile;
	bool rec_in_dbfile = false;
	if(dbfile.GetNext(rec_dbfile)) rec_in_dbfile = true;
	dbfile.Close();

	FILE *input_file = fopen(tbl_path, "r");
	Record rec_tblfile;
	bool rec_in_tblfile = false;
	if(rec_tblfile.SuckNextRecord(rel->schema(), input_file)){
			rec_in_tblfile = true;	
	}
	fclose(input_file);

	//first rec == last rec
	if(rec_in_dbfile && rec_in_tblfile)
		EXPECT_TRUE(rec_dbfile.IsEqual(&rec_tblfile));
	//first rec == null and last rec = null
	else if(!rec_in_dbfile && !rec_in_tblfile)
		EXPECT_TRUE(1);
	//first rec == null or last rec = null
	else
		EXPECT_TRUE(0);
}

TEST(GetNextWithCnf, CountRecords){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();

	CNF cnf_pred;
	Record literal;
	// constructs CNF predicate
	rel->get_cnf(cnf_pred, literal);

	Record temp;
	int count_dbfile = 0;
	while(dbfile.GetNext(temp, cnf_pred, literal)){
		count_dbfile++;
	}
	dbfile.Close();

	FILE *input_file = fopen(tbl_path, "r");
	ComparisonEngine comp;
	int count_tblfile = 0;
	while(temp.SuckNextRecord(rel->schema(), input_file)){
		if (comp.Compare (&temp, &literal, &cnf_pred)){
			count_tblfile++;
		}
	}
	fclose(input_file);

	EXPECT_EQ(count_dbfile, count_tblfile);
}

TEST(GetNextWithCnf, CompareFirstRecords){
	dbfile.Create(rel->path(), heap, NULL);
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.MoveFirst();

	CNF cnf_pred;
	Record literal;
	// constructs CNF predicate
	rel->get_cnf(cnf_pred, literal);

	Record rec_dbfile;
	bool rec_in_dbfile = false;
	if(dbfile.GetNext(rec_dbfile, cnf_pred, literal)) rec_in_dbfile = true;
	dbfile.Close();

	FILE *input_file = fopen(tbl_path, "r");
	ComparisonEngine comp;
	Record rec_tblfile;
	bool rec_in_tblfile = false;
	while(rec_tblfile.SuckNextRecord(rel->schema(), input_file)){
		if (comp.Compare (&rec_tblfile, &literal, &cnf_pred)){
			rec_in_tblfile = true;
			break;
		}
	}
	fclose(input_file);

	//first rec == last rec
	if(rec_in_dbfile && rec_in_tblfile)
		EXPECT_TRUE(rec_dbfile.IsEqual(&rec_tblfile));
	//first rec == null and last rec = null
	else if(!rec_in_dbfile && !rec_in_tblfile)
		EXPECT_TRUE(1);
	//first rec == null or last rec = null
	else
		EXPECT_TRUE(0);
}

bool in_array(const string &value, const std::vector<string> &list)
{
    return find(list.begin(), list.end(), value) != list.end();
}

int main(int argc, char **argv) {
	char *r = rel_name;
	if(argc > 1 && in_array(argv[1], rel_names)){
		r = argv[1];
	}

	cout<<"Running all tests for relation '"<<r<<"'"<<endl;

	testing::AddGlobalTestEnvironment(new DbiEnvironment(r));
    ::testing::InitGoogleTest(&argc, argv); 

    return RUN_ALL_TESTS();
}

#endif