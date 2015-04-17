#ifndef TEST1_H
#define TEST1_H
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include "File.h"
#include "DBFile.h"
#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include "gtest/gtest.h"
#include "time.h" 


using namespace std;

OrderMaker sortorder;
int runlen;
CNF cnf_pred;
Record literal;
DBFile dbfileh;
DBFile dbfiles;

int add_data (FILE *src, int numrecs, int &res) {
	dbfiles.Open (rel->pathsorted ());
	Record temp;
	int proc = 0;
	int xx = 20000;
	while ((res = temp.SuckNextRecord (rel->schema (), src)) && proc < numrecs-1) {
		proc++;
		dbfiles.Add (temp);
		if (proc == xx) cerr << "\t ";
		if (proc % xx == 0) cerr << ".";
	}
	if(res != 0){

		dbfiles.Add (temp);
		proc++;
	}
	cout<<"read all records from table file, close dbfile"<<endl;
	dbfiles.Close ();
	return proc;
}

class DbiEnvironment : public testing::Environment{
	public:
    virtual void SetUp()
    {
    	setup ();

		relation *rel_ptr[] = {n, r, c, p, ps, o, li, s};

		int findx = 0;
		while (findx < 1 || findx > 8) {
			cout << "\n select dbfile to use: \n";
			cout << "\t 1. nation \n";
			cout << "\t 2. region \n";
			cout << "\t 3. customer \n";
			cout << "\t 4. part \n";
			cout << "\t 5. partsupp \n";
			cout << "\t 6. orders \n";
			cout << "\t 7. lineitem \n";
			cout << "\t 8. supplier \n \t ";
			cin >> findx;
		}
		rel = rel_ptr [findx - 1];
		
		cout << "\t\n specify runlength:\n\t ";
		cin >> runlen;

		cout << "\t\n specify sort order:\n\t";
		rel->get_sort_order (sortorder);

		cout << "\t\n specify query:\n\t";
		rel->get_cnf(cnf_pred, literal);

		struct {OrderMaker *o; int l;} startup = {&sortorder, runlen};
		
		char tbl_path[100];
		sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 

		

		//create sorted dbfile
		dbfiles.Create (rel->pathsorted(), sorted, &startup);
		dbfiles.Close ();

		
		FILE *tblfile = fopen (tbl_path, "r");


		int proc = 1, res = 1, tot = 0;
		while (proc && res) {
			proc = add_data (tblfile, 10000, res);
			tot += proc;
			if (proc) 
				cout << "\n\t added " << proc << " recs..so far " << tot << endl;
		}

		fclose (tblfile);

		

		//create heap dbfile
		dbfileh.Create (rel->path(), heap, &startup);
		dbfileh.Load (*(rel->schema ()), tbl_path);
		dbfileh.Close ();

		//dbfiles.Create (rel->pathsorted(), sorted, &startup);
		//dbfiles.Load (*(rel->schema ()), tbl_path);
		//dbfiles.Close ();

        cout << "DbiEnvironment SetUP" << endl;
    }
    virtual void TearDown()
    {
        
		cleanup ();
        cout << "DbiEnvironment TearDown" << endl;
    }
};



// check num of recs in dbfiles = num of recs in dbfileh
TEST(A2_2Test, CheckNumOfRecs){

	EXPECT_EQ(dbfiles.GetRecNum(), dbfileh.GetRecNum());

}

// Scan the recs in sorted file to check if (rec) <= (rec++)
TEST(A2_2Test, CheckOrder){
	//open dbfiles
	dbfiles.Open(rel->pathsorted() );
	//movefirst
	dbfiles.MoveFirst();

	//getnext prev last
	Record prev;
	Record last;
	if( !(dbfiles.GetNext(prev) && dbfiles.GetNext(last)) ){
		cerr << "Error! Less than 2 records."  << endl;
		exit(1);
	}

	// check prev <= last 
	ComparisonEngine ceng;

	EXPECT_FALSE(ceng.Compare (&prev, &last, &sortorder) > 0);

	//move to next until done
	int flag1 = 1;
	int flag2 = 1;
	while(flag1 && flag2){
		
		flag1 = dbfiles.GetNext(prev);
		if(flag1)
			EXPECT_FALSE(ceng.Compare (&last, &prev, &sortorder) > 0);

		flag2 = dbfiles.GetNext(last);
		if(flag2)
			EXPECT_FALSE(ceng.Compare (&prev, &last, &sortorder) > 0);
	}
	dbfiles.Close();
}

//do the same query in both heap and sorted dbfile and check if num of recs are the same
TEST(A2_2Test, GetNextWithCNF){
	dbfiles.Open(rel->pathsorted() );
	dbfiles.MoveFirst ();
	dbfileh.Open(rel->path() );
	dbfileh.MoveFirst ();

	int cntheap = 0, cntsorted = 0;

	Record temp;

	clock_t start, finish;   
	double duration;

	start = clock(); 
	while (dbfiles.GetNext(temp, cnf_pred, literal)){

		cntsorted++;

	}

	finish = clock(); 
	duration = (double)(finish - start) / CLOCKS_PER_SEC;

	cout << "\n\t query time for sorted file is " << duration << endl;

	start = clock(); 
	while (dbfileh.GetNext(temp, cnf_pred, literal)){

		cntheap++;

	}

	finish = clock(); 
	duration = (double)(finish - start) / CLOCKS_PER_SEC;

	cout << "\n\t query time for heap file is " << duration << endl;

	EXPECT_EQ(cntheap, cntsorted);

	dbfiles.Close();
	dbfileh.Close();
}


int main(int argc, char **argv) {


	testing::AddGlobalTestEnvironment(new DbiEnvironment());
    ::testing::InitGoogleTest(&argc, argv); 

    return RUN_ALL_TESTS();
}

#endif